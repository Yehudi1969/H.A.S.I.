# -*- coding: utf8 -*-
###############################################################################
#                                  oralib.py                                  #
###############################################################################
# Copyright (C) 2023  Jens Janzen

# Change history
# V1.0: 2019-04-26 - Initial Release

import hashlib
import logging
import oracledb as db
import os
import seclib
import time
import unixlib


""" Use Thick client """
db.init_oracle_client()

# Description:
# Library contains helper functions for Oracle database access

# Constants
try:
    host = os.environ["HOST"]
except KeyError as e:
    host = "Unknown"
standard_encoding = 'utf8'

""" Sets logging console handler for debugging """
log = logging.getLogger("SESSION")


def get_engine(connection_key, connection_string):
    from sqlalchemy import create_engine
    cred = seclib.get_credentials(connection_key)
    if cred is False:
        return False
    try:
        user = cred[1]
        pw = cred[2]
        engine = create_engine("oracle+oracledb://{0}:{1}@{2}".format(user, pw, connection_string))
        con = engine.connect()
        con.outputtypehandler = output_typehandler
    except db.Error as exc:
        err, = exc.args
        log.error("Oracle-Error-Code: {0}".format(err.code))
        log.error("Oracle-Error-Message: {0}".format(err.message))
        log.error("Key: {0}".format(connection_key))
        raise
    return con


def get_connection(connection_key):
    cred = seclib.get_credentials(connection_key)
    if cred is False:
        return False
    try:
        con = db.connect(user="{0}".format(cred[1]),
                         password="{0}".format(cred[2]),
                         dsn="{0}".format(cred[0]))
        con.clientinfo = "H.A.S.I. on {0}".format(host)
        con.module = "oralib.py"
        con.action = "SQL Query"
        log.debug("Connected to {0}, version {1} successful.".format(con.dsn, con.version))
        con.outputtypehandler = output_typehandler
    except db.Error as exc:
        err, = exc.args
        log.error("Oracle-Error-Code: {0}".format(err.code))
        log.error("Oracle-Error-Message: {0}".format(err.message))
        log.error("Key: {0}".format(connection_key))
        raise
    return con


def convert_dtypes(table_obj, df):
    d_conv = {}
    for col in table_obj.column_list:
        if table_obj.d_col_data_type[col][0] == "NUMBER" and table_obj.d_col_data_type[col][2] == 0:
            d_conv[col] = "int64"
        elif table_obj.d_col_data_type[col][0] == "NUMBER" and table_obj.d_col_data_type[col][2] > 0:
            d_conv[col] = "float64"
        elif table_obj.d_col_data_type[col][0] == "DATE" or table_obj.d_col_data_type[col][0][:9] == "TIMESTAMP":
            d_conv[col] = "datetime64"
        else:
            d_conv[col] = "object"
    return df.convert_dtypes(d_conv)


def inconverter(value):
    return int(value)


def inputtypehandler(cursor, value, numrows):
    """ Oracle Inputtypehandler for Cursor
        Binds data type to Oracle object before writing to database.
        https://python-oracledb.readthedocs.io/en/latest/user_guide/bind.html
    """
    import numpy as np
    if isinstance(value, np.int64):
        return cursor.var(int, arraysize=numrows, inconverter=inconverter)


def output_typehandler(cursor, name, defaultType, size, precision, scale):
    """ Set workaround for CLOB / BLOB data types on connection level. """
    if defaultType == db.DB_TYPE_CLOB:
        return cursor.var(db.DB_TYPE_LONG, arraysize=cursor.arraysize)
    elif defaultType == db.DB_TYPE_BLOB:
        return cursor.var(db.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)


def sql_query(connection_key, query):
    """ Executes one or more queries against database instance.
    Needs a key representing connection, user and dsn and a string
    with one or more statements as arguments.
    Returns a list of all selections with each collected row as a tuple.
    If rows are inserted, updated or deleted, the number of rows of the last statement is returned.
    If one of the statements raises an error, all statements of the used connection are rolled back.
    """
    # Check whether statement ends with proper termination character
    if not query.endswith(';'):
        query += ";"
    # Check for multiple queries
    l_queries = query.split(";")
    l_queries = map(lambda s: s.strip(), l_queries)
    l_queries = filter(None, l_queries)

    # Open DB connection
    con = get_connection(connection_key)
    action = ""
    selection = []
    rowcount = 0
    cursor = None

    if con and con is not False:
        for query in l_queries:
            action = parse_query(query)
            log.debug(action)
            # Execute given queries
            try:
                cursor = con.cursor()
                log.debug("Execute query: {0}".format(query))
                obj_exec = cursor.execute(query)
                log.debug(obj_exec)
                if obj_exec:
                    selection.extend(obj_exec.fetchall())
                rowcount = cursor.rowcount
                if action in ("MERGE", "INSERT", "UPDATE", "DELETE"):
                    log.debug("{0} Statement successful. {1} rows affected.".format(action, rowcount))
                log.debug("Resultset: {0}".format(selection))
            except db.Error as exc:
                err, = exc.args
                log.error("Oracle-Error-Code: {0}".format(err.code))
                log.error("Oracle-Error-Message: {0}".format(err.message))
                con.rollback()
                cursor.close()
                con.close()
                return False
        con.commit()
        con.close()
        if action in ("CREATE", "DROP", "ALTER", "TRUNCATE"):
            return True
        elif action == "SELECT":
            return selection
        elif action in ("MERGE", "INSERT", "UPDATE", "DELETE"):
            return rowcount
        else:
            return True
    elif con is False:
        log.error("Could not open DB connection: {0}".format(con))
        return False


def parse_query(query):
    import re
    for literal in reversed(sorted(re.findall("'([^']*)'", query), key=len)):
        query = query.replace(literal, "")
    log.debug("Parse query: {0}".format(query))
    if "create" in query.lower():
        return "CREATE"
    elif "drop" in query.lower():
        return "DROP"
    elif "alter" in query.lower():
        return "ALTER"
    elif "truncate" in query.lower():
        return "TRUNCATE"
    elif "comment" in query.lower():
        return "COMMENT"
    elif "merge" in query.lower():
        return "MERGE"
    elif "insert into" in query.lower():
        return "INSERT"
    elif "update" in query.lower():
        return "UPDATE"
    elif "delete from" in query.lower():
        return "DELETE"
    elif "select" in query.lower():
        return "SELECT"
    else:
        return query.split(" ", 1)[0].upper()


def run_procedure(connection_key, proc_name, proc_args=None):
    """ Executes a stored procedure in the target database instance.
    Needs a key representing connection, user and dsn and a string
    with the name / parameters of the procedure as arguments.
    """
    # Obtain credentials
    cred = seclib.get_credentials(connection_key)
    cursor = ""
    result = None

    # Open DB connection
    con = get_connection(connection_key)

    if len(cred) == 3:
        try:
            cursor = con.cursor()
            log.debug("Enable DMBS Output: callproc(\"dbms_output.enable\")")
            cursor.callproc("dbms_output.enable")
            if proc_args:
                log.debug("Execute procedure: cursor.callproc({0}, {1})".format(proc_name, proc_args))
                result = cursor.callproc(proc_name, proc_args)
                log.info("Procedure {0} with arguments {1} successful. {2} rows affected.".format(
                    proc_name, proc_args, cursor.rowcount))
            else:
                log.debug("Execute procedure: cursor.callproc({0})".format(proc_name))
                result = cursor.callproc(proc_name)
                log.info("Procedure {0} successful. {1} rows affected.".format(proc_name, cursor.rowcount))
        except db.Error as exc:
            err, = exc.args
            log.error("Oracle-Error-Code:", err.code)
            log.error("Oracle-Error-Message:", err.message)
            con.rollback()
            cursor.close()
            con.close()
            return False
    con.commit()
    con.close()
    return result


def query_with_values(connection_key, query, values):
    """ Executes one query against database instance.
    Needs a key representing connection, user and dsn, a query and a value string.
    Can insert more than 4000 characters into a CLOB column.
    """
    # Check whether statement ends with proper termination character
    if query.endswith(";"):
        query = query[:-1]

    # Obtain credentials
    cred = seclib.get_credentials(connection_key)
    cursor = ""

    # Open DB connection
    con = get_connection(connection_key)

    action = parse_query(query)
    if len(cred) == 3:
        try:
            cursor = con.cursor()
            log.debug("Execute query: {0}".format(query))
            cursor.execute(query, values)
            log.debug("{0} Statement successful. {1} rows affected.".format(action, cursor.rowcount))
        except db.Error as exc:
            err, = exc.args
            log.error("Oracle-Error-Code:", err.code)
            log.error("Oracle-Error-Message:", err.message)
            con.rollback()
            cursor.close()
            con.close()
            return False
    con.commit()
    con.close()
    return True


def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def create_selector(l_intersect, o_src_table):
    """ Generate SQL statement for selector """
    l_selection = []
    """ XML workaround Oracle """
    xml_cols = list(x for x in l_intersect if o_src_table.d_col_data_type[x][0] == 'XMLTYPE')
    for x in l_intersect:
        if x in xml_cols:
            l_selection.append("xmltype.getclobval({0}.\"{1}\".\"{2}\") as {2}".format(
                o_src_table.schema, o_src_table.table_name, x))
        elif x not in xml_cols:
            l_selection.append("\"{0}\".\"{1}\".\"{2}\"".format(o_src_table.schema, o_src_table.table_name, x))
    return "SELECT {0} FROM \"{1}\".\"{2}\"".format(", ".join(l_selection), o_src_table.schema, o_src_table.table_name)


def create_union_stmt(o_src_table, o_fil_table, src_business_key, fil_business_key):
    l_union_sql = []
    for x, y in zip(src_business_key.split(","), fil_business_key.split(",")):
        stmt = "SELECT {0} FROM {1}.\"{2}\" JOIN {4}.\"{5}\" " \
                "ON {1}.\"{2}\".\"{3}\" = {4}.\"{5}\".\"{6}\"".format(
                    ", ".join(o_src_table.column_list), o_src_table.schema,
                    o_src_table.table_name, x, o_fil_table.schema, o_fil_table.table_name, y)
        l_union_sql.append(stmt)
    return " UNION ".join(l_union_sql)


def create_dml(l_intersect, o_src_table, o_tgt_table, src_filter, action):
    """ Generate SQL statement for dml """
    l_placeholder = list(":\"{0}\"".format(x) for x in l_intersect)
    """ XML workaround Oracle """
    xml_cols = list(x for x in l_intersect if o_tgt_table.d_col_data_type[x][0] == 'XMLTYPE')
    for ix, col in enumerate(l_intersect):
        if col in xml_cols:
            l_placeholder[ix] = "xmltype.createxml(:\"{0}\")".format(col)
    if action in ("INSERT", "MASK"):
        return "INSERT INTO \"{0}\".\"{1}\" (\"{2}\") VALUES ({3})".format(
            o_tgt_table.schema, o_tgt_table.table_name, "\", \"".join(l_intersect), ", ".join(l_placeholder))
    elif action in ("UPSERT", "UPSERT_MASK"):
        return "UPSERT INTO \"{0}\".\"{1}\" (\"{2}\") VALUES ({3})".format(
            o_tgt_table.schema, o_tgt_table.table_name, "\", \"".join(l_intersect), ", ".join(l_placeholder))
    elif action == "MERGE":
        exp_join = " and ".join(["src." + x + "=tgt." + x for x in o_tgt_table.primary_key_list])
        exp_merge_update = ", ".join(["tgt." + x + "=src." + x for x in o_tgt_table.column_list])
        exp_merge_tgt_rows = ", ".join(["tgt." + x for x in o_tgt_table.column_list])
        if len(o_tgt_table.column_list) - len(o_src_table.column_list) == 1 \
                and o_tgt_table.column_list[-1] == "TA_FEHLER":
            exp_merge_src_rows = ", ".join(["src." + x for x in o_tgt_table.column_list])
            tgt_query = "merge into {0}.{1} tgt using " \
                        "(select {2}, 'Primärschlüssel unvollständig' as TA_FEHLER " \
                        "from {3}.{4} {5}) src on ({6}) " \
                        "when matched then update set {7} " \
                        "when not matched then insert ({8}) values ({9});".format(
                            o_tgt_table.schema, o_tgt_table.table_name, ", ".join(o_src_table.column_list),
                            o_src_table.schema, o_src_table.table_name, src_filter, exp_join,
                            exp_merge_update, exp_merge_tgt_rows, exp_merge_src_rows)
            o_src_table.l_src_cols.append("'Primärschlüssel unvollständig.' as TA_FEHLER")
        else:
            exp_merge_src_rows = ", ".join(["src." + x for x in o_src_table.l_src_cols])
            tgt_query = "merge into {0}.{1} tgt using " \
                        "(select {2} from {3}.{4} {5}) src on ({6}) " \
                        "when matched then update set {7} " \
                        "when not matched then insert ({8}) values ({9});".format(
                            o_tgt_table.schema, o_tgt_table.table_name, ", ".join(o_src_table.column_list),
                            o_src_table.schema, o_src_table.table_name, src_filter, exp_join,
                            exp_merge_update, exp_merge_tgt_rows, exp_merge_src_rows)
        return tgt_query


def copy_data(src_dsn, src_schema, src_table, src_query, tgt_dsn, tgt_schema, tgt_table, tgt_query, max_memory):
    o_src_table = Table(src_dsn, src_table, schema=src_schema)
    chunksize = o_src_table.get_chunksize(max_memory)
    o_tgt_table = Table(tgt_dsn, tgt_table, schema=tgt_schema)
    xml_cols = list(x for x in o_src_table.column_list
                    if o_src_table.d_col_data_type[x][0] == 'XMLTYPE')
    if len(xml_cols) > 0:
        log.info("Ändere Paketgröße aufgrund von XML Datentypen auf 1000.")
        chunksize = 1000
    if o_src_table.column_list != o_tgt_table.column_list:
        log.warning("Structure between Source and Target Table does not match.")
    rows_affected = 0
    try:
        with get_connection(tgt_dsn) as tgt:
            with get_connection(src_dsn) as src:
                with tgt.cursor() as c2:
                    with src.cursor() as c1:
                        c1.execute(src_query)
                        db_types = tuple(d[1] for d in c1.description)
                        c2.setinputsizes(*db_types)
                        log.info("Copy data with blocksize: {0}".format(chunksize))
                        while True:
                            data = c1.fetchmany(chunksize)
                            if len(data) == 0:
                                break
                            c2.executemany(tgt_query, data)
                            rows_affected += c2.rowcount
                            tgt.commit()
    except db.Error as exc:
        err, = exc.args
        log.error("Oracle-Error-Code: {0}".format(err.code))
        log.error("Oracle-Error-Message: {0}".format(err.message))
        return False
    return rows_affected


def mask_data(src_dsn, src_schema, src_table, src_query, tgt_dsn, tgt_schema, tgt_table, tgt_query,
              l_cols, d_rules, max_memory, d_cache):
    import pandas as pd
    """ Create target table object """
    o_src_table = Table(src_dsn, src_table, schema=src_schema)
    chunksize = o_src_table.get_chunksize(max_memory)
    o_tgt_table = Table(tgt_dsn, tgt_table, schema=tgt_schema)
    xml_cols = list(x for x in o_src_table.column_list
                    if o_src_table.d_col_data_type[x][0] == 'XMLTYPE')
    if len(xml_cols) > 0:
        log.info("Change blocksize to 1000 rows, because table contains XML data type.")
        chunksize = 1000
    rows_affected = 0
    try:
        with get_connection(src_dsn) as src:
            with src.cursor() as c1:
                c1.execute(src_query)
                log.info("Mask data with blocksize: {0}".format(chunksize))
                while True:
                    data = c1.fetchmany(chunksize)
                    if len(data) == 0:
                        break
                    df = pd.DataFrame(data, columns=l_cols)
                    del data
                    df_mask = mask_dataframe(df, d_rules, o_src_table, o_tgt_table, d_cache)
                    if df_mask is False:
                        return False
                    num_mask_rows = o_tgt_table.dataframe_import(df_mask, action="BLOCK", query=tgt_query)
                    if num_mask_rows is False:
                        return False
                    rows_affected += num_mask_rows
    except db.Error as exc:
        err, = exc.args
        log.error("Oracle-Error-Code: {0}".format(err.code))
        log.error("Oracle-Error-Message: {0}".format(err.message))
        return False
    return rows_affected


def add_length_to_rule(d_col_data_type, d_rule):
    if isinstance(d_rule["ATTRIBUT"], list):
        d_rule["COLUMN_LENGTH"] = list(d_col_data_type[col][3] for col in d_rule["ATTRIBUT"])
    else:
        d_rule["COLUMN_LENGTH"] = d_col_data_type[d_rule["ATTRIBUT"]][3]
    return d_rule


def mask_dataframe(table_rows, d_rules, o_src_table, o_tgt_table, d_cache):
    import tdm_rules
    orig_structure = table_rows.shape
    log.info("Original block contains {0} rows in {1} columns.".format(orig_structure[0], orig_structure[1]))
    cached_rules = list(d_cache.keys())
    l_pk = o_src_table.primary_key_list
    for d_rule in d_rules.values():
        function_call = eval("tdm_rules.rule_{0}".format(d_rule["RULE_NAME"].lower()))
        d_rule = add_length_to_rule(o_tgt_table.d_col_data_type, d_rule)
        log.info("Verarbeite Regel {0} für Attribut {1} mit Länge {2}".format(
            d_rule["RULE_NAME"], d_rule["ATTRIBUT"], d_rule["COLUMN_LENGTH"]))
        if d_rule["RULE_NAME"] == "R12":
            table_rows = function_call(d_rule, table_rows, d_cache["R12_adr"], d_cache["R12_hsn"], l_pk)
        elif d_rule["RULE_NAME"] + "_cache" in cached_rules:
            log.debug("Cache für Regel {0} gefunden.".format(d_rule["RULE_NAME"]))
            table_rows = function_call(d_rule, table_rows, d_cache[d_rule["RULE_NAME"] + "_cache"])
        elif d_rule["RULE_NAME"] + "_cache" not in cached_rules:
            log.debug("Kein Cache für Regel {0} gefunden.".format(d_rule["RULE_NAME"]))
            table_rows = function_call(d_rule, table_rows)
        else:
            log.error("Rule {0} not found.".format(d_rule["RULE_NAME"]))
    mask_structure = table_rows.shape
    log.info("Masked block contains {0} rows in {1} columns.".format(mask_structure[0], mask_structure[1]))

    """ Fix for different column sequence between source and target """
    if o_src_table.column_list != o_tgt_table.column_list:
        table_rows = table_rows[o_tgt_table.column_list]

    """ Check structural differences between DataFrame and target table """
    if orig_structure != mask_structure:
        log.error("Different data structure found between original and masked dataframe.")
        return False
    else:
        return table_rows


# Database Classes
class Table(object):
    """ Klasse für eine Oracle Tabelle.
    Enthaltene Attribute:
    primary_key_list - Eindeutiger Schlüssel auf Tabelle
    column_list - Liste der Datenbankfelder
    d_file_cols - Dictionary mit Feldnamen und Datentypen für das sqlldr Controlfile
    d_col_data_type - Dictionary mit Feldnamen und Datentypen
    avro_schema - Enthält das AVRO Schema im JSON Format

    Methoden:
    get_owner - Prüft den Klassenparameter owner auf ein gültiges Datenbankschema.
    get_name - Prüft den Klassenparameter name auf einen gültigen Tabellennamen.
    get_primary_key_list - Gibt eine Liste mit dem Primärschlüssel, bzw. dem ersten unique index zurück
    get_column_list - Gibt eine Liste mit Spalten der Tabelle zurück
    get_d_col_data_type - Gibt ein Dictionary zurück {feldname : Datentyp}
    generate_statistics - Erstellt Statistiken auf der Tabelle (RUNSTATS)
    truncate_table - Leert die Tabelle
    csv_export(filename=None, delimiter="|", headline=True, custom_query=False) -
        Exportiert die vorgegebenen Spalten der Tabelle in eine Datei.
    """

    def __init__(self, connection_key, table_name, schema=None, primary_key_list=None, column_list=None,
                 d_col_data_type=None):
        """ Creates an object instance for a single Oracle table or view.
        Needs owner and table_name as argument.
        """
        cred = seclib.get_credentials(connection_key)
        self.sid = cred[0]
        self.owner = cred[1]
        self.__pwd = cred[2]
        self.connection_key = connection_key
        self.connection = get_connection(self.connection_key)
        self.table_name = table_name
        self.schema = schema if schema else self.owner
        if column_list:
            self.column_list = list(x for x in column_list)
        else:
            self.get_column_list()
            if len(self.column_list) == 0:
                log.warning("Table {0} does not (yet) exist.".format(self.table_name))
        if primary_key_list:
            self.primary_key_list = primary_key_list
        else:
            self.get_primary_key_list()
        if d_col_data_type:
            self.d_col_data_type = d_col_data_type
        else:
            self.get_d_col_data_type()
        self.d_file_cols = None
        self.avro_schema = None
        self.cons_name_list = None
        self.d_constraints = {}
        self.ddl = None
        self.db_type = "ORACLE"

    @property
    def __str__(self):
        return str(self.table_name)

    def exec_callproc(self, proc_name, proc_args=None):
        try:
            cursor = self.connection.cursor()
            log.debug("Enable DMBS Output: callproc(\"dbms_output.enable\")")
            cursor.callproc("dbms_output.enable")
            if proc_args and isinstance(proc_args, list) is True:
                log.debug("Execute procedure: cursor.callproc('{0}', {1})".format(proc_name, proc_args))
                cursor.callproc(proc_name, proc_args)
                log.info("Procedure {0} with arguments {1} successful. {2} rows affected.".format(
                    proc_name, proc_args, cursor.rowcount))
            elif proc_args and isinstance(proc_args, list) is False:
                log.error("Arguments for procedure have to be list data type.")
                return False
            elif not proc_args:
                log.debug("Execute procedure: cursor.callproc({0})".format(proc_name))
                cursor.callproc(proc_name)
                log.info("Procedure {0} successful. {1} rows affected.".format(proc_name, cursor.rowcount))
        except db.Error as exc:
            err, = exc.args
            log.error("Oracle-Error-Code:", err.code)
            log.error("Oracle-Error-Message:", err.message)
            return False
        cursor.close()
        return True

    def get_primary_key_list(self):
        """ Returns a list of key columns from system catalog. DEPRECATED.
        """
        query = "select a.column_name from all_cons_columns a, all_constraints b " \
                "where a.constraint_name = b.constraint_name " \
                "and a.owner = b.owner " \
                "and b.constraint_type = 'P' " \
                "and a.owner = '{0}' and a.table_name = '{1}' " \
                "order by a.position;".format(self.schema, self.table_name)
        result = sql_query(self.connection_key, query)
        self.primary_key_list = list(x[0] for x in result)
        return self.primary_key_list

    def get_d_constraints(self, check_p=True, check_u=True, check_c=True, check_r=True, enable_foreign_cons='ENABLE'):
        """ Returns a dictionary of constraint names as keys and details as values.
        """
        query = "select a.constraint_name, b.constraint_type, a.owner, a.table_name, a.column_name" \
                ", b.r_constraint_name, c.column_name as r_column_name" \
                ", c.owner as r_owner, c.table_name as r_table_name" \
                ", b.search_condition_vc, b.delete_rule, b.status " \
                "from all_cons_columns a " \
                "join all_constraints b " \
                "on a.constraint_name = b.constraint_name and a.owner=b.owner " \
                "left join all_cons_columns c " \
                "on b.r_constraint_name = c.constraint_name and b.owner=c.owner " \
                "where a.owner='{0}' and a.table_name='{1}' order by a.position;".format(self.schema, self.table_name)
        l_rows = sql_query(self.connection_key, query)
        l_cons = list(set(list(x[:2] for x in l_rows)))
        log.debug
        d_pk = {}
        d_unique_cons = {}
        d_check_cons = {}
        d_ref_cons = {}
        c_unique = 0
        for cons in l_cons:
            if cons[1] == 'P':
                column_names = list(x[4] for x in l_rows if x[0] == cons[0])
                d_pk['PK'] = "PRIMARY KEY (\"{0}\")".format("\", \"".join(column_names).upper())
                log.debug(d_pk)
            elif cons[1] == 'U':
                column_names = list(x[4] for x in l_rows if x[0] == cons[0])
                d_unique_cons["UNIQUE_{0}".format(c_unique)] = "UNIQUE (\"{0}\")".format("\", \"".join(column_names))
                log.debug(d_unique_cons)
                c_unique += 1
            elif cons[1] == 'C':
                search_condition = list(x[9] for x in l_rows if x[0] == cons[0])
                for entry in search_condition:
                    if "IS NOT NULL" in entry:
                        continue
                    else:
                        d_check_cons[cons[0]] = "CONSTRAINT \"{0}\" CHECK ({1}) ENABLE".format(
                            cons[0], "\", \"".join(search_condition))
                log.debug(d_check_cons)
            elif cons[1] == 'R':
                log.debug("Remote Constraint: {0}".format(cons))
                r_schema = list(x[7] for x in l_rows if x[0] == cons[0])
                log.debug(r_schema)
                if r_schema[0] is None:
                    continue
                r_table_name = list(x[8] for x in l_rows if x[0] == cons[0])
                log.debug(r_table_name)
                column_names = list(x[4] for x in l_rows if x[0] == cons[0])
                log.debug(column_names)
                r_column_names = list(x[6] for x in l_rows if x[0] == cons[0])
                log.debug(r_column_names)
                delete_rule = list(x[10] for x in l_rows if x[0] == cons[0])
                if len(delete_rule) > 0 and delete_rule[0] == "NO ACTION":
                    d_ref_cons[cons[0]] = "CONSTRAINT \"{0}\" FOREIGN KEY (\"{1}\") " \
                                          "REFERENCES \"{2}\".\"{3}\" (\"{4}\")".format(
                        cons[0], "\", \"".join(column_names), r_schema[0], r_table_name[0],
                        "\", \"".join(r_column_names))
                else:
                    d_ref_cons[cons[0]] = "CONSTRAINT \"{0}\" FOREIGN KEY (\"{1}\") " \
                                          "REFERENCES \"{2}\".\"{3}\" (\"{4}\") ON DELETE {5} {6}".format(
                        cons[0], "\", \"".join(column_names), r_schema[0], r_table_name[0],
                        "\", \"".join(r_column_names), delete_rule[0], enable_foreign_cons)
        self.d_constraints.update(d_pk) if check_p is True else None
        self.d_constraints.update(d_unique_cons) if check_u is True else None
        self.d_constraints.update(d_check_cons) if check_c is True else None
        self.d_constraints.update(d_ref_cons) if check_r is True else None
        return self.d_constraints

    def get_column_list(self):
        """ Returns a list of column names from system catalog.
        """
        query = "select column_name from all_tab_columns where owner = '{0}' and table_name = '{1}' " \
                "order by column_id;".format(self.schema, self.table_name)
        result = sql_query(self.connection_key, query)
        self.column_list = list(x[0] for x in result)
        return self.column_list

    def get_d_sqlldr_cols(self, dateformat="YYYY-MM-DD HH24:MI:SS", timestampformat="YYYY-MM-DD HH24:MI:SS.FF6"):
        """ Returns a dictionary with column_names and datatypes with lengths useable for DDL generation. """
        query = "select column_name, data_type, data_precision, data_scale, char_length " \
                "from all_tab_columns where owner = '{0}' and table_name = '{1}' " \
                "order by column_id;".format(self.schema, self.table_name)
        result = sql_query(self.connection_key, query)
        self.d_file_cols = {}
        for l_column in result:
            log.debug(l_column)
            if l_column[1] in ("CHAR", "VARCHAR2"):
                self.d_file_cols[l_column[0]] = "CHAR NULLIF {0}=BLANKS \"RTRIM(:{0})\"".format(l_column[0])
            elif l_column[1] in ("FLOAT", "NUMBER"):
                self.d_file_cols[l_column[0]] = "CHAR \"to_number(:{0})\"".format(l_column[0])
            elif l_column[1] == "DATE":
                self.d_file_cols[l_column[0]] = "{0} \"{1}\"".format(l_column[1], dateformat)
            elif l_column[1] in ("TIMESTAMP(0)", "TIMESTAMP(3)", "TIMESTAMP(6)", "TIMESTAMP(9)"):
                self.d_file_cols[l_column[0]] = "{0} \"{1}\"".format(l_column[1], timestampformat)
            else:
                self.d_file_cols[l_column[0]] = ""
        # Anpassung letztes Datenfeld wg. ORA-01722
        self.d_file_cols[self.column_list[-1]] = "CHAR TERMINATED BY WHITESPACE \"RTRIM(:{0})\"".format(
            self.column_list[-1])
        log.debug(self.d_file_cols)
        return self.d_file_cols

    def get_d_col_data_type(self):
        """ Returns a dictionary of data types for all columns in self.column_list.
        """
        query = "select column_name, data_type, " \
                "case when data_type='NUMBER' and data_precision is null " \
                "then 38 else data_precision end as data_precision, " \
                "case when data_type='NUMBER' and data_scale is null " \
                "then 0 else data_scale end as data_scale, " \
                "case when data_type in ('CHAR', 'VARCHAR2', 'NVARCHAR2') then char_length " \
                "when data_type in ('RAW') then data_length end as length, nullable " \
                "from all_tab_columns where owner = '{0}' and table_name = '{1}' and column_name in ('{2}') " \
                "order by column_id;".format(self.schema, self.table_name, '\',\''.join(self.column_list))
        result = sql_query(self.connection_key, query)
        d_data_types = {}
        for item in result:
            d_data_types[item[0]] = list(i for i in item[1:])
        self.d_col_data_type = d_data_types
        return self.d_col_data_type

    def get_avro_schema(self):
        """ MAPPING from Oracle to AVRO:
            FLOAT, NUMBER, INTERVAL* (mit Nachkommastellen) => float
            FLOAT, NUMBER, INTERVAL* (ohne Nachkommastellen) => long
            XMLTYPE => map
            Rest => string
            Returns a string that can be parsed as avro scheme by avro.schema.parse
        """
        import fastavro
        l_avro_columns = []
        query = "select column_name, case when data_type in ('FLOAT', 'NUMBER') and data_scale = 0 then 'long' " \
                "when data_type in ('FLOAT', 'NUMBER') and data_scale > 0 then 'float' " \
                "when data_type like 'INTERVAL%' and data_scale = 0 then 'long' " \
                "when data_type like 'INTERVAL%' and data_scale > 0 then 'float' " \
                "when data_type = 'XMLTYPE' then 'map' else 'string' end as avro_data_type, nullable " \
                "from all_tab_columns where owner = '{0}' and table_name = '{1}' and column_name in ('{2}') " \
                "order by column_id;".format(self.schema, self.table_name, '\',\''.join(self.column_list))
        l_descriptor = sql_query(self.connection_key, query)
        d_avro_schema = {"namespace": "aws.oracle.avro.{0}".format(self.schema),
                         "type": "record",
                         "name": "{0}".format(self.table_name),
                         "fields": []}
        for item in l_descriptor:
            d_field_entry = {}
            """ Check nullable columns """
            if item[2] == 'Y':
                d_field_entry["name"] = "{0}".format(item[0])
                d_field_entry["type"] = str(["{0}".format(item[1]), "null"])
            elif item[2] == 'N':
                d_field_entry["name"] = "{0}".format(item[0])
                d_field_entry["type"] = "{0}".format(item[1])
            log.debug(d_field_entry)
            l_avro_columns.append(d_field_entry.copy())
        d_avro_schema["fields"] = l_avro_columns
        self.avro_schema = fastavro.parse_schema(d_avro_schema)
        log.debug("AVRO Schema: {0}".format(self.avro_schema))
        return self.avro_schema

    def get_ddl(self, include_schema=True, include_grants=True, include_constraints=True, readable=False,
                table_name=None, body_only=False, check_p=True, check_u=True, check_c=True, check_r=True):
        """ Returns DDL of table as string
        """
        if not table_name:
            table_name = self.table_name

        query = "select column_name, " \
                "case " \
                "when data_type ='DATE' then data_type " \
                "when data_type like 'TIMESTAMP%' then data_type " \
                "when data_type = 'FLOAT' then data_type||'('||data_precision||')' " \
                "when data_type = 'NUMBER' and data_precision is null then data_type||'(*, 0)' " \
                "when data_type = 'NUMBER' and data_precision is not null " \
                "then data_type||'('||data_precision||', '||data_scale||')' " \
                "when data_type in ('CHAR', 'VARCHAR2') then data_type||'('||char_length||' CHAR)' " \
                "when data_type in ('NVARCHAR2') then data_type||'('||char_length||')' " \
                "when data_type in ('ANYDATA', 'BLOB', 'CLOB', 'LONG', 'XMLTYPE') then data_type " \
                "when data_type in ('RAW') then data_type||'('||data_length||')' " \
                "when data_type like 'INTERVAL%' then data_type end as data_type " \
                ", data_default " \
                ", nullable " \
                "from all_tab_columns " \
                "where owner='{0}' and table_name='{1}' and column_name in ('{2}') " \
                "order by column_id;".format(self.schema, self.table_name, '\', \''.join(self.column_list))
        cols = sql_query(self.connection_key, query)
        if include_schema is True:
            query = "select 'GRANT '|| PRIVILEGE ||' ON ' || OWNER ||'.'|| TABLE_NAME ||' TO '|| GRANTEE " \
                    "from user_tab_privs where owner='{0}' and table_name='{1}';".format(self.schema, self.table_name)
        else:
            query = "select 'GRANT '|| PRIVILEGE ||' ON ' || TABLE_NAME ||' TO '|| GRANTEE " \
                    "from user_tab_privs where owner='{0}' and table_name='{1}';".format(self.schema, self.table_name)
        result = sql_query(self.connection_key, query)
        grants = list(x[0] for x in result)
        if include_schema is True:
            header = "CREATE TABLE \"{0}\".\"{1}\" (\n\t".format(self.schema, table_name)
        else:
            header = "CREATE TABLE \"{0}\" (\n\t".format(table_name)
        body = []
        for row in cols:
            column = row[0]
            data_type = row[1]
            data_default = '' if row[2] is None else "DEFAULT {0}".format(row[2])
            null_value = '' if row[3] == 'Y' else 'NOT NULL ENABLE'
            body.append("{0} {1} {2} {3}".format(column, data_type, data_default, null_value))
        if include_constraints is True:
            self.get_d_constraints(check_p=check_p, check_u=check_u, check_c=check_c, check_r=check_r)
            for cons in self.d_constraints.values():
                body.append("{0}".format(cons))
        footer = "\n);"
        if grants and include_grants is True:
            footer = footer + "\n{0};".format(";\n".join(grants)) + "\n"
        else:
            footer = footer + "\n"
        if body_only is True:
            self.ddl = "{0}".format("\n\t, ".join(body))
        else:
            self.ddl = "{0}{1}{2}".format(header, "\n\t, ".join(body), footer)
        if readable is True:
            self.ddl = "-- DDL fuer Tabelle {0}.{1} created by oralib.py\n".format(
                self.schema, self.table_name) + self.ddl
        else:
            self.ddl = self.ddl.replace("\t", "").replace("\n", "")
        return self.ddl

    def generate_statistics(self, estimate_percent="DBMS_STATS.AUTO_SAMPLE_SIZE"):
        """
        Generate statistics on table.
        """
        try:
            # Install anonymous procedure in userspace
            query = "CREATE OR REPLACE PROCEDURE STATS_{1} AS BEGIN DBMS_STATS.GATHER_TABLE_STATS(ownname => '{0}', " \
                    "tabname => '{1}', estimate_percent => {2}, method_opt => 'FOR ALL COLUMNS SIZE AUTO', " \
                    "cascade => true); END;".format(self.schema, self.table_name, estimate_percent)
            con = get_connection(self.connection_key)
            cursor = con.cursor()
            log.debug("Execute query: {0}".format(query))
            cursor.execute(query)
            log.debug("Created temporary stored procedure STATS_{0} successfully.".format(
                self.table_name))
            # Execute procedure
            result = self.exec_callproc("STATS_{0}".format(self.table_name))
            log.info("Executed temporary stored procedure STATS_{0} successfully.".format(
                self.table_name)) if result is True else None
            # Drop procedure
            query = "DROP PROCEDURE STATS_{0};".format(self.table_name)
            result = sql_query(self.connection_key, query)
            log.debug("Dropped temporary stored procedure STATS_{0} successfully.".format(
                self.table_name)) if result is True else None
        except db.Error as exc:
            err, = exc.args
            log.error("Oracle-Error-Code: {0}".format(err.code))
            log.error("Oracle-Error-Message: {0}".format(err.message))
            return False
        return True

    def grant_to_role(self, access):
        query = "grant {0} on {1} to {2};".format(access, self.table_name, self.schema)
        result = sql_query(self.connection_key, query)
        if result is True:
            log.info("Successfully granted {0} permission on table {1} to {2}.".format(
                access, self.table_name, self.schema))
        else:
            log.info("Grant of access right {0} on table {1} to {2} failed.".format(
                access, self.table_name, self.schema))
        return result

    def truncate_table(self):
        """
            Truncate table. Enhanced due user restrictions.
            1. Try to find a procedure named PRC_TRUNCATE and use it
            2. Try to truncate table directly
            3. Delete all rows from table
        """
        prc_truncate = "{0}.PRC_TRUNCATE".format(self.schema)
        query = "select count(*) from all_procedures where owner='{0}' and object_name='{1}';".format(
            self.schema, prc_truncate.split(".")[1])
        proc_exists = sql_query(self.connection_key, query)[0][0]
        try:
            if proc_exists == 1:
                log.info("Found procedure {0} and use it to truncate table {1}.".format(
                    prc_truncate, self.table_name))
                proc_args = self.table_name.split(",")
                result = self.exec_callproc(prc_truncate, proc_args)
                return result
            elif proc_exists == 0:
                log.info("Try to truncate table {0}.{1};".format(self.schema, self.table_name))
                query = "truncate table {0}.{1};".format(self.schema, self.table_name)
                result = sql_query(self.connection_key, query)
                if result is True:
                    log.info("Table {0}.{1} truncated successfully.".format(
                        self.schema, self.table_name)) if result is True else None
                    return result
                elif result is False:
                    log.info("Try to delete rows from table {0}.{1};".format(self.schema, self.table_name))
                    query = "delete from {0}.{1};".format(self.schema, self.table_name)
                    result = sql_query(self.connection_key, query)
                    log.info("Rows in table {0}.{1} deleted successfully.".format(
                        self.schema, self.table_name)) if result is True else None
                    return result
            else:
                log.error("Something strange has happened.")
                return False
        except db.Error as exc:
            err, = exc.args
            log.error("Oracle-Error-Code: {0}".format(err.code))
            log.error("Oracle-Error-Message: {0}".format(err.message))
            return False

    def drop_table(self):
        """ Check for foreign constraints and drop them first before dropping referenced table. """
        stmt = "with tab_cons as (select a.constraint_name from all_cons_columns a, all_constraints b " \
               "where a.constraint_name = b.constraint_name and a.owner=b.owner and b.constraint_type in ('P', 'R') " \
               "and a.owner='{0}' and a.table_name='{1}') " \
               "select a.constraint_name tab_constraint, b.owner, b.table_name, b.constraint_name remote_constraint " \
               "from tab_cons a left join all_constraints b " \
               "on a.constraint_name = b.r_constraint_name;".format(self.schema, self.table_name)
        result = sql_query(self.connection_key, stmt)
        if result is False:
            return result
        if len(result) > 0:
            remote_cons = list(set(list(x[1:] for x in result if x[1])))
            tab_cons = list(set(list(x[0] for x in result if x[0] and x not in remote_cons)))
            for entry in remote_cons:
                r_owner = entry[0]
                r_table = entry[1]
                r_con = entry[2]
                stmt = "ALTER TABLE {0}.{1} DROP CONSTRAINT {2};".format(r_owner, r_table, r_con)
                result = sql_query(self.connection_key, stmt)
                if result is False:
                    continue
            for tab_con in tab_cons:
                stmt = "ALTER TABLE {0}.{1} DROP CONSTRAINT {2};".format(self.schema, self.table_name, tab_con)
                result = sql_query(self.connection_key, stmt)
                if result is False:
                    continue
        stmt = "DROP TABLE {0}.{1};".format(self.schema, self.table_name)
        result = sql_query(self.connection_key, stmt)
        return result

    def deduplicate_rows(self, criteria, l_keys):
        """
        :param criteria:
        :param l_keys:
        :return: True or False
        Method deletes rows from table, where criteria matches the MIN or MAX rowid in table.
        Data are grouped by given unique key list.
        """
        if criteria.upper() not in ('MIN', 'MAX'):
            log.error("Criteria for deletions of duplicate row ids must be MIN or MAX.")
            return False
        query = "select count(*) from {0}.{1} group by {2} having count(*)>1;".format(
            self.schema, self.table_name, ",".join(l_keys))
        result = sql_query(self.connection_key, query)
        num_loops = result[0][0] if result else 0
        if num_loops > 1:
            log.info("Bereinige {0} Dubletten in Tabelle {1}.".format(num_loops, self.table_name))
            for i in range(num_loops):
                query = "delete from {0}.{1} where rowid in (select {2}(rowid) from {0}.{1} group by {3} " \
                        "having count(*)>1);".format(self.schema, self.table_name, criteria, ",".join(l_keys))
                log.info("Deduplicate query: {0}".format(query))
                sql_query(self.connection_key, query)
                if result is False:
                    return False
        return True

    def table2dataframe(self, stmt=None):
        """
            Returns Pandas DataFrame from selection.
            Column names will be inherited.
            If no selection is assigned, all columns and data from the table object will be read.
            Custom selections can include data from multiple tables.
        """
        import pandas as pd
        if not stmt:
            stmt = "select * from {0}.{1}".format(self.schema, self.table_name)
        try:
            with self.connection.cursor() as cursor:
                obj_exec = cursor.execute(stmt)
                header = list(x[0].upper() for x in cursor.description)
                raw_data = obj_exec.fetchall()
                return pd.DataFrame(raw_data, columns=header)
        except db.Error as exc:
            err, = exc.args
            log.error("Oracle-Error-Code: {0}".format(err.code))
            log.error("Oracle-Error-Message: {0}".format(err.message))
            self.connection.rollback()
            return False

    def csv_export(self, directory, filename, timeoption=False, delimiter="|", headline=True, custom_query=None,
                   encoding="utf8", quoting=False, bom=False, write_log=False):
        """ Export defined table into a file. Columns may be specified when table instance is constructed
        with a column_list. If no filename is provided, filename is the same as table name.
        """
        import csv
        cursor = None

        if timeoption:
            filename = "{0}_{1}".format(time.strftime("%Y%m%d%H%M%S"), filename)

        if custom_query:
            query = custom_query
        else:
            l_cols = []
            for column in self.column_list:
                # RAW type conversion to HEX
                if self.d_col_data_type[column][0] == 'RAW':
                    l_cols.append('RAWTOHEX("{0}") "{0}"'.format(column))
                else:
                    l_cols.append('"{0}"'.format(column))
            query = "select {0} from {1}.{2}".format(', '.join(l_cols), self.schema, self.table_name)
        log.debug("Execute query: {0}".format(query))

        try:
            query = query.replace(";", "")
            log.debug("Query: {0}".format(query))
            cursor = self.connection.cursor()
            with open("{0}/{1}".format(directory, filename), "w", encoding=encoding, newline='') as fh:
                if quoting is True:
                    output = csv.writer(fh, dialect='excel', delimiter=delimiter, quoting=csv.QUOTE_ALL, quotechar='"')
                else:
                    output = csv.writer(fh, dialect='excel', delimiter=delimiter, quoting=csv.QUOTE_NONE)
                cursor.execute(query)
                if bom is True:
                    fh.write('\ufeff')
                if headline is True:
                    cols = list(x[0] for x in cursor.description)
                    log.debug("Headline: {0}".format(cols))
                    output.writerow(cols)
                for row_data in cursor:
                    log.debug(row_data)
                    output.writerow(row_data)
        except db.Error as exc:
            err, = exc.args
            log.error("Oracle-Error-Code:", err.code)
            log.error("Oracle-Error-Message:", err.message)
            return False
        finally:
            cursor.close()
        if write_log is True:
            with open("{0}/{1}.log".format(directory, filename), "w", encoding=encoding) as fh:
                md5sum = md5("{0}/{1}".format(directory, filename))
                fh.write("{0} {1}".format(md5sum, filename))
        return True

    def excel_export(self, directory, filename, custom_query=None, encoding="utf8", write_log=False):
        """
        Export table content to Excel file.
        """
        import pandas as pd
        if custom_query:
            query = custom_query
        else:
            query = "select \"{0}\" from {1}.{2}".format("\",\"".join(self.column_list), self.schema, self.table_name)
        query = query.replace(";", "")
        try:
            data = pd.read_sql(query, con=self.connection)
            with pd.ExcelWriter("{0}/{1}".format(directory, filename), engine="xlsxwriter") as writer:
                data.to_excel(writer, sheet_name='Exported Query', index=False)
        except OSError as err:
            log.error(err)
            return False
        if write_log is True:
            with open("{0}/{1}.log".format(directory, filename), "w", encoding=encoding) as fh:
                md5sum = md5("{0}/{1}".format(directory, filename))
                fh.write("{0} {1}".format(md5sum, filename))
        return True

    def avro_export(self, directory, filename, custom_query=None, chunk_size=100000, encoding="utf8", write_log=False):
        """ Export table content or defined resultset into an Avro binary file.
            Columns may be specified when table instance is constructed
            with a column_list.
        """
        import fastavro
        cursor = None
        if custom_query:
            log.warning("Selection with custom query selected. This may not be safe. Date and timestamp "
                        "columns have to be converted to strings manually.")
            query = custom_query
            lookup = custom_query.replace(";", "") + "  fetch first 1 rows only"
            log.debug(lookup)
            try:
                log.debug("Query: {0}".format(lookup))
                cursor = self.connection.cursor()
                cursor.execute(lookup)
                log.debug("Cursor Description: {0}".format(cursor.description))
                self.column_list = list(x[0] for x in cursor.description)
                self.get_avro_schema()
            except db.Error as exc:
                err, = exc.args
                log.error("Oracle-Error-Code:", err.code)
                log.error("Oracle-Error-Message:", err.message)
                return False
            finally:
                cursor.close()
        else:
            query_cols = list('"' + x + '"' for x in self.column_list)
            for i, v in enumerate(self.column_list):
                if self.d_col_data_type.get(v)[0] == "DATE":
                    query_cols[i] = "to_char(\"{0}\", 'YYYY-MM-DD')".format(self.column_list[i])
                elif "TIMESTAMP" in self.d_col_data_type.get(v)[0]:
                    query_cols[i] = "to_char(\"{0}\", 'YYYY-MM-DD HH24:MI:SS.FF')".format(self.column_list[i])
            query = "select {0} from {1}.{2}".format(",".join(query_cols), self.schema, self.table_name)
            log.debug("Execute query: {0}".format(query))
        try:
            self.get_avro_schema()
            query = query.replace(";", "")
            cursor = self.connection.cursor()
            with open("{0}/{1}".format(directory, filename), "wb") as f:
                f.close()
            with open("{0}/{1}".format(directory, filename), "a+b") as f:
                l_records = []
                cursor.execute(query)
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    for line in rows:
                        record = dict(zip(self.column_list, line))
                        l_records.append(record)
                    fastavro.writer(f, self.avro_schema, l_records, codec="deflate")
                    l_records = []
        except db.Error as exc:
            err, = exc.args
            log.error("Oracle-Error-Code:", err.code)
            log.error("Oracle-Error-Message:", err.message)
            return False
        finally:
            cursor.close()
        if write_log is True:
            with open("{0}/{1}.log".format(directory, filename), "w", encoding=encoding) as fh:
                md5sum = md5("{0}/{1}".format(directory, filename))
                fh.write("{0} {1}".format(md5sum, filename))
        return True

    def csv_import(self, directory, filename, action="TRUNCATE", delimiter="|",
                   headline=False, characterset="AL32UTF8"):
        """
            Generate control file and load csv file into table.
        """
        l_loader_columns = []
        self.d_file_cols = self.get_d_sqlldr_cols()
        for column in self.column_list:
            l_loader_columns.append("{0} {1}".format(column, self.d_file_cols.get(column)))
        c_filename = "{0}/{1}.cnt".format(directory, filename)
        badfile = "{0}/{1}.bad".format(directory, filename)
        logfile = "{0}/{1}.log".format(directory, filename)
        discardfile = "{0}/{1}.dsc".format(directory, filename)
        c_file_content = """LOAD DATA characterset {0} infile '{1}/{2}' 
        badfile '{3}' 
        discardfile '{4}' {5} 
        INTO TABLE {6}.{7} FIELDS TERMINATED BY '{8}' 
        OPTIONALLY ENCLOSED BY '"' trailing nullcols 
        ({9})""".format(characterset, directory, filename, badfile, discardfile, action, self.schema,
                        self.table_name, delimiter, ',\n'.join(l_loader_columns))
        if headline:
            c_file_content = "OPTIONS (SKIP=1)\n" + c_file_content
        with open(c_filename, "w") as fh:
            fh.write(c_file_content)
        command = "sqlldr {0}/\\\"{1}\\\"@{2} control={3} -errors=0".format(
            self.schema, self.__pwd, self.sid, c_filename)
        result = unixlib.run_system_command(command)
        if result is False:
            log.error("Load failed. Please look into log- and badfile.")
            # Print Oracle Badfile
            try:
                with open(badfile, 'r') as fh:
                    log.info("Badfile: \n")
                    log.info(fh.read())
                # Print Oracle control file
                with open(c_filename, 'r') as fh:
                    log.error("Controlfile: \n")
                    log.error(fh.read())
                # Print Logfile
                with open(logfile, "r") as fh:
                    log.info("Logfile: \n")
                    log.info(fh.read())
            except ValueError as err:
                log.error(err)
            finally:
                return result
        os.remove(c_filename) if os.path.exists(c_filename) else None
        os.remove(logfile) if os.path.exists(logfile) else None
        os.remove(badfile) if os.path.exists(badfile) else None
        return result

    def dataframe_import(self, df, loader="BLOCK", action=None, query=None, commit_rate=5000):
        """ Imports DataFrame into table object. Fails if table does not contain all columns from DataFrame.
        @param df: Name of Pandas DataFrame
        @param loader: block or single row processing
        @param action: Action REPLACE or None
        @param query: Custom SQL query or None
        @param commit_rate: Number of written rows before a connection commit occurs
        @return: Number of inserted rows or False if method fails.
        """
        import numpy as np
        self.connection = get_connection(self.connection_key)
        df_cols = list(df.columns)
        missing_cols = list(x for x in df_cols if x not in self.column_list)
        if len(missing_cols) > 0:
            log.error("One or more columns in DataFrame don't fit in table. {0}".format(
                ", ".join(missing_cols)))
            return False

        """ Replace action """
        if action:
            if action.upper() == "REPLACE":
                self.truncate_table()

        """ Set named binds for Insert statement """
        columns = list(":" + x for x in self.column_list)

        """ Behandlung von Nullwerten """
        df = df.replace({np.nan: None})

        """ Generate Insert SQL"""
        if not query:
            query = "INSERT INTO {0}.{1} ({2}) VALUES ({3})".format(self.schema, self.table_name,
                                                                    ", ".join(self.column_list), ", ".join(columns))
        log.debug("Insert Statement: {0}".format(query))
        data = df.to_dict("records")
        log.debug(data[0])
        del df
        rows_affected = 0
        with self.connection as tgt:
            if loader.upper() == "BLOCK":
                i = 0
                c = commit_rate
                log.info("Start block loading...")
                try:
                    while i < len(data):
                        c1 = tgt.cursor()
                        c1.executemany("""{0}""".format(query), data[i:i + c])
                        rows_affected += c1.rowcount
                        log.debug("{0} rows written.".format(rows_affected))
                        c1.close()
                        tgt.commit()
                        i = i + c
                except db.Error as exc:
                    err, = exc.args
                    log.error("Oracle-Error-Code: {0}".format(err.code))
                    log.error("Oracle-Error-Message: {0}".format(err.message))
                    tgt.rollback()
                    return False
            elif loader.upper() == "SINGLE":
                log.info("Start single row loading...")
                with tgt.cursor() as c1:
                    for x, row in enumerate(data):
                        try:
                            c1.execute(query, row)
                        except db.Error as exc:
                            log.error("Row: {0}, Error: {1}".format(x, exc))
                            log.error("Rowdata: {0}".format(row))
                            tgt.rollback()
                            return False
                        rows_affected += c1.rowcount
                    tgt.commit()
            else:
                log.error("Loader not defined.")
                return False
        log.info("{0} rows written.".format(rows_affected))
        return rows_affected

    def get_chunksize(self, mem):
        """
            mem: Max memory usage per single process
        """
        query = "select sum(data_length) as max_rowsize " \
                "from all_tab_columns where owner = '{0}' and table_name = '{1}' " \
                "and column_name in ('{2}')".format(self.schema, self.table_name, "', '".join(self.column_list))
        result = sql_query(self.connection_key, query)
        if result is False:
            return result
        else:
            max_rowsize = result[0][0]
            rowsize = int(mem / max_rowsize)
            if rowsize > 100000:
                return 100000
            else:
                return rowsize
