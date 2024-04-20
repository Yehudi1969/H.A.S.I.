# -*- coding: utf8 -*-
###############################################################################
#                                  hanadblib.py                               #
###############################################################################
# Copyright (C) 2023  Jens Janzen

# Change history
# V1.0: 2023-02-15 - Initial Release

import hashlib
import logging
import os
import time
from hdbcli import dbapi as db
import seclib
# import certifi

# Description:
# Library contains helper functions for SAP HANA database access

# Constants
try:
    host = os.environ["HOST"]
except KeyError as e:
    host = "Unknown"
standard_encoding = 'utf8'

""" Sets logging console handler for debugging """
log = logging.getLogger("SESSION")


def get_connection(connection_key):
    """ Returns DB-Connection """
    cred = seclib.get_credentials(connection_key)
    if cred is False:
        return False
    dsn = cred[0].split(":")
    hostname = dsn[0]
    port = dsn[1]
    username = cred[2]
    pw = cred[3]
    try:
        d_connection = {"address": hostname,
                        "port": port,
                        "user": username,
                        "password": pw,
                        "autocommit": False,
                        "compress": True,
                        "cursorHoldabilityType": "COMMIT",
                        "packetSize": 104857600,
                        "statementCacheSize": 2,
                        "statementRoutingWarnings": True,
                        "encrypt": True,
                        "sslValidateCertificate": False,
                        # "key": "hdbuserstore key",
                        # "sslTruststore: certifi.where()
                        }
        con = db.connect(**d_connection)
        log.debug("Connected successfully.")
    except db.DatabaseError as err:
        log.error("DB connection: {0}".format(connection_key))
        log.error("HANA-Error-Code: {0}".format(err))
        raise
    return con


def sql_query(connection_key, query):
    """ Executes one or more queries against database instance.
    Needs a key representing connection, user and dsn and a string
    with one or more statements as arguments.
    Returns a list of all selections with each collected row as a tuple.
    If one of the statements raises an error, all statements of the used connection are rolled back.
    """
    # Check whether statement ends with proper termination character
    if not query.endswith(";"):
        query += ";"
    # Check for multiple queries
    l_queries = query.split(";")
    l_queries = map(lambda s: s.strip(), l_queries)
    l_queries = filter(None, l_queries)

    # Obtain credentials
    cred = seclib.get_credentials(connection_key)
    if cred is False:
        return False
    cursor = None
    selection = None

    # Open DB connection
    con = get_connection(connection_key)

    for query in l_queries:
        # Execute given queries
        try:
            cursor = con.cursor()
            log.debug("Execute query: {0}".format(query))
            obj_exec = cursor.execute(query)
            log.debug("Cursor execute: {0}".format(obj_exec))
            log.debug("Cursor description: {0}".format(cursor.description))
            try:
                selection = cursor.fetchall()
            except db.ProgrammingError:
                selection = True
            rowcount = cursor.rowcount
            log.debug("Statement successful. {0} rows affected.".format(rowcount))
            log.debug("Selection: {0}".format(selection))
        except db.DatabaseError as err:
            log.error("HANA-Error-Code: {0}".format(err))
            con.rollback()
            cursor.close()
            con.close()
            raise
    con.commit()
    con.close()
    return selection


def run_procedure(connection_key, proc_name, proc_args=None):
    """ Executes a stored procedure in the target database instance.
    Needs a key representing connection, user and dsn and a string
    with the name / parameters of the procedure as arguments.
    """
    # Obtain credentials
    cred = seclib.get_credentials(connection_key)
    cursor = None
    result = None

    # Open DB connection
    con = get_connection(connection_key)

    if len(cred) == 3:
        try:
            cursor = con.cursor()
            if proc_args:
                log.debug("Execute procedure: cursor.callproc({0}, {1})".format(proc_name, proc_args))
                result = cursor.callproc(proc_name, proc_args)
                log.info("Procedure {0} with arguments {1} successful. {2} rows affected.".format(
                    proc_name, proc_args, cursor.rowcount))
            else:
                log.debug("Execute procedure: cursor.callproc({0})".format(proc_name))
                result = cursor.callproc(proc_name)
                log.info("Procedure {0} successful. {1} rows affected.".format(proc_name, cursor.rowcount))
        except db.DatabaseError as err:
            log.error("HANA-Error-Code: {0}".format(err))
            con.rollback()
            cursor.close()
            con.close()
            return False
    con.commit()
    con.close()
    return result


def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def process_data(conn_src, src_query, conn_tgt, tgt_query, num_rows=100000):
    """
    @param conn_src:
    @param src_query:
    @param conn_tgt:
    @param tgt_query:
    @param num_rows:
    @return:
    """
    src = None
    tgt = None
    c1 = None
    c2 = None
    try:
        tgt = get_connection(conn_tgt)
        c2 = tgt.cursor()
        src = get_connection(conn_src)
        c1 = src.cursor()
        rows_affected = 0
        c1.execute(src_query)
        while True:
            data = c1.fetchmany(num_rows)
            if len(data) == 0:
                break
            if data is False:
                return False
            c2.executemany(tgt_query, data)
            rows_affected += c2.rowcount
        tgt.commit()
    except db.DatabaseError as err:
        err, = err.args
        log.error("HANA-Error-Code: {0}".format(err))
        tgt.rollback()
        raise
    finally:
        c1.close()
        c2.close()
        src.close()
        tgt.close()
    return rows_affected


def create_selector(l_intersect, o_src_table):
    """ Generate SQL statement for selector """
    l_selection = []
    for x in l_intersect:
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
    l_placeholder = list(":{0}".format(x) for x in range(1, len(l_intersect) + 1))
    if action == "INSERT":
        return "INSERT INTO \"{0}\".\"{1}\" (\"{2}\") VALUES ({3})".format(
            o_tgt_table.schema, o_tgt_table.table_name, "\", \"".join(l_intersect), ", ".join(l_placeholder))
    if action == "MASK":
        return "INSERT INTO \"{0}\".\"{1}\" (\"{2}\") VALUES ({3})".format(
            o_tgt_table.schema, o_tgt_table.table_name, "\", \"".join(l_intersect), ", ".join(l_placeholder))
    elif action == "UPSERT":
        return "UPSERT \"{0}\".\"{1}\" (\"{2}\") VALUES ({3}) WITH PRIMARY KEY".format(
            o_tgt_table.schema, o_tgt_table.table_name, "\", \"".join(l_intersect), ", ".join(l_placeholder))
    elif action == "UPSERT_MASK":
        return "UPSERT \"{0}\".\"{1}\" (\"{2}\") VALUES ({3}) WITH PRIMARY KEY".format(
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
    """
        Copy data with db connectors that doesn't support context managers like SAP HANA hdbcli
    """
    src = None
    tgt = None
    c1 = None
    c2 = None
    rows_affected = 0
    o_src_table = Table(src_dsn, src_table, schema=src_schema)
    chunksize = o_src_table.get_chunksize(max_memory)
    o_tgt_table = Table(tgt_dsn, tgt_table, schema=tgt_schema)
    if o_src_table.column_list != o_tgt_table.column_list:
        log.warning("Structure between Source and Target Table does not match.")
    try:
        tgt = get_connection(tgt_dsn)
        c2 = tgt.cursor()
        src = get_connection(src_dsn)
        c1 = src.cursor()
        c1.execute(src_query)
        log.info("Copy data with blocksize: {0}".format(chunksize))
        while True:
            data = c1.fetchmany(chunksize)
            if len(data) == 0:
                break
            c2.executemany(tgt_query, data)
            rows_affected += c2.rowcount
            log.debug("Index: {0}".format(rows_affected))
            tgt.commit()
    except db.DatabaseError as err:
        log.error("HANA-Error-Code: {0}".format(err))
        return False
    finally:
        c1.close()
        c2.close()
        src.close()
        tgt.close()
    return rows_affected


def mask_data(src_dsn, src_schema, src_table, src_query, tgt_dsn, tgt_schema, tgt_table, tgt_query,
              l_cols, d_rules, max_memory, d_cache):
    import pandas as pd
    """ Create target table object """
    o_src_table = Table(src_dsn, src_table, schema=src_schema)
    chunksize = o_src_table.get_chunksize(max_memory)
    o_tgt_table = Table(tgt_dsn, tgt_table, schema=tgt_schema)
    """
        Maps data with db connectors that doesn't support context managers like SAP HANA hdbcli
    """
    src = None
    c1 = None
    rows_affected = 0
    try:
        src = get_connection(src_dsn)
        c1 = src.cursor()
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
    except db.DatabaseError as err:
        log.error("HANA-Error-Code: {0}".format(err))
        return False
    finally:
        c1.close()
        src.close()
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
        log.error("Check for Duplikates...\n{0}".format(
            table_rows.loc[table_rows.duplicated(subset=o_src_table.primary_key_list, keep=False)]))
        return False
    else:
        return table_rows


# Database Classes
class Table(object):
    """ Klasse für eine SAP HANA Tabelle.
    Enthaltene Attribute:
    primary_key_list - Eindeutiger Schlüssel auf Tabelle
    column_list - Liste der Datenbankfelder
    d_file_cols - Dictionary mit Feldnamen und Datentypen für das sqlldr Controlfile
    d_col_data_type - Dictionary mit Feldnamen und Datentypen
    avro_schema - Enthält das AVRO Schema im JSON Format

    Methoden:
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
        """ Creates an object instance for a single HANA table or view.
        Needs owner and table_name as argument.
        """
        self.connection_key = connection_key
        self.connection = get_connection(self.connection_key)
        self.table_name = table_name.upper()
        self.schema = schema.upper() if schema else None
        if primary_key_list:
            self.primary_key_list = primary_key_list.upper()
        else:
            self.get_primary_key_list()
        if column_list:
            self.column_list = list(x.upper() for x in column_list)
        else:
            self.get_column_list()
            if len(self.column_list) == 0:
                log.warning("Table {0} does not (yet) exist.".format(self.table_name))
        if d_col_data_type:
            self.d_col_data_type = d_col_data_type
        else:
            self.get_d_col_data_type()
        self.d_file_cols = None
        self.avro_schema = None
        self.cons_name_list = None
        self.d_constraints = {}
        self.ddl = None
        self.db_type = "HANA"

    @property
    def __str__(self):
        return str(self.table_name)

    def exec_callproc(self, proc_name, proc_args=None):
        """
        @param proc_name:
        @param proc_args:
        @return:
        """
        try:
            cursor = self.connection.cursor()
            if proc_args is None:
                log.debug("Execute procedure: cursor.callproc({0})".format(proc_name))
                cursor.callproc(proc_name)
                log.info("Procedure {0} successful. {1} rows affected.".format(proc_name, cursor.rowcount))
            else:
                log.debug("Execute procedure: cursor.callproc({0}, {1})".format(proc_name, proc_args))
                cursor.callproc(proc_name, proc_args)
                log.info("Procedure {0} with arguments {1} successful. {2} rows affected.".format(
                    proc_name, proc_args, cursor.rowcount))
        except db.DatabaseError as err:
            log.error("HANA-Error-Code: {0}".format(err))
            return False
        cursor.close()
        return True

    def get_primary_key_list(self):
        """ Returns a list of key columns from system catalog. DEPRECATED.
        """
        query = "select column_name from constraints where schema_name='{0}' and table_name='{1}' " \
                "and is_primary_key='TRUE' order by position;".format(self.schema, self.table_name)
        result = sql_query(self.connection_key, query)
        self.primary_key_list = list(x[0] for x in result)
        return self.primary_key_list

    def get_d_constraints(self, check_r=False):
        """ Returns a dictionary of constraint names as keys and details as values.
        check_r = False is a compatibility feature to other db adapters that support forein key constraints
        """
        log.debug(check_r)
        query = "select a.index_name, a.index_type, a.constraint, a.schema_name, a.table_name, b.column_name, " \
                "b.ascending_order, " \
                "c.referenced_constraint_name, c.referenced_schema_name, c.referenced_table_name, " \
                "c.referenced_column_name , c.update_rule, c.delete_rule " \
                "from indexes a join index_columns b " \
                "on a.table_oid = b.table_oid and a.index_name = b.index_name " \
                "left outer join referential_constraints c " \
                "on a.schema_name = c.schema_name and a.table_name = c.table_name " \
                "and b.column_name = c.column_name and a.index_name = c.constraint_name " \
                "where a.schema_name='{0}' and a.table_name='{1}' " \
                "order by a.index_name, b.position;".format(self.schema, self.table_name)
        l_rows = sql_query(self.connection_key, query)
        log.debug(l_rows)
        if l_rows is None:
            return self.d_constraints
        l_cons = list(set(list(x[0:6] for x in l_rows)))
        for cons in l_cons:
            index_name = cons[0]
            index_type = cons[1]
            constraint = cons[2]
            schema_name = cons[3]
            table_name = cons[4]
            column_names = list(x[5] for x in l_rows if x[0] == cons[0])
            ascending_order = list(x[6] for x in l_rows if x[0] == cons[0])
            if constraint == 'PRIMARY KEY':
                self.d_constraints[index_name] = "ALTER TABLE \"{0}\".\"{1}\" ADD CONSTRAINT \"{2}\" PRIMARY KEY " \
                                                 "(\"{3}\")".format(schema_name, table_name, index_name,
                                                                    "\", \"".join(column_names))
            elif constraint is None and ascending_order == 'TRUE':
                self.d_constraints[index_name] = "CREATE {0} INDEX \"{1}\" ON \"{2}\".\"{3}\" (\"{4}\" ASC)".format(
                    index_type, index_name, schema_name, table_name, "\", \"".join(column_names))
            elif constraint is None and ascending_order != 'TRUE':
                self.d_constraints[index_name] = "CREATE {0} INDEX \"{1}\" ON \"{2}\".\"{3}\" (\"{4}\")".format(
                    index_type, index_name, schema_name, table_name, "\", \"".join(column_names))
        return self.d_constraints

    def get_column_list(self):
        """ Returns a list of column names from system catalog.
        """
        query = "select column_name from table_columns where schema_name = '{0}' and table_name = '{1}' " \
                "order by position;".format(self.schema, self.table_name)
        result = sql_query(self.connection_key, query)
        self.column_list = list(x[0] for x in result)
        return self.column_list

    def get_d_col_data_type(self):
        """ Returns a list of data types for all columns in self.column_list.
            column_name
            datatype_name
            length: Length (character or numerical)
            scale: length of decimal places
            length: Length (character or numerical)
            Hint: In Oracle, PostgreSQL, etc. numerical and character length resides in two different columns.
            To stay compatible with different DB adapters, length is selected twice.
        """
        query = "select column_name, data_type_name, length, scale, length, is_nullable " \
                "from table_columns where schema_name = '{0}' and table_name = '{1}' and column_name in ('{2}') " \
                "order by position;".format(self.schema, self.table_name, '\',\''.join(self.column_list))
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
        query = "select column_name, " \
                "case when data_type_name in ('FLOAT', 'NUMBER') and scale = 0 then 'long' " \
                "when data_type_name in ('FLOAT', 'NUMBER') and scale > 0 then 'float' " \
                "when data_type_name like 'INTERVAL%' and scale = 0 then 'long' " \
                "when data_type_name like 'INTERVAL%' and scale > 0 then 'float' " \
                "when data_type_name = 'XMLTYPE' then 'map' else 'string' end as avro_data_type, is_nullable " \
                "from table_columns where schema_name = '{0}' and table_name = '{1}' and column_name in ('{2}') " \
                "order by position;".format(self.schema, self.table_name, '\', \''.join(self.column_list))
        l_descriptor = sql_query(self.connection_key, query)
        d_avro_schema = {"namespace": "aws.hana.avro.{0}".format(self.schema),
                         "type": "record",
                         "name": "{0}".format(self.table_name),
                         "fields": []}
        for item in l_descriptor:
            d_field_entry = {}
            """ Check nullable columns """
            if item[2] == 'Y':
                d_field_entry["name"] = "{0}".format(item[0])
                d_field_entry["type"] = ["{0}".format(item[1]), "null"]
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
                table_name=None, body_only=False):
        """ Returns create statement of table as string
        """
        if not table_name:
            table_name = self.table_name
        """ Select table type """
        query = "select table_type from tables " \
                "where schema_name='{0}' and table_name='{1}';".format(self.schema, self.table_name)
        result = sql_query(self.connection_key, query)
        table_type = ",".join(list(x for x in result[0] if result is not False))

        """ Group data types and select column data """
        query = "select column_name, " \
                "case " \
                "when data_type_name in ('DATE', 'TIMESTAMP', 'TIME', 'SECONDDATE') then data_type_name " \
                "when data_type_name in ('BOOLEAN', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT') then data_type_name " \
                "when data_type_name in ('DECIMAL', 'DOUBLE', 'REAL', 'SMALLDECIMAL') and scale is null " \
                "then data_type_name||'('||length||')' " \
                "when data_type_name in ('DECIMAL', 'DOUBLE', 'REAL', 'SMALLDECIMAL') and scale is not null " \
                "then data_type_name||'('||length||', '||scale||')' " \
                "when data_type_name in ('CHAR', 'NVARCHAR', 'VARCHAR', 'BINARY', 'VARBINARY') " \
                "then data_type_name||'('||length||')' " \
                "when data_type_name in ('BLOB', 'CLOB', 'NCLOB', 'NBLOB', 'TEXT') then data_type_name " \
                "when data_type_name in ('ST_GEOMETRY', 'ST_POINT') then data_type_name " \
                "else data_type_name end as data_type " \
                ", case when data_type_name in ('CHAR', 'NVARCHAR', 'VARCHAR') " \
                "then '''' || default_value || '''' end as default_value, is_nullable " \
                "from table_columns where schema_name = '{0}' and table_name = '{1}' and column_name in ('{2}') " \
                "order by position;".format(self.schema, self.table_name, '\', \''.join(self.column_list))
        cols = sql_query(self.connection_key, query)
        log.debug(cols)
        if include_schema is True:
            query = "select 'GRANT '|| PRIVILEGE ||' ON \"' || SCHEMA_NAME || '\".\"'|| OBJECT_NAME ||'\" " \
                    "TO '|| GRANTEE from granted_privileges " \
                    "where schema_name='{0}' and object_name='{1}' " \
                    "and grantee_type='USER';".format(self.schema, self.table_name)
        else:
            query = "select 'GRANT '|| PRIVILEGE ||' ON \"' || OBJECT_NAME || '\" TO '|| GRANTEE " \
                    "from granted_privileges " \
                    "where schema_name='{0}' and object_name='{1}' " \
                    "and grantee_type='USER';".format(self.schema, self.table_name)
        result = sql_query(self.connection_key, query)
        grants = list(x[0] for x in result)

        if include_schema is True:
            header = "CREATE {0} TABLE \"{1}\".\"{2}\" (\n\t".format(table_type, self.schema, table_name)
        else:
            header = "CREATE {0} TABLE \"{1}\" (\n\t".format(table_type, table_name)
        body = []
        for row in cols:
            column = '"' + row[0] + '"'
            data_type = row[1]
            data_default = "" if row[2] is None else "DEFAULT {0}".format(row[2])
            null_value = "" if row[3] == "TRUE" else "NOT NULL"
            body.append("{0} {1} {2} {3}".format(column, data_type, data_default, null_value))
        footer = ");\n"
        if len(grants) > 0 and include_grants is True:
            footer = footer + "\n{0};".format(";\n".join(grants)) + " \n"
        if include_constraints is True:
            self.get_d_constraints()
            for key in self.d_constraints.keys():
                footer = footer + "{0};".format(self.d_constraints[key]) + " \n"
        if body_only is True:
            self.ddl = "{0}".format("\n\t, ".join(body))
        else:
            self.ddl = "{0}{1}{2}".format(header, "\n\t, ".join(body), footer)
        if readable is True:
            self.ddl = "-- DDL fuer Tabelle {0}.{1} created by hanadblib.py\n".format(
                self.schema, self.table_name) + self.ddl
        else:
            self.ddl = self.ddl.replace("\t", "").replace("\n", "")
        return self.ddl

    def truncate_table(self):
        """ Truncate table.
        """
        query = "truncate table \"{0}\".\"{1}\";".format(self.schema, self.table_name)
        result = sql_query(self.connection_key, query)
        log.info("Table {0}.{1} truncated successfully.".format(
            self.schema, self.table_name)) if result is True else None
        return result

    def drop_table(self):
        """ Drop table """
        stmt = "DROP TABLE \"{0}\".\"{1}\";".format(self.schema, self.table_name)
        result = sql_query(self.connection_key, stmt)
        if result is False:
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
        num_loops = result[0][0] if len(result) > 0 else 0
        if num_loops > 1:
            log.info("Bereinige {0} Dubletten in Tabelle {1}.".format(num_loops, self.table_name))
            for i in range(num_loops):
                query = "delete from {0}.{1} where \"$rowid$\" in (select {2}(\"$rowid$\") from {0}.{1} group by {3} " \
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
        cursor = None
        if not stmt:
            stmt = "select * from {0}.{1}".format(self.schema, self.table_name)
        try:
            cursor = self.connection.cursor()
            cursor.execute(stmt)
            header = list(x[0] for x in cursor.description)
            raw_data = cursor.fetchall()
        except db.DatabaseError as err:
            log.error("HANA-Error-Code: {0}".format(err))
            self.connection.rollback()
            cursor.close()
            raise
        return pd.DataFrame(raw_data, columns=header)

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
        except db.DatabaseError as exc:
            err, = exc.args
            log.error("HANA-Error-Code::", err.code)
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
        import pandas
        if custom_query:
            query = custom_query
        else:
            query = "select \"{0}\" from {1}.{2}".format("\",\"".join(self.column_list), self.schema, self.table_name)
        query = query.replace(";", "")
        try:
            data = pandas.read_sql(query, con=self.connection)
            with pandas.ExcelWriter("{0}/{1}".format(directory, filename), engine="xlsxwriter") as writer:
                data.to_excel(writer, sheet_name='Exported Query', index=False)
        except OSError as err:
            log.error(err)
            return False
        if write_log is True:
            with open("{0}/{1}.log".format(directory, filename), "w", encoding=encoding) as fh:
                md5sum = md5("{0}/{1}".format(directory, filename))
                fh.write("{0} {1}".format(md5sum, filename))
        return True

    def avro_export(self, directory, filename, custom_query=None, numrows=100000, encoding="utf8", write_log=False):
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
            except db.DatabaseError as exc:
                err, = exc.args
                log.error("HANA-Error-Code::", err.code)
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
                    rows = cursor.fetchmany(numrows)
                    if not rows:
                        break
                    for line in rows:
                        record = dict(zip(self.column_list, line))
                        l_records.append(record)
                    fastavro.writer(f, self.avro_schema, l_records, codec="deflate")
                    l_records = []
        except db.DatabaseError as exc:
            err, = exc.args
            log.error("HANA-Error-Code::", err.code)
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
        pass

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

        """ Behandlung von Nullwerten """
        df = df.replace({np.nan: ""})

        """ Generate Insert SQL"""
        if not query:
            query = "INSERT INTO \"{0}\".\"{1}\" (\"{2}\") VALUES ({3})".format(
                self.schema, self.table_name, "\", \"".join(self.column_list),
                ", ".join(list(":{0}".format(x) for x in range(1, len(self.column_list) + 1))))
        log.debug("Insert Statement: {0}".format(query))
        # data = df.to_dict("records")
        data = list(zip(*map(df.get, df)))
        log.debug(data[0])
        del df
        rows_affected = 0
        tgt = self.connection
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
            except db.DatabaseError as err:
                log.error("HANA-Error-Code: {0}".format(err))
                tgt.rollback()
                return False
        elif loader.upper() == "SINGLE":
            log.info("Start single row loading...")
            c1 = tgt.cursor()
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

    def get_chunksize(self, mem=2147483648):
        """
            mem: Max memory usage per single process (Standard: 2 GB)
        """
        query = "select sum(length) as max_rowsize " \
                "from table_columns where schema_name = '{0}' and table_name = '{1}' " \
                "and data_type_name not in ('BLOB', 'CLOB', 'NCLOB') and column_name in ('{2}')".format(
                 self.schema, self.table_name, "', '".join(self.column_list))
        result = sql_query(self.connection_key, query)
        if result is False:
            return result
        else:
            max_rowsize = result[0][0]
            return int(mem / max_rowsize / 100)
