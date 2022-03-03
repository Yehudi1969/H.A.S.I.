# -*- coding: utf8 -*-
###############################################################################
#                                  oralib.py                                  #
###############################################################################
# Autor: Jens Janzen

# Change history
# V1.0: 2019-04-26 - Initial Release

import logging
import os
import time
import cx_Oracle
import seclib
import unixlib
import hashlib

# Description:
# Library contains helper functions for Oracle database access

# Constants
try:
    host = os.environ["HOST"]
except KeyError as e:
    host = "Unknown"
standard_encoding = 'utf8'

""" Sets logging console handler for debugging """
oralib_log = logging.getLogger("oralib")
oralib_log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
oralib_log.addHandler(console_handler)


def get_connection(connection_key):
    cred = seclib.get_credentials(connection_key)
    if cred is False:
        return False
    try:
        con = cx_Oracle.connect(user="{0}".format(cred[1]),
                                password="{0}".format(cred[2]),
                                dsn="{0}".format(cred[0]))
        con.clientinfo = "H.A.S.I. on {0}".format(host)
        con.module = "oralib.py"
        con.action = "SQL Query"
        oralib_log.debug("Connected to {0}, version {1} successful.".format(con.dsn, con.version))
        con.outputtypehandler = output_typehandler
    except cx_Oracle.Error as exc:
        err, = exc.args
        oralib_log.error("Oracle-Error-Code: {0}".format(err.code))
        oralib_log.error("Oracle-Error-Message: {0}".format(err.message))
        raise
    return con


def create_engine(connection_key):
    from sqlalchemy import create_engine
    from sqlalchemy.exc import SQLAlchemyError
    cred = seclib.get_credentials(connection_key)
    if cred is False:
        return False
    try:
        con = create_engine("oracle+cx_oracle://{0}:{1}@{2}".format(cred[1], cred[2], cred[0]))
    except SQLAlchemyError as err:
        oralib_log.error("SQLAlchemy error: {0}".format(err))
        raise
    return con


def output_typehandler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.DB_TYPE_CLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if defaultType == cx_Oracle.DB_TYPE_BLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)


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
    l_queries = filter(None, l_queries)
    l_queries = map(lambda s: s.strip(), l_queries)

    # Obtain credentials
    cred = seclib.get_credentials(connection_key)
    if cred is False:
        return False
    cursor = ""
    selection = []

    # Open DB connection
    con = get_connection(connection_key)

    for query in l_queries:
        action = parse_query(query)
        oralib_log.debug(action)
        # Execute given queries
        if len(cred) == 3:
            try:
                cursor = con.cursor()
                oralib_log.debug("Execute query: {0}".format(query))
                obj_exec = cursor.execute(query)
                oralib_log.debug(obj_exec)
                if obj_exec:
                    selection.extend(obj_exec.fetchall())
                rowcount = cursor.rowcount
                if action in ("MERGE", "INSERT", "UPDATE", "DELETE"):
                    oralib_log.debug("{0} Statement successful. {1} rows affected.".format(action, rowcount))
                oralib_log.debug(selection)
            except cx_Oracle.Error as exc:
                err, = exc.args
                oralib_log.error("Oracle-Error-Code: {0}".format(err.code))
                oralib_log.error("Oracle-Error-Message: {0}".format(err.message))
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


def parse_query(query):
    import re
    for literal in reversed(sorted(re.findall("'([^']*)'", query), key=len)):
        query = query.replace(literal, "")
    oralib_log.debug("Parse query: {0}".format(query))
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
            oralib_log.debug("Enable DMBS Output: callproc(\"dbms_output.enable\")")
            cursor.callproc("dbms_output.enable")
            if proc_args:
                oralib_log.debug("Execute procedure: cursor.callproc({0}, {1})".format(proc_name, proc_args))
                result = cursor.callproc(proc_name, proc_args)
                oralib_log.info("Procedure {0} with arguments {1} successful. {2} rows affected.".format(
                    proc_name, proc_args, cursor.rowcount))
            else:
                oralib_log.debug("Execute procedure: cursor.callproc({0})".format(proc_name))
                result = cursor.callproc(proc_name)
                oralib_log.info("Procedure {0} successful. {1} rows affected.".format(proc_name, cursor.rowcount))
        except cx_Oracle.DatabaseError as exc:
            err, = exc.args
            oralib_log.error("Oracle-Error-Code:", err.code)
            oralib_log.error("Oracle-Error-Message:", err.message)
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
            oralib_log.debug("Execute query: {0}".format(query))
            cursor.execute(query, values)
            oralib_log.debug("{0} Statement successful. {1} rows affected.".format(action, cursor.rowcount))
        except cx_Oracle.DatabaseError as exc:
            err, = exc.args
            oralib_log.error("Oracle-Error-Code:", err.code)
            oralib_log.error("Oracle-Error-Message:", err.message)
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


def process_data(conn_src, src_query, conn_tgt, tgt_query, num_rows=10000):
    try:
        with get_connection(conn_tgt) as tgt:
            with tgt.cursor() as c2:
                with get_connection(conn_src) as src:
                    with src.cursor() as c1:
                        rows_affected = 0
                        c1.execute(src_query)
                        while True:
                            data = c1.fetchmany(num_rows)
                            if len(data) == 0:
                                break
                            c2.executemany(tgt_query, data)
                            rows_affected += c2.rowcount
                        tgt.commit()
    except cx_Oracle.Error as err:
        err, = err.args
        oralib_log.error("Oracle-Error-Code: {0}".format(err.code))
        oralib_log.error("Oracle-Error-Message: {0}".format(err.message))
        raise
    return rows_affected


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
        self.table_name = table_name.upper()
        self.schema = schema.upper() if schema else self.owner.upper()
        self.pk_name = None
        if primary_key_list:
            self.primary_key_list = primary_key_list.upper()
        else:
            self.get_primary_key_list()
        if column_list:
            self.column_list = list(x.upper() for x in column_list)
        else:
            self.get_column_list()
            if len(self.column_list) == 0:
                oralib_log.warning("Table {0} does not (yet) exist.".format(self.table_name))
        if d_col_data_type:
            self.d_col_data_type = d_col_data_type
        else:
            self.get_d_col_data_type()
        self.d_file_cols = None
        self.avro_schema = None
        self.cons_name_list = None
        self.d_constraints = {}
        self.ddl = None

    @property
    def __str__(self):
        return str(self.table_name)

    def exec_callproc(self, proc_name, proc_args=None):
        try:
            cursor = self.connection.cursor()
            oralib_log.debug("Enable DMBS Output: callproc(\"dbms_output.enable\")")
            cursor.callproc("dbms_output.enable")
            if proc_args and isinstance(proc_args, list) is True:
                oralib_log.debug("Execute procedure: cursor.callproc('{0}', {1})".format(proc_name, proc_args))
                cursor.callproc(proc_name, proc_args)
                oralib_log.info("Procedure {0} with arguments {1} successful. {2} rows affected.".format(
                    proc_name, proc_args, cursor.rowcount))
            elif proc_args and isinstance(proc_args, list) is False:
                oralib_log.error("Arguments for procedure have to be list data type.")
                return False
            elif not proc_args:
                oralib_log.debug("Execute procedure: cursor.callproc({0})".format(proc_name))
                cursor.callproc(proc_name)
                oralib_log.info("Procedure {0} successful. {1} rows affected.".format(proc_name, cursor.rowcount))
        except cx_Oracle.DatabaseError as exc:
            err, = exc.args
            oralib_log.error("Oracle-Error-Code:", err.code)
            oralib_log.error("Oracle-Error-Message:", err.message)
            return False
        cursor.close()
        return True

    def get_primary_key_list(self):
        """ Returns a list of key columns from system catalog. DEPRECATED.
        """
        query = "select a.column_name from all_cons_columns a, all_constraints b " \
                "where a.constraint_name = b.constraint_name " \
                "and a.owner=b.owner " \
                "and b.constraint_type='P' " \
                "and a.owner='{0}' and a.table_name='{1}' " \
                "order by a.position;".format(self.schema, self.table_name)
        result = sql_query(self.connection_key, query)
        self.primary_key_list = list(x[0] for x in result)
        return self.primary_key_list

    def get_indices_list(self, constraint_type="P"):
        """ Returns a list of constraint names from system catalog.
        """
        query = "select a.constraint_name from all_cons_columns a, all_constraints b " \
                "where a.constraint_name = b.constraint_name " \
                "and a.owner = b.owner " \
                "and a.owner = '{0}' and a.table_name = '{1}' " \
                "and constraint_type = '{2}' " \
                "order by a.position;".format(self.schema, self.table_name, constraint_type)
        result = sql_query(self.connection_key, query)
        self.cons_name_list = list(x[0] for x in result)
        return self.cons_name_list

    def get_d_constraints(self, check_p=True, check_u=True, check_c=True, check_r=True):
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
                "on b.r_constraint_name = c.constraint_name " \
                "where a.owner='{0}' and a.table_name='{1}' order by a.position;".format(self.schema, self.table_name)
        l_rows = sql_query(self.connection_key, query)
        l_cons = list(set(list(x[0:2] for x in l_rows)))
        d_pk = {}
        d_unique_cons = {}
        d_check_cons = {}
        d_ref_cons = {}
        c_unique = 0
        for cons in l_cons:
            if cons[1] == 'P':
                column_names = list(x[4] for x in l_rows if x[0] == cons[0])
                d_pk['PK'] = "PRIMARY KEY (\"{0}\")".format("\", \"".join(column_names))
            elif cons[1] == 'U':
                column_names = list(x[4] for x in l_rows if x[0] == cons[0])
                d_unique_cons["UNIQUE_{0}".format(c_unique)] = "UNIQUE (\"{0}\")".format("\", \"".join(column_names))
                c_unique += 1
            elif cons[1] == 'C':
                search_condition = list(x[9] for x in l_rows if x[0] == cons[0])
                if "IS NOT NULL" in search_condition[0]:
                    continue
                else:
                    d_check_cons[cons[0]] = "CONSTRAINT \"{0}\" CHECK ({1}) ENABLE".format(
                        cons[0], "\", \"".join(search_condition))
            elif cons[1] == 'R':
                r_schema = list(set(list(x[7] for x in l_rows if x[0] == cons[0])))
                r_table_name = list(set(list(x[8] for x in l_rows if x[0] == cons[0])))
                column_names = list(x[4] for x in l_rows if x[0] == cons[0])
                r_column_names = list(x[6] for x in l_rows if x[0] == cons[0])
                d_ref_cons[cons[0]] = "CONSTRAINT \"{0}\" FOREIGN KEY (\"{1}\") " \
                                      "REFERENCES \"{2}\".\"{3}\" (\"{4}\") ENABLE".format(
                    cons[0], "\", \"".join(column_names), r_schema[0], r_table_name[0],
                    "\", \"".join(r_column_names))
        self.d_constraints.update(d_pk) if check_p else None
        self.d_constraints.update(d_unique_cons) if check_u else None
        self.d_constraints.update(d_check_cons) if check_c else None
        self.d_constraints.update(d_ref_cons) if check_r else None
        return self.d_constraints

    def get_column_list(self):
        """ Returns a list of column names from system catalog.
        """
        query = "select column_name from all_tab_columns where owner = '{0}' and table_name = '{1}' " \
                "order by column_id;".format(self.schema, self.table_name)
        result = sql_query(self.connection_key, query)
        self.column_list = list(x[0] for x in result)
        return self.column_list

    @property
    def get_d_sqlldr_cols(self, dateformat="YYYY-MM-DD HH24:MI:SS", timestampformat="YYYY-MM-DD HH24:MI:SS.FF6"):
        """ Returns a dictionary with column_names and datatypes with lengths useable for DDL generation. """
        query = "select column_name, data_type, data_precision, data_scale, char_length " \
                "from all_tab_columns where owner = '{0}' and table_name = '{1}' " \
                "order by column_id;".format(self.schema, self.table_name)
        result = sql_query(self.connection_key, query)
        self.d_file_cols = {}
        for l_column in result:
            oralib_log.debug(l_column)
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
        oralib_log.debug(self.d_file_cols)
        return self.d_file_cols

    def get_d_col_data_type(self):
        """ Returns a list of data types for all columns in self.column_list.
        """
        query = "select column_name, data_type, case " \
                "when data_type='NUMBER' and data_precision is null " \
                "then 38 else data_precision end as data_precision, data_scale, char_length " \
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
            oralib_log.debug(d_field_entry)
            l_avro_columns.append(d_field_entry.copy())
        d_avro_schema["fields"] = l_avro_columns
        self.avro_schema = fastavro.parse_schema(d_avro_schema)
        oralib_log.debug("AVRO Schema: {0}".format(self.avro_schema))
        return self.avro_schema

    def get_ddl(self, include_schema=True, include_grants=True, include_constraints=True, readable=False,
                table_name=None, body_only=False):
        """ Returns DDL of table as string
        """
        if not table_name:
            table_name = self.table_name
        query = "select column_name, " \
                "case " \
                "when data_type ='DATE' then data_type when data_type like 'TIMESTAMP%' then data_type " \
                "when data_type = 'FLOAT' then data_type||'('||data_precision||')' " \
                "when data_type = 'NUMBER' and data_precision is null then data_type||'(*,0)' " \
                "when data_type = 'NUMBER' and data_precision is not null " \
                "then data_type||'('||data_precision||', '||data_scale||')' " \
                "when data_type in ('CHAR', 'VARCHAR2', 'NVARCHAR2') then data_type||'('||char_length||' BYTE)' " \
                "when data_type in ('ANYDATA', 'BLOB', 'CLOB', 'LONG', 'LONG_RAW', 'RAW', 'ROWID', 'XMLTYPE') " \
                "then data_type " \
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
            self.get_d_constraints()
            for cons in self.d_constraints.values():
                body.append("{0}".format(cons))
        footer = "\n);"
        if grants and include_grants is True:
            footer = footer + "\n{0};".format(";\n".join(grants)) + "\n"
        else:
            footer = footer + "\n"
        if body_only is True:
            self.ddl = "{0}".format("\n\t,".join(body))
        else:
            self.ddl = "{0}{1}{2}".format(header, "\n\t,".join(body), footer)
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
        # Install anonymous procedure in userspace
        query = "CREATE OR REPLACE PROCEDURE STATS_{1} AS BEGIN DBMS_STATS.GATHER_TABLE_STATS(ownname => '{0}', " \
                "tabname => '{1}', estimate_percent => {2}, method_opt => 'FOR ALL COLUMNS SIZE AUTO', " \
                "cascade => true); END;".format(self.schema, self.table_name, estimate_percent)
        result = sql_query(self.connection_key, query)
        oralib_log.debug("Created temporary stored procedure STATS_{0} successfully.".format(
            self.table_name)) if result is True else None
        # Execute procedure
        result = self.exec_callproc("STATS_{0}".format(self.table_name))
        oralib_log.info("Executed temporary stored procedure STATS_{0} successfully.".format(
            self.table_name)) if result is True else None
        # Drop procedure
        query = "DROP PROCEDURE STATS_{0};".format(self.table_name)
        result = sql_query(self.connection_key, query)
        oralib_log.debug("Dropped temporary stored procedure STATS_{0} successfully.".format(
            self.table_name)) if result is True else None
        return True

    def grant_to_role(self, access):
        query = "grant {0} on {1} to {2};".format(access, self.table_name, self.schema)
        result = sql_query(self.connection_key, query)
        if result is True:
            oralib_log.info("Successfully granted {0} permission on table {1} to {2}.".format(
                access, self.table_name, self.schema))
        else:
            oralib_log.info("Grant of access right {0} on table {1} to {2} failed.".format(
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
        if proc_exists == 1:
            oralib_log.info("Found procedure {0} and use it to truncate table {1}.".format(
                prc_truncate, self.table_name))
            proc_args = self.table_name.split(",")
            result = self.exec_callproc(prc_truncate, proc_args)
            return result
        elif proc_exists == 0:
            oralib_log.info("Try to truncate table {0}.{1};".format(self.schema, self.table_name))
            query = "truncate table {0}.{1};".format(self.schema, self.table_name)
            result = sql_query(self.connection_key, query)
            if result is True:
                oralib_log.info("Table {0}.{1} truncated successfully.".format(
                    self.schema, self.table_name)) if result is True else None
            elif result is False:
                oralib_log.info("Try to delete rows from table {0}.{1};".format(self.schema, self.table_name))
                query = "delete from {0}.{1};".format(self.schema, self.table_name)
                result = sql_query(self.connection_key, query)
                oralib_log.info("Rows in table {0}.{1} deleted successfully.".format(
                    self.schema, self.table_name)) if result is True else None
        else:
            oralib_log.error("Something strange has happened.")
            return False
        return result

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
            tab_cons = list(set(list(x[0] for x in result if x[0])))
            remote_cons = list(set(list(x[1:] for x in result if x[1])))
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
            oralib_log.error("Übergebenes Kriterium für ROWID muss MIN oder MAX sein.")
            return False
        query = "select count(*) from {0}.{1} group by {2} having count(*)>1;".format(
            self.schema, self.table_name, ",".join(l_keys))
        result = sql_query(self.connection_key, query)
        num_loops = result[0][0] if result else 0
        if num_loops > 1:
            oralib_log.info("Bereinige {0} Dubletten in Tabelle {1}.".format(num_loops, self.table_name))
            for i in range(num_loops):
                query = "delete from {0}.{1} where rowid in (select {2}(rowid) from {0}.{1} group by {3} " \
                        "having count(*)>1);".format(self.schema, self.table_name, criteria, ",".join(l_keys))
                oralib_log.info("Deduplicate query: {0}".format(query))
                sql_query(self.connection_key, query)
                if result is False:
                    return False
        return True

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
        oralib_log.debug("Execute query: {0}".format(query))

        try:
            query = query.replace(";", "")
            oralib_log.debug("Query: {0}".format(query))
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
                    oralib_log.debug("Headline: {0}".format(cols))
                    output.writerow(cols)
                for row_data in cursor:
                    oralib_log.debug(row_data)
                    output.writerow(row_data)
        except cx_Oracle.DatabaseError as exc:
            err, = exc.args
            oralib_log.error("Oracle-Error-Code:", err.code)
            oralib_log.error("Oracle-Error-Message:", err.message)
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
            oralib_log.error(err)
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
            oralib_log.warning("Selection with custom query selected. This may not be safe. Date and timestamp "
                               "columns have to be converted to strings manually.")
            query = custom_query
            lookup = custom_query.replace(";", "") + "  fetch first 1 rows only"
            oralib_log.debug(lookup)
            try:
                oralib_log.debug("Query: {0}".format(lookup))
                cursor = self.connection.cursor()
                cursor.execute(lookup)
                oralib_log.debug("Cursor Description: {0}".format(cursor.description))
                self.column_list = list(x[0] for x in cursor.description)
                self.get_avro_schema()
            except cx_Oracle.DatabaseError as exc:
                err, = exc.args
                oralib_log.error("Oracle-Error-Code:", err.code)
                oralib_log.error("Oracle-Error-Message:", err.message)
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
            oralib_log.debug("Execute query: {0}".format(query))
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
        except cx_Oracle.DatabaseError as exc:
            err, = exc.args
            oralib_log.error("Oracle-Error-Code:", err.code)
            oralib_log.error("Oracle-Error-Message:", err.message)
            return False
        finally:
            cursor.close()
        if write_log is True:
            with open("{0}/{1}.log".format(directory, filename), "w", encoding=encoding) as fh:
                md5sum = md5("{0}/{1}".format(directory, filename))
                fh.write("{0} {1}".format(md5sum, filename))
        return True

    def load_file(self, directory, filename, action="TRUNCATE", delimiter="|",
                  headline=False, characterset="AL32UTF8"):
        """
            Generate control file and load csv file into table.
        """
        l_loader_columns = []
        self.d_file_cols = self.get_d_sqlldr_cols
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
            oralib_log.error("Load failed. Please look into log- and badfile.")
            # Print Oracle Badfile
            try:
                with open(badfile, 'r') as fh:
                    oralib_log.info("Badfile: \n")
                    oralib_log.info(fh.read())
                # Print Oracle control file
                with open(c_filename, 'r') as fh:
                    oralib_log.error("Controlfile: \n")
                    oralib_log.error(fh.read())
                # Print Logfile
                with open(logfile, "r") as fh:
                    oralib_log.info("Logfile: \n")
                    oralib_log.info(fh.read())
            except ValueError as err:
                oralib_log.error(err)
            finally:
                return result
        os.remove(c_filename) if os.path.exists(c_filename) else None
        os.remove(logfile) if os.path.exists(logfile) else None
        os.remove(badfile) if os.path.exists(badfile) else None
        os.remove(filename) if os.path.exists(filename) else None
        return result
