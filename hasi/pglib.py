# -*- coding: utf8 -*-
###############################################################################
#                                   pglib.py                                  #
###############################################################################
# Copyright (C) 2023  Jens Janzen

# Change history
# v0.1: 2020-06-19 - Initial Release

import hashlib
import logging
import time
import psycopg2 as db
import seclib

# Description:
# Library contains helper functions for PostgreSQL database access

# Constants
standard_encoding = 'utf8'

""" Sets logging console handler for debugging """
log = logging.getLogger("SESSION")


def get_connection(connection_key, sslmode="require", sslrootcert="/opt/Certificates/rds-ca.pem"):
    """
    Returns a PostgreSQL database connection.
    :param connection_key:
    :param sslmode:
    :param sslrootcert:
    :return: database connection
    """
    cred = seclib.get_credentials(connection_key)
    if cred is False:
        return False
    address = cred[0].split(":")
    if len(address) == 2:
        hostname = address[0]
        port = address[1]
    else:
        hostname = address[0]
        port = 5432
    dbname = cred[1]
    username = cred[2]
    pw = cred[3]
    try:
        d_connection = {"dbname": dbname,
                        "user": username,
                        "password": pw,
                        "host": hostname,
                        "port": port,
                        "sslmode": sslmode,
                        "sslrootcert": sslrootcert}
        con = db.connect(**d_connection)
        log.debug("Connected to {0} with user {1}.".format(cred[1], cred[2]))
    except db.DatabaseError as err:
        log.error("Error-Message: ", err)
        return False
    return con


def sql_query(connection_key, query):
    """
    :param connection_key: Key for function to lookup credentials from seclib
    :param query: Single query or list of queries separated with a semicolon to be executed on database
    :return: False if anything gone wrong. True if there are rows changed and a resultset
    if the query returned data as result of a selection.
    """
    if not query.endswith(';'):
        query += ";"
    l_queries = query.split(";")
    l_queries = filter(None, l_queries)
    l_queries = map(lambda s: s.strip(), l_queries)

    con = get_connection(connection_key)
    cursor = None
    selection = []

    for query in l_queries:
        action = parse_query(query)
        try:
            cursor = con.cursor()
            log.debug("Execute query: {0}".format(query))
            cursor.execute(query)
            selection.extend(cursor.fetchall()) if action == "SELECT" else None
            log.info("{0} Statement successful. {1} rows affected.".format(action, cursor.rowcount))
            log.debug("Resultset: {0}".format(selection))
        except db.DatabaseError as err:
            log.error("Error-Message: ", err)
            con.rollback()
            cursor.close()
            con.close()
            return False
    con.commit()
    con.close()
    return selection


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
    elif "call" in query.lower():
        return "CALL"
    else:
        return query.split(" ", 1)[0].upper()


def md5(fname):
    """
    :param fname: Filename
    :return: MD5 Hash value of the file
    """
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


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
    l_placeholder = list(":\"{0}\"".format(x) for x in l_intersect)
    if action in ("INSERT", "MASK"):
        return "INSERT INTO \"{0}\".\"{1}\" (\"{2}\") VALUES ({3})".format(
            o_tgt_table.schema, o_tgt_table.table_name, "\", \"".join(l_intersect), ", ".join(l_placeholder))
    elif action in ("UPSERT", "UPSERT_MASK"):
        l_tgt_attributes = list(x for x in o_tgt_table.column_list if x not in o_tgt_table.primary_key_list)
        exp_upsert = ", ".join(list(x + " = EXCLUDED." + x for x in l_tgt_attributes))
        tgt_query = "INSERT INTO \"{0}\".\"{1}\" (\"{2}\") VALUES ({3}) ON CONFLICT ({4}) " \
                    "DO UPDATE SET {5}".format(o_tgt_table.schema, o_tgt_table.table_name, "\", \"".join(l_intersect),
                                               ", ".join(l_placeholder), ", ".join(o_tgt_table.primary_key_list),
                                               exp_upsert)
        return tgt_query
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
    except db.DatabaseError as err:
        err, = err.args
        log.error("Postgres Error: {0}".format(err.code))
        log.error("Postgres Error Message: {0}".format(err.message))
        return False
    return rows_affected


def mask_data(src_dsn, src_schema, src_table, src_query, tgt_dsn, tgt_schema, tgt_table, tgt_query,
              l_cols, d_rules, max_memory, d_cache):
    import pandas as pd
    """ Create target table object """
    o_src_table = Table(src_dsn, src_table, schema=src_schema)
    chunksize = o_src_table.get_chunksize(max_memory)
    o_tgt_table = Table(tgt_dsn, tgt_table, schema=tgt_schema)
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
    except Exception as err:
        err, = err.args
        log.error("Postgres Error: {0}".format(err.code))
        log.error("Postgres Error Message: {0}".format(err.message))
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
    """
    Class for a PostgreSQL table with various methods.
    """

    def __init__(self, connection_key, table_name, schema=None, primary_key_list=None, column_list=None,
                 d_col_data_type=None):
        """
        :param connection_key: Key for function to lookup credentials from seclib
        :param table_name: Name of table
        :param schema: Name of databse schema
        :param primary_key_list: List with primary key
        :param column_list: List with columns of a selection or with all columns of the referenced table
        :param d_col_data_type: Dictionary with columns and data types
        """
        self.connection_key = connection_key
        try:
            self.connection = get_connection(self.connection_key)
        except db.DatabaseError as err:
            log.error("Error-Message: ", err)
        self.schema = schema if schema else None
        self.table_name = table_name
        if column_list:
            self.column_list = list(x for x in column_list)
        else:
            self.get_column_list()
            if len(self.column_list) == 0:
                log.warning("Table {0} does not (yet) exist.".format(self.table_name))
        if self.column_list:
            self.primary_key_list = self.get_primary_key_list() if primary_key_list is None else primary_key_list
            self.d_col_data_type = self.get_d_col_data_type() if d_col_data_type is None else d_col_data_type
            self.avro_schema = self.get_avro_schema()
        else:
            log.warning("Table {0} does not (yet) exist.".format(self.table_name))
        self.connection.close()
        self.db_type = "POSTGRESQL"

    @property
    def __str__(self):
        """
        :return: Returns the table name
        """
        return str(self.table_name)

    def exec_stmt(self, query):
        """
        :param query: SQL query
        :return: False if anything gone wrong. True if there are rows changed and a resultset
        if the query returned data as result of a selection.
        """
        action = parse_query(query)
        cursor = None
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall() if action in ("SELECT", "CALL") else True
            log.debug("{0} Statement successful. {1} rows affected.".format(action, cursor.rowcount))
        except db.DatabaseError as err:
            log.error("Error-Message: ", err)
            self.connection.rollback()
            cursor.close()
            return False
        self.connection.commit()
        return result

    def get_primary_key_list(self):
        """ Returns a list of key columns from system catalog.
        """
        query = "select b.column_name from information_schema.table_constraints a " \
                "join information_schema.key_column_usage b " \
                "on a.constraint_name = b.constraint_name " \
                "and a.constraint_schema = b.constraint_schema " \
                "and a.constraint_name = b.constraint_name " \
                "where a.constraint_type = 'PRIMARY KEY' " \
                "and b.table_schema='{0}' and b.table_name='{1}' " \
                "order by b.ordinal_position;".format(self.schema, self.table_name)
        result = self.exec_stmt(query)
        self.primary_key_list = list(x[0] for x in result)
        log.debug("Primary Keys:\n{0}".format(self.primary_key_list))
        return self.primary_key_list

    def get_column_list(self):
        """ Returns a list of column names from system catalog.
        """
        query = "select column_name from information_schema.columns where table_schema = '{0}' " \
                "and table_name = '{1}' order by ordinal_position;".format(self.schema, self.table_name)
        result = self.exec_stmt(query)
        self.column_list = list(x[0] for x in result)
        log.debug("Column list:\n{0}".format(self.column_list))
        return self.column_list

    def get_d_col_data_type(self):
        """ Returns a list of data types for all columns in self.column_list.
        """
        query = "select column_name, udt_name, numeric_precision, numeric_scale, character_maximum_length " \
                "from information_schema.columns " \
                "where table_schema = '{0}' and table_name = '{1}' and column_name in ('{2}') " \
                "order by ordinal_position;".format(self.schema, self.table_name, '\',\''.join(self.column_list))
        result = self.exec_stmt(query)
        d1 = {}
        for item in result:
            d1[item[0]] = list(i for i in item[1:])
        self.d_col_data_type = d1
        log.debug("Dictionary Columns with data types:\n{0}".format(self.d_col_data_type))
        return self.d_col_data_type

    def get_avro_schema(self, cursor_description=None):
        """
        MAPPING from PostgreSQL to AVRO:
        numeric, double_precision, real, interval with fractional digits => float
        numeric, double_precision, real, interval without fractional digits => long
        smallint, integer, bigint => long
        Rest => string
        Returns a string that can be parsed as avro scheme by avro.schema.parse
        :param cursor_description:
        :return:
        """
        import fastavro
        l_descriptor = []
        l_avro_columns = []
        log.debug("Cursor Desc: {}".format(cursor_description))
        if cursor_description:
            log.debug("Ja")
            for entry in cursor_description:
                log.debug("Entry: {0}".format(entry))
                if entry[1] in (700, 701, 1700):
                    data_type = "float"
                elif entry[1] in (20, 21, 23):
                    data_type = "long"
                else:
                    data_type = 'string'
                nullable = "YES" if entry[0] not in self.primary_key_list else "NO"
                l_descriptor.append((entry[0], data_type, nullable))
        else:
            log.debug("Nein")
            query = "select column_name, case " \
                    "when udt_name in ('numeric', 'float8', 'float4', 'interval') " \
                    "and numeric_scale = 0 then 'long' " \
                    "when udt_name in ('numeric', 'float8', 'float4', 'interval') " \
                    "and numeric_scale > 0 then 'float' " \
                    "when udt_name in ('int2', 'int4', 'int8') then 'long' " \
                    "else 'string' end as avro_data_type, is_nullable " \
                    "from information_schema.columns where table_schema = '{0}' and table_name = '{1}' " \
                    "and column_name in ('{2}') order by ordinal_position;".format(
                     self.schema, self.table_name, '\',\''.join(self.column_list))
            l_descriptor = self.exec_stmt(query)
        log.debug("AVRO Descriptor: {0}".format(l_descriptor))
        d_avro_schema = {"namespace": "aws.postgres.avro.{0}".format(self.schema),
                         "type": "record",
                         "name": "{0}".format(self.table_name),
                         "fields": []}
        for item in l_descriptor:
            d_field_entry = {}
            """ Check nullable columns """
            if item[2] == 'YES':
                d_field_entry["name"] = "{0}".format(item[0])
                d_field_entry["type"] = ["{0}".format(item[1]), "null"]
            elif item[2] == 'NO':
                d_field_entry["name"] = "{0}".format(item[0])
                d_field_entry["type"] = "{0}".format(item[1])
            l_avro_columns.append(d_field_entry.copy())
        d_avro_schema["fields"] = l_avro_columns
        self.avro_schema = fastavro.parse_schema(d_avro_schema)
        log.debug("AVRO Schema:\n{0}".format(self.avro_schema))
        return self.avro_schema

    def analyze_table_over_pk(self, vacuum_table=False):
        """
            Analyze the current table object and vacuum empty leafes if selected.
        """
        self.connection = get_connection(self.connection_key)
        if vacuum_table is True:
            log.debug("Isolation Level: {0}".format(self.connection.isolation_level))
            self.connection.set_isolation_level(0)
            query = "VACUUM ANALYZE {0}.{1} ({2});".format(
                self.schema, self.table_name, ", ".join(self.primary_key_list))
            result = self.exec_stmt(query)
            self.connection.set_isolation_level(1)
        else:
            query = "ANALYZE {0}.{1} ({2});".format(self.schema, self.table_name, ", ".join(self.primary_key_list))
            result = self.exec_stmt(query)
        log.info("Table {0} successfully analyzed.".format(self.table_name)) if result is True else None
        self.connection.close()
        return True

    def truncate_table(self):
        """
        :return: True if Truncate was successful
        """
        self.connection = get_connection(self.connection_key)
        query = "Truncate table {0}.{1};".format(self.schema, self.table_name)
        result = self.exec_stmt(query)
        log.info("Table {0}.{1} truncated successfully.".format(
            self.schema, self.table_name)) if result is True else None
        self.connection.close()
        log.debug("Truncate: {0}".format(result))
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
                cursor.execute(stmt)
                header = list(x[0] for x in cursor.description)
                raw_data = cursor.fetchall()
                return pd.DataFrame(raw_data, columns=header)
        except db.DatabaseError as err:
            err, = err.args
            log.error("Postgres Error: {0}".format(err.code))
            log.error("Postgres Error Message: {0}".format(err.message))
            self.connection.rollback()
            return False

    def csv_export(self, directory, filename, timeoption=False, delimiter="|", headline=True, custom_query=None,
                   encoding="utf8", quoting=False, bom=False, write_log=True):
        """
        Export defined table into a file. Columns may be specified when table instance is constructed
        with a column_list. If no filename is provided, filename is the same as table name.
        :param directory: Location of file in filesystem
        :param filename: Name of file
        :param timeoption: Adds timestamp to filename if True
        :param delimiter: Field separator in file
        :param headline: Adds column names to first row if True
        :param custom_query: Overrides selection of table columns
        :param encoding: Sets file encoding (default: utf8
        :param quoting: Sets double quotes to fields
        :param bom: Sets Byte order mark to file (utf-16 and above)
        :param write_log: Creates logfile with MD5 hash of file
        :return: Returns True if successful, else False
        """

        import csv
        cursor = None

        if timeoption:
            filename = "{0}_{1}".format(time.strftime("%Y%m%d%H%M%S"), filename)

        if custom_query:
            query = custom_query
            log.debug("Custom query selected:\n{0}".format(query))
        else:
            query = "select \"{0}\" from {1}.{2}".format('","'.join(self.column_list), self.schema, self.table_name)

        try:
            self.connection = get_connection(self.connection_key)
            query = query.replace(";", "")
            cursor = self.connection.cursor()
            with open("{0}/{1}".format(directory, filename), "w", encoding=encoding) as fh:
                if quoting is True:
                    output = csv.writer(fh, dialect='excel', delimiter=delimiter, quoting=csv.QUOTE_NONNUMERIC,
                                        quotechar='"')
                else:
                    output = csv.writer(fh, dialect='excel', delimiter=delimiter, quoting=csv.QUOTE_NONE)
                cursor.execute(query)
                if bom is True:
                    fh.write('\ufeff')
                if headline is True:
                    cols = list(x[0] for x in cursor.description)
                    output.writerow(cols)
                for row_data in cursor:
                    output.writerow(row_data)
            log.info("Selection exported to {0}/{1} successfully".format(directory, filename))
        except db.DatabaseError as err:
            log.error("Error-Message: ", err)
            self.connection.close()
            return False
        finally:
            cursor.close()
            self.connection.close()
        if write_log is True:
            with open("{0}/{1}.log".format(directory, filename), "w", encoding=encoding) as fh:
                md5sum = md5("{0}/{1}".format(directory, filename))
                fh.write("{0} {1}".format(md5sum, filename))
        return True

    def excel_export(self, directory, filename, custom_query=None, encoding="utf8", write_log=True):
        """
        Export table content to Excel file.
        :param directory: Location of file in filesystem
        :param filename: Name of file
        :param custom_query: Overrides selection of table columns
        :param encoding: Sets file encoding (default: utf8
        :param write_log: Creates logfile with MD5 hash of file
        :return: Returns True if successful, else False
        """
        import pandas
        if custom_query:
            query = custom_query
            log.debug("Custom query selected:\n{0}".format(query))
        else:
            query = "select \"{0}\" from {1}.{2}".format("\",\"".join(self.column_list), self.schema, self.table_name)
        query = query.replace(";", "")
        try:
            self.connection = get_connection(self.connection_key)
            data = pandas.read_sql(query, con=self.connection)
            with pandas.ExcelWriter("{0}/{1}".format(directory, filename), engine="xlsxwriter") as writer:
                data.to_excel(writer, sheet_name='Exported Query', index=False)
            log.info("Selection exported to {0}/{1} successfully".format(directory, filename))
        except Exception as err:
            log.error("Error-Message: ", err)
            self.connection.close()
            return False
        finally:
            self.connection.close()
        if write_log is True:
            with open("{0}/{1}.log".format(directory, filename), "w", encoding=encoding) as fh:
                md5sum = md5("{0}/{1}".format(directory, filename))
                fh.write("{0} {1}".format(md5sum, filename))
        return True

    def avro_export(self, directory, filename, custom_query=None, numrows=100000, encoding="utf8", write_log=True):
        """
        Export table content or defined resultset into an Avro binary file.
        Columns may be specified when table instance is constructed
        with a column_list. If no filename is provided, filename is the same as table name with suffix ".avro".
        :param directory: Location of file in filesystem
        :param filename: Name of file
        :param custom_query: Overrides selection of table columns
        :param numrows: Chunk size / number of rows
        :param encoding: Sets file encoding (default: utf8
        :param write_log: Creates logfile with MD5 hash of file
        :return: Returns True if successful, else False
        """
        cursor = None
        import fastavro
        if custom_query:
            log.warning("Selection with custom query selected. This may not be safe. Date and timestamp "
                        "columns have to be converted to strings manually.")
            query = custom_query
            log.debug("Custom query selected:\n{0}".format(query))
            lookup = custom_query.replace(";", "") + "  fetch first 1 rows only;"
            try:
                self.connection = get_connection(self.connection_key)
                log.debug("Query: {0}".format(lookup))
                cursor = self.connection.cursor()
                cursor.execute(lookup)
                first_row = cursor.fetchall()
                log.debug("First Row: {}".format(first_row))
                log.debug("Cursor Description: {0}".format(cursor.description))
                self.column_list = list(x[0] for x in cursor.description)
                log.debug("Columnns: {0}".format(self.column_list))
                self.get_avro_schema(cursor_description=cursor.description)
            except db.DatabaseError as err:
                log.error("Error-Message: ", err)
                self.connection.close()
                return False
            finally:
                cursor.close()
                self.connection.close()
        else:
            log.debug("Whole table selected.")
            query_cols = list('"' + x + '"' for x in self.column_list)
            for i, v in enumerate(self.column_list):
                if self.d_col_data_type.get(v)[0] == "date":
                    query_cols[i] = "to_char(\"{0}\", 'YYYY-MM-DD')".format(self.column_list[i])
                elif "timestamp" in self.d_col_data_type.get(v)[0]:
                    query_cols[i] = "to_char(\"{0}\", 'YYYY-MM-DD HH24:MI:SS')".format(self.column_list[i])
            query = "select {0} from {1}.{2}".format(",".join(query_cols), self.schema, self.table_name)
            log.debug("Modified query: {0}".format(query))
        try:
            self.connection = get_connection(self.connection_key)
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
                    fastavro.writer(f, self.avro_schema, l_records, codec='deflate', validator=True)
                    l_records = []
            log.info("Selection exported to {0}/{1} successfully".format(directory, filename))
        except db.DatabaseError as err:
            log.error("Error-Message: ", err)
            self.connection.close()
            return False
        finally:
            cursor.close()
            self.connection.close()
        if write_log is True:
            with open("{0}/{1}.log".format(directory, filename), "w", encoding=encoding) as fh:
                md5sum = md5("{0}/{1}".format(directory, filename))
                fh.write("{0} {1}".format(md5sum, filename))
        return True

    def csv_import(self, directory, filename, truncate=True, delimiter="|", quoting_string='"', headline=True,
                   characterset="utf8"):
        """
        Copy csv file into table from remote client via psql command as subprocess
        :param directory: Location of file in filesystem
        :param filename: Name of file
        :param truncate: If True, table will be truncated before import
        :param delimiter: Column separator, default is '|'
        :param quoting_string: Quote character in csv file, default is '"'
        :param headline: True, if headline exists with column names, False if not
        :param characterset: Character encoding in file, default is utf8
        :return:
        """
        self.truncate_table() if truncate is True else None
        option_header = "CSV HEADER" if headline is True else ""
        stmt = "\\copy {0}.{1} FROM {2}/{3} DELIMITER '{4}' {5} ENCODING '{6}' QUOTE '{7}'".format(
            self.schema, self.table_name, directory, filename,
            delimiter, option_header, characterset, quoting_string)
        log.debug("Statement: {0}".format(stmt))
        try:
            cred = seclib.get_credentials(self.connection_key)
            import subprocess
            cmd = ['psql',
                   '--host={}'.format(cred[0]),
                   '--username={}'.format(cred[2]),
                   '--dbname={}'.format(cred[1]),
                   '--command={}'.format(stmt)]
            log.debug("Command: {0}".format(cmd))
            stmt = subprocess.run(cmd, capture_output=True)
            log.debug("Result: {0}".format(stmt))
            log.info("File {0}/{1} imported into table {2} successfully.".format(
                directory, filename, self.table_name))
        except OSError as err:
            log.error("Error-Message: ", err)
            return False

    def dataframe_import(self, df, action=None, commit_rate=5000):
        """ Imports DataFrame into table object. Fails if table does not contain all columns from DataFrame.
        @param df: Name of Pandas DataFrame
        @param action: Action REPLACE or None
        @param commit_rate: Number of written rows before a connection commit occurs
        @return: Number of inserted rows or False if method fails.
        """
        import pandas as pd
        self.connection = get_connection(self.connection_key)
        df_cols = list(df.columns)
        missing_cols = list(x for x in df_cols if x not in self.column_list)
        if len(missing_cols) > 0:
            log.error("One or more columns in DataFrame don't fit in table. {0}".format(
                ", ".join(missing_cols)))
            return False

        """ Replace action """
        if action == "REPLACE":
            self.truncate_table()

        """ Convert timestamps and dates to strings """
        columns = []
        for index, col in enumerate(self.column_list):
            data_type = df[col].dtypes
            """ Date and Timestamp conversion to string and SQL conversion back to date, timestamp. """
            if "TIMESTAMP" in self.d_col_data_type[col][0]:
                columns.append("to_timestamp(:{0}, 'YYYY-MM-DD HH24:MI:SS.FF')".format(index + 1))
                if data_type == "datetime64[ns]":
                    df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                    df[col].fillna(value='', inplace=True)
                elif data_type == "object":
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                    df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                    df[col].fillna(value='9999-12-31 00:00:00.000000', inplace=True)
            elif self.d_col_data_type[col][0] == "DATE":
                columns.append("to_date(:{0}, 'YYYY-MM-DD HH24:MI:SS')".format(index + 1))
                if data_type == "datetime64[ns]":
                    df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
                    df[col].fillna(value='', inplace=True)
                elif data_type == "object":
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                    df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
                    df[col].fillna(value='9999-12-31 00:00:00', inplace=True)
            else:
                columns.append(":{0}".format(index + 1))

        """ Behandlung von Nullwerten"""
        df.fillna('', axis=1, inplace=True)

        """ Generate Insert SQL"""
        query = "INSERT INTO {0}.{1} ({2}) VALUES ({3})".format(self.schema, self.table_name,
                                                                ", ".join(self.column_list), ", ".join(columns))
        log.debug("Insert Statement: {0}".format(query))
        data = list(df.itertuples(index=False, name=None))
        del df
        rows_affected = 0
        connection = self.connection
        c1 = connection.cursor()
        """ Set commit point every n rows """
        i = 0
        c = commit_rate
        while i < len(data):
            try:
                c1.executemany("""{0}""".format(query), data[i:i + c])
                rows_affected += c1.rowcount
                log.debug("{0} rows written.".format(rows_affected))
                connection.commit()
                i = i + c
            except db.DatabaseError as err:
                err, = err.args
                log.error("HANA-Error-Code: {0}".format(err))
                connection.rollback()
        connection.close()
        log.info("{0} rows written.".format(rows_affected))
        return rows_affected

    def get_chunksize(self, mem=2147483648):
        """
            mem: Max memory usage per single process (Standard: 2 GB)
        """
        query = "select sum(length) as max_rowsize from information_schema.columns " \
                "where table_schema = '{0}' and table_name = '{1}' " \
                "and column_name in ('{2}')".format(self.schema, self.table_name, "', '".join(self.column_list))
        result = sql_query(self.connection_key, query)
        if result is False:
            return result
        else:
            max_rowsize = result[0][0]
            return int(mem / max_rowsize)
