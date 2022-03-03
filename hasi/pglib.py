# -*- coding: utf8 -*-
###############################################################################
#                                   pglib.py                                  #
###############################################################################
# Autor: Jens Janzen

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
log = logging.getLogger("pglib")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)


def connect(connection_key):
    """
    Returns a PostgreSQL database connection.
    :param connection_key:
    :return: database connection
    """
    cred = seclib.get_credentials(connection_key)
    if cred is False:
        return False
    hostname = cred[0]
    dbname = cred[1]
    username = cred[2]
    pw = cred[3]
    try:
        connection_string = "dbname='{1}' user='{2}' password='{3}' host='{0}'".format(
            hostname, dbname, username, pw)
        con = db.connect(connection_string)
        log.debug("Connected to {0} with user {1}.".format(cred[1], cred[2]))
    except db.Error as err:
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

    con = connect(connection_key)
    cursor = ""
    selection = []

    for query in l_queries:
        action = parse_query(query)
        try:
            cursor = con.cursor()
            log.debug("Execute query: {0}".format(query))
            cursor.execute(query)
            selection.extend(cursor.fetchall()) if action == "Select" else None
            log.info("{0} Statement successful. {1} rows affected.".format(action, cursor.rowcount))
            log.debug("Resultset: {0}".format(selection))
        except db.Error as err:
            log.error("Error-Message: ", err)
            con.rollback()
            cursor.close()
            con.close()
            return False
    con.commit()
    con.close()
    return selection


def parse_query(query):
    """
    Parses a SQL query to determine kind of sql action.
    :param query: SQL query
    :return: String with action
    """
    if "create" in query.lower():
        return "Create"
    elif "drop" in query.lower():
        return "Drop"
    elif "alter" in query.lower():
        return "Alter"
    elif "merge" in query.lower():
        return "Merge"
    elif "insert" in query.lower():
        return "Insert"
    elif "update" in query.lower():
        return "Update"
    elif "delete" in query.lower():
        return "Delete"
    elif "select" in query.lower():
        return "Select"
    elif "call" in query.lower():
        return "Call"
    else:
        return query.split(" ", 1)[0]


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
            self.connection = connect(connection_key)
        except db.Error as err:
            log.error("Error-Message: ", err)
        self.schema = schema if schema else None
        self.table_name = table_name
        self.column_list = self.get_column_list if column_list is None else column_list.upper()
        if self.column_list:
            self.primary_key_list = self.get_primary_key_list if primary_key_list is None else primary_key_list.upper()
            self.d_col_data_type = self.get_d_col_data_type if d_col_data_type is None else d_col_data_type
            self.avro_schema = self.get_avro_schema()
        self.connection.close()

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
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall() if action in ("Select", "Call") else True
            log.debug("{0} Statement successful. {1} rows affected.".format(action, cursor.rowcount))
        except db.Error as err:
            log.error("Error-Message: ", err)
            self.connection.rollback()
            cursor.close()
            return False
        self.connection.commit()
        return result

    @property
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

    @property
    def get_column_list(self):
        """ Returns a list of column names from system catalog.
        """
        query = "select column_name from information_schema.columns where table_schema = '{0}' " \
                "and table_name = '{1}' order by ordinal_position;".format(self.schema, self.table_name)
        result = self.exec_stmt(query)
        self.column_list = list(x[0] for x in result)
        log.debug("Column list:\n{0}".format(self.column_list))
        return self.column_list

    @property
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
        self.connection = connect(self.connection_key)
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
        self.connection = connect(self.connection_key)
        query = "Truncate table {0}.{1};".format(self.schema, self.table_name)
        result = self.exec_stmt(query)
        log.info("Table {0}.{1} truncated successfully.".format(
            self.schema, self.table_name)) if result is True else None
        self.connection.close()
        log.debug("Truncate: {0}".format(result))
        return True

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
            self.connection = connect(self.connection_key)
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
        except db.Error as err:
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
            self.connection = connect(self.connection_key)
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

    def avro_export(self, directory, filename, custom_query=None, numRows=100000, encoding="utf8", write_log=True):
        """
        Export table content or defined resultset into an Avro binary file.
        Columns may be specified when table instance is constructed
        with a column_list. If no filename is provided, filename is the same as table name with suffix ".avro".
        :param directory: Location of file in filesystem
        :param filename: Name of file
        :param custom_query: Overrides selection of table columns
        :param numRows: Chunk size / number of rows
        :param encoding: Sets file encoding (default: utf8
        :param write_log: Creates logfile with MD5 hash of file
        :return: Returns True if successful, else False
        """
        import fastavro
        if custom_query:
            log.warning("Selection with custom query selected. This may not be safe. Date and timestamp "
                        "columns have to be converted to strings manually.")
            query = custom_query
            log.debug("Custom query selected:\n{0}".format(query))
            lookup = custom_query.replace(";", "") + "  fetch first 1 rows only;"
            try:
                self.connection = connect(self.connection_key)
                log.debug("Query: {0}".format(lookup))
                cursor = self.connection.cursor()
                cursor.execute(lookup)
                first_row = cursor.fetchall()
                log.debug("First Row: {}".format(first_row))
                log.debug("Cursor Description: {0}".format(cursor.description))
                self.column_list = list(x[0] for x in cursor.description)
                log.debug("Columnns: {0}".format(self.column_list))
                self.get_avro_schema(cursor_description=cursor.description)
            except db.Error as err:
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
            self.connection = connect(self.connection_key)
            cursor = self.connection.cursor()
            with open("{0}/{1}".format(directory, filename), "wb") as f:
                f.close()
            with open("{0}/{1}".format(directory, filename), "a+b") as f:
                l_records = []
                cursor.execute(query)
                while True:
                    rows = cursor.fetchmany(numRows)
                    if not rows:
                        break
                    for line in rows:
                        record = dict(zip(self.column_list, line))
                        l_records.append(record)
                    fastavro.writer(f, self.avro_schema, l_records, codec='deflate', validator=True)
                    l_records = []
            log.info("Selection exported to {0}/{1} successfully".format(directory, filename))
        except db.Error as err:
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

    def import_csv(self, directory, filename, truncate=True, delimiter="|", quoting_string='"', headline=True,
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
