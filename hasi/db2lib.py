# -*- coding: utf8 -*-
###############################################################################
#                                  db2lib.py                                  #
###############################################################################
# Copyright (C) 2023  Jens Janzen

# Change history
# V1.0: 2020-07-02 - Initial Release

# Description:
# Library contains helper functions for DB2 database access

import logging
import os
import time
import ibm_db_dbi as db
import seclib

try:
    host = os.environ["HOST"]
except KeyError as e:
    host = "Unknown"
standard_encoding = 'utf8'
os.environ['DB2OPTIONS'] = '-x'

""" Sets logging console handler for debugging """
db2log = logging.getLogger("db2lib")
db2log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
db2log.addHandler(console_handler)


def get_connection(connection_key):
    cred = seclib.get_credentials(connection_key)
    if cred is False:
        return False
    hostname = cred[0].split(":")[0]
    portnumber = cred[0].split(":")[1]
    try:
        con = db.connect("DATABASE=cred[1];HOSTNAME=hostname;PORT=portnumber;"
                         "PROTOCOL=TCPIP;UID=cred[2];PWD=cred[3];'';''")
        db2log.debug("Connected to {0}, version {1} successful.".format(con.dsn, con.version))
    except db.Error as err:
        db2log.error("DB2-Error-Code: {0}".format(err))
        db2log.error("Key: {0}".format(connection_key))
        raise
    return con


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
            db2log.debug(action)
            # Execute given queries
            try:
                cursor = con.cursor()
                db2log.debug("Execute query: {0}".format(query))
                obj_exec = cursor.execute(query)
                db2log.debug(obj_exec)
                if obj_exec:
                    selection.extend(obj_exec.fetchall())
                rowcount = cursor.rowcount
                if action in ("MERGE", "INSERT", "UPDATE", "DELETE"):
                    db2log.debug("{0} Statement successful. {1} rows affected.".format(action, rowcount))
                db2log.debug("Resultset: {0}".format(selection))
            except db.Error as exc:
                err, = exc.args
                db2log.error("Oracle-Error-Code: {0}".format(err.code))
                db2log.error("Oracle-Error-Message: {0}".format(err.message))
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
        db2log.error("Could not open DB connection: {0}".format(con))
        return False


def parse_query(query):
    import re
    for literal in reversed(sorted(re.findall("'([^']*)'", query), key=len)):
        query = query.replace(literal, "")
    db2log.debug("Parse query: {0}".format(query))
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


def result_list(result):
    """ Convert a database resultset into a data structure of list type.
    Each element of the list holds one column value from the selection.
    Use to format a resultset from a SQL query for evaluation.
    """
    if result is False:
        return False
    else:
        result = result.replace('\n', '|')
        result = re.split(r'\s*[|\n\s]\s*', result)
        return result


# Database Classes
class Table(object):
    """ Klasse für eine DB2 Tabelle.
    Enthaltene Attribute:
    __scheme - Schema
    __name - Tabellen- oder Viewname
    __primary_key - Eindeutiger Schlüssel auf Tabelle
    __column_list - Liste der Datenbankfelder
    __data_type_list - Liste der Datentypen der Spalten
    __column_size_list - Liste mit Feldlängen
    
    Methoden:
    get_scheme - Prüft den Klassenparameter scheme auf ein gültiges Datenbankschema.
    get_name - Prüft den Klassenparameter name auf einen gültigen Tabellennamen.
    get_primary_key - Gibt eine Liste mit dem Primärschlüssel, bzw. dem ersten unique index zurück
    get_column_list - Gibt eine Liste mit Spalten der Tabelle zurück
    get_data_type_list - Gibt eine Liste mit den Datentypen der Spalten zurück
    get_column_size_list - Gibt eine Liste mit Spaltenlängen zurück.
    generate_statistics - Erstellt Statistiken auf der Tabelle (RUNSTATS)
    groom_table - Löscht unbelegten, der Tabelle zugewiesenen Speicherplatz, sofern der Benutzer Rechte hat (REORG).
                  Während der Aktion kann nur lesend auf die Tabelle zugegriffen werden.
    truncate_table - Leert die Tabelle
    csv_export(query) - Exportiert das Ergebnis der Abfrage in eine Datei im aktuellen Verzeichnis,
    welche den Namen der Tabelle trägt.
    cleanup_status - Setzt die Tabelle wieder in den regulären Zugriff im Falle von folgenden Situationen:
                  Copy pending, Load pending
    """

    def __init__(self, scheme, name, primary_key=None, column_list=None, data_type_list=None,
                 column_size_list=None):
        """ Creates an instance for a single db2 table or view.
        Needs scheme and name as argument.
        """
        self.scheme = self.get_scheme(scheme)
        self.name = self.get_name(name)
        self.__primary_key = primary_key
        self.__column_list = column_list
        self.__data_type_list = data_type_list
        self.__column_size_list = column_size_list

    @property
    def __str__(self):
        return str(self.name)

    @staticmethod
    def get_scheme(scheme):
        """ Returns database scheme.
        """
        query = "select count(*) from sysibm.sysschemata where name = '{}';".format(scheme)
        result = sql_query(query)
        if result == '1':
            return scheme
        else:
            print("The specified scheme {} was not found in sysibm.sysschemata.".format(scheme))
            return False

    def get_name(self, name):
        """ Returns table name.
        """
        if self.scheme is False:
            return False
        query = "select count(*) from sysibm.systables where creator = '{}' and name = '{}';".format(self.scheme, name)
        result = sql_query(query)
        if result == '1':
            return name
        else:
            print("The specified table {} was not found in scheme {}.".format(name, self.scheme))
            return False

    @property
    def get_primary_key(self):
        """ Returns a list of key columns from system table.
        """
        query = "select colnames from sysibm.sysindexes where creator='{}'and tbname='{}' " \
                "and uniquerule in ('P','U') order by uniquerule, name " \
                "fetch first 1 rows only;".format(self.scheme, self.name)
        result = sql_query(query)
        result = result.strip('+')
        self.__primary_key = re.split(r'\s*[|\n\s]\s*', result)
        return self.__primary_key

    @property
    def get_column_list(self):
        """ Returns a list of column names from system table.
        """
        query = "select name from sysibm.syscolumns where tbcreator='{}' and tbname = '{}' " \
                "order by colno;".format(self.scheme, self.name)
        result = sql_query(query)
        self.__column_list = re.split(r'\s*[|\n\s]\s*', result)
        return self.__column_list

    @property
    def get_data_type_list(self):
        """ Returns a list of data type names from system table.
        """
        query = "select coltype from sysibm.syscolumns where tbcreator='{}' and tbname = '{}' " \
                "order by colno;".format(self.scheme, self.name)
        result = sql_query(query)
        self.__data_type_list = re.split(r'\s*[|\n\s]\s*', result)
        return self.__data_type_list

    @property
    def get_column_size_list(self):
        """ Returns a list of column sizes from system table.
        """
        query = "select length from sysibm.syscolumns where tbcreator='{}' and tbname = '{}' " \
                "order by colno;".format(self.scheme, self.name)
        result = sql_query(query)
        self.__column_size_list = re.split(r'\s*[|\n\s]\s*', result)
        return self.__column_size_list

    def generate_statistics(self):
        """ Generate statistics on table.
        """
        query = "runstats on table {}.{} with distribution and indexes all" \
                " shrlevel change;".format(self.scheme, self.name)
        result = sql_query(query)
        return result

    def groom_table(self):
        """ Reorganize table and release unused memory. Needs DBA permissions for the user to work.
        """
        query = "reorg table {}.{} allow read access;".format(self.scheme, self.name)
        result = sql_query(query)
        return result

    def truncate_table(self):
        """ Truncate table.
        """
        query = "truncate table {}.{} immediate;".format(self.scheme, self.name)
        result = sql_query(query)
        return result

    def export_table(self, file_type='ixf'):
        """ Export table into a file. Need a name and the selection query as arguments.
        Query must end with a termination sign ';'
        """
        query = None
        if file_type == 'ixf':
            query = "export to ./{0}.{1}.{2} of {2} select * from {0}.{1}".format(self.scheme, self.name, file_type)
        elif file_type == 'del':
            query = "export to ./{0}.{1}.{2} of {2} modified by coldel| timestampformat=\"YYYY-MM-DD HH:MM:SS\" " \
                    "select * from {0}.{1}".format(self.scheme, self.name, file_type)
        elif file_type == 'postgresql':
            v_file_type = 'del'
            self.__column_list = self.get_column_list
            self.__data_type_list = self.get_data_type_list
            for column, data_type in zip(self.__column_list, self.__data_type_list):
                if data_type == "TIME":
                    self.__column_list = list(x.replace(column, "char(" + column + ", JIS)")
                                              for x in self.__column_list)
            s_selection = ','.join(self.__column_list)
            query = "export to ./{0}.{1}.{2} of {2} modified by coldel| timestampformat=\"YYYY-MM-DD HH:MM:SS\" " \
                    "select {3} from {0}.{1}".format(self.scheme, self.name, v_file_type, s_selection)
        print(query)
        result = sql_query(query)
        return result

    def cleanup_status(self):
        """ Examine unaccessible status of table and try to end operation.
        Works with status:
        COPY PENDING - Set integrity command
        LOAD PENDING - Load terminate command
        """
        query = "select status from syscat.tables where tabschema='{}' " \
                "and tabname='{}' with ur;".format(self.scheme, self.name)
        result = sql_query(query)
        if result == 'C':
            query = "set integrity for {}.{} immediate checked;"
            result = sql_query(query)
            return result
        elif result == 'L':
            query = "load from /dev/null of del into {}.{} terminate nonrecoverable;"
            result = sql_query(query)
            return result
        elif result == 'N':
            return True
