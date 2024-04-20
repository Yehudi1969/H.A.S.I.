# -*- coding: utf8 -*-
###############################################################################
#                                  nzlib.py                                   #
###############################################################################
# Copyright (C) 2023  Jens Janzen
# Datum: 2019-04-10

# Description:
# Library contains helper functions for Netezza database access
import os
import re
import shlex
import subprocess
import time

standard_encoding = 'utf8'


# Helper functions
def nzsql_query(credentials, query, filename=None):
    """ Execute a SQL Query. Uses Popen to communicate with nzsql client.
    Needs a list with credentials and the SQL query.
    """
    if len(credentials) != 4:
        print("Number of credentials is wrong.")
        return False
    host = credentials[0]
    db = credentials[1]
    dbu = credentials[2]
    dbpw = credentials[3]
    query_file = "{}.{}.tmpquery".format(os.getpid(), os.path.basename(__file__))

    """ Write query to file """
    with open(query_file, 'w') as f_query:
        f_query.write(query)

    """ Check whether query ends with proper termination character """
    if not query.endswith(';'):
        query += ';'

    """ Execute given query """
    if filename:
        command = "nzsql -host {0} -db {1} -u {2} -pw {3} -t -f \"{4}\" -o {5}".format(
            host, db, dbu, dbpw, query_file, filename)
    else:
        command = "nzsql -host {0} -db {1} -u {2} -pw {3} -t -f \"{4}\"".format(host, db, dbu, dbpw, query_file)
    l_command = shlex.split(command)
    stmt = subprocess.Popen(l_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            close_fds=True)
    try:
        output, error = stmt.communicate()
    except OSError as e:
        print(e)
        return False

    """ Remove query file """
    os.remove(query_file) if os.path.exists(query_file) else None

    """ Error handling """
    result = output.decode(standard_encoding).strip()
    if stmt.returncode != 0:
        print(str(error.decode(standard_encoding).strip()))
        return False
    elif stmt.returncode == 0 and len(result) == 0 and filename is None:
        print("Warning: Query returned zero rows.\n{}".format(query))
    return result


def nz_procedure(credentials, query):
    """ Call a procedure
    """
    if len(credentials) != 4:
        print("Number of credentials is wrong.")
        return False
    host = credentials[0]
    db = credentials[1]
    dbu = credentials[2]
    dbpw = credentials[3]

    """ Check whether query ends with proper termination character """
    if not query.endswith(';'):
        query += ';'

    """ Execute given query """
    command = "nzsql -host {0} -db {1} -u {2} -pw {3} -t -c \"{4}\"".format(host, db, dbu, dbpw, query)
    l_command = shlex.split(command)
    stmt = subprocess.Popen(l_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            close_fds=True)
    try:
        output, error = stmt.communicate()
    except OSError as e:
        print(e)
        return False

    """ Error handling """
    result = error.decode(standard_encoding).strip()
    if stmt.returncode != 0:
        return 'False ' + result
    else:
        return result


def nzsession_abort_session(credentials, cmd, sess_nr):
    """ Execute an nzsession command. """
    if len(credentials) != 4:
        print("Number of credentials is wrong.")
        return False
    host = credentials[0]
    dbu = credentials[2]
    dbpw = credentials[3]

    command = "nzsession {0} -host {1} -u {2} -pw {3} -id {4} -force".format(cmd, host, dbu, dbpw, sess_nr)
    l_command = shlex.split(command)
    stmt = subprocess.Popen(l_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            close_fds=True)
    try:
        output, error = stmt.communicate()
    except OSError as e:
        print(e)
        return False
    """ Error handling """
    result = output.decode(standard_encoding).strip()
    if stmt.returncode != 0 and len(result) > 0:
        print(result)
        return False
    else:
        print("Session {} aborted successfully.".format(sess_nr))
        return result


def nz_export_to_file(credentials, query, filename, delimiter="|", column_list=None):
    """
    Execute a SQL Query. Uses Popen to communicate with nzsql client.
    Should be replaced when Python Netezza interface is available.
    Needs a list with credentials and the SQL query.
    """
    if len(credentials) != 4:
        print("Number of credentials is wrong.")
        return False
    host = credentials[0]
    db = credentials[1]
    dbu = credentials[2]
    dbpw = credentials[3]
    query_file = "{}.{}.tmpquery".format(os.getpid(), os.path.basename(__file__))

    """ Write query to file """
    with open(query_file, 'w') as f_query:
        f_query.write(query)

    """ Check whether query ends with proper termination character """
    if not query.endswith(';'):
        query += ';'

    """ Execute given query """
    command = "nzsql -host {0} -db {1} -u {2} -pw {3} -t -f \"{4}\" -F \"{5}\" -A -o {6}".format(
        host, db, dbu, dbpw, query_file, delimiter, filename)
    l_command = shlex.split(command)
    stmt = subprocess.Popen(l_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            close_fds=True)
    try:
        output, error = stmt.communicate()
    except OSError as e:
        print(e)
        return False

    # Add header to csv file
    if column_list:
        os.rename(filename, "{0}.body".format(filename))
        with open(filename, "wb") as fh_write:
            header = "{0}".format(delimiter).join(column_list)
            fh_write.write(bytes(header + "\n", standard_encoding))
        with open(filename, "ab") as fh_write:
            with open("{0}.body".format(filename), "rb") as fh_read:
                for line in fh_read.readlines():
                    fh_write.write(line)
        os.remove("{0}.body".format(filename)) if os.path.exists("{0}.body".format(filename)) else None

    """ Remove query file """
    os.remove(query_file) if os.path.exists(query_file) else None

    """ Error handling """
    result = output.decode(standard_encoding).strip()
    if stmt.returncode != 0:
        result = "Statement failed." + str(error)
    return result


def jdbc_query(credentials, query):
    """ Execute a SQL Query. Uses jaydebeapi and native python database api.
        Needs a list with credentials and the SQL query.
        """
    import jaydebeapi
    conn = None
    driver = "org.netezza.Driver"
    if len(credentials) != 4:
        print("Number of credentials is wrong.")
        return False
    url = "jdbc:netezza://{0}:5480/{1};user={2};" \
          "password={3};securityLevel=preferredSecured;loglevel=1;logdirpath=.".format(
           credentials[0], credentials[1], credentials[2], credentials[3])
    try:
        path = "{0}/nzjdbc.jar".format(os.environ["JDBC_HOME"])
    except KeyError:
        path = "{0}/jdbc/nzjdbc.jar".format(os.environ["PYTHONPATH"])
    try:
        print("Connection String: jaydebeapi.connect({0}, {1}, {2}".format(
            driver, url, path))
        conn = jaydebeapi.connect(driver, url, path, )
        cursor = conn.cursor()
        cursor.execute(query)
        while True:
            result = cursor.fetchall()
            if result is None:
                break
            print(result)
        cursor.close()
        conn.close()
        return result
    except jaydebeapi.Error as e:
        print(e)
        conn.close()
        return False


def check_col_values(data_type, value):
    """ Check and convert common data types from SAP to Netezza.
    Needs a data type and the value of the column as arguments.
    """
    if data_type == '16' and value.lower() == 'true':  # Netezza boolean
        value = '1'
    elif data_type == '16' and value.lower() == 'false':  # Netezza boolean
        value = '0'
    elif data_type in ('18', '1042', '1043'):  # Netezza character types and varchar
        value = value.replace('|', '')
        # .replace('\'', '').replace('%', '')
    elif data_type in ('20', '21', '23'):  # Netezza integer types
        if len(value) > 0:
            if value[-1] == '-':
                value = "-{}".format(value[:-1])  # adjust signed fields if sign is a suffix
            value = int(value)  # Convert to integer
    elif data_type in '1700':  # Netezza numeric type
        if len(value) > 0:
            value = value.replace(' ', '').replace(',', '.')  # Replace comma with decimal point and trim blanks
            if value[-1] == '-':
                value = "-{}".format(value[:-1])  # adjust signed fields if sign is a suffix
            if 'E' in value or 'e' in value:
                value = str(float(value))  # Try to convert exponential value
            if 'e' in value:
                index = value.index('e')
                value = value[0:index]  # If value is still inconvertible, set it to all numbers before e
        elif len(value) == 0:
            value = 0.00
    elif data_type in '1082':  # Netezza date
        if len(value.rstrip()) < 8:
            return ""
        else:
            value = convert_datetime(value)
    elif data_type == '1083':  # Netezza time
        if len(value.rstrip()) != 6:
            return ""
        else:
            value = convert_datetime(value)
    elif data_type == '1184':  # Netezza timestamp
        if len(value.rstrip()) == 0:
            return ""
        if len(value) == 8:
            value += '000000'
        value = convert_datetime(value)  # Map SAP standard value to regular date
    elif data_type == '1186':  # Netezza interval
        pass
    elif data_type == '2500':  # Netezza byte int
        pass
    elif data_type == '2530':  # Netezza national character
        pass
    return value


def check_xls_values(data_type, value, date_format=None, time_format=None, timestamp_format=None):
    import datetime
    """ Check and convert common data types from SAP to Netezza.
    Needs a data type and the value of the column as arguments.
    """
    if data_type == '16' and value.lower() == 'true':  # Netezza boolean
        value = 'true'
    elif data_type == '16' and value.lower() == 'false':  # Netezza boolean
        value = 'false'
    elif data_type in ('18', '1042', '1043'):  # Netezza character types and varchar
        value = value.replace('|', '')
    elif data_type in ('20', '21', '23'):  # Netezza integer types
        if len(value) > 0:
            if value[-1] == '-':
                value = "-{}".format(value[:-1])  # adjust signed fields if sign is a suffix
            value = int(value)  # Convert to integer
    elif data_type in '1700':  # Netezza numeric type
        if len(value) > 0:
            value = value.replace(' ', '').replace(',', '.')  # Replace comma with decimal point and trim blanks
            if value[-1] == '-':
                value = "-{}".format(value[:-1])  # adjust signed fields if sign is a suffix
            if 'E' in value or 'e' in value:
                value = str(float(value))  # Try to convert exponential value
            if 'e' in value:
                index = value.index('e')
                value = value[0:index]  # If value is still inconvertible, set it to all numbers before e
        elif len(value) == 0:
            value = 0.00
    elif data_type in '1082' and date_format:  # Netezza date
        value = datetime.datetime.strptime(value, date_format).strftime("%Y-%m-%d")
        print("No date format passed.") if not date_format else None
    elif data_type == '1083' and time_format:  # Netezza time
        value = datetime.datetime.strptime(value, time_format).strftime("%H:%M:%S")
        print("No time format passed.") if not time_format else None
    elif data_type == '1184' and timestamp_format:  # Netezza timestamp
        value = datetime.datetime.strptime(value, timestamp_format).strftime("%Y-%m-%d %H:%M:%S")
        print("No timestamp format passed.") if not timestamp_format else None
    elif data_type == '1186':  # Netezza interval
        pass
    elif data_type == '2500':  # Netezza byte int
        pass
    elif data_type == '2530':  # Netezza national character
        pass
    return value


def get_default_values(data_type, date_format=None, time_format=None, timestamp_format=None):
    """ Generates default values for the given Netezza data type from system view.
    Needs the data type as argument.
    """
    s_date = str(time.strftime("%Y-%m-%d")) if not date_format else str(time.strftime(date_format))
    if not timestamp_format:
        s_timestamp = str(time.strftime("%Y-%m-%d %H:%M:%S.000000"))
    else:
        s_timestamp = str(time.strftime(timestamp_format))
    s_time = str(time.strftime("%H:%M:%S")) if not time_format else str(time.strftime(time_format))
    s_default = ' '
    i_default = 0
    n_default = 0.0
    value = 0
    if data_type in ('18', '1042', '1043'):  # Netezza character types and varchar
        value = s_default
    elif data_type in ('20', '21', '23'):  # Netezza integer types
        value = i_default
    elif data_type == '1700':
        value = n_default
    elif data_type == '1082':  # Netezza date
        value = s_date
    elif data_type == '1083':  # Netezza time
        value = s_time
    elif data_type == '1184':  # Netezza timestamp
        value = s_timestamp
    elif data_type == '1186':  # Netezza interval
        value = i_default
    elif data_type == '2500':  # Netezza byte int
        value = i_default
    elif data_type == '2530':  # Netezza national character
        value = s_default
    return value


def nzload_file_importer(credentials, query):
    """ Execute load operation into a Netezza table. Uses Popen to communicate with nzload utility.
    Needs filename of the control document as argument.
    """
    if len(credentials) != 4:
        print("Number of credentials is wrong.")
        return False
    host = credentials[0]
    db = credentials[1]
    dbu = credentials[2]
    dbpw = credentials[3]
    query_file = "{}.{}.tmpquery".format(os.getpid(), os.path.basename(__file__))

    """ Write query to control file """
    with open(query_file, 'w') as f_query:
        f_query.write(query)

    """ Execute given query """
    command = "nzload -host {0} -db {1} -u {2} -pw {3} -cf {4}".format(host, db, dbu, dbpw, query_file)
    l_command = shlex.split(command)
    stmt = subprocess.Popen(l_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            close_fds=True)
    print("Database access to NZ {}...".format(db))
    try:
        output, error = stmt.communicate()
    except OSError as e:
        print(e)
        return False

    """ Remove control file """
    os.remove(query_file) if os.path.exists(query_file) else None

    """ Error handling """
    result = output.decode(standard_encoding).strip()
    if stmt.returncode != 0:
        print(result)
        return False

    """ Return resultset """
    return result


def convert_datetime(x):
    """ Parses and converts SAP date/time values that are not useable in Netezza.
    Needs a value as argument.
    Expects default date & time format YYYYMMDDHH24MISSMS - year (4 digits), month (2 digits), day (2 digits),
    hour (2 digits), minute (2 digits), second (2 digits), microseconds (up to 6 digits)
    Other date formats are not supported. You have been warned.
    """
    x = re.sub(r'[^\d.]+', '', x)
    if x[:8] == '00000000':
        x = "19000101{}".format(x[8:])
    if len(x) == 5 and x == "00000":
        return "00:00:00.000000"
    if len(x) == 6:
        return "{}:{}:{}.000000".format(x[0:2], x[2:4], x[4:])
    elif len(x) == 8:
        if x[:4] == '9999':
            return '9999-12-31'
        else:
            return "{}-{}-{}".format(x[:4], x[4:6], x[6:])
    elif len(x) == 14:
        return "{}-{}-{} {}:{}:{}.000000".format(x[:4], x[4:6], x[6:8], x[8:10], x[10:12], x[12:14])
    elif 14 < len(x) < 21:
        return "{}-{}-{} {}:{}:{}.{}".format(x[:4], x[4:6], x[6:8], x[8:10], x[10:12], x[12:14], x[14:])
    elif 21 < len(x) and "." in x:
        return "{}-{}-{} {}:{}:{}{}".format(x[:4], x[4:6], x[6:8], x[8:10], x[10:12], x[12:14], x[14:21])
    else:
        return False


def result_list(result):
    """ Convert a database resultset into a data structure of list type.
    Each element of the list holds one column value from the selection.
    Use to format a resultset from a SQL query for evaluation.
    """
    if result is False:
        return False
    else:
        result = result.replace('\n', '|')
        result = list(filter(None, re.split(r'\s*[|\n\s]\s*', result)))
        return result


# Database classes
class NetezzaTable(object):
    """ Klasse für eine Netezza Tabelle.
    Enthaltene Attribute:
    __host - Name oder IP-Adresse des Datenbankservers (vererbt von NetezzaConnection)
    __dsn - Name der Datenbank (vererbt von NetezzaConnection)
    __user - DB User
    __password - DB Passwort
    scheme - Schema
    name - Tabellen- oder Viewname
    __primary_key - Eindeutiger Schlüssel der Tabelle
    __business_key - Operativer Schlüssel der Tabelle
    __column_list - Liste der Datenbankfelder
    __data_type_list - Liste der Datentypen der Spalten
    __column_size_list - Liste mit Feldlängen
    """

    def __init__(self, host, dsn, user, password, scheme, name, primary_key=None, business_key=None, column_list=None,
                 data_type_list=None, column_size_list=None, description=None):
        """ Creates an instance of a Netezza table.
        Needs at least the database host, dsn, user, password, scheme and name of the table as arguments.
        Scheme can be ''. Because nzsql process the query together with the logon to the database server
        , credentials must be provided.
        Example: foo = nzlib.NetezzaTable(<host>,<dsn>,<user>,<password>,<scheme>,<name>
        ... other arguments are optional)
        """
        self.__host = host
        self.__dsn = dsn
        self.__user = user
        self.__password = password
        self.scheme = scheme
        self.name = self.get_name(name)
        self.column_list = self.get_column_list if column_list is None else column_list
        self.primary_key = self.get_primary_key if primary_key is None else primary_key
        self.business_key = business_key  # Not implemented yet since there is no info in Netezza system view
        self.data_type_list = self.get_data_type_list if data_type_list is None else data_type_list
        self.column_size_list = self.get_column_size_list if column_size_list is None else column_size_list
        self.description = self.get_description if description is None else description

    @property
    def __str__(self):
        """ Returns table name """
        return str(self.name)

    def get_name(self, name):
        """ Returns the name of the table """
        query = "select count(1) from _v_table where tablename = '{}';".format(name)
        result = nzsql_query([self.__host, self.__dsn, self.__user, self.__password], query)
        if result != "1":
            print("The specified table {} was not found in database {}.".format(name, self.__dsn))
            return False
        return name

    @property
    def get_primary_key(self):
        """ Returns a list of key columns for table """
        query = "select attname from _v_relation_keydata where relation='{}' and contype = 'p' " \
                "order by conseq;".format(self.name)
        cred = [self.__host, self.__dsn, self.__user, self.__password]
        result = result_list(nzsql_query(cred, query))
        if len(result) == 0:
            return None
        return result

    @property
    def get_column_list(self):
        """ Returns a list of all columns of table """
        query = "select attname from _v_relation_column where name = '{}' order by attnum;".format(self.name)
        cred = [self.__host, self.__dsn, self.__user, self.__password]
        result = result_list(nzsql_query(cred, query))
        if len(result) == 0:
            return None
        return result

    @property
    def get_data_type_list(self):
        """
            Returns a list of data types for all columns in self.column_list.
            Can lead to unintended results when order of columns is changed.
            Use get_data_type_dict instead.
        """
        query = "select atttypid from _v_relation_column where name = '{0}' and attname in ('{1}') " \
                "order by attnum;".format(self.name, '\',\''.join(self.column_list))
        cred = [self.__host, self.__dsn, self.__user, self.__password]
        result = result_list(nzsql_query(cred, query))
        if len(result) == 0:
            return None
        return result

    @property
    def get_data_type_dict(self):
        """
            Returns a list of data types for all columns in self.column_list.
        """
        query = "select attname, atttypid from _v_relation_column where name = '{0}' " \
                "and attname in ('{1}') ".format(self.name, '\',\''.join(self.column_list))
        cred = [self.__host, self.__dsn, self.__user, self.__password]
        result = nzsql_query(cred, query)
        if len(result) == 0:
            return None
        l_entries = list(x for x in result.split("\n"))
        d_data_types = {}
        for entry in l_entries:
            s_key = entry.split("|")[0].strip()
            s_value = entry.split("|")[1].strip()
            d_data_types["{0}".format(s_key)] = "{0}".format(s_value)
        return d_data_types

    @property
    def get_column_size_list(self):
        """ Returns a list with column sizes for each column in table """
        query = "select attcolleng from _v_relation_column where name = '{0}' and attname in ('{1}') " \
                "order by attnum;".format(self.name, '\',\''.join(self.column_list))
        cred = [self.__host, self.__dsn, self.__user, self.__password]
        result = result_list(nzsql_query(cred, query))
        if len(result) == 0:
            return None
        return result

    @property
    def get_business_key(self):
        """ Returns the business key of the table """
        return None

    @property
    def get_description(self):
        """ Returns a description of table """
        command = "nzsql -host {} -db {} -u {} -pw {} -t -c '\d {}'".format(self.__host, self.__dsn, self.__user,
                                                                            self.__password, self.name)
        l_command = shlex.split(command)
        stmt = subprocess.Popen(l_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                close_fds=True)
        try:
            output, error = stmt.communicate()
        except OSError as e:
            print(e)
            return False
        # Error handling
        result = output.decode(standard_encoding).strip()
        if stmt.returncode != 0:
            print(result)
            return False
        return result

    def generate_statistics(self):
        """ Generates statistics on table """
        query = "generate statistics on {};".format(self.name)
        cred = [self.__host, self.__dsn, self.__user, self.__password]
        result = nzsql_query(cred, query)
        return result

    def groom_table(self):
        """ Groom table and release unused memory """
        query = "groom table {};".format(self.name)
        cred = [self.__host, self.__dsn, self.__user, self.__password]
        result = nzsql_query(cred, query)
        return result

    def truncate_table(self):
        """ Truncates table """
        query = "truncate table {};".format(self.name)
        cred = [self.__host, self.__dsn, self.__user, self.__password]
        result = nzsql_query(cred, query)
        return result

    def excel_export(self, filename, delimiter="|", filter_stmt=None):
        """ Exports data to csv file and converts file to MS Excel format
        """
        import csv
        from openpyxl import Workbook
        if filename.endswith(".xlsx"):
            excel_file = filename
            csv_file = "{0}.csv".format(filename[:-5])
        elif filename.endswith(".csv"):
            excel_file = "{0}.xlsx".format(filename[:-4])
            csv_file = filename
        else:
            excel_file = "{0}.xlsx".format(filename)
            csv_file = "{0}.csv".format(filename)
        result = self.csv_export(csv_file, headers=True, delimiter=delimiter, filter_stmt=filter_stmt)
        if result is False:
            return False

        # Lookup data types for given column list
        dic_data_types = self.get_data_type_dict

        with open(csv_file) as fh:
            i = -1
            for i, l in enumerate(fh):
                pass
            line_numbers = i
        print("CSV file has {0} lines.".format(line_numbers))
        if line_numbers > 1000000:
            print("Error. Excel file will hold {0} entries. The file may be too big to be opened.\n"
                  "Consider to filter your query to get a smaller result set.".format(line_numbers))
            return False

        print("Converting {0} to {1}".format(csv_file, excel_file))
        with open(csv_file, "rU") as fh:
            wb = Workbook()
            ws = wb.active
            reader = csv.reader(fh, delimiter=delimiter)
            ws.append(next(reader))
            for row in reader:
                content = list(check_xls_values(dic_data_types.get(y), row[x].strip())
                               for x, y in enumerate(self.column_list))
                ws.append(content)
            wb.save(filename=excel_file)
        os.remove(csv_file) if os.path.exists(csv_file) else None

    def csv_export(self, filename, headers=False, delimiter="|", query=None, filter_stmt=None):
        """
            Exports table columns to CSV File and returns True if successful.
        """
        # Standard query assignment if not existent
        if not query and not filter_stmt:
            query = "select {0} from {1};".format(",".join(self.column_list), self.name)
        elif not query and filter_stmt:
            query = "select {0} from {1} {2};".format(",".join(self.column_list), self.name, filter_stmt)

        # Set CSV filename
        if filename.endswith(".csv"):
            csv_file = filename
        else:
            csv_file = "{0}.csv".format(filename)

        print("Exporting selection from table {0} to file {1}\nQuery: {2}".format(self.name, csv_file, query))

        # Get credentials
        cred = [self.__host, self.__dsn, self.__user, self.__password]
        if headers:
            result = nz_export_to_file(cred, query, filename=csv_file, delimiter=delimiter,
                                       column_list=self.column_list)
        else:
            result = nz_export_to_file(cred, query, filename=csv_file, delimiter=delimiter)
        if result is False:
            return False
        return True

    def csv_import(self, filename, delim="|", skiprows=0, datestyle="YMD", datedelim="-", decimaldelim=".",
                   timedelim=":", encoding="Internal", timestyle="24hour"):
        """
        :param filename: Filename of data file
        :param delim: Row delimiter (default= "|")
        :param skiprows: Rows to skip (default: 0)
        :param datestyle: Date format (default: "YMD")
        :param datedelim: Date delimiter (default: "-")
        :param decimaldelim: Decimal delimiter (default: ".")
        :param timedelim: Time delimiter (default: ":")
        :param encoding: File encoding (default: "Internal")
        :param timestyle: Time format (default: "24hour")
        :return: Output of stdout or False if an error occurred.
        """
        cred = [self.__host, self.__dsn, self.__user, self.__password]
        logfile = "nz_{}_import.log".format(self.name)
        badfile = "nz_{}_import.bad".format(self.name)
        query = "DATAFILE {}\n{{\nDatabase {}\nTableName {}\nDelimiter '{}'\nSkipRows {}\n" \
                "DateStyle '{}'\nDateDelim '{}'\nDecimalDelim '{}'\nTimeDelim '{}'\nEncoding '{}'\n" \
                "TimeStyle '{}'\nLogfile '{}'\nBadfile '{}'\n}}".format(filename, self.__dsn, self.name, delim,
                                                                        skiprows, datestyle, datedelim,
                                                                        decimaldelim, timedelim, encoding,
                                                                        timestyle, logfile, badfile)
        result = nzload_file_importer(cred, query)
        if result is False:
            print(result)
            return False

        # Print Netezza Logfile
        with open(logfile, 'r') as output:
            print(output.read())
        output.close()
        os.remove(logfile) if os.path.exists(logfile) else None
        return result
