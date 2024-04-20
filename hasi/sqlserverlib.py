# -*- coding: utf8 -*-
###############################################################################
#                               sqlserverlib.py                               #
###############################################################################
# Copyright (C) 2023  Jens Janzen

# Change history
# V1.0: 2019-04-26 - Initial Release

# Description:
# Library contains helper functions for Microsoft SQL server access
import os
import shlex
import subprocess

standard_encoding = 'utf8'


# Helper functions
def mssql_query(credentials, query, port=1433, encrypt='true'):
    """ Execute a SQL Query. Uses jaydebeapi and native python database api.
    Needs a list with credentials and the SQL query.
    """
    import jaydebeapi
    conn = None
    if len(credentials) != 4:
        print("Number of credentials is wrong.")
        return False
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    url = "jdbc:sqlserver://{0}:{4};databaseName={1};user={2};password={3};encrypt={5}".format(
        credentials[0], credentials[1], credentials[2], credentials[3], port, encrypt)
    try:
        path = "{0}/sqljdbc4.jar".format(os.environ["JDBC_HOME"])
    except KeyError:
        path = "{0}/jdbc/sqljdbc4.jar".format(os.environ["PYTHONPATH"])
    try:
        conn = jaydebeapi.connect(driver, url, path, )
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        print(e)
        if conn:
            conn.close()
        return False


def sqlcmd_export_to_file(credentials, query, fileobject):
    """ Execute bcp export utility for table replication (currently returns no rows)
    1. Query should not have a terminator
    2. outfile name of the export data file
    """
    if len(credentials) != 4:
        print("Number of credentials is wrong.")
        return False
    host = credentials[0]
    db = credentials[1]
    dbu = credentials[2]
    dbpw = credentials[3]
    file_name = "{}.{}.tmpquery".format(os.getpid(), os.path.basename(__file__))
    # write query to file
    with open(file_name, 'w') as f_query:
        f_query.write(query)
        f_query.close()
    # execute given query
    command = "sqlcmd -S {} -d {} -U {} -P {} -i {} -h -1 -s '|' -W -X -o {}".format(
        host, db, dbu, dbpw, file_name, fileobject)
    print(command)
    l_command = shlex.split(command)
    stmt = subprocess.Popen(l_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            close_fds=True)
    output = None
    error = None
    try:
        output, error = stmt.communicate()
    except OSError as e:
        print(e)
    except ValueError as e:
        print(e)
    # remove query file
    os.remove(file_name) if os.path.exists(file_name) else None
    # Error handling
    if error:
        print("Error occured during SQL execution: {} {}".format(error, output))
        return False
    if str(output.rstrip()) == '':
        print("Warning: Query returned zero rows.")
        print(query)
        return None
    # Return resultset
    return output.decode(standard_encoding).strip()
