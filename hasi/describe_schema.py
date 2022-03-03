# -*- coding: utf8 -*-
###############################################################################
#                              describe_schema.py                             #
###############################################################################
# Autor: Jens Janzen
# Datum: 2021-06-09

import argparse
import textwrap
import oralib

# ***********************
# ****  M  A  I  N  *****
# ***********************
# Evaluate arguments
desc = """Das Programm gibt die DDLs aller Tabellen im angegebenen Schema aus.
Folgende Optionen müssen gesetzt werden:
1. Connection - Datenbankverbindungsname
2. Schema - Datenbankschema

Beispiel:
python app_config.py -c=SHUKAUSWSITU_T -s=TSHUKAUSWDAT01
"""

options = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                  description=textwrap.dedent(desc))
options.add_argument("-c", "--conn", dest="connection",
                     help="Name der Datenbankverbindung", required=True)
options.add_argument("-s", "--schema", dest="schema",
                     help="Name des Datenbankschemas", required=True)

args = options.parse_args()
conn = args.connection.upper()
schema = args.schema.upper()
print("-- Connection: {0}".format(conn))
print("-- Schema: {0}".format(schema))

query = "select table_name from all_tables where owner='{0}' order by table_name;".format(schema)
result = oralib.sql_query(conn, query)
tables = list(x[0] for x in result)
for table in tables:
    tab = oralib.Table(conn, table)
    tab.get_ddl(readable=True)
    print(tab.ddl)
