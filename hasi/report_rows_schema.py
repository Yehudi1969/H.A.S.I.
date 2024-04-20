# -*- coding: utf8 -*-
###############################################################################
#                             report_rows_schema.py                           #
###############################################################################
# Copyright (C) 2023  Jens Janzen
# Datum: 2021-05-17

import argparse
import logging
import oralib
import seclib
import sys
import textwrap

# ***********************
# ****  M  A  I  N  *****
# ***********************
# Evaluate arguments
desc = """Prüfen eines Schemas auf Anzahl von Datensätzen aus einer Query.
Wenn mindestens ein Datensatz gefunden wird, werden die Tabelle und Anzahl der Rows ausgegeben.

Beispiel:
python report_rows_schema.py -conn=RAW1 -column="TA_LAD%DATUM"
"""

# Logging
log = logging.getLogger("report_rows_schema")
log.setLevel(logging.DEBUG)
# formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
formatter = logging.Formatter("%(message)s")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

options = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                  description=textwrap.dedent(desc))
options.add_argument("-conn", "--connection", dest="dsn", help="Name/ID der Datenbankverbindung", required=True)
options.add_argument("-col", "--column", dest="column", help="Name des Datums-/Timestampfelds", required=True)
options.add_argument("-ot", "--object_type", dest="object", help="Objekttyp (TABLE / VIEW)", required=True)

args = options.parse_args()
conn = args.dsn.upper()
column = args.column.upper()
object_type = args.object.upper()

""" Verbindungen und Schemata definieren """
schema = seclib.get_credentials(conn)[1]

""" Collect Entities from User """
if object_type == "TABLE":
    query = "select table_name from ALL_TABLES where owner='{0}' order by table_name;".format(schema)
elif object_type == "VIEW":
    query = "select view_name from ALL_VIEWS where owner='{0}' order by view_name;".format(schema)
else:
    log.error("Objekttyp kann nur TABLE oder VIEW sein.")
    sys.exit(1)
result = oralib.sql_query(conn, query)
l_entities = [x[0] for x in result]

for entry in l_entities:
    query = "select column_name from all_tab_columns " \
            "where owner='{0}' and TABLE_NAME='{1}' and column_name like '{2}';".format(schema, entry, column)
    result = oralib.sql_query(conn, query)
    if len(result) == 1:
        col = result[0][0]
        query = "select count(1) from {0}.{1} " \
                "where {2}=to_date('1753-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');".format(schema, entry, col)
        num_rows = oralib.sql_query(conn, query)[0][0]
        if num_rows > 0:
            log.info("Objekt: {0}.{1}\t\tRows: {2}".format(schema, entry, num_rows))
    else:
        log.info("No column found like {0} in entity {1}".format(column, entry))
log.info("Programmende.")
