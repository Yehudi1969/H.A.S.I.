# -*- coding: utf8 -*-
###############################################################################
#                              repo_dev2prod.py                               #
###############################################################################
# Copyright (C) 2023  Jens Janzen
# Datum: 2022-01-24

import logging
import os
import oralib
import datetime
import sys


# Functions
def copy_metadata(l_tables):
    for table_name in l_tables:
        log.info("Verarbeite Tabelle {0}".format(table_name))
        o_src_table = oralib.Table(conn_src, table_name)
        ddl = o_src_table.get_ddl(include_schema=False, include_grants=False)
        o_tgt_table = oralib.Table(conn_tgt, table_name)
        if len(o_tgt_table.column_list) == 0:
            log.info("Zieltabelle {0} ist nicht vorhanden und wird angelegt.".format(table_name))
            oralib.sql_query(conn_tgt, ddl)
        columns = o_tgt_table.get_column_list()
        src_query = "select {0} from {1}".format(", ".join(columns), table_name)
        tgt_query = "insert into {0} ({1}) values ({2})".format(
            table_name, ", ".join(columns), ", ".join(list(":{0}".format(x) for x in range(len(columns)))))
        log.info("Tabelle wird geleert.")
        o_tgt_table.truncate_table()
        log.info("Kopiere Daten {0}".format(datetime.datetime.now()))
        successful_rows = oralib.process_data(conn_src, src_query, conn_tgt, tgt_query)
        log.info("Fertig {0}".format(datetime.datetime.now()))
        log.info("{0} Datensätze in Tabelle {1} übertragen.".format(successful_rows, table_name))
    return True


""" Directories """
HOME = os.environ['HOME'] = os.path.expanduser('~')
if os.getenv("HASI_HOME"):
    path = os.environ["HASI_HOME"]
else:
    path = "{0}/data".format(HOME)
sqlldr_path = path.replace("\\", "/")
scriptname = "repo_dev2prod"

""" Delete logfile """
os.remove("{0}/{1}.log".format(path, scriptname)) if os.path.exists("{0}/{1}.log".format(path, scriptname)) else None

""" Sets logging console handler for debugging """
log = logging.getLogger("SESSION")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler = logging.FileHandler("{0}/{1}.log".format(path, scriptname))
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
log.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

""" Connections """
conn_src = "V_TDM_REFDATA"
conn_tgt = "P_TDM_REFDATA"

repo_tables = ["OBJ_APPLICATION", "OBJ_ATT_PROJECT", "OBJ_CUSTOMER_EMAIL", "OBJ_DATE",
               "OBJ_DDIC_CATALOG", "OBJ_EVENTS", "OBJ_JOB", "OBJ_JOB_PLAN", "OBJ_MAPPING",
               "OBJ_PARAMETER", "OBJ_RULESET", "OBJ_SIGNATURES", "OBJ_STYLES", "META_ENTITIES"]
result = copy_metadata(repo_tables)
if result is False:
    log.info("Fehler aufgetreten.")
    sys.exit(1)
log.info("Verarbeitung vollständig.")
sys.exit(0)
