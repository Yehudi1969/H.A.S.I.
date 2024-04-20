# -*- coding: utf8 -*-
###############################################################################
#                                meta_ladelauf.py                             #
###############################################################################
# Copyright (C) 2023  Jens Janzen
# Datum: 2022-07-25
# Change history:

# Libs
import argparse
import logging
import os
import sys
import textwrap
from datetime import datetime
import pandas as pd
import oralib as db


def table2dataframe(dsn, schema, table, stmt=None):
    """
        Gibt ein DataFrame aus einem Select zurück.
        Die Spaltenköpfe entsprechen den selektierten Feldnamen.
        Wird keine Query übergeben, wird die Tabelle vollständig gelesen.
        Wird eine Query übergeben, werden die Angaben für Schema und Tabelle nicht verwendet.
    """
    if not stmt:
        stmt = "select * from {0}.{1}".format(schema, table)
    connection = db.get_connection(dsn)
    cursor = connection.cursor()
    obj_exec = cursor.execute(stmt)
    header = list(x[0] for x in cursor.description)
    raw_data = obj_exec.fetchall()
    df = pd.DataFrame(raw_data, columns=header)
    log.debug("Header: {0}".format(header))
    return df


def check_ladelauf(wid):
    stmt = "select systimestamp from dual;"
    ts_now = db.sql_query(con_meta, stmt)[0][0]
    while True:
        df = read_ladelauf(wid)
        if df.loc[(df["STA"] == "in Arbeit") | (df["STA"] == "NOK")].empty is False:
            df_row = df.loc[(df["STA"] == "in Arbeit") | (df["STA"] == "NOK")]
            lid = df_row["LID"].squeeze()
            sta = "in Arbeit"
            attr = "LTV"
            log.info("Satz mit Status 'in Arbeit' gefunden. LadeID {0} wird benutzt.".format(lid))
            result = update_status(wid, lid, sta, attr, ts_now)
            return result
        elif df.loc[df["STA"] == "Eingeplant"].empty is False:
            df_row = df.loc[df["STA"] == "Eingeplant"]
            lid = df_row["LID"].squeeze()
            sta = "in Arbeit"
            attr = "LTV"
            log.info("Ältester Satz mit Status 'Eingeplant' gefunden. LadeID {0} wird benutzt.".format(lid))
            result = update_status(wid, lid, sta, attr, ts_now)
            return result
        elif df.loc[df["STA"] == "OK"].empty is False:
            df_row = df.loc[df["STA"] == "OK"]
            lid = df_row["LID"].squeeze()
            ttv = df_row["TTB"].squeeze()
            log.info("Vorläufer Satz mit LadeID {0} Status 'OK' gefunden. Folgesatz wird angelegt.".format(lid))
            result = plan_new_ladelauf(wid, ttv)
            if result is False:
                return False
        elif df.loc[df["STA"] == "OK"].empty is True:
            log.info("Es wurde kein Ladelauf zum Workflow {0} gefunden. Initialer Ladelauf wird angelegt.".format(wid))
            result = plan_init_ladelauf(wid)
            if result is False:
                return False


def read_ladelauf(wid):
    """ Lese folgende Informationen aus der Meta_Lade_lauf in ein DataFrame:
        1. Den letzten erfolgreichen Ladelauf zum Workflow
        2. Den ältesten eingeplanten Ladelauf zum Workflow
        3. Den in Arbeit befindlichen bzw. abgebrochenen Workflow
    """
    stmt = "select lid, ltv, ltb, wid, sta, ttv, ttb, rank from (" \
           "select lid, ltv, ltb, wid, sta, ttv, ttb, " \
           "rank() over (partition by wid order by lid desc) rank from meta_lade_lauf " \
           "where wid = {0} and sta = 'OK' union " \
           "select lid, ltv, ltb, wid, sta, ttv, ttb, " \
           "rank() over (partition by wid order by lid) rank from meta_lade_lauf " \
           "where wid = {0} and sta = 'Eingeplant' union " \
           "select lid, ltv, ltb, wid, sta, ttv, ttb, 1 as rank from meta_lade_lauf " \
           "where wid = {0} and sta in ('in Arbeit', 'NOK')" \
           ") t where rank = 1".format(wid)
    df = table2dataframe(con_meta, None, None, stmt=stmt)
    return df


def plan_init_ladelauf(wid, cutoff_time=None):
    """ Plane Zeitscheibe für Initiallauf ein. Zeitraum ist der 1.1.1900 bis heute 0:00 Uhr """
    if cutoff_time:
        ttb = cutoff_time
    else:
        ttb = "systimestamp"
    stmt = "insert into meta_lade_lauf(wid, sta, ttv, ttb, kom) values " \
           "({0}, 'Eingeplant', to_timestamp('1900-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF'), " \
           "{1}, 'Automatisch generiert am: ' || to_char({1}, 'YYYY-MM-DD HH24:MI:SS.FF'));".format(wid, ttb)
    result = db.sql_query(con_meta, stmt)
    return result


def plan_new_ladelauf(wid, ttv, cutoff_time=None):
    """ Neuen Ladelauf einstellen """
    s_ttv = datetime.strftime(ttv, '%Y-%m-%d %H:%M:%S.%f')
    if cutoff_time:
        ttb = cutoff_time
    else:
        ttb = "systimestamp"
    stmt = "insert into meta_lade_lauf(wid, sta, ttv, ttb, kom) values " \
           "({0}, 'Eingeplant', to_timestamp('{1}', 'YYYY-MM-DD HH24:MI:SS.FF'), {2}, " \
           "'Automatisch generiert am: ' || to_char({2}, 'YYYY-MM-DD HH24:MI:SS'));".format(wid, s_ttv, ttb)
    result = db.sql_query(con_meta, stmt)
    return result


def update_status(wid, lid, sta, attr, ts_now):
    s_now = datetime.strftime(ts_now, '%Y-%m-%d %H:%M:%S.%f')
    log.info("Aktualisiere {0} mit {1} und Status {2}".format(attr, s_now, sta))
    stmt = "update meta_lade_lauf set {0} = to_timestamp('{1}', 'YYYY-MM-DD HH24:MI:SS.FF'), " \
           "sta = '{2}' where lid = {3};".format(attr, s_now, sta, lid)
    result = db.sql_query(con_meta, stmt)
    if result is False:
        return False
    log.info("{0} Rows updated in META_LADE_LAUF.".format(result))
    log.info("Updating LTV")
    stmt = "update meta_lade_parameter set parameter_wert = '{0}' " \
           "where wid = {1} and session_name='GLOBAL' " \
           "and parameter_name='{2}';".format(s_now, wid, attr)
    result = db.sql_query(con_meta, stmt)
    log.info("{0} Rows updated in META_LADE_PARAMETER.".format(result))
    log.info("Updating LID")
    stmt = "update meta_lade_parameter set parameter_wert = '{0}' " \
           "where wid = {1} and session_name='GLOBAL' " \
           "and parameter_name='{2}';".format(lid, wid, 'LID')
    result = db.sql_query(con_meta, stmt)
    log.info("{0} Rows updated in META_LADE_PARAMETER.".format(result))
    return False if result is False else True


def end_ladelauf(wid):
    stmt = "select lid from meta_lade_lauf where wid = {0} and sta = 'in Arbeit';".format(wid)
    lid = db.sql_query(con_meta, stmt)[0][0]
    log.info("Schließe Ladelauf mit LID {0} und Status OK ab.".format(lid))
    stmt = "update meta_lade_lauf set ltb = systimestamp, sta='OK' where lid = {0}".format(lid)
    result = db.sql_query(con_meta, stmt)
    log.info("{0} Rows updated in META_LADE_LAUF.".format(result))
    return False if result is False else True


""" Constants """
IO_BUFFER_SIZE = -1  # 1048576 = 1 MB Buffer, 1 = line buffer size, -1 OS default buffer size

# ***********************
# ****  M  A  I  N  *****
# ***********************
desc = """Aktionen auf der Tabelle META_LADE_LAUF

Beispiel:
python meta_ladelauf.py -p=AWS -wid=1000 -a=start

Beschreibung:
Das Programm schreibt und aktualisiert Datensätze in der Tabelle META_LADE_LAUF.
Die folgenden Parameter sind verbindlich:
1. Projektname aus H.A.S.I. zur Ermittlung der Ablageorte im Filesystem
2. Workflow ID aus META_LADE_WORKFLOWS
3. Aktion (start|end) : Starte Zeitscheibe | Schließe Zeitscheibe ab

Es gibt 3 definierte Stati in Metaladeläufen:
OK : Abgeschlossener Ladelauf
Eingeplant: Zur Verarbeitung anstehender Ladelauf
in Arbeit: In Verarbeitung befindlicher Ladelauf.
Ziel ist es, einen Ladelauf in den Status "in Arbeit" zu setzen oder falls es bereits einen gibt,
das Datum LTV zu aktualisieren.
Dieser Ladelauf wird von den verarbeitenden Folgejobs benutzt.
"""

""" Deklaration Homeverzeichnis für Linux und Windows """
HOME = os.environ['HOME'] = os.path.expanduser('~')
if os.getenv("HASI_HOME"):
    path = os.environ["HASI_HOME"]
else:
    path = "{0}/data".format(HOME)
con_meta = "HASI"

""" Logging """
log = logging.getLogger("log_meta")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

""" Arguments """
options = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                  description=textwrap.dedent(desc))
options.add_argument("-p", "--project", dest="project", help="Name des Projects", type=str, required=True)
options.add_argument("-wid", "--workflow_id", dest="workflow_id", help="ID des Workflows", required=True)
options.add_argument("-a", "--action", dest="action", help="Durchzuführende Aktion (start|end)", required=True)
args = options.parse_args()
project = args.project
workflow_id = args.workflow_id
action = args.action

""" Project and Directories """
query = "select key_text, value_text from obj_att_project where project='{0}';".format(project)
d_filesystem_path = dict(db.sql_query(con_meta, query))

""" MAIN """
if action == 'start':
    check_ladelauf(workflow_id)
elif action == 'end':
    end_ladelauf(workflow_id)
else:
    log.error("Aktion nicht definiert. Muss den Wert 'start' oder 'end' haben.")
    sys.exit(1)
sys.exit(0)
