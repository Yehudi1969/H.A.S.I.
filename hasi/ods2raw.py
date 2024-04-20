# -*- coding: utf8 -*-
###############################################################################
#                                 ods2raw.py                                  #
###############################################################################
# Copyright (C) 2023  Jens Janzen
# Datum: 2021-07-02

import argparse
import datetime
import logging
import oralib as db
import os
import textwrap

""" Directories """
HOME = os.environ['HOME'] = os.path.expanduser('~')
if os.getenv("HASI_HOME"):
    path = os.environ["HASI_HOME"]
else:
    path = "{0}/data".format(HOME)
scriptname = "ods2raw.py"
sqlldr_path = path.replace("\\", "/")

""" Delete logfile """
os.remove("{0}/{1}.log".format(path, scriptname)) if os.path.exists("{0}/{1}.log".format(path, scriptname)) else None

""" Set logging handler """
log = logging.getLogger("ods2raw")
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

""" Metadata columns """
meta_src_cols = ['HIS_POLICY_ID', 'HIS_EDITING_ID', 'HIS_PROCESS_ID', 'HIS_VERSION_ID', 'HIS_VERSICHERUNG_ID',
                 'HIS_VERSION_NUMBER', 'HIS_POLICY_STATUS', 'HIS_VALID_FROM', 'HIS_FINISHED']
meta_tgt_cols = ['LID', 'HID', 'SID', 'HIS_POLICY_ID', 'TTS']


# ***********************
# ** F U N C T I O N S **
# ***********************
def analyze_model():
    """ Reset metadata table """
    stmt = "update meta_entities set active=0 where umgebung='{0}' and active=1;".format(environment)
    db.sql_query(conn_tgt, stmt)

    """ Auswahl aller möglichen Objekte"""
    stmt = "select src_schema, src_table, tgt_schema, tgt_table from meta_entities where umgebung='{0}' " \
           "order by src_schema, src_table;".format(environment)
    l_analyze = db.sql_query(conn_tgt, stmt)

    """ Liste der Änderungsobjekte """
    l_changes = []

    for entry in l_analyze:
        src_schema = entry[0]
        src_table = entry[1]
        tgt_schema = entry[2]
        tgt_table = entry[3]

        """ Untersuchung Quelle und Ziel """
        o_src_table = db.Table(conn_src, src_table, schema=src_schema)
        src_cols = o_src_table.column_list
        o_tgt_table = db.Table(conn_tgt, tgt_table, schema=tgt_schema)
        tgt_cols = o_tgt_table.column_list

        """ Ist eine der Tabellen nicht existent? Nimm das nächste Element. """
        if len(src_cols) == 0 or len(tgt_cols) == 0:
            continue

        """ Ermittlung der Attribute """
        src_att_cols = list(col for col in src_cols if col not in meta_src_cols)
        tgt_att_cols = list(col for col in tgt_cols if col not in meta_tgt_cols)
        src_missing_cols = list(col for col in tgt_att_cols if col not in src_att_cols)
        tgt_missing_cols = list(col for col in src_att_cols if col not in tgt_att_cols)
        tgt_diff_cols = src_missing_cols + tgt_missing_cols
        comp_cols = list(col for col in src_att_cols if col in tgt_att_cols)

        datatype_difference = 0
        all_checks_ok = False

        """ Vergleich Datentypen """
        for column in comp_cols:
            if o_src_table.d_col_data_type[column] != o_tgt_table.d_col_data_type[column]:
                log.info("{0}.{2}{3} : {1}.{2}{4}".format(src_table, tgt_table, column,
                                                          o_src_table.d_col_data_type[column],
                                                          o_tgt_table.d_col_data_type[column]))
                datatype_difference += 1

        """ Ergebnis der Analyse """
        if src_att_cols != tgt_att_cols:
            log.info("Tabellenstrukturen zwischen {0}.{1} und {2}.{3} weichen ab.".format(
                src_schema, src_table, tgt_schema, tgt_table))
            log.info("ODS: {0}".format(src_att_cols))
            log.info("RAW: {0}".format(tgt_att_cols))
        elif src_att_cols == tgt_att_cols and datatype_difference > 0:
            log.info("Datentypen zwischen gleichnamigen Feldern in {0} und {1} weichen bei "
                     "{2} Feldern voneinander ab.".format(src_table, tgt_table, datatype_difference))
        elif set(src_att_cols) == set(tgt_att_cols) and len(tgt_diff_cols) != 0:
            log.info("Die Feldreihenfolge zwischen {0}.{1} und {2}.{3} ist unterschiedlich.".format(
                src_schema, src_table, tgt_schema, tgt_table))
            log.info("ODS: {0}".format(src_att_cols))
            log.info("RAW: {0}".format(tgt_att_cols))
        elif src_att_cols != tgt_att_cols:
            log.info("Die Tabellenstruktur zwischen {0}.{1} und {2}.{3} ist unterschiedlich.".format(
                src_schema, src_table, tgt_schema, tgt_table))
            log.info("ODS: {0}".format(src_att_cols))
            log.info("RAW: {0}".format(tgt_att_cols))
        elif src_att_cols == tgt_att_cols and datatype_difference == 0:
            log.info("Tabellenstrukturen zwischen {0}.{1} und {2}.{3} stimmen ueberein.".format(
                src_schema, src_table, tgt_schema, tgt_table))
            all_checks_ok = True

        """ Persistierung der Migrationskandidaten in den Metadaten """
        if all_checks_ok is False:
            stmt = "update meta_entities set active=1 where umgebung='{0}' and src_schema='{1}' " \
                   "and src_table='{2}';".format(environment, src_schema, src_table)
            db.sql_query(conn_tgt, stmt)
            l_changes.append("{0}.{1}->{2}.{3}".format(src_schema, src_table, tgt_schema, tgt_table))

    log.info("\nDie folgenden Tabellenbeziehungen sind unterschiedlich und muessen in Informatica angepasst werden:\n"
             "{0}".format("\n".join(l_changes)))
    return True


def check_tables():
    """ Reset metadata table """
    stmt = "update meta_entities set active=0 where umgebung='{0}' and active=1;".format(environment)
    db.sql_query(conn_tgt, stmt)

    """ Auswahl aller möglichen Objekte"""
    stmt = "select src_schema, src_table, tgt_schema, tgt_table from meta_entities where umgebung='{0}' " \
           "order by src_schema, src_table;".format(environment)
    l_analyze = db.sql_query(conn_tgt, stmt)

    for entry in l_analyze:
        src_schema = entry[0]
        src_table = entry[1]
        tgt_schema = entry[2]
        tgt_table = entry[3]

        """ Untersuchung Quelle und Ziel """
        o_src_table = db.Table(conn_src, src_table, schema=src_schema)
        src_cols = o_src_table.column_list
        o_tgt_table = db.Table(conn_tgt, tgt_table, schema=tgt_schema)
        tgt_cols = o_tgt_table.column_list

        """ Ist eine der Tabellen nicht existent? Nimm das nächste Element. """
        if len(src_cols) == 0 or len(tgt_cols) == 0:
            continue

        """ Vergleiche Satzanzahl ODS View mit RAW Tabelle."""
        stmt = "select count(*) from {0}.{1}".format(src_schema, src_table)
        src_rows = db.sql_query(conn_src, stmt)[0][0]
        stmt = "select count(*) from {0}.{1}".format(tgt_schema, tgt_table)
        tgt_rows = db.sql_query(conn_tgt, stmt)[0][0]
        log.info("Satzanzahl {0}.{1} : {2}.{3} -> {4} : {5} Rows.".format(
            src_schema, src_table, tgt_schema, tgt_table, src_rows, tgt_rows))


def migrate_table(l_object):
    src_schema = l_object[0]
    src_table = l_object[1]
    tgt_schema = l_object[2]
    tgt_table = l_object[3]
    if len(tgt_table) > 25:
        src_table_temp = "{0}_TEMP".format(src_table[:24])
    else:
        src_table_temp = "{0}_TEMP".format(src_table)
    if len(tgt_table) > 25:
        tgt_table_temp = "{0}_TEMP".format(tgt_table[:24])
    else:
        tgt_table_temp = "{0}_TEMP".format(tgt_table)

    """ Untersuchung Quelle und Ziel """
    o_src_table = db.Table(conn_src, src_table, schema=src_schema)
    src_cols = o_src_table.column_list
    o_tgt_table = db.Table(conn_tgt, tgt_table, schema=tgt_schema)
    tgt_cols = o_tgt_table.column_list
    o_meta_table = db.Table(conn_tgt, "RAW_POLICY_HIST", schema=tgt_schema)
    meta_cols = o_meta_table.column_list
    o_tgt_table_temp = db.Table(conn_tgt, tgt_table_temp, schema=tgt_schema)

    """ Ermittlung der Attribute """
    src_tech_cols = list(col for col in src_cols if col in meta_src_cols)
    src_att_cols = list(col for col in src_cols if col not in meta_src_cols)
    tgt_tech_cols = list(col for col in tgt_cols if col in meta_tgt_cols)
    tgt_att_cols = list(col for col in src_att_cols)
    lkp_hist_cols = list(col for col in meta_cols if col in meta_tgt_cols)
    log.info("Quellschlüssel: {0}".format(src_tech_cols))
    log.info("Quellattribute: {0}".format(src_att_cols))
    log.info("Zielschlüssel: {0}".format(tgt_tech_cols))
    log.info("Zielattribute: {0}".format(tgt_att_cols))

    """ Export Quelltabelle in CSV 
    src_csv_filename = "{0}.csv".format(src_table)
    log.info("Export aus {0} in {0}.csv.".format(src_table))
    o_src_table.csv_export(path, src_csv_filename, quoting=True)
    log.info("Ok.")
    """

    """ Anlage temporäre Kopie der Quelltabelle """
    stmt = "select count(*) from all_tables where owner='{0}' and table_name='{1}';".format(tgt_schema, src_table_temp)
    table_exists = db.sql_query(conn_tgt, stmt)[0][0]
    if table_exists == 1:
        log.info("Leere vorhandene Tabelle {0}.{1}".format(tgt_schema, src_table_temp))
        stmt = "truncate table {0}.{1}".format(tgt_schema, src_table_temp)
    elif table_exists == 0:
        log.info("Erzeuge {0}.{1}".format(tgt_schema, src_table_temp))
        stmt = o_src_table.get_ddl(include_schema=False, include_grants=False, table_name=src_table_temp)
    else:
        log.error("Error occurred.")
        return False
    db.sql_query(conn_tgt, stmt)
    log.info("Ok.")

    """ Übertragen der Daten """
    log.info("Lade {0}.{1} in {2}.{3}".format(src_schema, src_table, tgt_schema, src_table_temp))
    reader_sql = "select {2} from {0}.{1}".format(src_schema, src_table, ", ".join(src_cols))
    writer_sql = "insert into {0} ({1}) values ({2})".format(
        src_table_temp, ", ".join(src_cols), ", ".join(list(":{0}".format(x) for x in range(len(src_cols)))))
    log.debug(reader_sql)
    log.debug(writer_sql)
    log.info("Transaktion Start {0}".format(datetime.datetime.now()))
    successful_rows = db.process_data(conn_src, reader_sql, conn_tgt, writer_sql)
    log.info("Transaktion Ende {0}".format(datetime.datetime.now()))
    log.info("Copied table {0} rows to table {1}.".format(successful_rows, src_table_temp))

    """ Anlage temporäre Zieltabelle mit technischen Feldern des Ziels und Attributen aus der Quelltabelle """
    stmt = "select count(*) from all_tables where owner='{0}' and table_name='{1}';".format(tgt_schema, tgt_table_temp)
    table_exists = db.sql_query(conn_tgt, stmt)[0][0]
    if table_exists == 1:
        stmt = "truncate table {0}.{1}".format(tgt_schema, tgt_table_temp)
        log.info("Leere vorhandene Tabelle {0}.{1}".format(tgt_schema, tgt_table_temp))
        db.sql_query(conn_tgt, stmt)
    elif table_exists == 0:
        o_tgt_table.column_list = tgt_tech_cols
        body_tech_cols = o_tgt_table.get_ddl(include_schema=False, include_grants=False,
                                             include_constraints=False, body_only=True)
        o_tgt_table.get_column_list()
        o_src_table.column_list = src_att_cols
        body_att_cols = o_src_table.get_ddl(include_schema=False, include_grants=False,
                                            include_constraints=False, body_only=True)
        o_src_table.get_column_list()
        stmt = "CREATE TABLE {0} ({1},{2});".format(tgt_table_temp, body_tech_cols, body_att_cols)
        log.info("Erzeuge {0}.{1}".format(tgt_schema, tgt_table_temp))
        log.info(stmt)
        result = db.sql_query(conn_tgt, stmt)
        if result is False:
            return result
        log.info("Ok.")
    else:
        log.error("Error occurred.")
        return False

    """ Aktualisieren der temporären Tabelle """
    new_tgt_cols = tgt_tech_cols + src_att_cols
    src_join = " AND ".join(list("A.{0} = B.{0}".format(col) for col in src_tech_cols))
    tgt_join_cols = list(col for col in tgt_tech_cols if col in new_tgt_cols and col in lkp_hist_cols)
    if "ID" not in tgt_join_cols:
        tgt_join_cols.append("ID")
    tgt_join = " AND ".join(list("SRC.{0} = TGT.{0}".format(col) for col in tgt_join_cols))
    selection = "WITH ODS AS (SELECT B.{0}, A.{1} FROM {2}.{3} A " \
                "LEFT JOIN RAW_POLICY_HIST B ON {4}) " \
                "SELECT TGT.{5}, SRC.{6} " \
                "FROM ODS SRC JOIN {7} TGT " \
                "ON {8}".format(
                    ", B.".join(lkp_hist_cols), ", A.".join(src_att_cols), tgt_schema, src_table_temp, src_join,
                    ", TGT.".join(tgt_tech_cols), ", SRC.".join(src_att_cols), tgt_table, tgt_join)
    stmt = "INSERT INTO {0}.{1} ({2}) {3}".format(tgt_schema, tgt_table_temp, ", ".join(new_tgt_cols), selection)
    log.info("Lade Daten von {0}.{1} in {0}.{2}".format(tgt_schema, src_table_temp, tgt_table_temp))
    log.info(stmt)
    result = db.sql_query(conn_tgt, stmt)
    log.info(result)
    if result is False:
        return result
    log.info("Ok.")

    if new_tgt_cols == tgt_cols:
        """ Merge auf Zieltabelle """
        log.info("Merge Daten von {0}.{1} in {0}.{2}".format(tgt_schema, tgt_table_temp, tgt_table))
        merge_tgt_cols = tgt_tech_cols + src_att_cols
        merge_update = list("TGT.{0} = SRC.{0}".format(col) for col in merge_tgt_cols if col != "ID")
        merge_stmt = "MERGE INTO {0} TGT USING (SELECT {1} FROM {2}) SRC ON (SRC.ID = TGT.ID) " \
                     "WHEN MATCHED THEN UPDATE SET {3} " \
                     "WHEN NOT MATCHED THEN INSERT ({4}) VALUES (SRC.{5});".format(
                        tgt_table, ", ".join(merge_tgt_cols), tgt_table_temp, ", ".join(merge_update),
                        ", ".join(merge_tgt_cols), ", SRC.".join(merge_tgt_cols))
        log.info(merge_stmt)
        log.info("Aktualisiere {0}.{1}".format(tgt_schema, tgt_table_temp))
        result = db.sql_query(conn_tgt, merge_stmt)
        log.info(result)
        if result is False:
            return result
        log.info("Ok.")
    else:
        """ Drop, create & insert in Rawtabelle """
        log.info("Attributvergleich urspruengliche RAW Tabelle:\n{0}".format(o_tgt_table.get_column_list()))
        log.info("Attributvergleich Tabelle mit neuen Feldern:\n{0}".format(o_tgt_table_temp.get_column_list()))
        if o_tgt_table.column_list != o_tgt_table_temp.column_list:
            log.info("Die Tabellenstruktur der Zieltabelle weicht von der ODS View ab. "
                     "Die folgenden SQLs sollten ausgeführt werden.")
            log.info("DROP TABLE {0};".format(tgt_table))
            create_target = o_tgt_table_temp.get_ddl(include_schema=False, include_grants=False, table_name=tgt_table)
            log.info(create_target)
            log.info("INSERT INTO {0} SELECT * FROM {1};".format(tgt_table, tgt_table_temp))
        else:
            """ Drop temporary tables """
            log.info("Lösche temporäre Tabellen...")
            stmt = "drop table {0};".format(src_table_temp)
            db.sql_query(conn_tgt, stmt)
            stmt = "drop table {0};".format(tgt_table_temp)
            db.sql_query(conn_tgt, stmt)
    return True


# ***********************
# ****  M  A  I  N  *****
# ***********************

desc = """Das Programm prüft in der Tabelle META_ENTITIES definierte ODS Tabellen im Schema TSCHUKDAT01 gegen
RAW Tabellen im Schema TSCHUKAUSWDAT01.  

Folgende Argumente sind zu übergeben:
1. Umgebung aus META_ENTITIES
2. DSN für ODS
3. DSN für RAW
Hinweis: Die DSN Namen stellen Schlüsselworte für eine Verbindung dar. Die konkreten Anmeldeinformationen 
werden über die seclib ermittelt.

Beispiele:
    python ods2raw.py -env=KIES_SITU -src=D_SHUK_SITU -tgt=D_SHUKAUSW_SITU -meta=D_HASI -a=1 -m=0 -c=0
"""

options = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                  description=textwrap.dedent(desc))
options.add_argument("-env", "--environment", dest="environment", help="Umgebung aus META_ENTITES", default=None)
options.add_argument("-src", "--source", dest="source_connection", help="Name der Quellverbindung", default=None)
options.add_argument("-tgt", "--target", dest="target_connection", help="Name der Zielverbindung", default=None)
options.add_argument("-meta", "--meta", dest="meta_connection", help="Name der Metadatenverbindung", default=None)
options.add_argument("-a", "--analyze", dest="flag_analyze",
                     help="Flag Analyse gesetzt/nicht gesetzt (1, 0)", default=1)
options.add_argument("-m", "--migrate", dest="flag_migrate",
                     help="Flag Migration gesetzt/nicht gesetzt (1, 0)", default=1)
options.add_argument("-c", "--check", dest="flag_check",
                     help="Flag Check gesetzt/nicht gesetzt (1, 0)", default=1)

args = options.parse_args()
environment = args.environment.upper()
conn_src = args.source_connection.upper()
conn_tgt = args.target_connection.upper()
conn_meta = args.meta_connection.upper()
flg_analyze = True if args.flag_analyze == '1' else False
flg_migration = True if args.flag_migrate == '1' else False
flg_check = True if args.flag_check == '1' else False

if flg_analyze is True:
    log.info("Analyselauf in {0}.".format(environment))
    analyze_model()

if flg_check is True:
    log.info("Plausibilitätschecks auf Tabellen in {0}.".format(environment))
    check_tables()

if flg_migration is True:
    log.info("Migrationslauf in {0}.".format(environment))
    """ Auswahl der aktiv markierten Objekte aus den Metadaten """
    query = "select src_schema, src_table, tgt_schema, tgt_table " \
            "from meta_entities where umgebung='{0}' and active=1 " \
            "order by src_schema, src_table;".format(environment)
    l_tables = db.sql_query(conn_tgt, query)
    for table in l_tables:
        check = migrate_table(table)
        if check is True:
            log.info("Tabelle {0}.{1} wurde erfolgreich auf Tabelle {2}.{3} migriert.".format(
                table[0], table[1], table[2], table[3]))
