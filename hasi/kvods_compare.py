# -*- coding: utf8 -*-
###############################################################################
#                             kvods_compare.py                                #
###############################################################################
# Copyright (C) 2023  Jens Janzen
# Datum: 2021-05-11

import argparse
import logging
import oralib
import os
import textwrap

""" Directories """
HOME = os.environ['HOME'] = os.path.expanduser('~')
if os.getenv("HASI_HOME"):
    path = os.environ["HASI_HOME"]
else:
    path = "{0}/data".format(HOME)
scriptname = "kvods_compare.py"

""" Delete logfile """
os.remove("{0}/{1}.log".format(path, scriptname)) if os.path.exists("{0}/{1}.log".format(path, scriptname)) else None

""" Set logging handler """
log = logging.getLogger("kvods_compare")
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


def f_generate_ddl(c_src, c_tgt, src_table, tgt_table):
    o_quelle = oralib.Table(c_src, src_table.split(".")[1], schema=src_table.split(".")[0])
    o_ziel = oralib.Table(c_tgt, tgt_table.split(".")[1], schema=tgt_table.split(".")[0])
    l_technical_cols = ['SID', 'LIV', 'LIB', 'LTV', 'LTB', 'TTV', 'TTB']

    """ Fachliche Felder aus der Quelle """
    v_query = "select column_name, " \
              "case " \
              "when data_type ='DATE' then data_type when data_type like 'TIMESTAMP%' then data_type " \
              "when data_type = 'FLOAT' then data_type||'('||data_precision||')' " \
              "when data_type = 'NUMBER' and data_precision is null then data_type||'(*,0)' " \
              "when data_type = 'NUMBER' and data_precision is not null " \
              "then data_type||'('||data_precision||', '||data_scale||')' " \
              "when data_type in ('CHAR', 'VARCHAR2', 'NVARCHAR2') then data_type||'('||char_length||' BYTE)' " \
              "when data_type in ('ANYDATA', 'BLOB', 'CLOB', 'LONG', 'LONG_RAW', 'RAW', 'ROWID', 'XMLTYPE') " \
              "then data_type " \
              "when data_type like 'INTERVAL%' then data_type end as data_type " \
              ", data_default " \
              ", nullable " \
              "from all_tab_columns " \
              "where owner='{0}' and table_name='{1}' and column_name in ('{2}') " \
              "order by column_id;".format(o_quelle.schema, o_quelle.table_name, '\', \''.join(o_quelle.column_list))
    cols_fachlich = oralib.sql_query(c_src, v_query)

    """ Vorhandene Technische Felder aus dem Ziel """
    v_query = "select column_name, " \
              "case " \
              "when data_type ='DATE' then data_type when data_type like 'TIMESTAMP%' then data_type " \
              "when data_type = 'FLOAT' then data_type||'('||data_precision||')' " \
              "when data_type = 'NUMBER' and data_precision is null then data_type||'(*,0)' " \
              "when data_type = 'NUMBER' and data_precision is not null " \
              "then data_type||'('||data_precision||', '||data_scale||')' " \
              "when data_type in ('CHAR', 'VARCHAR2', 'NVARCHAR2') then data_type||'('||char_length||')' " \
              "when data_type in ('ANYDATA', 'BLOB', 'CLOB', 'LONG', 'LONG_RAW', 'RAW', 'ROWID', 'XMLTYPE') " \
              "then data_type " \
              "when data_type like 'INTERVAL%' then data_type end as data_type " \
              ", data_default " \
              ", nullable " \
              "from all_tab_columns " \
              "where owner='{0}' and table_name='{1}' and column_name in ('{2}') " \
              "order by column_id;".format(o_ziel.schema, o_ziel.table_name, '\', \''.join(l_technical_cols))
    cols_technisch = oralib.sql_query(c_tgt, v_query)
    if len(o_ziel.column_list) == 0:
        cols_technisch = [('SID', 'NUMBER(*,0)', None, 'N'),
                          ('LIV', 'NUMBER(*,0)', None, 'N'),
                          ('LIB', 'NUMBER(*,0)', None, 'N'),
                          ('LTV', 'TIMESTAMP(6)', None, 'N'),
                          ('LTB', 'TIMESTAMP(6)', None, 'N'),
                          ('TTV', 'TIMESTAMP(6)', None, 'N'),
                          ('TTB', 'TIMESTAMP(6)', None, 'N')]

    """ Indices """
    d_indices = o_ziel.get_d_constraints()

    """ Grants auf Zieltabelle """
    v_query = "select 'GRANT '|| PRIVILEGE ||' ON ' || OWNER ||'.'|| TABLE_NAME ||' TO '|| GRANTEE " \
              "from user_tab_privs where owner='{0}' and table_name='{1}';".format(o_ziel.schema, o_ziel.table_name)
    v_result = oralib.sql_query(c_tgt, v_query)
    if len(v_result) > 0:
        grant_clause = v_result[0][0] + ";\n"
    else:
        grant_clause = ''

    header = "\n\n--DDL fuer Tabelle {0}.{1} created by kvods_compare.py\n" \
             "DROP TABLE \"{0}\".\"{1}\";\n" \
             "CREATE TABLE \"{0}\".\"{1}\" (\n\t".format(
                o_ziel.schema, o_ziel.table_name)
    body = []
    for row in cols_technisch:
        column = row[0]
        data_type = row[1]
        data_default = '' if row[2] is None else "DEFAULT {0}".format(row[2])
        null_value = '' if row[3] == 'Y' else 'NOT NULL ENABLE'
        body.append("{0} {1} {2} {3}\n".format(column, data_type, data_default, null_value))
    for row in cols_fachlich:
        column = row[0]
        data_type = row[1]
        data_default = '' if row[2] is None else "DEFAULT {0}".format(row[2])
        null_value = '' if row[3] == 'Y' else 'NOT NULL ENABLE'
        body.append("{0} {1} {2} {3}\n".format(column, data_type, data_default, null_value))
    if d_indices:
        body.extend(list(x for x in d_indices.values()))
    footer = "\n);\n{0}".format(grant_clause)
    ddl = "{0}{1}{2}".format(header, "\t,".join(body), footer)
    return ddl


desc = """Das Programm vergleicht in der Tabelle META_ENTITIES definierte Tabellenpärchen.  

Folgende Argumente sind zu übergeben:
1. Umgebung aus META_ENTITIES
2. DSN für Quelle
3. DSN für Ziel
Hinweis: Die DSN Namen stellen Schlüsselworte für eine Verbindung dar. Die konkreten Anmeldeinformationen 
werden über die seclib ermittelt.

Beispiele:
    python kvods_compare.py -env=KVSTAT_T -src=KVENTW2_T -tgt=KVSTAT_T
"""
options = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                  description=textwrap.dedent(desc))
options.add_argument("-env", "--environment", dest="environment", help="Umgebung aus META_ENTITES", default=None)
options.add_argument("-src", "--source", dest="source_connection", help="Name der Quelle", default=None)
options.add_argument("-tgt", "--target", dest="target_connection", help="Name des Ziels", default=None)

args = options.parse_args()
env = args.environment.upper()
src_conn = args.source_connection.upper()
tgt_conn = args.target_connection.upper()

log.info("Vergleiche Datenquelle {0} mit Datenziel {1} für Umgebung {2}".format(src_conn, tgt_conn, env))

""" Sonderfall 1: Zu überlesende, technische Felder definieren """
l_col_ignore = ['SID', 'LIV', 'LIB', 'LTV', 'LTB', 'TTV', 'TTB']

""" Dictionary d_entities füllen """
query = "select src_schema||'.'||src_table as quelle, tgt_schema||'.'||tgt_table as keys from meta_entities " \
        "where umgebung = '{0}' order by 1;".format(env)
d_entities = dict(oralib.sql_query(tgt_conn, query))

""" Pärchen aus Quell- und Zieltabelle miteinander vergleichen """
l_tables_to_check = []
for entry in d_entities.keys():
    log.info("Vergleiche Tabelle {0}.{1} mit {2}.{3}".format(
        entry.split(".")[0], entry.split(".")[1], d_entities[entry].split(".")[0], d_entities[entry].split(".")[1]))
    quelle = oralib.Table(src_conn, entry.split(".")[1], schema=entry.split(".")[0])
    ziel = oralib.Table(tgt_conn, d_entities[entry].split(".")[1], schema=d_entities[entry].split(".")[0])
    """ Prüfe auf Existenz """
    if len(quelle.column_list) == 0:
        log.info("Quelltabelle {0} wurde nicht gefunden.".format(entry))
        continue
    if len(ziel.column_list) == 0:
        log.info("Zieltabelle {0} wurde nicht gefunden.".format(d_entities[entry]))
        continue

    """ Metadatenfelder nicht betrachten """
    l_src_columns = quelle.column_list
    l_tgt_columns = list(x for x in ziel.column_list if x not in l_col_ignore)
    """ Unterschiede im Vorhandensein von Feldern oder in der Reihenfolge der Felder """
    if l_src_columns != l_tgt_columns:
        l_missing_in_src = list(x for x in l_tgt_columns if x not in l_src_columns)
        if len(l_missing_in_src) > 0:
            log.warning("Die folgenden Felder fehlen in der Quelle: {0}".format(l_missing_in_src))
        l_missing_in_tgt = list(x for x in l_src_columns if x not in l_tgt_columns)
        if len(l_missing_in_tgt) > 0:
            log.warning("Die folgenden Felder fehlen im Ziel: {0}".format(l_missing_in_tgt))
        l_tables_to_check.append("{0}.{1}".format(quelle.schema, quelle.table_name))
    elif l_src_columns == l_tgt_columns:
        """ Unterschiede in der Felddefinition von Quell- und Zielfeldern """
        l_data_types_diff = []
        for x in quelle.column_list:
            if quelle.d_col_data_type[x] != ziel.d_col_data_type[x]:
                log.warning("Datentypen weichen im Feld {0} ab:\nQuelltabelle: {1}\nZieltabelle: {2}".format(
                    x, quelle.d_col_data_type[x], ziel.d_col_data_type[x]))
                l_tables_to_check.append("{0}.{1}".format(quelle.schema, quelle.table_name))

log.info("Die folgenden Tabellen zeigen Abweichungen und müssen betrachtet werden:\n{0}".format(
    "\n".join(l_tables_to_check)))

""" Neuanlage der abweichenden Tabellen im Zielschema """
for quelltabelle in l_tables_to_check:
    log.info(f_generate_ddl(src_conn, tgt_conn, quelltabelle, d_entities[quelltabelle]))
