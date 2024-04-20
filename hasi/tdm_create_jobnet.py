# -*- coding: utf8 -*-
###############################################################################
#                              tdm_create_jobnet.py                           #
###############################################################################
# Copyright (C) 2023  Jens Janzen
# Datum: 2021-08-20

import argparse
import functools
import locale
import textwrap
import logging
import os
import oralib as db
import pandas as pd
import seclib
import sys
import sqlalchemy

""" Sets logging console handler for debugging """
log = logging.getLogger("tdm_jobnet")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)


def lookup_applications(app=None):
    l_applications = []
    if app:
        l_applications.append(app)
    else:
        v_query = "select app_name from obj_application where project='{0}' " \
                  "order by app_name;".format(project)
        l_applications = [x[0] for x in db.sql_query(conn_meta, v_query)]
    return l_applications


def create_applications():
    """ Lege für ein Projekt eine Applikation pro Anwendung an, sofern noch nicht vorhanden """
    stmt = "select cast(substr(app_name, instr(app_name, '_',-1,1)+1) as integer) " \
           "from obj_application where project='{0}' order by app_name;".format(project)
    v_result = db.sql_query(conn_meta, stmt)
    if len(v_result) == 0:
        v_next = 0
    else:
        l_app_numbers = list(x[0] for x in v_result)
        l_app_numbers.sort()
        l_range = list(x for x in range(1, l_app_numbers[-1] + 1))
        l_free_numbers = list(x for x in l_range if x not in l_app_numbers)
        if len(l_free_numbers) == 0:
            v_next = l_app_numbers[-1] + 1
        elif len(l_app_numbers) == 0:
            v_next = 0
        else:
            v_next = l_free_numbers[0]
    if v_next == 0:
        stmt = "insert into obj_application " \
               "(APP_NAME, APP_DESCRIPTION, PROJECT, SUBPROJECT, ACTIVE, RUN_CYCLE, PRECEDING_APP, FOLLOWER_APP) " \
               "with repo as " \
               "(select '{0}' || '_' || lpad(to_char(row_number() over(order by anwendung)), 4, '0') as app_name" \
               ", anwendung, count(*) from tdm_mdm_tabellenrepository " \
               "where anwendung in (" \
               "select distinct a.anwendung from tdm_mdm_tabellenrepository a " \
               "left join " \
               "(select subproject from obj_application where project='{0}') b " \
               "on a.anwendung = b.subproject " \
               "where b.subproject is null) group by anwendung) " \
               "select app_name, 'Lauf {0} für '|| anwendung as app_description, '{0}' project, " \
               "anwendung subproject, 1 active, 'S' run_cycle, app_name preceding_app, 'NO_DEPS' follower_app " \
               "from repo;".format(project)
        v_result = db.sql_query(conn_meta, stmt)
        log.info("Applikationen für Projekt {0} "
                 "mit allen in TDM_MDM_TABELLENREPOSITORY ermittelten Anwendungen angelegt.".format(project))
    else:
        stmt = "create sequence {0} start with {1} increment by 1 nocache nocycle; " \
               "insert into obj_application " \
               "(APP_NAME, APP_DESCRIPTION, PROJECT, SUBPROJECT, ACTIVE, RUN_CYCLE, PRECEDING_APP, FOLLOWER_APP) " \
               "with repo as (select '{0}' || '_' as app_name, anwendung, count(*) from tdm_mdm_tabellenrepository " \
               "where anwendung in (select distinct a.anwendung from tdm_mdm_tabellenrepository a " \
               "left join obj_application b on a.anwendung = b.subproject " \
               "where b.subproject is null) group by anwendung) " \
               "select app_name || lpad({0}.nextval, 4, '0'), 'Lauf {0} für '|| anwendung as app_description, " \
               "'{0}' project, anwendung subproject, " \
               "1 active, 'S' run_cycle, app_name preceding_app, 'NO_DEPS' follower_app " \
               "from repo; " \
               "drop sequence {0};".format(project, v_next)
        v_result = db.sql_query(conn_meta, stmt)
        log.info("Fehlende Applikationen für Projekt {0} "
                 "mit allen in TDM_MDM_TABELLENREPOSITORY ermittelten Anwendungen angelegt.".format(project))
    return v_result


def create_jobnet(app):
    """ Erzeugt ein Jobnetz für die übergebene Applikation in der Tabelle OBJ_JOB. """
    stmt = "select count(*) from (" \
           "select a.anwendung, a.tabelle from tdm_mdm_tabellenrepository a " \
           "join obj_application b on a.anwendung=b.subproject and b.app_name='{1}') a " \
           "left join (select a.subproject, b.job_description from obj_application a, obj_job b " \
           "where a.app_name=b.app_name and a.project='{0}' and a.app_name='{1}') b " \
           "on a.anwendung=b.subproject and a.tabelle=b.job_description " \
           "where b.job_description is null;".format(project, app)
    v_result = db.sql_query(conn_meta, stmt)[0][0]
    if v_result == 0:
        log.info("Application {0} for project {1} has no missing jobs. Skipping.".format(app, project))
        return True
    stmt = "select max(to_number(substr(job_name, -6))) +1 start_number " \
           "from obj_job where to_number(substr(job_name, -6)) < 900000 " \
           "and substr(job_name, 1, 4) = '{0}';".format(job_prefix)
    v_result = list(x[0] for x in db.sql_query(conn_meta, stmt))
    first_job = v_result[0] if v_result[0] is not None else 1
    stmt = "select subproject from obj_application where app_name='{0}';".format(app)
    subproject = list(x[0] for x in db.sql_query(conn_meta, stmt))[0]
    if len(subproject) == 0:
        log.warning("Application {0} contains no entities and is skipped.")
        return True
    stmt = "select tabelle from tdm_mdm_tabellenrepository where anwendung='{0}' order by tabelle;".format(subproject)
    ref_entities = list(x[0] for x in db.sql_query(conn_meta, stmt))
    stmt = "select job_description from obj_job where app_name='{0}';".format(app)
    job_entities = list(x[0] for x in db.sql_query(conn_meta, stmt))
    l_entities = sorted(list(set(ref_entities).difference(set(job_entities))),
                        key=functools.cmp_to_key(locale.strcoll))
    l_job_numbers = list("{:06d}".format(x) for x in range(first_job,
                                                           len(l_entities) * job_interval + first_job, job_interval))
    l_follower = list("{:06d}".format(x) for x in range(first_job + 1,
                                                        len(l_entities) * job_interval + first_job + 1, job_interval))
    l_predecessor = list("{:06d}".format(x) for x in range(first_job - 1,
                                                           len(l_entities) * job_interval + first_job, job_interval))
    l_header = ["APP_NAME", "JOB_NAME", "JOB_DESCRIPTION", "ACTIVE", "JOB_TYPE",
                "PROG_NAME", "PROG_ARGS", "PRECEDING_JOB", "FOLLOWER_JOB"]
    data = []
    for job_number, tabelle, preceding_job, follower_job in zip(l_job_numbers, l_entities, l_predecessor, l_follower):
        row = ["{0}".format(app),
               "{0}{1}".format(job_prefix, job_number),
               "{0}".format(tabelle),
               1,
               "MAPPING",
               None,
               None,
               "{0}{1}".format(job_prefix, preceding_job),
               "{0}{1}".format(job_prefix, follower_job)]
        data.append(row)
    """ Mailreport erhält Nummer oberhalb von 9000000. """
    stmt = "select max(to_number(substr(job_name, -6))) +1 start_number " \
           "from obj_job where to_number(substr(job_name, -6)) >= 900000 " \
           "and substr(job_name, 1, 4) = '{0}';".format(job_prefix)
    v_result = list(x[0] for x in db.sql_query(conn_meta, stmt))
    report_job = v_result[0] if v_result[0] is not None else 900001
    """ Setze letzten Job als Vorläufer für Mailreport """
    preceding_job = data[-1][1]
    row = ["{0}".format(app), "{0}{1}".format(job_prefix, report_job),
           "Erstelle Report für Applikation  {0}".format(app), 1, "MAILREPORT", None, None,
           "{0}".format(preceding_job), "END"]
    data.append(row)
    df = pd.DataFrame(data, columns=l_header)
    if df.empty:
        log.info("Application {0} with subproject {1} contains no tables to process.".format(app, subproject))
        return True
    """ Erster Job wird als Startjob markiert. """
    df.at[0, 'PRECEDING_JOB'] = "START"
    """ Aktualisiere Vorläufer """
    last_index = len(df.index) - 2
    df.at[last_index, "FOLLOWER_JOB"] = "{0}{1}".format(job_prefix, report_job)
    df.to_sql('obj_job', engine, schema='TTDMETLREFDATA', if_exists='append', index=False)
    """ Jobnetz als Grafik ausgeben """
    log.info("Plotting application {0} - {1} to PNG file.".format(app, subproject))
    plot_app(app_name, subproject, 'png')
    return True


def create_mappings(app):
    stmt = "delete from obj_mapping where app_name='{0}';".format(app)
    db.sql_query(conn_meta, stmt)
    stmt = "insert into obj_mapping (APP_NAME, JOB_NAME" \
           ", SRC_TYPE, SRC_DSN, SRC_SCHEMA, SRC_OBJ, SRC_BUSINESS_KEY" \
           ", FIL_TYPE, FIL_DSN, FIL_SCHEMA, FIL_OBJ, FIL_BUSINESS_KEY" \
           ", TGT_TYPE, TGT_DSN, TGT_SCHEMA, TGT_OBJ, TGT_BUSINESS_KEY" \
           ", RULESET_ID)" \
           "with job as (" \
           "select a.project, a.subproject, a.app_name, b.job_name, c.object" \
           ", c.string1 src_type, c.string2 src_dsn, c.string3 src_schema, c.string4 src_obj" \
           ", c.string5 src_business_key" \
           ", d.string1 fil_type, d.string2 fil_dsn, d.string3 fil_schema, d.string4 fil_obj" \
           ", d.string5 fil_business_key" \
           ", e.string1 tgt_type, e.string2 tgt_dsn, e.string3 tgt_schema, e.string4 tgt_obj" \
           ", e.string5 tgt_business_key " \
           ", case " \
           "when f.verarbeitungstyp = 'KOPIEREN' and f.flg = 0 then 7 " \
           "when f.verarbeitungstyp = 'REDUZIEREN' and f.flg = 0 then 8 " \
           "when f.verarbeitungstyp = 'LEEREN' then 9 " \
           "when f.verarbeitungstyp = 'IGNORIEREN' then 10 " \
           "when f.verarbeitungstyp = 'SONDERVERARBEITUNG' and f.flg = 0 then 11 " \
           "when f.verarbeitungstyp = 'KOPIEREN' and f.flg = 1 then 12 " \
           "when f.verarbeitungstyp = 'REDUZIEREN' and f.flg = 1 then 13 " \
           "end as ruleset_id " \
           "from obj_application a " \
           "join obj_job b on a.app_name=b.app_name " \
           "join obj_parameter c on a.project=c.project and a.subproject=c.subproject " \
           "and b.job_description=c.object and c.parameter='SOURCE' " \
           "join obj_parameter d on a.project=d.project and a.subproject=d.subproject " \
           "and b.job_description=d.object and d.parameter='FILTER' " \
           "join obj_parameter e on a.project=e.project and a.subproject=e.subproject " \
           "and b.job_description=e.object and e.parameter='TARGET' " \
           "join (select repo.anwendung, repo.tabelle, repo.verarbeitungstyp " \
           ", case when regel.tabelle is null then 0 " \
           "when regel.tabelle is not null then 1 end as flg " \
           "from tdm_mdm_tabellenrepository repo left join (" \
           "select distinct anwendung, tabelle from tdm_mdm_maskierung_zuordnung" \
           ") regel on repo.anwendung=regel.anwendung and repo.tabelle=regel.tabelle " \
           "where repo.tdm_bis = to_date('9999-12-31 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) f " \
           "on a.subproject=f.anwendung and c.object=f.tabelle" \
           ")" \
           "select a.app_name, a.job_name" \
           ", a.src_type, a.src_dsn, a.src_schema, a.src_obj, a.src_business_key" \
           ", a.fil_type, a.fil_dsn, a.fil_schema, a.fil_obj, a.fil_business_key" \
           ", a.tgt_type, a.tgt_dsn, a.tgt_schema, a.tgt_obj, a.tgt_business_key" \
           ", a.ruleset_id " \
           "from job a " \
           "join tdm_mdm_tabellenrepository b on a.subproject=b.anwendung " \
           "and a.object=b.tabelle " \
           "left join obj_mapping d " \
           "on a.job_name=d.job_name " \
           "where b.tdm_bis = to_date('9999-12-31 00:00:00', 'YYYY-MM-DD HH24:MI:SS') " \
           "and d.job_name is null;".format(app)
    v_result = db.sql_query(conn_meta, stmt)
    stmt = "select subproject from obj_application where app_name='{0}';".format(app)
    subproject = list(x[0] for x in db.sql_query(conn_meta, stmt))[0]
    """ Jobnetz als Grafik ausgeben """
    log.info("Plotting application {0} - {1} to PNG file.".format(app, subproject))
    plot_app(app_name, subproject, 'png')
    return v_result


def insert_report_job(app):
    stmt = "select max(job_name) from obj_job where app_name='{0}';".format(app)
    last_job = db.sql_query(conn_meta, stmt)[0][0]
    report_job = "TDM_{:06d}".format(int(last_job[4:]) + 2)
    next_offset = int(last_job[4:]) + 4
    stmt = "update obj_job set preceding_job='START' " \
           "where job_name = (select min(job_name) from obj_job where app_name='{0}');".format(app)
    db.sql_query(conn_meta, stmt)
    stmt = "insert into obj_job (job_name, app_name, job_description, active, job_type, prog_name, " \
           "prog_args, preceding_job, follower_job) values (" \
           "'{1}', '{0}', 'E-Mail Report', 1, 'MAILREPORT', '', '', '{2}', 'END'" \
           ");".format(app, report_job, last_job)
    db.sql_query(conn_meta, stmt)
    return next_offset


def plot_app(app, subproject, file_format):
    from graphviz import Digraph
    digraph_scriptname = "{0}/{1}".format(path, app)
    output_filename = "{0}/{1}.{2}".format(path, app, file_format)
    stmt = "select job_name, preceding_job, active from obj_job where app_name='{0}';".format(app)
    v_result = db.sql_query(conn_meta, stmt)
    dot = Digraph(name="{0}".format(app), filename=digraph_scriptname)
    dot.format = file_format
    dot.graph_attr['label'] = "Anwendung: {0} - Applikation: {1}".format(subproject, app)
    dot.graph_attr['rankdir'] = 'LR'
    """ Create nodes """
    for x in v_result:
        name = x[0]
        active = x[2]
        if active == 0:
            dot.attr('node', fillcolor='lightgrey')
        dot.node(name="{0}".format(name), shape='box', style='filled')
    """ Create arrows with column preceding_job """
    for x in v_result:
        name = x[0]
        predecessor = x[1].split(",")
        if len(predecessor) == 0 or predecessor[0] == "START":
            pass
        elif len(predecessor) == 1:
            dot.edge(predecessor[0], name)
        elif len(predecessor) > 1:
            for y in predecessor:
                dot.edge(y, name)
    dot.render()
    os.remove(digraph_scriptname) if os.path.exists(digraph_scriptname) else None
    return output_filename


# ***********************
# ****  M  A  I  N  *****
# ***********************
desc = """Das Programm erzeugt Jobketten für TDM.
Folgende Argumente sind zu übergeben:
1. PROJEKTNAME - Eintrag für die Ermittlung von Parametern in der OBJ_ATT_PROJECT
Hinweis: Die DSN Namen stellen Schlüsselworte für eine Verbindung dar. Die konkreten Anmeldeinformationen 
werden über die seclib ermittelt.
Beispiele:
    python tdm_create_jobnet.py -p=VPROD2SITU
"""

options = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                  description=textwrap.dedent(desc))
options.add_argument("-p", "--project", dest="project", help="Projektname für die Jobketten", required=True)
options.add_argument("-app", "--application", dest="application", help="Existierende Applikation", default=None)
options.add_argument("-ca", "--create_apps", dest="FLG_CA", help="Flag zum Erstellen von Applikationen",
                     type=int, default=0)
options.add_argument("-cj", "--create_jobs", dest="FLG_CJ", help="Flag zum Erstellen von Jobs", type=int, default=0)
options.add_argument("-cm", "--create_mappings", dest="FLG_CM", help="Flag zum Erstellen von Mappings",
                     type=int, default=0)
options.add_argument("-pre", "--job_prefix", dest="job_prefix", help="Alphanumerischer Teil des Jobnamens",
                     type=str, default="TDM_")
options.add_argument("-i", "--job_interval", dest="job_interval",
                     help="Abstände zwischen dem numerischen Teil des Jobnamens", type=int, default=1)

args = options.parse_args()
project = args.project
application = args.application
flg_create_apps = True if args.FLG_CA == 1 else False
flg_create_jobs = True if args.FLG_CJ == 1 else False
flg_create_mappings = True if args.FLG_CM == 1 else False
job_prefix = args.job_prefix.upper()
job_interval = int(args.job_interval)

""" Directories """
HOME = os.environ['HOME'] = os.path.expanduser('~')
if os.getenv("HASI_HOME"):
    path = os.environ["HASI_HOME"]
else:
    path = "{0}/data".format(HOME)
sqlldr_path = path.replace("\\", "/")

""" DB Connection """
conn_meta = "TTDMETLREFDATA_TDM_P"
conn_pd = seclib.get_credentials(conn_meta)
engine = sqlalchemy.create_engine('oracle+cx_oracle://{0}:{1}@{2}'.format(conn_pd[1], conn_pd[2], conn_pd[0]))

""" Erstellen von Applikationen für ein Projekt """
if flg_create_apps is True:
    success = create_applications()
    if success is False:
        log.error("Programmehler aufgetreten.")
        sys.exit(1)

""" Erstellen von Jobs für alle Applikationen eines Projekts """
if flg_create_jobs is True:
    l_app_names = lookup_applications(app=application)
    for app_name in l_app_names:
        """ Jobnetz anlegen """
        log.info("Creating jobnet for application {0}".format(app_name))
        success = create_jobnet(app_name)
        if success is False:
            log.error("Programmehler aufgetreten.")
            sys.exit(1)

""" Erstellen von Mappings pro Job, falls in der Parametertabelle ein Eintrag für die Tabelle existiert """
if flg_create_mappings is True:
    l_app_names = lookup_applications(app=application)
    for app_name in l_app_names:
        """ Mapping erzeugen """
        log.info("Creating mappings for application {0}".format(app_name))
        success = create_mappings(app_name)
        log.info(success)
        if success is False:
            log.error("Programmehler aufgetreten.")
            sys.exit(1)
