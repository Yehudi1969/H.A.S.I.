# -*- coding: utf8 -*-
###############################################################################
#                                  job_gen.py                                 #
###############################################################################
# Copyright (C) 2023  Jens Janzen
# Datum: 2020-04-29
# Change history:

# Libs
import argparse
import logging
import sys
import textwrap
import oralib

# ***********************
# ****  M  A  I  N  *****
# ***********************
# Evaluate arguments
desc = """Jobgenerierung für H.A.S.I. Extraktionsjobs aus DB Tabellen

Beispiel:
python job_gen.py \
-sys=LF \
-dsn=LEBENSTAT \
-sc=TLEBENLFDAT01 \
-dir="lf" \
-app=TESTLF \
-desc="Testjobs für Life Factory Leben" \
-p=AWS \
-sp=LIFE_FACTORY \
-cy=S \
-st=5000 \
-i=1 \
-ac=SFTP_AWS_SBX
"""

# Logging
log = logging.getLogger("job_gen")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

options = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                  description=textwrap.dedent(desc))
options.add_argument("-sys", "--source_system", dest="src_system", help="Name der Quelltabelle", type=str,
                     required=True)
options.add_argument("-dsn", "--data_source_name", dest="dsn", help="Datenquelle", type=str, required=True)
options.add_argument("-sc", "--schema", dest="schema", help="Datenbankschema", type=str, required=True)
options.add_argument("-dir", "--directory", dest="directory", help="Name des Zielverzeichnisses", required=True)
options.add_argument("-app", "--app_name", dest="app_name", help="Name der Applikation", type=str, required=True)
options.add_argument("-desc", "--description", dest="desc", help="Beschreibung der Applikation", type=str,
                     required=True)
options.add_argument("-p", "--project", dest="project", help="Name des Projekts", type=str, required=True)
options.add_argument("-sp", "--subproject", dest="subproject", help="Name der Teilprojekts", type=str, required=True)
options.add_argument("-cy", "--cycle", dest="run_cycle", help="Ladezyklus der Applikation", type=str, required=True)
options.add_argument("-st", "--start_number", dest="start_number", help="Nummer des ersten Jobs",
                     type=int, required=True)
options.add_argument("-i", "--interval", dest="interval", help="Intervall zwischen zwei Jobs", type=int, required=True)
options.add_argument("-ac", "--account", dest="account", help="AWS Zielaccount", type=str, required=True)

args = options.parse_args()
src_system = args.src_system
dsn = args.dsn.upper()
schema = args.schema.upper()
directory = args.directory
app_name = args.app_name
desc = args.desc
project = args.project
subproject = args.subproject
run_cycle = args.run_cycle
active = 1
start_number = args.start_number
interval = args.interval
tgt_system = args.account

log.info("Parameter:")
log.info("System: {0}".format(src_system))
log.info("Datenquelle: {0}".format(dsn))
log.info("Schema: {0}".format(schema))
log.info("Zielsystem: {0}".format(tgt_system))
log.info("Zielverzeichnis: {0}".format(directory))
log.info("Applikation: {0}".format(app_name))
log.info("Beschreibung: {0}".format(desc))
log.info("Projekt: {0}".format(project))
log.info("Teilprojekt: {0}".format(subproject))
log.info("Ladezyklus: {0}".format(run_cycle))
log.info("Startnummer: {0}".format(start_number))
log.info("Intervall: {0}".format(interval))

query = "select count(*) from THASI.OBJ_APPLICATION where app_name='{0}';".format(app_name)
result = oralib.sql_query("HASI", query)[0][0]
if result is False:
    sys.exit(1)
if result == 0:
    log.info("Generate application in table OBJ_APPLICATION")
    query = "INSERT INTO THASI.OBJ_APPLICATION (APP_NAME, APP_DESCRIPTION, PROJECT, SUBPROJECT, ACTIVE, RUN_CYCLE, " \
            "PRECEDING_APP, FOLLOWER_APP, VALID_FROM, VALID_TILL) VALUES " \
            "('{0}', '{1}', '{2}', '{3}', {4}, '{5}', '{0}', 'NO_DEPS', " \
            "current_date, to_date('9999-12-31', 'YYYY-MM-DD'));".format(
                app_name, desc, project, subproject, active, run_cycle)
    result = oralib.sql_query("HASI", query)
    log.info("Successful.")

""" Get table names from DSN. """
query = "select table_name from ALL_TABLES where owner='{0}' union " \
        "select view_name from ALL_VIEWS where owner='{0}';".format(schema)
result = oralib.sql_query(dsn, query)
l_tables = list(x[0] for x in result)
c_jobs = len(l_tables)

""" Generate job names """
statistic_job_nr = start_number
first_job = "PBI" + str(statistic_job_nr) + "A"
num = start_number + 1
num2 = start_number + 2
extraction_jobs = range(num, num + c_jobs * interval, interval)
upload_jobs = range(num2, num2 + c_jobs * interval, interval)
l_jobs_extraction = list(x for x in extraction_jobs)
l_jobs_upload = list(x for x in upload_jobs)

""" Insert start job """
log.info("Generate start job for collecting statistics.")
follower_job = "PBI" + str(l_jobs_extraction[0]) + "A"
query = "insert into OBJ_JOB (job_name, app_name, job_description, active, job_type, " \
        "preceding_job, follower_job, valid_from, valid_till) values " \
        "('{0}', '{1}', 'Joblaufzeiten ermitteln', 1, 'SHOW_APP_STATS','START','{2}', " \
        "current_date, to_date('9999-12-31', 'YYYY-MM-DD'));".format(first_job, app_name, follower_job)
result = oralib.sql_query("HASI", query)
log.info("Successful.")

""" Insert worker jobs for extraction """
for job_number, table_name in zip(l_jobs_extraction, l_tables):
    job_name = "PBI" + str(job_number) + "A"
    preceding_job = "PBI" + str(job_number - 1) + "A"
    follower_job = "PBI" + str(job_number + 1) + "A"
    filename = table_name.lower()
    query = "select count(1) from OBJ_JOB where job_name = '{0}'".format(job_name)
    result = oralib.sql_query("HASI", query)[0][0]
    if result == 1:
        log.warning("Job {0} already exists in repository table. Skipping.".format(job_name))
        continue
    query = "insert into OBJ_JOB (job_name, app_name, job_description, active, job_type, prog_name, prog_args, " \
            "src_dsn, src_obj, tgt_dsn, tgt_obj, preceding_job, follower_job, valid_from, valid_till) values " \
            "('{1}', '{2}', 'Datenabzug aus {3}', 1, 'PYTHON', '130_Table_to_aws.py', " \
            "'-sys={0} -conn={4} -sc={5} -tab={3} -f={6}.csv -tgt={7} -dir={8}', " \
            "'{5}', '{3}','S3','{6}.csv', '{9}', '{10}', current_date, to_date('9999-12-31', 'YYYY-MM-DD'));".format(
                src_system, job_name, app_name, table_name, dsn, schema,
                filename, tgt_system, directory, preceding_job, follower_job)
    result = oralib.sql_query("HASI", query)
    if result is False:
        sys.exit(1)
    log.info("Job {0} inserted successfully.".format(job_name))

""" Insert job for Logging"""
job_number += 1
job_name = "PBI" + str(job_number) + "A"
preceding_job = "PBI" + str(job_number - 1) + "A"
follower_job = "PBI" + str(job_number + 1) + "A"
query = "insert into OBJ_JOB (job_name, app_name, job_description, active, job_type, prog_name, prog_args, " \
        "src_dsn, src_obj, tgt_dsn, tgt_obj, preceding_job, follower_job, valid_from, valid_till) values " \
        "('{1}', '{2}', 'Logging der übertragenen Dateien', 1, 'PYTHON', '140_build_logfile.py', " \
        "'-sys={0} -tgt={3} -dir={4}', 'FILESYSTEM', '{6}_log.csv', 'S3', '{6}_log.csv', '{5}', '{6}', " \
        "current_date, to_date('9999-12-31', 'YYYY-MM-DD'));".format(
            src_system, job_name, app_name, tgt_system, directory, preceding_job, follower_job)
"""python 140_build_logfile.py -sys=CRM -tgt=SFTP_AWS_SBX -dir=crm/DEV"""
result = oralib.sql_query("HASI", query)

""" Insert job for Mailreport. """
preceding_job = job_name
job_name = follower_job
query = "insert into OBJ_JOB (job_name, app_name, job_description, active, job_type, " \
        "preceding_job, follower_job, valid_from, valid_till) values " \
        "('{0}', '{1}', 'Versand E-Mailreport', 1, 'MAILREPORT','{2}','END', " \
        "current_date, to_date('9999-12-31', 'YYYY-MM-DD'));".format(job_name, app_name, preceding_job)
result = oralib.sql_query("HASI", query)

log.info("Job chain {0} generated successfully.".format(app_name))
# Exit program
sys.exit(0)
