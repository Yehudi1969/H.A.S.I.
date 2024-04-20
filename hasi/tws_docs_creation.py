# -*- coding: utf8 -*-
################################################################################
#                              tws_docs_creation.py                            #
################################################################################
# Copyright (C) 2023  Jens Janzen

# Change history
# V1.0: 2019-04-26 - Initial Release

# Import standard python libs
import argparse
import logging
import os
import shutil
import sys
import textwrap
import oralib

# Description
"""
Program generates start scripts as destination executables for TWS job control as well as JCL and Prose documents.
Example: python tws_docs_creation -p=CORE
"""

""" Sets logging console handler for debugging """
log = logging.getLogger("projectaction")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)


# Functions
def check_directory(directory):
    if not os.path.exists(directory):
        log.info("Directory {0} missing. Try to create it...".format(directory))
        os.makedirs(directory)
        os.chmod(directory, 0o770)


""" Constants """
standard_encoding = "utf8"
default_name = "Jens"
default_family_name = "Janzen"
default_title = "Herr"
default_gender = "M"
default_email = "jens.janzen@devk.de"

# *********************************
# ************ M A I N ************
# *********************************
# Evaluate arguments
desc = """Script creates job, jcls and prose scripts for TWS jobcontrol.
The jobscripts stay in the job directory, while the JCL and Prose files must be inserted in host
libraries AZOP.TWS.KPWC.SERVER.JOBLIB and AZOP.TWS.KPWC.PROSE as members during production planning.

Since access to these production libraries is restricted, there are libraries for preparation that can
be used to save these information. These libraries are related to working area.
Use for Business Intelligence:
AZOP.KAR.KB.CNTL
AZOP.KAR.KB.PROSE
Use for Prolog:
AZOP.KAR.PL.CNTL
AZOP.KAR.PL.PROSE
Use for Data Warehouse (old)
AZOP.KAR.DW.CNTL
AZOP.KAR.DW.PROSE

All Applications and jobs found in H.A.S.I. Repository will be processed to the file system.
A joblist is created and checked for already assigned jobs in file system.
Existing jobfiles won't be overwritten.
To generate all scripts from scratch, existing files have to be deleted before script runs.

Needed arguments:
1. Name of the project
2. Action: Create (default), Delete

Beispiel: python tws_docs_creation -p=DWH
"""
options = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                  description=textwrap.dedent(desc))
options.add_argument("-p", "--project", dest="project", help="Name of the project", metavar="project name",
                     type=str, required=True)
options.add_argument("-a", "--action", dest="action", help="Action: Create/Delete", metavar="c/d",
                     type=str, default='c')
args = options.parse_args()
project_name = args.project.upper()
action = args.action.upper()
conn = "TDM_HASI"

# Lookup project parameters and check directories
homedir = "{0}".format(os.environ["HASI_HOME"][:-5])
query = "select value_text from obj_att_project where project = '{0}' and key_text = 'PROJECTDIR';".format(project_name)
projectdir = oralib.sql_query(conn, query)[0][0]
query = "select value_text from obj_att_project where project = '{0}' and key_text = 'SRCDIR';".format(project_name)
srcdir = oralib.sql_query(conn, query)[0][0]
query = "select value_text from obj_att_project where project = '{0}' and key_text = 'TGTDIR';".format(project_name)
tgtdir = oralib.sql_query(conn, query)[0][0]
query = "select value_text from obj_att_project where project = '{0}' and key_text = 'LOGDIR';".format(project_name)
logdir = oralib.sql_query(conn, query)[0][0]

log.info("Homedir: {0}".format(homedir))
log.info("Projectdir: {0}".format(projectdir))
log.info("Srcdir: {0}".format(srcdir))
log.info("Tgtdir: {0}".format(tgtdir))
log.info("Logdir: {0}".format(logdir))
log.info("Src Archiv: {0}/ARCHIV".format(srcdir))
log.info("Log Archiv: {0}/ARCHIV".format(logdir))
log.info("BadFiles: {0}/BadFiles/{1}".format(homedir, project_name))
log.info("SessLogs: {0}/SessLogs/{1}".format(homedir, project_name))
log.info("LkpFiles: {0}/LkpFiles/{1}".format(homedir, project_name))
log.info("WorkflowLogs: {0}/WorkflowLogs/{1}".format(homedir, project_name))
# log.info("Control-M Job directory: {0}/jobs".format(homedir))
# log.info("Control-M JCL directory: {0}/jobs/jcl".format(homedir))
# log.info("Control-M Prose directory: {0}/jobs/prose".format(homedir))

if action == "C":
    check_directory(projectdir)
    check_directory(srcdir)
    check_directory(tgtdir)
    check_directory(logdir)
    check_directory("{0}/ARCHIV".format(srcdir))
    check_directory("{0}/ARCHIV".format(logdir))
    check_directory("{0}/BadFiles/{1}".format(homedir, project_name))
    check_directory("{0}/SessLogs/{1}".format(homedir, project_name))
    check_directory("{0}/LkpFiles/{1}".format(homedir, project_name))
    check_directory("{0}/WorkflowLogs/{1}".format(homedir, project_name))
    # check_directory("{0}/jobs".format(homedir))
    # check_directory("{0}/jobs/jcl".format(homedir))
    # check_directory("{0}/jobs/prose".format(homedir))
elif action == "D":
    log.info("Delete project directories and files...")
    shutil.rmtree(projectdir, ignore_errors=True)
    shutil.rmtree(srcdir, ignore_errors=True)
    shutil.rmtree(tgtdir, ignore_errors=True)
    shutil.rmtree("{0}/BadFiles/{1}".format(homedir, project_name), ignore_errors=True)
    shutil.rmtree("{0}/SessLogs/{1}".format(homedir, project_name), ignore_errors=True)
    shutil.rmtree("{0}/LkpFiles/{1}".format(homedir, project_name), ignore_errors=True)
    shutil.rmtree("{0}/WorkflowLogs/{1}".format(homedir, project_name), ignore_errors=True)
    log.info("Done.")
    sys.exit(0)

# Check obj_customer_email table
query = "select count(*) from obj_customer_email where project='{0}';".format(project_name)
result = oralib.sql_query(conn, query)[0][0]
if result == 0:
    query = "select distinct subproject from obj_application where project='{0}'".format(project_name)
    result = oralib.sql_query(conn, query)
    l_subprojects = list(x[0] for x in result)
    for subproject_name in l_subprojects:
        query = "insert into obj_customer_email (project, subproject, forename, name, title, email, gender, active) " \
                "values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', 1)".format(
                 project_name, subproject_name, default_name, default_family_name,
                 default_title, default_email, default_gender)
        result = oralib.sql_query(conn, query)
        log.info(result)

# Lookup H.A.S.I. program destination
query = "SELECT value_text FROM obj_att_project WHERE project = 'HASI' AND key_text = 'ROOTDIR';"
hasi_root_directory = oralib.sql_query(conn, query)[0][0]

# Lookup job directory in repository
query = "SELECT value_text FROM obj_att_project WHERE project = 'CONTROL-M' AND key_text = 'JOBDIR';"
jobdir = oralib.sql_query(conn, query)[0][0]

# Lookup applications for project
query = "select app_name from obj_application where project = '{0}';".format(project_name)
result = oralib.sql_query(conn, query)
if len(result) == 0:
    log.info("No applications were found for project {0}.".format(project_name))
    sys.exit(0)
l_apps = list(x[0] for x in result)
log.info(l_apps)

# Iterate over application list
for i in l_apps:
    # Generate TWS user name from application name
    tws_user_name = "KOPC{}".format(i[1:3])
    # Lookup jobs from repository
    query = "select job_name from obj_job where app_name = '{0}' order by 1;".format(i)
    result = oralib.sql_query(conn, query)
    if len(result) == 0:
        log.info("No jobs were found in application {}.".format(i))
        continue
    l_jobs = list(x[0] for x in result)
    log.info(l_jobs)

    # Iterate over job list
    for j in l_jobs:
        query = "select job_description from obj_job where app_name = '{0}' and job_name = '{1}';".format(
            i, j)
        job_description = oralib.sql_query(conn, query)[0][0]
        log.info(job_description)

# Jobscripts werden nicht mehr gebraucht, H.A.S.I. wird mit Applikations- und Jobnamen aufgerufen.

#         # Check whether start script already exists
#         if os.path.exists("{0}/{1}".format(jobdir, j)):
#             log.info("Start script for application {0}, job {1} already exists. "
#                   "If you want it to be recreated, please delete it and restart program.".format(i, j))
#             continue

#         # Write job script
#         with open("{0}/{1}".format(jobdir, j), 'w') as fh:
#             job_template = """#!/bin/ksh
# # Jobtemplate für HOST JCL auf Unix
# # {0}, Jens Janzen, Business Intelligence DEVK
# # Application Name: {1}
# # Jobname: {2}
#
# python {3}/py -app={1} -job={2}
# RC=$?
# exit $RC
# """.format(time.strftime("%Y-%m-%d"), i, j, hasi_root_directory)
#             fh.write(job_template)
#             os.chmod("{0}/{1}".format(jobdir, j), 0o750)

# HOST Dokumente werden nicht mehr gebraucht, Control-M wird von Kollegen gepflegt.

#         # Write JCL for TWS library
#         with open("{0}/jcl/{1}".format(jobdir, j), 'w') as fh:
#             jcl_template = """      //*%OPC SCAN
#       ##HEADER
#       #JOBNAME=&OJOBNAME
#       SYSID=   &OWSID
#       USER=    {1}
#       CLIENT=  999
#       ##END
#       #############
#       # THE STEPS #
#       #############
#       # {2}
#       ##EXTSTEP
#       PROGRAM={0}
#       ##END
# """.format(j, tws_user_name, job_description)
#             fh.write(jcl_template)

#         # Write prose for TWS library
#         with open("{0}/prose/{1}".format(jobdir, j), 'w') as fh:
#             header = """Location   :{0}/KB/BATCH
#       **********************************************************************\n""".format(j)
#             query = "select prose, alert_action_1, alert_action_2 from obj_job where job_name = '{0}';".format(j)
#             result = oralib.sql_query(conn, query)
#             log.info(result[0])
#             s_result = " ".join(list(x for x in result[0] if x is not None))
#             log.info(s_result)
#             prose_text = textwrap.wrap(s_result, width=66)
#             footer = """**********************************************************************"""
#             fh.write("      " + header)
#             for line in prose_text:
#                 if len(line) < 66:
#                     line += ' ' * (66 - len(line))
#                 fh.write("      * {0} *\n".format(line))
#             fh.write("      " + footer)

sys.exit(0)
