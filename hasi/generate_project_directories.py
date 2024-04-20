# -*- coding: utf8 -*-
################################################################################
#                              tws_docs_creation.py                            #
################################################################################
# Copyright (C) 2023  Jens Janzen

# Change history
# V1.0: 2021-12-07 - Initial Release

# Import standard python libs
import argparse
import logging
import os
import shutil
# import sys
import textwrap
import oralib

# Description
"""
Program generates start scripts as destination executables for TWS job control as well as JCL and Prose documents.
Example: python generate_project_directories.py -p=CORE
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
desc = """Script creates project directories in ETL server.
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

# Lookup project parameters and check directories
homedir = "{0}".format(os.environ["HASI_HOME"][:-5])
query = "select value_text from obj_att_project where project = '{0}' and key_text = 'PROJECTDIR';".format(project_name)
projectdir = oralib.sql_query("HASI", query)[0][0]
query = "select value_text from obj_att_project where project = '{0}' and key_text = 'SRCDIR';".format(project_name)
srcdir = oralib.sql_query("HASI", query)[0][0]
query = "select value_text from obj_att_project where project = '{0}' and key_text = 'TGTDIR';".format(project_name)
tgtdir = oralib.sql_query("HASI", query)[0][0]
query = "select value_text from obj_att_project where project = '{0}' and key_text = 'LOGDIR';".format(project_name)
logdir = oralib.sql_query("HASI", query)[0][0]

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

# Check obj_customer_email table
query = "select count(*) from obj_customer_email where project='{0}';".format(project_name)
result = oralib.sql_query("HASI", query)[0][0]
if result == 0:
    query = "select distinct subproject from obj_application where project='{0}'".format(project_name)
    result = oralib.sql_query("HASI", query)
    l_subprojects = list(x[0] for x in result)
    for subproject_name in l_subprojects:
        query = "insert into obj_customer_email (project, subproject, forename, name, title, email, gender, active) " \
                "values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', 1)".format(
                 project_name, subproject_name, default_name, default_family_name,
                 default_title, default_email, default_gender)
        result = oralib.sql_query("HASI", query)
        log.info(result)
