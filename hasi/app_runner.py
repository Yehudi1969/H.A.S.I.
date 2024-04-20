# -*- coding: utf8 -*-
###############################################################################
#                                app_runner.py                                #
###############################################################################
# Copyright (C) 2023  Jens Janzen

# Change history
# V1.0: 2019-04-26 - Initial Release

import argparse
import configparser
import logging
import os
import sys
import textwrap

# Globals
standard_encoding = "utf8"
hasi_root_directory = os.path.dirname(os.path.realpath(__file__))
os.chdir(hasi_root_directory)

parser = configparser.ConfigParser()
try:
    parser.read("config.ini")
except FileNotFoundError as e:
    print(e)
    raise

# Environment settings read from config.ini
os.environ["HASI_HOME"] = hasi_root_directory
env_config = {}
l_sections = [section for section in parser]
for s_section in l_sections:
    l_keys = [option for option in parser["{0}".format(s_section)]]
    for s_key in l_keys:
        env_config[s_key.upper()] = parser[s_section][s_key]
        if s_key.upper() == "PYTHONPATH":
            l_python_path = parser[s_section][s_key].split(":")
            sys.path.extend([x for x in l_python_path if x not in sys.path])
for s_key, s_value in env_config.items():
    os.environ[s_key] = s_value
    if s_key == "DSN":
        DSN = s_value
    elif s_key == "DB_BACKEND":
        db_backend = s_value.upper()
        """ Database Backend """
        if db_backend == "POSTGRESQL":
            import pglib as db
        elif db_backend == "ORACLE":
            import oralib as db
        elif db_backend == "DB2":
            import db2lib as db
        elif db_backend == "NETEZZA":
            import nzlib as db
    elif s_key == "PYTHONPATH":
        os.environ["PATH"] = env_config["PYTHONPATH"] + os.pathsep + os.environ["PATH"]
sys.path = list(set(sys.path))

""" Sets logging console handler for debugging """
log = logging.getLogger("SESSION")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)


class AppRunner(object):
    def __init__(self, app, b_force, b_ignore_errors):
        self.conn = DSN
        self.app = app
        self.b_force = b_force
        self.b_ignore_errors = b_ignore_errors
        if self.b_force:
            log.info("Forcing reset of application {0}.".format(self.app))
            query = "update obj_job_plan set status=0, status_text='PLANNED', ts_ins=systimestamp, " \
                    "ts_upd=null " \
                    "where app_name='{0}'".format(self.app)
            log.debug("{0}, {1}".format(self.conn, query))
            db.sql_query(self.conn, query)
        self.stmt = None
        query = "select job_name, preceding_job from OBJ_JOB_PLAN " \
                "where APP_NAME='{0}' order by job_name;".format(self.app)
        result = db.sql_query(self.conn, query)
        self.l_remain = []
        self.d_pred = {}
        for line in result:
            key = line[0]
            value = line[1].split(",")
            self.d_pred[key] = value
        log.debug("Jobs in Applikation {0}: ".format(self.app, [x for x in self.d_pred.keys()]))

        # Is the application already in status running?
        query = "select count(*) from OBJ_LOG_APPLICATION " \
                "where APP_NAME='{0}' and status='1' order by app_id;".format(self.app)
        result = db.sql_query(self.conn, query)
        restart = True if result[0][0] > 0 else False
        # Obtain jobs that are finished already
        if self.b_force is True:
            query = "select job_name from OBJ_JOB_PLAN " \
                    "where APP_NAME='{0}' and status in ('2', '3') order by job_name;".format(self.app)
        else:
            query = "select job_name from OBJ_JOB_PLAN " \
                    "where APP_NAME='{0}' and status='2' order by job_name;".format(self.app)
        jobs_finished = [x[0] for x in db.sql_query(self.conn, query)]
        log.debug("Jobs finished: {0}".format(jobs_finished))
        if restart:
            self.l_finished = jobs_finished
        elif len(jobs_finished) > 0:
            self.l_finished = jobs_finished
        else:
            self.l_finished = []
        self.l_remain = [x for x in self.d_pred.keys() if x not in self.l_finished]
        self.state = 1

    def run_subprocess(self, env=None):
        import subprocess
        import shlex
        env = os.environ.copy() if not env else env
        l_command = shlex.split(self.stmt)
        stmt = subprocess.Popen(l_command, stdin=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True, env=env)
        try:
            output, error = stmt.communicate()
        except OSError as error:
            print(error)
            return False
        if stmt.returncode != 0:
            print(error.decode(standard_encoding).strip())
        else:
            print(output.decode(standard_encoding).strip()) if output else None
        return stmt.returncode

    def job_start(self, job):
        log.info("Start Job: {0}".format(job))
        self.stmt = "python {0}/hasi.py -app={1} -job={2}".format(hasi_root_directory, self.app, job)
        result = self.run_subprocess()
        return job, result

    def next_action(self):
        l_start = []
        log.debug("Jobs queue: {0}".format(self.l_remain))
        log.debug("Jobs finished: {0}".format(self.l_finished))
        for job in self.l_remain:
            l_pred = [x for x in self.d_pred[job] if x not in self.l_finished]
            if self.d_pred[job] in self.l_finished:
                log.debug("Job {0} has already been finished.".format(job))
            elif len(l_pred) == 1 and l_pred[0] == 'START':
                log.debug("Startjob {0} in Application {1} found.".format(job, self.app))
                l_start.append(job)
            elif len(l_pred) > 0:
                log.debug("Job {0} has unfinished predecessors.".format(job))
            elif len(l_pred) == 0:
                l_start.append(job)
        if len(self.l_remain) > 0:
            from multiprocessing import Pool
            from multiprocessing import cpu_count
            num_proc = len(l_start) if len(l_start) <= cpu_count() else cpu_count()
            with Pool(processes=num_proc) as p:
                result = p.map(self.job_start, l_start)
                for item in result:
                    if not self.b_ignore_errors:
                        self.l_finished.append(item[0]) if item[1] == 0 else None
                        if item[1] != 0:
                            self.state = 0
                            return False
                    elif self.b_ignore_errors:
                        self.l_finished.append(item[0])
                self.l_remain = [x for x in self.d_pred.keys() if x not in self.l_finished]
        else:
            log.info("No job left to start. Make sure that jobs to be started are in status 'planned'.")
        self.state = 0 if len(self.l_remain) == 0 else 1


# *******************
# ***** M A I N *****
# *******************
# Program runs standalone when called directly by operating system
if __name__ == "__main__":
    # Evaluate arguments
    desc = """H.A.S.I. Application Runner:
    Programm wurde direkt aufgerufen.
    Benötigte Parameter:
    1. Name der Applikation
    Beispiel: python app_runner.py -app=<application name>
    """
    options = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                      description=textwrap.dedent(desc))
    options.add_argument("-app", "--app_name", dest="app_name", help="Name der TWS Applikaton", required=True)
    options.add_argument("-f", "--force", dest="force", help="Die Applikation wird zurückgesetzt ", type=int, default=0)
    options.add_argument("-ie", "--ignore_errors", dest="ignore_errors", help="Die Kette läuft weiter, wenn einzelne "
                                                                              "Jobs abbrechen", type=int, default=0)
    args = options.parse_args()

    # Create project instance
    app_name = args.app_name
    force: bool = True if args.force == 1 else False
    ignore_errors: bool = True if args.ignore_errors == 1 else False
    obj_app = AppRunner(app_name, force, ignore_errors)
    i_runs = 0
    while obj_app.state == 1:
        action = obj_app.next_action()
        if action is False:
            sys.exit(1)
    sys.exit(0)
# M A I N End
