# -*- coding: utf8 -*-
################################################################################
#               Highly Available Steering and Information System               #
################################################################################
#   Copyright (C) 2024  Jens Janzen
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <https://www.gnu.org/licenses/>.

""" Import standard python modules """
import argparse
import configparser
import datetime

from dateutil.relativedelta import *
import getpass
import glob
import maplib
import os
import shutil
import sys
import textwrap
import time
import seclib
import unixlib

""" Globals """
standard_encoding = "utf8"
hasi_root_directory = os.path.dirname(os.path.realpath(__file__))
os.chdir(hasi_root_directory)

parser = configparser.ConfigParser()
try:
    parser.read("config.ini")
except FileNotFoundError as e:
    print(e)
    raise

""" Environment settings read from config.ini """
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
    elif s_key == "CUSTOMER":
        customer = s_value
sys.path = list(set(sys.path))

"""
Description:
H.A.S.I. is a job scheduling and reporting application.
Registering session needs 2 arguments: Application name and Job name.
Example: python hasi.py -app=<application name> -job=<job_name>
"""


# Functions
def convert_duration(s):
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    return "{:02d}:{:02d}:{:02d}".format(h, m, s)


# Classes
class HasiApplication(object):
    """ Class for H.A.S.I. application.
    Attributes:
        host_name - Host where H.A.S.I. Service runs
        hasi_root_directory - Root directory of application
        user_name - name of the user logged in
        __cred - Credentials for repository connection
        app_logger - Application Logging with Python logging module
        environment - runtime environment (DEV, QA, PROD)
    """

    def __init__(self, v_app_name):
        """
        :param v_app_name:
        """

        """ Arguments """
        self.app_name = v_app_name

        """ Predefined variables """
        global hasi_root_directory
        global customer
        global DSN

        """ Hostname """
        import socket
        self.host_name = socket.gethostname()

        """ H.A.S.I. Root directory"""
        self.hasi_root_directory = hasi_root_directory

        """ Customer Company """
        self.customer = customer
        """ User """
        self.user_name = getpass.getuser()

        """ Setup logging for application """
        import logging
        self.app_logger = logging.getLogger(__name__)
        self.app_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.app_logger.setLevel(logging.DEBUG)
        self.app_file_handler = logging.FileHandler("{0}/hasi.{1}.log".format(self.hasi_root_directory, __name__))
        self.app_file_handler.setLevel(logging.INFO)
        self.app_file_handler.setFormatter(self.app_formatter)
        self.app_logger.addHandler(self.app_file_handler)
        self.app_console_handler = logging.StreamHandler()
        self.app_console_handler.setLevel(logging.INFO)
        self.app_console_handler.setFormatter(self.app_formatter)
        self.app_logger.addHandler(self.app_console_handler)

        """ Credentials for repository access """
        self.__cred = seclib.get_credentials(DSN)
        self.check_app_status("HASI", -9009) if self.__cred is False else None

        """ Check Environment """
        os.chdir(self.hasi_root_directory)
        query = "select value_text from obj_att_project where project = 'ALL' " \
                "and key_text = 'ENVIRONMENT';"
        result = db.sql_query(DSN, query)
        self.check_app_status('HASI', -9010) if not result else None
        self.environment = result[0][0]
        self.app_logger.info("H.A.S.I. main program running in {0} environment.".format(self.environment))

        """ Check whether application is found in repository """
        query = "select count(1) from obj_application where app_name = '{0}';".format(self.app_name)
        result = db.sql_query(DSN, query)
        self.check_app_status('HASI', -9000) if result[0][0] != 1 else None

    @property
    def __str__(self):
        return self.host_name, self.environment, self.app_name

    def check_app_status(self, event_class, event_code):
        """
        Checks event for severity, reads event text from repository and exits the application in case
        of a fatal error.
        """
        query = "select event_msg, event_action from obj_events where event_class = '{0}' " \
                "and event_code = '{1}';".format(event_class, event_code)
        result = db.sql_query(DSN, query)
        if not result:
            event_msg = "Event not found"
            event_action = "ERROR"
        else:
            event_msg = result[0][0]
            event_action = result[0][1]
        if event_action == "SUCCESS":
            self.app_logger.info(event_msg)
        elif event_action == "WARNING":
            self.app_logger.warning(event_msg)
        elif event_action == "ERROR":
            self.app_logger.error(event_msg)
            sys.exit(1)

    def plot_app(self, path, file_format):
        from graphviz import Digraph
        digraph_scriptname = "{0}/{1}".format(path, self.app_name)
        output_filename = "{0}/{1}.{2}".format(path, self.app_name, file_format)
        query = "select job_name, preceding_job, active, status from obj_job_plan where app_name='{0}';".format(
            self.app_name)
        result = db.sql_query(DSN, query)
        dot = Digraph(name="{0}".format(self.app_name), filename=digraph_scriptname)
        dot.format = file_format
        dot.graph_attr['label'] = "{0}".format(self.app_name)
        dot.graph_attr['rankdir'] = 'LR'
        """ Create nodes """
        for x in result:
            name = x[0]
            active = x[2]
            status = x[3]
            if active == 0:
                dot.attr('node', fillcolor='lightgrey')
            elif status == 0:
                dot.attr('node', fillcolor='white')
            elif status == 1:
                dot.attr('node', fillcolor='yellow')
            elif status == 2:
                dot.attr('node', fillcolor='green')
            elif status == 3:
                dot.attr('node', fillcolor='red')
            dot.node(name="{0}".format(name), shape='box', style='filled')
        """ Create arrows with column preceding_job """
        for x in result:
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


class HasiProject(HasiApplication):
    """
    Attributes:
        project_name - Project name
        subproject_name - Name of subproject
        project_directory - Project directory for executables (depends on the project)
        source_directory - Source file directory (depends on the project)
        target_directory - Target file directory (depends on the project)
        log_directory - Logfile directory (depends on the project)
        list_customers - List of recipients for reports
        plan_date - Day of data status for the next ETL run
        app_id - Repository generated ID for each application instance
    """

    def __init__(self, v_app_name):
        """ Creates an instance of a H.A.S.I. project.
        Needs the name of the application and the name of the job.
        """

        """ Predefined variables """
        self.app_id = None
        self.project_name = None
        self.subproject_name = None
        self.project_directory = None
        self.source_directory = None
        self.target_directory = None
        self.log_directory = None
        self.plan_date = None

        """ Inherit Application """
        HasiApplication.__init__(self, v_app_name)

        """ Sets project and subproject name """
        query = "select project, subproject from OBJ_APPLICATION where app_name = '{0}';".format(self.app_name)
        result = db.sql_query(DSN, query)
        self.check_app_status('HASI', -9026) if not result else None
        self.project_name = result[0][0]
        self.subproject_name = result[0][1]

        """ Sets the project directory."""
        query = "select value_text from obj_att_project " \
                "where project = '{0}' and key_text = 'PROJECTDIR';".format(self.project_name)
        result = db.sql_query(DSN, query)
        self.check_app_status('HASI', -9027) if not result else None
        self.project_directory = result[0][0]
        self.check_directory(self.project_directory, True, False, True)

        """ Sets the source directory. """
        query = "select value_text from obj_att_project where project = '{0}' and key_text = 'SRCDIR';".format(
            self.project_name)
        result = db.sql_query(DSN, query)
        self.check_app_status('HASI', -9028) if not result else None
        self.source_directory = result[0][0]
        self.check_directory(self.source_directory, True, False, True)

        """ Sets the target directory. """
        query = "select value_text from obj_att_project where project = '{0}' and key_text = 'TGTDIR';".format(
            self.project_name)
        result = db.sql_query(DSN, query)
        self.check_app_status('HASI', -9029) if not result else None
        self.target_directory = result[0][0]
        self.check_directory(self.target_directory, True, True, True)

        """ Sets the logfile directory. """
        query = "select value_text from obj_att_project where project = '{0}' and key_text = 'LOGDIR';".format(
            self.project_name)
        result = db.sql_query(DSN, query)
        self.check_app_status('HASI', -9030) if not result else None
        self.log_directory = result[0][0]
        self.check_directory(self.log_directory, True, True, True)

        """ Obtain plan date for whole job chain """
        query = "SELECT value_text FROM obj_att_project WHERE project = '{0}' AND key_text = 'PLAN_DATE';".format(
            self.project_name)
        result = db.sql_query(DSN, query)
        if not result:
            self.plan_date = time.strftime("%Y-%m-%d")
        else:
            self.plan_date = result[0][0]
            if len(self.plan_date) > 10:
                self.plan_date = self.plan_date[:10]
        self.app_logger.debug("Plan date for current job chain: " + str(self.plan_date))

        """ Initialize application if not yet done """
        self.log_application(1)

    @property
    def __str__(self):
        return self.__name__, self.project_name, self.project_directory, self.source_directory, self.target_directory, \
               self.log_directory, self.app_id

    def log_application(self, status):
        """
        Create log entry in obj_log_application table
        """
        sequence_dialect = ""
        if db_backend == "ORACLE":
            sequence_dialect = "LOG_APP_ID.nextval"
        elif db_backend == "POSTGRESQL":
            sequence_dialect = "nextval('log_app_id')"
        elif db_backend == "DB2" or db_backend == "NETEZZA":
            sequence_dialect = "next value for log_app_id"

        if status == 1:
            while self.app_id is None:
                query = "select app_id from obj_log_application where app_name = '{0}' " \
                        "and status = 1;".format(self.app_name)
                result = db.sql_query(DSN, query)
                self.app_logger.debug(result)
                if not result:
                    query = "insert into obj_log_application (app_id, app_name, project, subproject, " \
                            "plan_date, status, status_text) values ({4}," \
                            "'{0}', '{1}', '{2}', to_date('{3}', 'YYYY-MM-DD'), '1', 'RUNNING');".format(
                             self.app_name, self.project_name,
                             self.subproject_name, self.plan_date, sequence_dialect)
                    result = db.sql_query(DSN, query)
                    self.check_app_status("HASI", -9014) if result is False else None
                else:
                    self.app_id = result[0][0]
        elif status == 2:
            query = "select app_id, ts_start, current_timestamp " \
                    "from obj_log_application where app_name = '{0}' and status = '1';".format(self.app_name)
            result = db.sql_query(DSN, query)
            self.app_logger.debug(result)
            self.check_app_status('HASI', -9038) if not result else None
            self.app_id = result[0][0]
            ts_start = result[0][1]
            ts_end = result[0][2]
            seconds_taken = int((ts_end - ts_start).total_seconds())
            duration = convert_duration(seconds_taken)
            query = "update obj_log_application set plan_date = to_date('{0}', 'YYYY-MM-DD'), " \
                    "status = '{1}', status_text = '{2}', ts_end = to_timestamp('{3}', 'YYYY-MM-DD HH24:MI:SS.FF6'), " \
                    "app_duration = '{4}', seconds_taken = {5}, " \
                    "ts_upd = to_timestamp('{6}', 'YYYY-MM-DD HH24:MI:SS.FF6') where app_id = '{7}';".format(
                     self.plan_date, status, "FINISHED", ts_end, duration, seconds_taken, ts_end, self.app_id)
            result = db.sql_query(DSN, query)
            self.app_logger.debug(result)
            self.check_app_status("HASI", -9014) if result is False else None

    def check_directory(self, directory, read=False, write=False, execute=False):
        """
        Checks file directories for existence and proper access rights
        """
        if not os.path.isdir(directory):
            self.app_logger.error("{0}: Directory {1} does not exist.".format(
                time.strftime("%Y-%m-%d %H.%M.%S"), directory))
            self.check_app_status("HASI", -9003)
        if read is True and not os.access(directory, os.R_OK):
            self.app_logger.error("{0}: User {1} cannot read from directory {2}".format(
                time.strftime("%Y-%m-%d %H.%M.%S"), os.environ["USER"], directory))
            self.check_app_status("HASI", -9004)
        elif write is True and not os.access(directory, os.W_OK):
            self.app_logger.error(
                "{0}: User {1} cannot write to directory {2}".format(time.strftime("%Y-%m-%d %H.%M.%S"),
                                                                     os.environ["USER"], directory))
            self.check_app_status("HASI", -9005)
        elif execute is True and not os.access(directory, os.X_OK):
            self.app_logger.error(
                "{0}: User {1} cannot switch to directory {2}".format(time.strftime("%Y-%m-%d %H.%M.%S"),
                                                                      os.environ["USER"], directory))
            self.check_app_status("HASI", -9006)


class HasiSession(HasiProject):
    """
    Attributes:
        session_logger - Logging instance for session
        check_app_is_active - Flag is set when the application is marked active
        jobnet_exists - Flag is set if the jobnet for application exists in plan table
        app_name - Application name
        job_name - Job name (TWS name from scheduler)
        job_id - Repository generated ID for job instance
        job_type - Process name (Python script, Informatica Workflow, etc.)
        prog_name- Session name (Name of script, program or Workflow)
        prog_args - Parameters passed to the program or folder where Informatica Workflow exists.
        session_active - Flag decides whether the job is executed or skipped
        job_status - Flag for session status (0, 1, 2, 3)
        job_status_text - Text for session status (Planned, Running, Finished, Failed)
        job_created - Date, when job was created in repository table obj_job
        report - placeholder for application log that holds all session logs for mail reporting
    """

    def __init__(self, v_app_name, v_job_name):
        """
        Creates an instance of a H.A.S.I. session.
        Needs the name of the application and the name of the job.
        :param v_app_name:
        :param v_job_name:
        """
        self.session_logger = None
        self.session_file_handler = None
        self.session_console_handler = None
        self.session_formatter = None
        self.job_id = None
        self.event_class = None
        self.event_code = None
        self.event_msg = None
        self.event_action = None
        self.c_hash = None
        self.c_saved = None
        self.pid = os.getpid()
        self.rows_written = 0

        """ Report attributes """
        self.attachment = []
        self.list_customers = []

        """ Inherit class Project """
        HasiProject.__init__(self, v_app_name)

        """ Job """
        self.job_name = v_job_name

        """ Check active status of application """
        query = "select active from obj_application where app_name = '{0}';".format(self.app_name)
        result = db.sql_query(DSN, query)
        self.check_app_status('HASI', -9000) if not result else None
        check_app_is_active = result[0][0]
        if check_app_is_active == 0:
            self.check_app_status("HASI", -9001)
            self.job_type = "SKIP_APPLICATION"
            self.program_name = "Internal_function_call"
            self.program_string = "Internal_function_call"
            self.end_session(0)

        """ Check existence of jobnet """
        query = "select count(*) from obj_job_plan where app_name = '{0}';".format(self.app_name)
        result = db.sql_query(DSN, query)
        jobnet_exists = result[0][0]
        if jobnet_exists == 0:
            self.app_logger.warning("No jobnet found for application {0}. Generating jobnet...".format(
                self.app_name))
            self.job_type = "GENERATE_JOBNET"
            self.program_name = "Internal_function_call"
            self.program_string = "Internal_function_call"
            self.generate_jobnet()
        else:
            self.app_logger.info("A jobnet for application {0} was found.".format(self.app_name))

        """ Check valid job """
        query = "select count(*) from obj_job_plan where app_name = '{0}' and job_name = '{1}';".format(
            self.app_name, self.job_name)
        result = db.sql_query(DSN, query)
        if result[0][0] != 1:
            self.job_type = "JOB_NOT_FOUND"
            self.program_name = "Unknown"
            self.program_string = "Unknown"
            event_class = 'HASI'
            event_code = -9007
            self.check_event_status(event_class, event_code)

        """ Get metadata from repository """
        query = "select job_type, prog_name, prog_args, active, status, status_text, ts_ins " \
                "from obj_job_plan where app_name = '{0}' and job_name = '{1}';".format(
                 self.app_name, self.job_name)
        result = db.sql_query(DSN, query)

        """ Job Type """
        self.job_type = result[0][0]
        if self.job_type is None:
            self.app_logger.error("Empty Job type is not allowed. Check repository entry job_type for "
                                  "application {0}, Job {1}".format(self.app_name, self.job_name))
            self.check_event_status("HASI", -9018)
        elif self.job_type.strip() == "":
            self.app_logger.error("Empty Job type is not allowed. Check repository entry job_type for "
                                  "application {0}, Job {1}".format(self.app_name, self.job_name))
            self.check_event_status("HASI", -9018)

        """ Program Name """
        if result[0][1] is None:
            self.program_name = self.job_type
        elif result[0][1].strip() == "":
            self.program_name = self.job_type
        else:
            self.program_name = result[0][1]
        self.program_string = "".join(c for c in self.program_name if c.isalnum())

        """ Program Arguments """
        self.prog_args = result[0][2]

        """ Session (active / deactivated) """
        self.session_active = result[0][3]

        """ Jobstatus """
        self.job_status = result[0][4]
        self.job_status_text = result[0][5]

        """ Job creation date """
        self.job_created = result[0][6]

        """ Session logging """
        self.get_session_logger()

        """ Initialize entry in table obj_log_session """
        self.log_session(1)

        """ Skip inactive job """
        if self.session_active != 1:
            self.skip_deactivated_job()

        """ Run session instance """
        self.run_session()

    @property
    def __str__(self):
        return self.__name__, self.job_type, self.program_name, self.prog_args, self.session_active

    def get_session_logger(self):
        """
        Sets logging file handler for session
        """
        import logging
        self.session_logger = logging.getLogger("SESSION")
        self.session_logger.setLevel(logging.DEBUG)
        self.session_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.session_file_handler = logging.FileHandler("{0}/{1}.{2}.{3}.{4}.log".format(
            self.log_directory, self.subproject_name, self.job_type, self.program_string, os.getpid()))
        self.session_file_handler.setLevel(logging.INFO)
        self.session_file_handler.setFormatter(self.session_formatter)
        self.session_logger.addHandler(self.session_file_handler)
        self.session_console_handler = logging.StreamHandler()
        self.session_console_handler.setLevel(logging.INFO)
        self.session_console_handler.setFormatter(self.session_formatter)
        self.session_logger.addHandler(self.session_console_handler)

    def end_session(self, exit_code):
        """
        Closes log handler, exits session
        """
        if not self.session_logger:
            self.get_session_logger()
        self.session_file_handler.close()
        sys.exit(exit_code)

    def check_event_status(self, event_class, event_code):
        """
        Checks application exit code in table obj_events.
        If the event code does not match, an error is raised.
        """
        if not self.session_logger:
            self.get_session_logger()
        query = "select event_msg, event_action from obj_events where event_class = '{0}' " \
                "and event_code = '{1}';".format(event_class, event_code)
        result = db.sql_query(DSN, query)
        self.session_logger.debug(result)
        # Set class attributes
        self.event_class = event_class
        self.event_code = event_code
        if not result:
            event_msg = "Error: Event action for event class {0}, event_code {1} is not defined. " \
                        "Exiting with error 1.".format(event_class, event_code)
            event_code = "1"
            event_action = "ERROR"
            self.check_session_status(event_class, event_code, event_action, event_msg)
        else:
            self.event_msg = result[0][0]
            self.event_action = result[0][1]
        self.session_logger.debug("Event Message: {0}".format(self.event_msg))
        self.session_logger.debug("Event Action: {0}".format(self.event_action))
        self.check_session_status()

    def check_session_status(self):
        """ Checks attribute return code and logs events using logger and calls methods to update job status,
            log session details and sends a Mailreport in case of error.
            Calls end_session() if session gets error status.
        """
        if self.event_action == "SUCCESS":
            self.session_logger.info(self.event_msg)
            self.session_logger.info("Session {0}, program {1} ended successfully.".format(
                self.job_name, self.program_name))
            self.update_job_status(2)
            self.log_obj_sess_detail()
            if self.job_type == "MAILREPORT":
                # Cleanup log files before exit
                file_list = glob.glob("{0}/{1}.*".format(self.log_directory, self.subproject_name))
                for item in file_list:
                    os.remove(item) if os.path.exists(item) else None
                self.log_application(2)
            self.end_session(0)
        elif self.event_action == "WARNING":
            self.session_logger.warning(self.event_msg)
            self.session_logger.warning("Session {0} ended with returncode {1}. Process will continue.".format(
                self.job_name, self.event_code))
            self.update_job_status(2)
            self.log_obj_sess_detail()
            self.end_session(0)
        elif self.event_action == "ERROR":
            self.session_logger.error(self.event_msg)
            self.session_logger.error("Session {0}, program {1} aborted with returncode {2}.".format(
                self.job_name, self.program_name, self.event_code))
            self.update_job_status(3)
            self.log_obj_sess_detail()
            self.send_mail_report(self.event_code)
            self.end_session(6)

    def skip_deactivated_job(self):
        """
        Logs inactive jobs with warning to session logger.
        """
        if not self.session_logger:
            self.get_session_logger()
        self.session_logger.info("H.A.S.I. skips deactivated job {0} and exits silently.".format(self.job_name))
        self.check_event_status("HASI", -9013)

    def update_job_status(self, job_status):
        """
        Updates repository table obj_job_plan with current job status and calls log_session method.
        """
        self.job_status = job_status
        if self.job_status == 0:
            self.job_status_text = "PLANNED"
        elif self.job_status == 1:
            self.job_status_text = "RUNNING"
        elif self.job_status == 2:
            self.job_status_text = "FINISHED"
        elif self.job_status == 3:
            self.job_status_text = "FAILED"
        else:
            self.job_status_text = "UNDEFINED"
        query = "select count(*) from obj_job_plan where app_name = '{0}' and job_name = '{1}';".format(
            self.app_name, self.job_name)
        result = db.sql_query(DSN, query)
        self.session_logger.debug("Jobs found in query: " + str(result))
        if result[0][0] == 1:
            query = "update obj_job_plan set status = '{0}', status_text = '{1}', " \
                    "ts_upd = current_timestamp where app_name = '{2}' and job_name = '{3}';".format(
                     self.job_status, self.job_status_text, self.app_name, self.job_name)
            result = db.sql_query(DSN, query)
            self.session_logger.debug("Update obj_job_plan: " + str(result))
        self.log_session(2)

    def log_session(self, status):
        """
        Logs session information to repository table obj_log_session and calls method check_event_status
        when something's going wrong.
        """
        sequence_dialect = ""
        if db_backend == "ORACLE":
            sequence_dialect = "LOG_JOB_ID.nextval"
        elif db_backend == "POSTGRESQL":
            sequence_dialect = "nextval('log_job_id')"
        elif db_backend == "DB2" or db_backend == "NETEZZA":
            sequence_dialect = "next value for log_job_id"

        if status == 1:
            while self.job_id is None:
                query = "select job_id from obj_log_session where app_id = '{0}' " \
                        "and job_name = '{1}' and status = '1';".format(self.app_id, self.job_name)
                result = db.sql_query(DSN, query)
                self.session_logger.debug("Lookup job_id from obj_log_session: " + str(result))
                cleansed_arguments = self.prog_args.replace('"', '').replace(';', '') if self.prog_args else " "
                if not result:
                    query = "insert into obj_log_session (job_id, app_id, app_name, job_name, plan_date, job_type, " \
                            "prog_name, prog_args, status, status_text) values ({7}, {0}, '{1}', " \
                            "'{2}', to_date('{3}', 'YYYY-MM-DD'), '{4}', '{5}', '{6}', '1','RUNNING');".format(
                             self.app_id, self.app_name, self.job_name, self.plan_date, self.job_type,
                             self.program_name, cleansed_arguments, sequence_dialect)
                    result = db.sql_query(DSN, query)
                    self.session_logger.debug("New entry in obj_log_session: " + str(result))
                    """ If row could not be inserted into session log table, abort with event code. """
                    self.check_event_status("HASI", -9015) if result is False else None
                else:
                    self.job_id = result[0][0]
        elif status == 2:
            query = "select job_id, ts_start, current_timestamp from obj_log_session where app_id = '{0}' " \
                    "and job_name = '{1}' and status = '1' order by job_id fetch first 1 rows only;".format(
                     self.app_id, self.job_name)
            result = db.sql_query(DSN, query)
            self.session_logger.debug("Job information from obj_log_session: " + str(result))
            self.end_session(1) if not result else None
            self.job_id = result[0][0]
            ts_start = result[0][1]
            ts_end = result[0][2]
            seconds_taken = int((ts_end - ts_start).total_seconds())
            duration = convert_duration(seconds_taken)
            query = "update obj_log_session set status = '{0}', status_text = '{1}', event_class = '{2}'" \
                    ", event_code = '{3}', ts_end = to_timestamp('{4}', 'YYYY-MM-DD HH24:MI:SS.FF6')" \
                    ", job_duration = '{5}', seconds_taken = {6}, rows_written = {7}" \
                    ", ts_upd = to_timestamp('{8}', 'YYYY-MM-DD HH24:MI:SS.FF6') " \
                    "where app_id = {9} and job_id = {10};".format(
                     self.job_status, self.job_status_text, self.event_class, self.event_code, ts_end, duration,
                     seconds_taken, self.rows_written, ts_end, self.app_id, self.job_id)
            result = db.sql_query(DSN, query)
            self.session_logger.debug("Update entry obj_log_session: " + str(result))
            self.check_app_status("HASI", -9024) if result is False else None
        else:
            pass

    def log_obj_sess_detail(self):
        """
        Inserts whole session logfile into repository table obj_log_sess_detail for history analysis.
        """
        session_logfile_name = "{0}/{1}.{2}.{3}.{4}.log".format(self.log_directory, self.subproject_name,
                                                                self.job_type, self.program_string, os.getpid())
        if not os.path.exists(session_logfile_name):
            open(session_logfile_name, "a").close()
        size = os.path.getsize(session_logfile_name)
        with open(session_logfile_name, "r") as fh:
            """ Set max. 1.000.000 characters length """
            list_content = fh.readlines(1000000)
            """ Remove special characters """
            for occurrence in ["\'", ";", "§"]:
                list_content = [x.replace(occurrence, "") for x in list_content]
            if size > 999990:
                list_content.append("Job {0} log entry in table is truncated and shows only parts of the session log. "
                                    "However you can read the full log in the mail report if available.".format(
                                     self.job_name))
            content = "".join(list_content)

        if db_backend == "ORACLE":
            query = "insert into obj_log_sess_detail (JOB_ID, REPORT) values (:JOB_ID, :REPORT)"
            t_values = (self.job_id, content)
            result = db.query_with_values(DSN, query, t_values)
        else:
            query = "insert into obj_log_sess_detail (JOB_ID, REPORT) values ({0}, '{1}')".format(self.job_id, content)
            result = db.sql_query(DSN, query)
        self.session_logger.debug("New entry in obj_log_sess_detail: " + str(result))

    def generate_jobnet(self):
        """
        Generates a new jobnet in repository table obj_job_plan.
        :return: event_code
        """
        if not self.session_logger:
            self.get_session_logger()
        self.session_logger.info("Delete old jobnet for application {0}...".format(self.app_name))
        query = "delete from obj_job_plan where app_name = '{0}';".format(self.app_name)
        result = db.sql_query(DSN, query)
        self.session_logger.debug("Delete old Jobnet: " + str(result))
        self.check_event_status("HASI", -9002) if result is False else None
        self.session_logger.info("Generating new jobnet for application {0}...".format(self.app_name))
        query = "insert into obj_job_plan (app_name, job_name, project, subproject, job_type, " \
                "prog_name, prog_args, preceding_job, follower_job, " \
                "active, status, status_text, ts_ins, ts_upd) " \
                "select app.app_name, job.job_name, app.project, app.subproject, " \
                "job.job_type, job.prog_name, job.prog_args, job.preceding_job, " \
                "job.follower_job, job.active, case when job.active=1 then 0 else -1 end as status, " \
                "case when job.active=1 then 'PLANNED'  else 'DEACTIVATED' " \
                "end as status_text, current_timestamp, current_timestamp " \
                "from OBJ_APPLICATION app, obj_job job " \
                "where app.app_name = job.app_name and app.app_name = '{0}';".format(self.app_name)
        result = db.sql_query(DSN, query)
        self.session_logger.debug("Create new jobnet: " + str(result))
        self.check_event_status("HASI", -9002) if result is False else None
        return 0

    def generate_jobchain(self):
        """
        Generates a new jobnet in repository table obj_job_plan.
        """
        if not self.session_logger:
            self.get_session_logger()
        self.session_logger.info("Delete old jobnet for application {0}...".format(self.app_name))
        query = "delete from obj_job_plan where app_name in (select app_name from obj_application);"
        result = db.sql_query(DSN, query)
        self.check_event_status("HASI", -9002) if result is False else None
        self.session_logger.info("Generating new jobnet for application {0}...".format(self.app_name))
        query = "insert into obj_job_plan (app_name, job_name, project, subproject, job_type, " \
                "prog_name, prog_args, preceding_job, follower_job, " \
                "active, status, status_text, ts_ins, ts_upd) " \
                "select app.app_name, job.job_name, app.project, app.subproject, " \
                "job.job_type, job.prog_name, job.prog_args, job.preceding_job, " \
                "job.follower_job, job.active, case when job.active=1 then 0 else -1 end as status, " \
                "case when job.active=1 then 'PLANNED' else 'DEACTIVATED' end as status_text, " \
                "current_timestamp, current_timestamp from obj_application app, obj_job job " \
                "where app.app_name = job.app_name;"
        result = db.sql_query(DSN, query)
        self.session_logger.debug("Create new jobnet for all applications.")
        self.check_event_status("HASI", -9002) if result is False else None
        return 0

    def show_app_duration(self):
        """
        Obtains average runtime of application and session within the last month
        from repository tables obj_log_application and obj_log_session and logs this
        information to session logger.
        """
        calc_time_interval = ""
        if db_backend == "ORACLE":
            calc_time_interval = "add_months(current_timestamp, -1)"
        elif db_backend == "POSTGRESQL" or db_backend == "NETEZZA":
            calc_time_interval = "current_timestamp - interval '1 month'"
        elif db_backend == "DB2":
            calc_time_interval = "current_timestamp -1 month"
        import pandas
        pandas.set_option('display.width', 1000)
        pandas.set_option('display.max_columns', 500)
        if not self.session_logger:
            self.get_session_logger()
        query = "select distinct a.app_name as app_name, a.project as project, " \
                "a.subproject as subproject, case when b.zeit_minuten is null " \
                "or b.zeit_minuten < 1.00 then '< 1 minute' else to_char(b.zeit_minuten, '99999.99')||' Minuten' " \
                "end as laufzeit_letzter_monat from obj_job_plan a left join ( select app_name, " \
                "sum(zeit_minuten) zeit_minuten from (select app_name, avg(seconds_taken) * 1 / 60 " \
                "as zeit_minuten from obj_log_application where app_name = '{0}' and status = 2 " \
                "and ts_ins >= {1} group by app_name) job group by app_name ) b " \
                "on a.app_name = b.app_name where a.app_name = '{0}';".format(self.app_name, calc_time_interval)
        result = db.sql_query(DSN, query)
        l_header = ["APP_NAME", "PROJECT", "SUBPROJECT", "LAUFZEIT_LETZTER_MONAT"]
        l_data = [x for x in result]
        data = pandas.DataFrame(l_data, columns=l_header)
        self.session_logger.info("\nDurchschnittliche Laufzeit {0} im letzten Monat:\n{1}".format(
            self.app_name, data))

        query = "select a.app_name as app_name, a.job_name as job_name, a.project as project, " \
                "a.subproject as subproject, a.job_type as job_type, a.prog_name as prog_name, " \
                "a.prog_args as prog_args, a.active as active, case when b.zeit_minuten is null " \
                "or b.zeit_minuten < 1.00 then '< 1 Minute' else to_char(b.zeit_minuten, '99999.99')|| ' Minuten' " \
                "end as laufzeit_letzter_monat " \
                "from obj_job_plan a left join ( select app_name, job_name, job_type, prog_name, " \
                "prog_args, avg(seconds_taken) * 1 / 60 as zeit_minuten " \
                "from obj_log_session where status = 2 and ts_ins >= {1} " \
                "group by app_name, job_name, job_type, prog_name, prog_args, status ) b on a.job_name = b.job_name " \
                "and a.prog_name = b.prog_name where a.app_name = '{0}' and a.active = 1 " \
                "order by a.app_name, a.job_name;".format(self.app_name, calc_time_interval)
        result = db.sql_query(DSN, query)
        l_header = ["APP_NAME", "JOB_NAME", "PROJECT", "SUBPROJECT", "JOB_TYPE", "PROG_NAME", "PROG_ARGS", "ACTIVE",
                    "LAUFZEIT_LETZTER_MONAT"]
        l_data = [x for x in result]
        data = pandas.DataFrame(l_data, columns=l_header)
        self.session_logger.info("\nDurchschnittliche Laufzeit der Jobs im letzten Monat:\n{0}".format(data))
        return 0

    @staticmethod
    def get_dynamic_values(value):
        """
        :param value:
        :return:
        """
        if value == "@I_CURRENT_MONTH":
            return str(time.strftime("%Y%m"))
        elif value == "@I_TODAY":
            return str(time.strftime("%Y%m%d"))
        elif value == "@D_TODAY":
            return "'{0}'".format(str(time.strftime("%Y-%m-%d")))
        elif value == "@I_YESTERDAY":
            yesterday = datetime.date.today() - datetime.timedelta(days=1)
            return "{0}{1:02d}{2:02d}".format(yesterday.year, yesterday.month, yesterday.day)
        elif value == "@D_YESTERDAY":
            yesterday = datetime.date.today() - datetime.timedelta(days=1)
            return "'{0}-{1:02d}-{2:02d}'".format(yesterday.year, yesterday.month, yesterday.day)
        elif value == "@I_LAST_MONTH":
            last_month = datetime.date.today() - relativedelta(months=1)
            return "{0}{1:02d}".format(last_month.year, last_month.month)
        elif value == "@D_LAST_MONTH":
            last_month = datetime.date.today() - relativedelta(months=1)
            return "'{0}-{1:02d}-{2:02d}'".format(last_month.year, last_month.month, last_month.day)
        else:
            return value

    def run_session(self):
        """
        Starts session execution referenced by job_type.
        """
        os.chdir(self.project_directory)
        import shlex
        self.update_job_status(1)
        if self.job_type == "SET_PLAN_DATE":
            """ Set event class for HASI return codes """
            event_class = "HASI"
            self.plan_date = time.strftime("%Y-%m-%d")
            self.session_logger.info("Update obj_att_project table with plan date for current plan.")
            """ Check existence of entry in obj_att_project table """
            query = "SELECT count(*) FROM obj_att_project WHERE project = 'ALL' AND key_text = 'PLAN_DATE';"
            result = db.sql_query(DSN, query)
            if result[0][0] == 0:
                query = "INSERT INTO obj_att_project (project, key_text, value_text) " \
                        "SELECT 'ALL', 'PLAN_DATE', '{0}' FROM dual " \
                        "WHERE NOT exists (SELECT project, key_text, value_text FROM obj_att_project " \
                        "WHERE project = 'ALL' AND key_text = 'PLAN_DATE');".format(self.plan_date)
                result = db.sql_query(DSN, query)
                event_code = -9024 if result is False else 0
            else:
                query = "UPDATE obj_att_project SET value_text = '{0}', " \
                        "ts_upd = current_timestamp " \
                        "WHERE project = 'ALL' AND key_text = 'PLAN_DATE';".format(self.plan_date)
                result = db.sql_query(DSN, query)
                event_code = -9024 if result is False else 0
            """ Update application in obj_log_application """
            query = "update obj_log_application set plan_date = to_date('{0}', 'YYYY-MM-DD') " \
                    "where app_id = {1};".format(self.plan_date, self.app_id)
            result = db.sql_query(DSN, query)
            self.app_logger.debug(result)
            self.check_app_status(event_class, -9014) if result is False else None
            self.generate_jobchain()
            self.check_event_status(event_class, event_code)

        elif self.job_type == "GENERATE_JOBNET":
            """ Set event class for HASI return codes """
            event_class = "HASI"
            self.session_logger.info("Start jobnet generation for application {0} with user {1}.".format(
                self.app_name, self.user_name))
            event_code = self.generate_jobnet()
            self.check_event_status(event_class, event_code)

        elif self.job_type == "SHOW_APP_STATS":
            """ Set event class for HASI return codes """
            event_class = "HASI"
            self.session_logger.info("Show application and job statistics with job {0} with user {1}.".format(
                self.job_name, self.user_name))
            event_code = self.show_app_duration()
            self.check_event_status(event_class, event_code)

        elif self.job_type == "PYTHON":
            """ Set event class for Python return codes """
            event_class = "UNIX_PROGRAM"
            file_found = self.signature_check()
            if file_found is False:
                event_code = 127
                self.check_event_status(event_class, event_code)
            self.session_logger.info("Start Python program {0} for job {1} with user {2}.".format(
                self.program_name, self.job_name, self.user_name))
            if self.prog_args is None:
                readable_command = "python {0}/{1}".format(self.project_directory, self.program_name)
            else:
                readable_command = "python {0}/{1} {2}".format(self.project_directory, self.program_name,
                                                               self.prog_args)
            shell_env = os.environ.copy()
            cmd = shlex.split(readable_command)
            self.session_logger.debug("Command: {0}".format(cmd))
            event_code = self.run_subprocess(cmd, shell_env)
            self.check_event_status(event_class, event_code)

        elif self.job_type == "SHELLSCRIPT":
            """ Set event class for Shellscript return codes """
            event_class = "UNIX_PROGRAM"
            file_found = self.signature_check()
            if file_found is False:
                event_code = 127
                self.check_event_status(event_class, event_code)
            self.session_logger.info("Start Shellscript {0} for job {1} with user {2}.".format(
                self.program_name, self.job_name, self.user_name))
            if self.prog_args is None:
                readable_command = "{0}/{1}".format(self.project_directory, self.program_name, )
            else:
                readable_command = "{0}/{1} {2}".format(self.project_directory, self.program_name,
                                                        self.prog_args)
            shell_env = os.environ.copy()
            cmd = shlex.split(readable_command)
            event_code = self.run_subprocess(cmd, shell_env)
            self.check_event_status(event_class, event_code)

        elif self.job_type == "WORKFLOW":
            """ Set event class for Informatica PowerCenter return codes """
            runstatus_code = None
            session_logfile_name = None
            event_class = "POWERCENTER"
            self.session_logger.info("Lookup PowerCenter Version for project {0}.".format(self.project_name))
            query = "select value_text from obj_att_project where project = '{0}' " \
                    "and key_text = 'INF_VERSION';".format(self.project_name)
            result = db.sql_query(DSN, query)
            self.check_app_status('HASI', -9031) if not result else None
            infa_version = result[0][0]
            self.session_logger.info("PowerCenter Version for project {0} is {1}.".format(
                self.project_name, infa_version))

            """ Obtain Informatica environment """
            query = "select value_text from obj_att_project where project = '{0}' and key_text " \
                    "= 'INFA_HOME';".format(infa_version)
            result = db.sql_query(DSN, query)
            self.check_app_status('HASI', -9025) if not result else None
            infa_home = result[0][0]
            self.session_logger.debug("INFA_HOME: {0}".format(infa_home))
            query = "select value_text from obj_att_project where project = '{0}' and key_text " \
                    "= 'DOMAIN_NAME';".format(infa_version)
            result = db.sql_query(DSN, query)
            self.check_app_status('HASI', -9020) if not result else None
            infa_domain = result[0][0]
            self.session_logger.debug("DOMAIN_NAME: {0}".format(infa_domain))
            query = "select value_text from obj_att_project where project = '{0}' and key_text " \
                    "= 'INTEGRATION_SERVICE';".format(infa_version)
            result = db.sql_query(DSN, query)
            self.check_app_status('HASI', -9021) if not result else None
            infa_integration_service = result[0][0]
            self.session_logger.debug("INTEGRATION_SERVICE: {0}".format(infa_integration_service))
            query = "select value_text from obj_att_project where project = '{0}' and key_text " \
                    "= 'REPOSITORY_CONNECTION';".format(infa_version)
            result = db.sql_query(DSN, query)
            self.check_app_status('HASI', -9033) if not result else None
            infa_repository_db = result[0][0]
            self.session_logger.debug("REPOSITORY_CONNECTION: {0}".format(infa_repository_db))
            query = "select value_text from obj_att_project where project = '{0}' and key_text " \
                    "= 'PM_USER_ADM';".format(infa_version)
            result = db.sql_query(DSN, query)
            self.check_app_status('HASI', -9034) if not result else None
            pm_user_adm = result[0][0]
            self.session_logger.debug("PM_USER_ADM: {0}".format(pm_user_adm))
            query = "select value_text from obj_att_project where project = '{0}' and key_text " \
                    "= 'PM_PWD_ADM';".format(infa_version)
            result = db.sql_query(DSN, query)
            self.check_app_status('HASI', -9035) if not result else None
            pm_pwd_adm = result[0][0]
            self.session_logger.debug("PM_PWD_ADM: {0}".format(pm_pwd_adm))
            query = "select value_text from obj_att_project where project = '{0}' and key_text " \
                    "= 'ODBC_HOME';".format(infa_version)
            result = db.sql_query(DSN, query)
            self.check_app_status('HASI', -9036) if not result else None
            odbc_home = result[0][0]
            query = "select value_text from obj_att_project where project = '{0}' and key_text " \
                    "= 'INFA_TRUSTSTORE';".format(infa_version)
            result = db.sql_query(DSN, query)
            self.check_app_status('HASI', -9039) if not result else None
            infa_truststore = result[0][0]
            self.session_logger.debug("INFA_TRUSTSTORE: {0}".format(infa_truststore))
            query = "select value_text from obj_att_project where project = '{0}' and key_text " \
                    "= 'INFA_TRUSTSTORE_PASSWORD';".format(infa_version)
            result = db.sql_query(DSN, query)
            self.check_app_status('HASI', -9047) if not result else None
            infa_truststore_password = result[0][0]
            self.session_logger.debug("INFA_TRUSTSTORE_PASSWORD: {0}".format(infa_truststore_password))
            self.session_logger.debug("ODBC_HOME: {0}".format(odbc_home))
            infa_folder = self.prog_args
            self.session_logger.debug("Informatica Folder: {0}".format(infa_folder))
            infa_workflow = self.program_name
            self.session_logger.debug("Informatica Workflow: {0}".format(infa_workflow))

            """ Read os environment """
            infa_env = {}

            """ Set PowerCenter environment """
            infa_env["INFA_HOME"] = infa_home
            infa_env["INFA_DOMAINS_FILE"] = "{0}/domains.infa".format(infa_home)
            infa_env["PM_USER_ADM"] = pm_user_adm
            infa_env["PM_PWD_ADM"] = pm_pwd_adm
            path = "{0}:{0}/server/bin:{1}/bin".format(infa_home, odbc_home)
            infa_env["PATH"] = path
            ld_library_path = "{0}/server/bin:{1}/lib".format(infa_home, odbc_home)
            infa_env["LD_LIBRARY_PATH"] = ld_library_path
            infa_env["INFA_TRUSTSTORE_PASSWORD"] = infa_truststore_password
            infa_env["INFA_TRUSTSTORE"] = infa_truststore

            self.session_logger.info("Start Informatica Workflow {0} for job {1} with user {2}.".format(
                self.program_name, self.job_name, pm_user_adm))

            """ Start Informatica Workflow """
            readable_command = "{0}/server/bin/pmcmd startworkflow -d {1} -sv {2} -uv PM_USER_ADM " \
                               "-pv PM_PWD_ADM -f {3} -wait {4}".format(
                                infa_home, infa_domain, infa_integration_service,
                                infa_folder, infa_workflow)
            self.session_logger.debug("{0}\n{1}".format(infa_env, readable_command))
            cmd = shlex.split(readable_command)
            event_code = self.run_subprocess(cmd, infa_env)
            self.session_logger.debug("Returncode: {0}".format(event_code))

            """ Set latest Run_ID of selected workflow """
            query = "select max(workflow_run_id) from rep_sess_log " \
                    "where subject_area='{0}' and workflow_name='{1}';".format(infa_folder, infa_workflow)
            result = db.sql_query(infa_repository_db, query)
            if not result:
                workflow_run_id = 0
            else:
                workflow_run_id = result[0][0]
            self.session_logger.debug("Workflow Run ID: {0}".format(result))

            if workflow_run_id is not None and workflow_run_id > 0:
                query = "select session_timestamp, session_name, successful_rows, " \
                        "failed_rows, session_log_file, " \
                        "case " \
                        "when run_status_code = 1 then 'Succeeded' " \
                        "when run_status_code = 2 then 'Disabled' " \
                        "when run_status_code = 3 then 'Failed' " \
                        "when run_status_code = 4 then 'Stopped' " \
                        "when run_status_code = 5 then 'Aborted' " \
                        "when run_status_code = 6 then 'Running' " \
                        "when run_status_code = 7 then 'Suspending' " \
                        "when run_status_code = 8 then 'Suspended' " \
                        "when run_status_code = 9 then 'Stopping' " \
                        "when run_status_code = 10 then 'Aborting' " \
                        "when run_status_code = 11 then 'Waiting' " \
                        "when run_status_code = 12 then 'Scheduled' " \
                        "when run_status_code = 13 then 'Unscheduled' " \
                        "when run_status_code = 14 then 'Unknown' " \
                        "when run_status_code = 15 then 'Terminated' " \
                        "end as run_status_code " \
                        "from rep_sess_log where workflow_run_id = {0} order by session_timestamp;".format(
                         workflow_run_id)
                result = db.sql_query(infa_repository_db, query)
                if len(result) > 0:
                    for session_instance in result:
                        self.session_logger.debug("Get Informatica session information: " + str(session_instance))
                        session_timestamp = session_instance[0]
                        session_name = session_instance[1]
                        rows_processed = session_instance[2]
                        rows_rejected = session_instance[3]
                        session_logfile_name = session_instance[4]
                        runstatus_code = session_instance[5]
                        self.session_logger.info("--------------------------------------------------------")
                        self.session_logger.info("Session ended on:  {0}".format(session_timestamp))
                        self.session_logger.info("Sessionname:       {0}".format(session_name))
                        self.session_logger.info("Rows processed:    {0}".format(rows_processed))
                        self.session_logger.info("Rows rejected:     {0}".format(rows_rejected))
                        self.session_logger.info("Logfile:           {0}".format(session_logfile_name))
                        self.session_logger.info("Session status:    {0}".format(runstatus_code))
                        self.session_logger.info("--------------------------------------------------------")
                    """ Add session logfile to mail attachments when session failed and log file exists. """
                    if runstatus_code == 'Failed':
                        if not session_logfile_name:
                            pass
                        elif os.path.exists(session_logfile_name):
                            self.attachment.append(session_logfile_name)
                        elif os.path.exists("{0}.bin".format(session_logfile_name)):
                            self.attachment.append("{0}.bin".format(session_logfile_name))
            self.check_event_status(event_class, event_code)

        elif self.job_type == "MICROSTRATEGY":
            # Set event class for SSH return codes
            event_class = "SSH"
            # Obtain credentials for SSH server
            cred = seclib.get_credentials("MSTR")
            self.check_event_status("HASI", -9009) if cred is False or len(cred) < 3 else None
            self.session_logger.info("Start remote shell {0} on server {1} with user {2}.".format(
                self.program_name, cred[0], cred[1], cred[2]))
            # Execute remote shell
            event_code = self.run_remote_ssh(cred)
            self.check_event_status(event_class, event_code)

        elif self.job_type == "REMOTE_SHELL":
            """ Set event class for SSH return codes """
            event_class = "SSH"
            cred = seclib.get_credentials("SSH_NZ")
            self.check_event_status("HASI", -9009) if cred is False or len(cred) < 3 else None
            self.session_logger.info("Start remote shell {0} on server {1} with user {2}.".format(
                self.program_name, cred[0], cred[1]))
            event_code = self.run_remote_ssh(cred)
            self.check_event_status(event_class, event_code)

        elif self.job_type == "STORED_PROCEDURE":
            """ Set event class for Stored Procedure return codes """
            event_class = "STORED_PROCEDURE"
            event_code = 0
            l_args = self.prog_args.split(",")
            for index in range(0, len(l_args)):
                l_args[index] = self.get_dynamic_values(l_args[index])
            if self.prog_args:
                conn = l_args.pop(0)
                s_args = ",".join(l_args).strip()
                self.session_logger.info("Calling stored procedure {0} with database connection {1}".format(
                    self.program_name, conn))
                query = "call {0}({1})".format(self.program_name, s_args)
                self.session_logger.info(query)
                result = db.run_procedure(conn, query)
                if result.startswith("False"):
                    self.session_logger.error(result)
                    event_code = 1
                else:
                    self.session_logger.info(result)
            else:
                self.session_logger.error("Argument {0} seems to be empty.".format(self.prog_args))
                event_code = 1
            self.check_event_status(event_class, event_code)

        elif self.job_type == "MAPPING":
            """ Metadata based processing
                Status dictionary d_status is used to control data flow:
                EVENT_CODE: action defined in OBJ_EVENTS
                ROWS_WRITTEN: number of rows processed in mapping 
            """
            event_class = "MAPPING"
            result = maplib.check_mapping(self.app_name, self.job_name, DSN)
            if result is False:
                self.session_logger.error("Mapping for application {0}, job {1} does not exist.".format(
                    self.app_name, self.job_name))
                self.check_event_status("HASI", -9037)
            self.session_logger.info("Run mapping for application {0} job {1}.".format(
                self.app_name, self.job_name))
            o_mapping = maplib.Mapping(self.app_name, self.job_name, DSN)
            d_status = o_mapping.create_objects()
            if d_status["EVENT_CODE"] != 0:
                self.check_event_status(event_class, d_status["EVENT_CODE"])
            d_status = o_mapping.create_source_actions()
            if d_status["EVENT_CODE"] != 0:
                self.check_event_status(event_class, d_status["EVENT_CODE"])
            d_status = o_mapping.create_target_actions()
            if d_status["EVENT_CODE"] != 0:
                self.check_event_status(event_class, d_status["EVENT_CODE"])
            d_status = o_mapping.run_mapping()
            if d_status["EVENT_CODE"] != 0:
                self.rows_written = d_status["ROWS_WRITTEN"]
                self.check_event_status(event_class, d_status["EVENT_CODE"])
            else:
                event_code = d_status["EVENT_CODE"]
                self.rows_written = d_status["ROWS_WRITTEN"]
                self.check_event_status(event_class, event_code)

        elif self.job_type == "CHECK_STATUS":
            """ Set event class for HASI return codes """
            event_class = "HASI"
            self.session_logger.info("Job status info: Job {0} in application {1} was created on {2}".format(
                self.job_name, self.app_name, self.job_created))
            self.session_logger.info("Current status: {0} (Statusflag: {1}".format(
                self.job_status_text, self.job_status))
            if self.session_active == "1":
                self.session_logger.info("Job is active.")
            else:
                self.session_logger.info("Job is deactivated.")
            event_code = 0
            self.check_event_status(event_class, event_code)

        elif self.job_type == "TRIGGER":
            """ Set event class for HASI return codes """
            event_class = "HASI"
            self.session_logger.info("Fire trigger for application {0}...".format(self.app_name))
            """ Obtain trigger arguments """
            l_args = self.prog_args.split(",")
            if len(l_args) < 3:
                event_code = -9023
                self.check_event_status(event_class, event_code)
            lookup_key = l_args[0]
            trigger_class = l_args[1]
            trigger_name = l_args[2]

            """ Obtain classpath for trigger and set classpath """
            l_jars = glob.glob("{0}/lib/*.jar".format(self.hasi_root_directory))
            l_jars.append("{0}".format(self.hasi_root_directory))
            classpath = ":".join(l_jars)
            os.environ["CLASSPATH"] = classpath
            """ Obtain user for selected connection """
            cred_trigger = seclib.get_credentials(lookup_key)
            url = cred_trigger[0]
            user_name = cred_trigger[1]
            password = "'{0}'".format(cred_trigger[2])
            user_context = "AD"
            """ Build command """
            readable_command = "{0} -classpath {1} {2} {3} {4} {5} {6} {7}".format(
                self.program_name, classpath, trigger_class, url, user_name, password, user_context, trigger_name)
            cmd = shlex.split(readable_command)
            event_code = self.run_subprocess(cmd)
            if event_code == 1 and self.program_name == "java":
                event_code = 0
            self.session_logger.info("Event code {0}...".format(event_code))
            self.check_event_status(event_class, event_code)

        elif self.job_type == "MAILREPORT":
            """ Set event class for HASI return codes """
            event_class = "HASI"
            self.session_logger.info("Send Mailreport for application {0}...".format(self.app_name))
            event_code = self.send_mail_report(0)
            self.check_event_status(event_class, event_code)

        else:
            self.session_logger.error("Unknown job type. Check repository entry job_type for application {0}, "
                                      "Job {1}".format(self.app_name, self.job_name))
            self.check_event_status("HASI", -9018)

    def run_subprocess(self, popen_command, env=None):
        """
        :param popen_command:
        :param env:
        :return:
        """
        error = None
        env = os.environ.copy() if not env else env
        self.session_logger.debug("Environment set:\n{0}".format(env))
        import subprocess
        stmt = subprocess.Popen(popen_command, stdin=subprocess.PIPE, stderr=subprocess.PIPE,
                                stdout=subprocess.PIPE, close_fds=True, env=env)
        output = None
        try:
            output, error = stmt.communicate()
        except OSError as err:
            self.session_logger.error(err)
        # Error handling
        result = output.decode(standard_encoding).strip() + error.decode(standard_encoding).strip()
        if stmt.returncode == 0:
            self.session_logger.info(result)
        else:
            self.session_logger.error(result)
        return stmt.returncode

    def run_remote_ssh(self, cred):
        """
        :param cred:
        :return:
        """
        import paramiko
        remote_host = cred[0]
        remote_user = cred[1]
        remote_password = cred[2]
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            ssh.connect(remote_host, port=22, username=remote_user, password=remote_password, timeout=30)
        except paramiko.ssh_exception.SSHException as error:
            """ Session does not exist """
            self.session_logger.info(error)
            event_code = 2
            self.check_event_status("SSH", event_code)
        except IOError as error:
            """ Bad hostname """
            self.session_logger.info(error)
            event_code = 2
            self.check_event_status("SSH", event_code)
        except paramiko.PasswordRequiredException as error:
            """ Bad password """
            self.session_logger.info(error)
            event_code = 2
            self.check_event_status("SSH", event_code)

        remote_command = "{0} {1}".format(self.program_name, self.prog_args)
        try:
            stdin, stdout, stderr = ssh.exec_command(remote_command, timeout=3600)
            output = stdout.read().decode(standard_encoding)
            error = stderr.read().decode(standard_encoding)
            self.session_logger.info(output)
            self.session_logger.info(error)
            event_code = stdout.channel.recv_exit_status()
            self.session_logger.info(event_code)
            if stdout.channel.recv_exit_status() != "0":
                self.session_logger.info(stderr.read().decode(standard_encoding))
            ssh.close()
        except paramiko.ssh_exception as error:
            self.session_logger.info(error)
            ssh.close()
            event_code = 68
            self.check_event_status("SSH", event_code)
        return event_code

    def signature_check(self):
        """
        Checks for valid key to check for corrupted or changed programs.
        Logs a warning when file has changed.
        Returns False when file doesn't exist. Returns True when signature is up to date.
        """
        import hashlib
        """ Check file existence """
        if not os.path.exists("{0}/{1}".format(self.project_directory, self.program_name)):
            return False
        """ Open program file and create hash value """
        with open("{0}/{1}".format(self.project_directory, self.program_name), "rb") as fh:
            file_content = fh.read()
            self.c_hash = hashlib.sha512(file_content).hexdigest()
        """ Look for existent hash value for program in table obj_signatures """
        query = "select count(*) from obj_signatures where job_name = '{0}' and obj_name = '{1}';".format(
            self.job_name, self.program_name)
        result = db.sql_query(DSN, query)
        if result[0][0] == 0:
            """ Insert hash """
            query = "insert into obj_signatures (job_name, obj_name, signature) " \
                    "values ('{0}', '{1}', '{2}');".format(self.job_name, self.program_name, self.c_hash)
            result = db.sql_query(DSN, query)
            return False if result is False else None
        elif result[0][0] == "1":
            """ Compare file hash with hash from table and update if necessary """
            query = "select signature from obj_signatures where job_name = '{0}' and obj_name = '{1}';".format(
                self.job_name, self.program_name)
            result = db.sql_query(DSN, query)
            self.c_saved = result[0][0]
            if self.c_saved != self.c_hash:
                self.session_logger.warning("Change detected in job {0}, directory {1}, program {2}.".format(
                    self.job_name, self.project_directory, self.program_name))
                query = "update obj_signatures set signature = '{0}', obj_name = '{1}', " \
                        "ts_upd = sysdate where job_name = '{2}' and obj_name = '{1}';".format(
                         self.c_hash, self.program_name, self.job_name)
                result = db.sql_query(DSN, query)
                self.session_logger.debug(result)
                self.check_app_status("HASI", -9024) if result is False else None
        return True

    def decrypt_program(self):
        """
        Decrypt session file
        :return:
        """
        if len(self.c_hash) != 128:
            return False
        key = self.c_hash[:32]
        file_name = "{0}/{1}".format(self.project_directory, self.program_name)
        result = unixlib.decrypt_file(file_name, file_name + ".dec", key)
        shutil.move(file_name + ".dec", file_name) if os.path.exists(file_name + ".dec") else None
        if file_name.endswith(".ksh"):
            os.chmod(file_name, 0o750)
        else:
            os.chmod(file_name, 0o640)
        return result

    def send_mail_report(self, status):
        """
        1. Collect style data from repository
        2. Collect log files
        3. Build mail report
        4. Send mail report
        5. Cleanup log files
        6. Set and return event_code
        """
        subject = None
        if status == 0:
            subject = "SUCCESS: Application {0} in Project {1} and Subproject {2}".format(
                self.app_name, self.project_name, self.subproject_name)
            self.log_session(2)
        elif status != 0:
            subject = "ERROR: Job {0} failed in application {1}, Project {2} and Subproject {3} " \
                      "with error code {4}".format(self.job_name, self.app_name,
                                                   self.project_name, self.subproject_name, self.event_code)
        self.session_logger.debug(subject)

        current_date = str(time.strftime("%Y-%m-%d %H.%M.%S"))
        current_year = str(time.strftime("%Y"))
        query = "SELECT style_content FROM obj_styles WHERE customer = '{0}' AND " \
                "style_element_name = 'CSS';".format(self.customer)
        result = db.sql_query(DSN, query)
        self.check_app_status('HASI', -9040) if not result else None
        css_style = result[0][0]

        query = "SELECT style_content FROM obj_styles WHERE customer = '{0}' AND " \
                "style_element_name = 'HEADER';".format(self.customer)
        result = db.sql_query(DSN, query)
        self.check_app_status('HASI', -9041) if not result else None
        header = result[0][0]
        """ Replace position parameter with project, subproject and current datetime """
        header = header.format(css_style, self.project_name, self.subproject_name, current_date)

        query = "SELECT style_content FROM obj_styles WHERE customer = '{0}' AND " \
                "style_element_name = 'BODY';".format(self.customer)
        result = db.sql_query(DSN, query)
        self.check_app_status('HASI', -9042) if not result else None
        body_content = result[0][0]
        """ Replace position parameter with project, subproject and current datetime """
        body_content = body_content.format(self.project_name, self.subproject_name, current_date)

        query = "SELECT style_content FROM obj_styles WHERE customer = '{0}' AND " \
                "style_element_name = 'FOOTER';".format(self.customer)
        result = db.sql_query(DSN, query)
        self.check_app_status('HASI', -9043) if not result else None
        footer = result[0][0]

        """ Replace position parameter with current year """
        footer = footer.format(current_year)
        query = "SELECT style_content FROM obj_styles WHERE customer = '{0}' AND " \
                "style_element_name = 'BACKGROUND_COLOR_1';".format(self.customer)
        result = db.sql_query(DSN, query)
        self.check_app_status('HASI', -9044) if not result else None
        background_color_1 = result[0][0]

        query = "SELECT style_content FROM obj_styles WHERE customer = '{0}' AND " \
                "style_element_name = 'BACKGROUND_COLOR_2';".format(self.customer)
        result = db.sql_query(DSN, query)
        self.check_app_status('HASI', -9045) if not result else None
        background_color_2 = result[0][0]

        query = "SELECT style_content FROM obj_styles WHERE customer = '{0}' AND " \
                "style_element_name = 'BACKGROUND_COLOR_3';".format(self.customer)
        result = db.sql_query(DSN, query)
        self.check_app_status('HASI', -9046) if not result else None
        background_color_3 = result[0][0]

        cell_start = "<tr><td bgcolor = '{0}'><pre>".format(background_color_3)
        cell_end = "</pre></td></tr>"
        body = cell_start + body_content + cell_end

        """ Collect log files """
        os.chdir(self.log_directory)
        l_logfiles = glob.glob("{0}.*.log".format(self.subproject_name))
        l_logfiles.sort(key=os.path.getmtime)
        l_ppids = list(set(list(",".join(x.split(".")[-2:-1]) for x in l_logfiles)))

        """ Build report """
        report_filename = "{0}.{1}.{2}.{3}.html".format(
            self.environment, self.project_name, self.subproject_name, os.getpid())
        plot_filename = HasiApplication.plot_app(self, self.log_directory, "svg")
        self.attachment.append(plot_filename)

        """ Write report file """
        with open(report_filename, "w") as fh:
            counter_logs = 1
            fh.write(header)
            for item in l_logfiles:
                with open(item, "r") as current_logfile:
                    if counter_logs % 2 == 0:
                        cell_start = "<tr><td bgcolor = '{0}'><pre>".format(background_color_1)
                        cell_end = "</pre></td></tr>"
                    else:
                        cell_start = "<tr><td bgcolor = '{0}'><pre>".format(background_color_2)
                        cell_end = "</pre></td></tr>"
                    content = cell_start + current_logfile.read() + cell_end
                    fh.write(content)
                    counter_logs += 1
            fh.write(footer)
        self.attachment.append(report_filename)

        """ Get attached document files """
        doctypes = ["*.docx", "*.xlsx", "*.pptx", "*.pdf", "*.png"]
        l_documents = []
        for file in doctypes:
            l_documents.extend(glob.glob(file))
        l_docs = list(x for x in l_documents if x not in l_logfiles)
        l_del_attachment = []
        for x in l_docs:
            file = x
            l_del_attachment.append(file)
            ppid = x.split(".")[-2:-1]
            if ppid[0] in l_ppids:
                self.attachment.append(file)

        """ Set active customers for reporting """
        query = "select email from obj_customer_email where project = '{0}' and subproject = '{1}' and active = 1;" \
                "".format(self.project_name, self.subproject_name)
        result = db.sql_query(DSN, query)
        self.session_logger.debug(result)
        if not result:
            os.remove("{0}/{1}".format(self.log_directory, report_filename)) if os.path.exists("{0}/{1}".format(
                self.log_directory, report_filename)) else None
            self.check_app_status('HASI', -9008)
        self.list_customers = list(x[0] for x in result)

        """ Generate report """
        report = unixlib.MailObject(self.project_name, self.subproject_name, subject, body,
                                    self.list_customers, self.attachment)

        """ Send email """
        result = report.send_mail()
        if result is False:
            event_code = -9012
            self.session_logger.error(result)
        else:
            event_code = 0
            self.session_logger.debug("Versand erfolgreich? {0}".format(result))
        if event_code == 0:
            result = unixlib.archive_file("{0}".format(self.log_directory), "{0}/ARCHIV".format(self.log_directory),
                                          "{0}".format(report_filename))
            self.session_logger.error("Archiving failed.") if result is False else None
            for file in l_del_attachment:
                unixlib.archive_file("{0}".format(self.log_directory), "{0}/ARCHIV".format(self.log_directory),
                                     "{0}".format(file))
        else:
            os.remove("{0}/{1}".format(self.log_directory, report_filename)) if os.path.exists("{0}/{1}".format(
                self.log_directory, report_filename)) else None
        return event_code


# *******************
# ***** M A I N *****
# *******************
# Program runs standalone when called directly by operating system
if __name__ == "__main__":
    desc = """H.A.S.I.
    Programm wurde direkt aufgerufen.
    Benötigte Parameter:
    1. Name der Applikation
    2. Name des Jobs
    Beispiel: python hasi.py -app=<application name> -job=<job_name>
    """
    options = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                      description=textwrap.dedent(desc))
    options.add_argument("-app", "--app_name", dest="app_name", help="Name der Applikaton", metavar="App_Name",
                         required=True)
    options.add_argument("-job", "--job_name", dest="job_name", help="Name des Jobs", metavar="Job Name",
                         required=True)
    options.add_argument("-debug", "--debug", dest="debug", help="Debug Mode", metavar="Debug",
                         type=bool, default=False)
    args = options.parse_args()
    """ Create session instance """
    session = HasiSession(args.app_name, args.job_name)
    sys.exit(session)

# M A I N End
