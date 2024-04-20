# -*- coding: utf8 -*-
###############################################################################
#                                tdm_runner.py                                #
###############################################################################
# Copyright (C) 2023  Jens Janzen

# Change history
# V1.0: 2024-02-14 - Initial Release

import argparse
import logging
import oralib
import os
import sys
import textwrap
import unixlib

# Globals
standard_encoding = "utf8"
hasi_root_directory = os.path.dirname(os.path.realpath(__file__))
os.chdir(hasi_root_directory)

""" Sets logging console handler for debugging """
log = logging.getLogger("SESSION")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)


def create_pool(l_tasks, ie):
    l_finished = []
    l_failed = []
    l_tasks = list(zip(l_tasks, list(ie for x in range(len(l_tasks)))))
    from multiprocessing import Pool
    from multiprocessing import cpu_count
    num_proc = cpu_count() - 1
    with Pool(processes=num_proc) as p:
        for result in p.map(start_app, l_tasks):
            if result[1] == 0:
                l_finished.append(result[0][0])
            else:
                l_failed.append(result[0][0])
        log.info("Beendet: {0}".format(l_finished))
        log.info("Abgebrochen: {0}".format(l_failed))


def start_app(app):
    log.info("Start Applikation: {0}".format(app[0]))
    ie = app[1]
    if ie is True:
        cmd = "python {0}/app_runner.py -app={1} -ie=1".format(hasi_root_directory, app[0])
    else:
        cmd = "python {0}/app_runner.py -app={1}".format(hasi_root_directory, app[0])
    rc = unixlib.run_system_command(cmd)
    return app, 0 if rc is True else app, 1


# *******************
# ***** M A I N *****
# *******************
# Program runs standalone when called directly by operating system
if __name__ == "__main__":
    # Evaluate arguments
    desc = """H.A.S.I. TDM Runner:
    Programm wurde direkt aufgerufen.
    Benötigte Parameter:
    1. Name des Projekts / des Bestückungslaufs
    Beispiel: python tdm_runner.py -p=VPROD2SITU
    """
    options = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                      description=textwrap.dedent(desc))
    options.add_argument("-p", "--project", dest="project", help="Name des Bestückungslaufes", required=True)
    options.add_argument("-f", "--force", dest="force", help="Die Applikation wird zurückgesetzt ", type=int, default=0)
    args = options.parse_args()

    # Create project instance
    project = args.project
    force: bool = True if args.force == 1 else False
    query = "select app_name from obj_application where project='{0}' and active=1 order by app_name".format(project)
    l_apps = list(x[0] for x in oralib.sql_query("TDM_HASI", query))
    log.info("Die folgenden Applikationen werden verarbeitet:\n{0}".format(l_apps))
    create_pool(l_apps, True)
    query = "select app_name from obj_job_plan where app_name in ('{0}') " \
            "and status=3 group by app_name".format("', '".join(l_apps))
    l_apps = list(x[0] for x in oralib.sql_query("TDM_HASI", query))
    create_pool(l_apps, False)
    sys.exit(0)
# M A I N End
