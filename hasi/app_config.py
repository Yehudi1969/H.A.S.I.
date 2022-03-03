# -*- coding: utf8 -*-
###############################################################################
#                                app_config.py                                #
###############################################################################
# Autor: Jens Janzen
# Datum: 2021-06-09

import argparse
import logging
import oralib
import pandas
import textwrap


# Functions
def list_apps(v_app_name):
    if v_app_name:
        v_query = "select PROJECT, SUBPROJECT, APP_NAME, APP_DESCRIPTION, RUN_CYCLE, ACTIVE " \
                  "from obj_application where app_name='{0}' order by app_name;".format(v_app_name)
    else:
        v_query = "select PROJECT, SUBPROJECT, APP_NAME, APP_DESCRIPTION, RUN_CYCLE, ACTIVE " \
                  "from obj_application order by app_name;"
    return oralib.sql_query(conn, v_query)


def toggle_activation(value):
    v_query = "update obj_application set active={0} where app_name='{1}';".format(value, app_name)
    return oralib.sql_query(conn, v_query)


def output_results(v_result):
    l_header = ['PROJECT', 'SUBPROJECT', 'APP_NAME', 'APP_DESCRIPTION', 'RUN_CYCLE', 'ACTIVE']
    l_data = [x for x in v_result]
    output = pandas.DataFrame(l_data, columns=l_header)
    return output


# ***********************
# ****  M  A  I  N  *****
# ***********************
# Evaluate arguments
desc = """Das Programm führt in der Umgebung, in der es gestartet wird, 
Aktionen auf der H.A.S.I. Tabelle obj_application durch.

Folgende Aktionen sind möglich:
1. Aktionen:
    LIST: Wenn keine Applikation übergeben wird, werden alle Applikationen gelistet, sonst nur die übergebene.
    ACTIVATE: Aktiviert die übergebene Applikation
    DEACTIVATE: Deaktiviert die übergebene Applikation
2. Applikationsname - Name der Application

Beispiele:
    python app_config.py -action=list
    python app_config.py -action=activate -app=<app_name>
    python app_config.py -action=deactivate -app=<app_name>
"""

# Logging
log = logging.getLogger("app_conf")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

options = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                  description=textwrap.dedent(desc))
options.add_argument("-action", "--action", dest="action", help="Name der Aktion", required=True)
options.add_argument("-app", "--application", dest="application", help="Name der Applikation", default=None)

conn = "HASI"
args = options.parse_args()
action = args.action.upper()
app_name = args.application.upper() if args.application else None

# Set Output Frame
pandas.set_option('display.width', 1000)
pandas.set_option('display.max_rows', 500)
pandas.set_option('display.max_columns', 500)

# Action
if action.upper() == "DEACTIVATE" and app_name is not None:
    log.info("Vorher:")
    print(output_results(list_apps(app_name)))
    toggle_activation(0)
    log.info("Nachher:")
    print(output_results(list_apps(app_name)))
elif action.upper() == "ACTIVATE" and app_name is not None:
    log.info("Vorher:")
    print(output_results(list_apps(app_name)))
    toggle_activation(1)
    log.info("Nachher:")
    print(output_results(list_apps(app_name)))
elif action.upper() == "LIST":
    log.info("Status:")
    print(output_results(list_apps(app_name)))
