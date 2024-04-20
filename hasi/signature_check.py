# -*- coding: utf8 -*-
################################################################################
#                              signature_check.py                              #
################################################################################
# Copyright (C) 2023  Jens Janzen
# Change history
# V1.0: 2020-02-25 - Initial Release

# Import standard python libs
import glob
import logging
import magic
import os
import oralib
import pandas

try:
    import pwd
except ImportError:
    import winpwd as pwd

# Description
"""
Das Programm untersucht SHA512 Signaturen auf Programmdateien. Falls sich ein Programm geändert hat,
wird eine Warnung ausgegeben.
Die folgenden Programme werden untersucht:
1. Alle Python Programme im Hasi Verzeichnis.
2. Alle aktiven jobs im H.A.S.I. Repository mit dem Jobtyp SHELLSCRIPT oder PYTHON.
Beispiel: python signature_check.py
"""

# Globals
magic_number = len(os.path.basename(__file__))
hasi_root_directory = os.path.realpath(__file__)[:-magic_number]
hasi_root_directory = hasi_root_directory[:-1] if hasi_root_directory.endswith("/") else None
conn = "HASI"

""" Sets logging console handler for debugging """
log = logging.getLogger("sigcheck")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)


def signature_check(p_job_name, p_directory, p_prog_name):
    """
    Checks for valid key to check for corrupted or changed programs.
    Logs a warning when file has changed.
    """
    global query
    global result
    import hashlib

    if os.path.islink("{0}/{1}".format(p_directory, p_prog_name)):
        log.warning("Datei {0}/{1} ist ein symbolischer Link. Wird nicht bearbeitet.".format(p_directory, p_prog_name))
        return True
    elif not os.path.exists("{0}/{1}".format(p_directory, p_prog_name)):
        log.error("Datei {0}/{1} nicht gefunden.".format(p_directory, p_prog_name))
        return True
    with open("{0}/{1}".format(p_directory, p_prog_name), 'rb') as fh:
        is_encoded = b"DEVK1886"
        identifier = fh.read(len(is_encoded))
        log.debug("Identifier: {0}".format(identifier))
        if identifier == is_encoded:
            log.warning("Datei {0}/{1} ist verschlüsselt und muss vor dem Erzeugen einer "
                        "Signatur entschlüsselt werden.".format(p_directory, p_prog_name))
            return False

    log.debug("Signaturcheck für Job: {0} Datei: {1}/{2}".format(i, p_directory, p_prog_name))
    with open("{0}/{1}".format(p_directory, p_prog_name), 'rb') as fh:
        ds = fh.read()
        c_hash = hashlib.sha512(ds).hexdigest()
        log.debug("c_hash: {0}".format(c_hash))

    query = "select count(1) from obj_signatures where job_name = '{0}' and obj_name = '{1}';".format(
        p_job_name, p_prog_name)
    c_exists = oralib.sql_query(conn, query)[0][0]
    log.debug("Job: {0}, Program: {1}, c_exists: {2}".format(p_job_name, p_prog_name, c_exists))
    if c_exists == 0:
        log.info("Neuer Job gefunden: {0}, Programm: {1}.".format(p_job_name, p_prog_name))
        query = "insert into obj_signatures (job_name, obj_name, signature) " \
                "values ('{0}', '{1}', '{2}');".format(p_job_name, p_prog_name, c_hash)
        oralib.sql_query(conn, query)
        return True
    elif c_exists == 1:
        query = "select signature from obj_signatures where job_name = '{0}' and obj_name = '{1}';".format(
            p_job_name, p_prog_name)
        c_saved = oralib.sql_query(conn, query)[0][0]
        if c_saved != c_hash:
            log.info("Job {0}, Programm {1}/{2} wurde geändert.".format(p_job_name, p_directory, p_prog_name))
            query = "update obj_signatures set signature='{0}', ts_upd=sysdate " \
                    "where job_name='{2}' and obj_name='{1}';".format(
                        c_hash, p_prog_name, p_job_name)
            result = oralib.sql_query(conn, query)
            return False if result is False else True


# *********************************
# ************ M A I N ************
# *********************************
log.debug("H.A.S.I. Directory {0} is used.".format(hasi_root_directory))
pandas.set_option('display.width', 1000)
pandas.set_option('display.max_rows', 500)
pandas.set_option('display.max_columns', 500)

if __name__ == "__main__":
    # Screening of H.A.S.I. core objects
    os.chdir(hasi_root_directory)
    for i in glob.glob("*.py"):
        signature_check("HASI", hasi_root_directory, i)
        file_name = "{0}/{1}".format(hasi_root_directory, i)
        check_line_break = magic.from_file(file_name) if os.path.exists(file_name) else None
        log.debug("Programm {0} hat das Format {1}.".format(file_name, check_line_break))
        if "CRLF" in check_line_break:
            log.error("Datei {0} enthält Windows/DOS Zeilenumbrüche!".format(file_name))

    # Screening of all dependent program objects inside H.A.S.I. repository
    query = "select job_name from obj_job where job_type in ('SHELLSCRIPT', 'PYTHON') " \
            "and active = 1 order by 1;"
    l_jobs = list(x[0] for x in oralib.sql_query(conn, query))
    log.debug(l_jobs)

    for i in l_jobs:
        query = "select c.value_text, a.prog_name from obj_job a, obj_application b, obj_att_project c " \
                "where a.app_name = b.app_name and b.project = c.project and c.key_text = 'PROJECTDIR' " \
                "and a.job_name = '{}';".format(i)
        result = oralib.sql_query(conn, query)
        directory = result[0][0]
        prog_name = result[0][1]
        file_name = "{0}/{1}".format(directory, prog_name)
        check_line_break = magic.from_file(file_name) if os.path.exists(file_name) else None
        log.debug("Programm {0} hat das Format {1}.".format(file_name, check_line_break))
        if check_line_break and "CRLF" in check_line_break:
            log.error("Datei {0} enthält Windows/DOS Zeilenumbrüche!".format(file_name))
        signature_check(i, directory, prog_name)

    # Report of last changes in repository
    query = "select job_name, obj_name, ts_ins, ts_upd from obj_signatures " \
            "where ts_ins != ts_upd and ts_upd >= sysdate -7 order by ts_upd;"
    result = oralib.sql_query(conn, query)
    l_header = ["Job", "Programm", "Angelegt am", "Aktualisiert am"]
    l_data = [x for x in result]
    data = pandas.DataFrame(l_data, columns=l_header)
    log.info("\nÄnderungen von Programmen innerhalb der letzten 7 Tage:\n{0}".format(data))
