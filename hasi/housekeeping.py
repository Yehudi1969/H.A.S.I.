# -*- coding: utf8 -*-
###############################################################################
#                               housekeeping.py                               #
###############################################################################
# Copyright (C) 2023  Jens Janzen
# Datum: 2019-05-24
# Change history:

# Libs
import glob
import oralib
import os
import shutil
import socket
import subprocess
import sys
import time

# Constants
standard_encoding = 'utf8'


def pairwise(it):
    it = iter(it)
    while True:
        yield next(it), next(it)


def shell_execute(v_cmd):
    import shlex
    try:
        l_command = shlex.split(v_cmd)
        v_result = subprocess.run(l_command, check=True)
    except subprocess.CalledProcessError as error:
        print(error)
        return error
    else:
        return v_result


def archive_files(v_srcdir, v_tgtdir, extension, v_num_days2archive):
    if not os.path.exists(v_tgtdir):
        try:
            os.makedirs(v_tgtdir)
        except OSError as error:
            print(error)
        try:
            os.chmod(v_tgtdir, 0o770)
        except OSError as error:
            print(error)
    if not os.path.exists(v_srcdir):
        print("{0}: Source Directory {1} not found. Skipping.".format(time.strftime("%Y-%m-%d %H.%M.%S"), v_srcdir))
        return True
    for file in os.listdir(v_srcdir):
        s_long_file = os.path.join(v_srcdir, file)
        t_long_file = os.path.join(v_tgtdir, file)
        if os.path.isfile(s_long_file) and os.stat(s_long_file).st_mtime < now - v_num_days2archive * 86400 \
                and s_long_file.lower().endswith(extension):
            print("Move file {0} to {1}.".format(s_long_file, t_long_file))
            try:
                shutil.copyfile(s_long_file, t_long_file)
                os.remove(s_long_file) if os.path.exists(s_long_file) else None
            except OSError as error:
                print(error)
                raise
        else:
            continue
    return True


def delete_files(v_tgtdir, extension):
    if not os.path.exists(v_tgtdir):
        return True
    else:
        for file in os.listdir(v_tgtdir):
            t_long_file = os.path.join(v_tgtdir, file)
            if os.path.isfile(t_long_file) and t_long_file.lower().endswith(extension):
                print("Remove file {0}.".format(t_long_file))
                os.remove(t_long_file)


def set_work_directory(v_tgtdir):
    if not os.path.exists(v_tgtdir):
        try:
            os.makedirs(v_tgtdir)
        except OSError as error:
            print(error)
            return False
        try:
            os.chmod(v_tgtdir, 0o770)
        except OSError as error:
            print(error)
            return True
    return True


# **************************************
# **************** MAIN ****************
# **************************************
if __name__ == '__main__':
    # Set current time
    now = time.time()

    # Get host name
    host_name = socket.gethostname()

    # Set num_days2archive to 7 days
    num_days2archive = 7

    # Lookup base directory
    query = "select value_text from OBJ_ATT_PROJECT where project='DWH' and key_text='INF_VERSION'"
    repo = oralib.sql_query("HASI", query)[0][0]
    query = "select value_text from OBJ_ATT_PROJECT where project='{0}' and key_text='PMROOTDIR'".format(repo)
    basedir = oralib.sql_query("HASI", query)[0][0]

    # Set work directory to ExtProc
    workdirectory = "{0}/ExtProc".format(basedir)
    target_directory = "{0}/Backup/ExtProc".format(basedir)
    set_work_directory(target_directory)

    print("{0}: Archive Reports older than {1} days.".format(
        time.strftime("%Y-%m-%d %H.%M.%S"), num_days2archive))
    os.chdir(workdirectory)
    l_directories = [x for x in glob.glob("*") if os.path.isdir(x)]
    for project_directory in sorted(l_directories):
        srcdir = "{0}/{1}/LOGS/ARCHIV".format(workdirectory, project_directory)
        tgtdir = "{0}/{1}/LOGS/ARCHIV".format(target_directory, project_directory)
        archive_files(srcdir, tgtdir, ('.html', '.log', '.zip', '.gz'), num_days2archive)

    print("{0}: Deleting temporary database query files.".format(time.strftime("%Y-%m-%d %H.%M.%S")))
    os.chdir(workdirectory)
    l_directories = [x for x in glob.glob("*") if os.path.isdir(x)]
    for project_directory in sorted(l_directories):
        srcdir = "{0}/{1}/LOGS".format(workdirectory, project_directory)
        delete_files(srcdir, ('.tmpquery', '.tmpresult', '.tmperror', '.tmpcols'))

    # Set work directory to SrcFiles
    workdirectory = "{0}/SrcFiles".format(basedir)
    target_directory = "{0}/Backup/SrcFiles".format(basedir)
    set_work_directory(target_directory)

    print("{0}: Archive source files with file extension '.gz' and '.zip' older than {1} days.".format(
        time.strftime("%Y-%m-%d %H.%M.%S"), num_days2archive))
    os.chdir(workdirectory)
    l_directories = [x for x in glob.glob("*") if os.path.isdir(x)]
    for project_directory in sorted(l_directories):
        srcdir = "{0}/{1}/ARCHIV".format(workdirectory, project_directory)
        tgtdir = "{0}/{1}/ARCHIV".format(target_directory, project_directory)
        archive_files(srcdir, tgtdir, ('.zip', '.gz'), num_days2archive)

    # Set work directory to TgtFiles
    workdirectory = "{0}/TgtFiles".format(basedir)
    target_directory = "{0}/Backup/TgtFiles".format(basedir)
    set_work_directory(target_directory)

    print("{0}: Archive target files with file extension '.csv', '.gz' and '.zip' older than {1} days.".format(
        time.strftime("%Y-%m-%d %H.%M.%S"), num_days2archive))
    os.chdir(workdirectory)
    l_directories = [x for x in glob.glob("*") if os.path.isdir(x)]
    for project_directory in sorted(l_directories):
        srcdir = "{0}/{1}/ARCHIV".format(workdirectory, project_directory)
        tgtdir = "{0}/{1}/ARCHIV".format(target_directory, project_directory)
        archive_files(srcdir, tgtdir, ('.zip', '.gz', '.csv', '*'), num_days2archive)

    # Set num_days2archive to 30 days
    num_days2archive = 30

    # Set work directory to SessLogs
    workdirectory = "{0}/SessLogs".format(basedir)
    target_directory = "{0}/Backup/SessLogs".format(basedir)
    set_work_directory(target_directory)

    print("{0}: Archive Session Logfiles older than {1} days.".format(
        time.strftime("%Y-%m-%d %H.%M.%S"), num_days2archive))
    os.chdir(workdirectory)
    l_directories = [x for x in glob.glob("*") if os.path.isdir(x)]
    for project_directory in sorted(l_directories):
        srcdir = "{0}/{1}".format(workdirectory, project_directory)
        tgtdir = "{0}/{1}".format(target_directory, project_directory)
        archive_files(srcdir, tgtdir, ('.bin', '.log', '*'), num_days2archive)

    # Set work directory to WorkflowLogs
    workdirectory = "{0}/WorkflowLogs".format(basedir)
    target_directory = "{0}/Backup/WorkflowLogs".format(basedir)
    set_work_directory(target_directory)

    print("{0}: Archive Workflow Logfiles older than {1} days.".format(
        time.strftime("%Y-%m-%d %H.%M.%S"), num_days2archive))
    os.chdir(workdirectory)
    l_directories = [x for x in glob.glob("*") if os.path.isdir(x)]
    for project_directory in sorted(l_directories):
        srcdir = "{0}/{1}".format(workdirectory, project_directory)
        tgtdir = "{0}/{1}".format(target_directory, project_directory)
        archive_files(srcdir, tgtdir, ('.bin', '.log', '*'), num_days2archive)

    # Set work directory to ParFiles
    # workdirectory = "{0}/ParFiles".format(basedir)
    # target_directory = "{0}/Backup/ParFiles".format(basedir)
    # set_work_directory(target_directory)

    # print("{0}: Archive Parameter Files older than {1} days.".format(
    #     time.strftime("%Y-%m-%d %H.%M.%S"), num_days2archive))
    # os.chdir(workdirectory)
    # l_directories = [x for x in glob.glob("*") if os.path.isdir(x)]
    # for project_directory in sorted(l_directories):
    #     srcdir = "{0}/{1}/ARCHIV".format(workdirectory, project_directory)
    #     tgtdir = "{0}/{1}/ARCHIV".format(target_directory, project_directory)
    #     archive_files(srcdir, tgtdir, '*', num_days2archive)

    print("{0}: Checking file space left on {1}.hv.devk.de".format(
        time.strftime("%Y-%m-%d %H.%M.%S"), host_name))
    cmd = "df -h {0}".format(basedir)
    shell_execute(cmd)

sys.exit(0)
