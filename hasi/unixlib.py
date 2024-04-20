# -*- coding: utf8 -*-
###############################################################################
#                                 unixlib.py                                  #
###############################################################################
# Copyright (C) 2023  Jens Janzen

# Change history
# V1.0: 2019-04-26 - Initial Release

# Description:
# Library contains classes and helper functions for Unix paths and file access.
# Functions using unix os commands.
# Change history

import datetime
import gzip
import logging
import os
import shutil
import subprocess
import time

import oralib

standard_encoding = "utf8"

""" Sets logging console handler for debugging """
log = logging.getLogger("SESSION")


# Helper functions
def read_file(o_file, chunk_size):
    while True:
        data = o_file.readlines(chunk_size)
        if not data:
            break
        yield data


def scp_remote_files(host, user, path, filename, local_path="."):
    """ Download files from a remote server via scp, that matches a pattern.
    Authentication must be set via RSA key exchange.
    Needs following arguments:
    1. Hostname
    2. Path
    3. Username
    4. Filename or file pattern (Works with joker signs)
    5. Local directory
    """
    import shlex
    command = "scp {0}@{1}:{2}/{3} {4}/".format(user, host, path, filename, local_path)
    l_command = shlex.split(command)
    stmt = subprocess.Popen(l_command, stdin=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
    try:
        output, error = stmt.communicate()
    except OSError as e:
        log.error(e)
        return False

    # Error handling
    if stmt.returncode != 0 and "No such file or directory" in error.decode(standard_encoding).strip():
        log.error(error.decode(standard_encoding).strip())
        return True
    elif stmt.returncode != 0:
        log.error(error.decode(standard_encoding).strip())
        return False
    elif output:
        log.info(output.decode(standard_encoding).strip())
        return True
    else:
        return True


def sftp_remote_files(host, user, path, filename, local_path=".", action="get"):
    """ Download or upload files from/to a remote server via sftp, that matches a pattern.
    Authentication must be set via RSA key exchange.
    Needs following arguments:
    1. Hostname
    2. Username
    3. Remote path
    4. Local path
    6. Filename or file pattern (Works with joker signs)
    """
    import shlex
    os.chdir(local_path)
    port = 22
    command = "sftp -P {0} {1}@{2}".format(port, user, host)
    l_command = shlex.split(command)
    stmt = ""
    output = None
    try:
        stmt = subprocess.Popen(l_command, stdin=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        time.sleep(2)
        stmt.stdin.write(bytes("pwd \n", standard_encoding))
        stmt.stdin.write(bytes("chdir {0}\n".format(path), standard_encoding))
        stmt.stdin.write(bytes("lcd {0}\n".format(local_path), standard_encoding))
        if action.lower() == "get":
            stmt.stdin.write(bytes("get {0}/{1} \n".format(path, filename), standard_encoding))
        elif action.lower() == "put":
            stmt.stdin.write(bytes("put {0} \n".format(filename), standard_encoding))
        stmt.stdin.write(bytes("bye \n", standard_encoding))
        output, error = stmt.communicate()
    except FileNotFoundError as e:
        log.error(e)
    except OSError as e:
        log.error(e)
    # Error handling
    if stmt.returncode == 0 and output is None:
        return None
    elif stmt.returncode != 0:
        return False
    else:
        result = output.decode(standard_encoding).strip()
        return result


def scp_transfer_files(host, user, filename, remote_path):
    """ Upload file object to a remote server via scp. Authentication must be set via RSA key exchange.
    Needs following arguments:
    1. Hostname
    2. Username
    2. Remote directory
    3. Local directory
    4. Filename or list of filenames
    """
    import shlex
    command = "scp {0} {1}@{2}:{3}/".format(filename, user, host, remote_path)
    l_command = shlex.split(command)
    stmt = subprocess.Popen(l_command, stdin=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
    output = None
    error = None
    try:
        output, error = stmt.communicate()
    except OSError as e:
        log.error(e)

    # Error handling
    if stmt.returncode != 0:
        log.error(error.decode(standard_encoding).strip())
        return False
    elif output:
        log.info(output.decode(standard_encoding).strip())
        return True
    else:
        return True


def delete_remote_files(host, user, path, filename):
    """ Delete processed files from a remote server.
    Needs following arguments:
    1. Hostname
    2. Username
    3. Remote path
    4. Filename or a list containing file names
    Function works with a single filename as a string or multiple files in a list.
    """
    import shlex
    port = 22
    command = "sftp -P {0} {1}@{2}".format(port, user, host)
    l_command = shlex.split(command)
    stmt = subprocess.Popen(l_command, stdin=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
    stmt.stdin.write(bytes("chdir {0} \n".format(path), standard_encoding))
    if not isinstance(filename, str):
        for item in filename:
            stmt.stdin.write(bytes("rm {0} \n".format(item), standard_encoding))
    else:
        stmt.stdin.write(bytes("rm {0} \n".format(filename), standard_encoding))
    stmt.stdin.write(bytes("bye \n", standard_encoding))
    output = None
    try:
        output, error = stmt.communicate()
    except OSError as e:
        log.error(e)

    # Error handling
    if stmt.returncode == 0 and output is None:
        return None
    elif stmt.returncode != 0:
        return False
    else:
        # result = output.decode(standard_encoding).strip()
        return True


def archive_file(srcdir, arcdir, filename):
    """ Compress processed files and put into into archive directory.
    Needs the source directory, the archive directory and a file name.
    """
    source_file = "{0}/{1}".format(srcdir, filename)
    target_file = "{0}/{1}.{2}.gz".format(arcdir, time.strftime("%Y%m%d%H%M%S"), filename)
    try:
        with open(source_file, "rb") as f_in:
            with gzip.open(target_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(source_file) if os.path.exists(source_file) else None
        return True
    except OSError as e:
        log.error(e)
        return False


def archive_filelist(srcdir, arcdir, archive_name, filelist):
    """ Put a list of files into a zip archive and move into into archive directory.
    Needs the source directory, the archive directory, archive name and file list name.
    """
    import zipfile
    try:
        import zlib
        compression = zipfile.ZIP_DEFLATED
    except OSError:
        compression = zipfile.ZIP_STORED
    archive_name = "{0}_{1}.zip".format(time.strftime("%Y%m%d%H%M%S"), archive_name)
    zipped = zipfile.ZipFile(archive_name, mode="w")
    try:
        for f in filelist:
            zipped.write(f, compress_type=compression)
    except Exception as e:
        log.error(e)
        raise
    finally:
        zipped.close()
    shutil.move("{0}/{1}".format(srcdir, archive_name), "{0}/{1}".format(arcdir, archive_name)) if os.path.exists(
        "{0}/{1}".format(srcdir, archive_name)) else None


def get_columns_from_fixed_file(row, offset, *args):
    """ Use a generator to cut a line of text into slices given by a list of positions.
    Use offset +1 if the line has field separators.
    Use to process files with fixed line length.
    """
    position = 0
    for length in args:
        yield row[position:position + int(length)]
        position = position + int(length) + offset


def encrypt_file(infile, outfile, key):
    from Crypto.Cipher import AES
    from Crypto import Random
    chunk_block_size = 64 * 1024
    is_encoded = b"_@_"
    iv = Random.new().read(AES.block_size)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    # Check whether file is already encrypted
    with open(infile, 'rb') as fh_in:
        identifier = fh_in.read(len(is_encoded))
        if identifier == is_encoded:
            return False
    with open(infile, 'rb') as fh_in:
        with open(outfile, 'wb') as fh_out:
            fh_out.write(is_encoded)
            fh_out.write(iv)
            while True:
                chunk = fh_in.read(chunk_block_size)
                if len(chunk) == chunk_block_size:
                    fh_out.write(cipher.encrypt(chunk))
                elif len(chunk) > 0 < chunk_block_size:
                    fh_out.write(cipher.encrypt(chunk + b"\0" * (chunk_block_size - len(chunk) % chunk_block_size)))
                    break
                else:
                    break
    return True


def decrypt_file(infile, outfile, key):
    from Crypto.Cipher import AES
    chunk_block_size = 64 * 1024
    is_encoded = b"_@_"
    with open(infile, 'rb') as fh_in:
        identifier = fh_in.read(len(is_encoded))
        if identifier != is_encoded:
            return False
        iv = fh_in.read(AES.block_size)
        cipher = AES.new(key, AES.MODE_CBC, iv)
        with open(outfile, 'wb') as fh_out:
            while True:
                chunk = fh_in.read(chunk_block_size)
                plaintext = cipher.decrypt(chunk)
                if len(plaintext) == 0:
                    break
                fh_out.write(plaintext.rstrip(b"\0"))
    return True


def remote_shell(host, user, command, password=None):
    import paramiko
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(host, port=22, username=user, password=password, timeout=30)
    except paramiko.ssh_exception.SSHException as error:
        """ Session does not exist """
        log.error(error)
        return False
    except IOError as error:
        """ Bad hostname """
        log.error(error)
        return False
    except paramiko.PasswordRequiredException as error:
        """ Bad password """
        log.error(error)
        return False

    try:
        stdin, stdout, stderr = ssh.exec_command(command, timeout=360)
        log.info(stdout.read().decode(standard_encoding))
        v_event_code = stdout.channel.recv_exit_status()
        if stdout.channel.recv_exit_status() != "0":
            log.error(stderr.read().decode(standard_encoding))
        ssh.close()
        return v_event_code
    except paramiko.ssh_exception as error:
        log.error(error)
        ssh.close()
        return False


def run_system_command(command):
    import shlex
    l_command = shlex.split(command)
    stmt = subprocess.run(l_command, capture_output=True)
    if stmt.returncode == 0:
        return True
    else:
        return False


def convert_date(x, date_format, lazy=False):
    fixed_export_format = "%Y-%m-%d"
    if lazy is True:
        s_format = date_format.replace(
            "%Y", "YYYY").replace("%m", "MM").replace("%d", "DD")
        pos_year = (s_format.find("YYYY"))
        pos_month = (s_format.find("MM"))
        pos_day = (s_format.find("DD"))
        year = x[pos_year:pos_year + 4]
        month = x[pos_month:pos_month + 2]
        day = x[pos_day:pos_day + 2]
        year = "1900" if year < "1900" else year
        month = "01" if month == "00" or month > "12" else month
        day = "01" if day == "00" or day > "31" else day
        log.debug("{0}-{1}-{2}".format(year, month, day))
        try:
            datetime.datetime.strptime("{0}{1}{2}".format(year, month, day), '%Y%m%d')
        except ValueError as e:
            log.error(e)
            return "INVALID"
        return "{0}-{1}-{2}".format(year, month, day)
    else:
        try:
            d_date = datetime.datetime.strptime(x, date_format)
        except ValueError as e:
            log.error(e)
            return "INVALID"
        return d_date.strftime(fixed_export_format)


def convert_time(x, time_format, lazy=False):
    fixed_export_format = "%H:%M:%S"
    if lazy is True:
        s_format = time_format.replace(
            "%H", "HH").replace("%M", "MI").replace("%S", "SS")
        pos_hour = (s_format.find("HH"))
        pos_minute = (s_format.find("MI"))
        pos_second = (s_format.find("SS"))
        hour = x[pos_hour:pos_hour + 2]
        minute = x[pos_minute:pos_minute + 2]
        second = x[pos_second:pos_second + 2]
        hour = "00" if hour > "23" else hour
        minute = "00" if minute > "59" else minute
        second = "00" if second > "59" else second
        log.debug("{0}:{1}:{2}".format(hour, minute, second))
        try:
            datetime.datetime.strptime("{0}{1}{2}".format(hour, minute, second), '%H%M%S')
        except ValueError as e:
            log.error(e)
            return "INVALID"
        return "{0}:{1}:{2}".format(hour, minute, second)
    else:
        try:
            d_time = datetime.datetime.strptime(x, time_format)
        except ValueError as e:
            log.error(e)
            return "INVALID"
        return d_time.strftime(fixed_export_format)


def convert_timestamp(x, timestamp_format, lazy=False):
    fixed_export_format = "%Y-%m-%d %H:%M:%S"
    if lazy is True:
        s_format = timestamp_format.replace("%Y", "YYYY").replace(
            "%m", "MM").replace("%d", "DD").replace(
            "%H", "HH").replace("%M", "MI").replace("%S", "SS")
        pos_year = (s_format.find("YYYY"))
        pos_month = (s_format.find("MM"))
        pos_day = (s_format.find("DD"))
        pos_hour = (s_format.find("HH"))
        pos_minute = (s_format.find("MI"))
        pos_second = (s_format.find("SS"))
        year = x[pos_year:pos_year + 4]
        month = x[pos_month:pos_month + 2]
        day = x[pos_day:pos_day + 2]
        hour = x[pos_hour:pos_hour + 2]
        minute = x[pos_minute:pos_minute + 2]
        second = x[pos_second:pos_second + 2]
        year = "1900" if year < "1900" else year
        month = "01" if month == "00" or month > "12" else month
        day = "01" if day == "00" or day > "31" else day
        hour = "00" if hour > "23" else hour
        minute = "00" if minute > "59" else minute
        second = "00" if second > "59" else second
        log.debug("{0}-{1}-{2} {3}:{4}:{5}".format(year, month, day, hour, minute, second))
        try:
            datetime.datetime.strptime("{0}{1}{2}{3}{4}{5}".format(
                year, month, day, hour, minute, second), "%Y%m%d%H%M%S")
        except ValueError as e:
            log.error(e)
            return "INVALID"
        return "{0}-{1}-{2} {3}:{4}:{5}".format(year, month, day, hour, minute, second)
    else:
        try:
            d_timestamp = datetime.datetime.strptime(x, timestamp_format)
        except ValueError as e:
            log.error(e)
            return "INVALID"
        return d_timestamp.strftime(fixed_export_format)


def get_default_values(data_type, date_format="%Y-%m-%d", time_format="%H:%M:%S", timestamp_format="%Y-%m-%d %H:%M:%S"):
    """ Generates default values for data types.
    Needs the data type as argument.
    """
    s_date = str(time.strftime(date_format))
    s_timestamp = str(time.strftime(timestamp_format))
    s_time = str(time.strftime(time_format))
    s_default = None
    i_default = 0
    n_default = 0
    value = None
    if data_type[0] in ('CHAR', 'CLOB', 'LONG', 'NVARCHAR2', 'VARCHAR2'):  # character types and varchar
        value = s_default
    elif data_type[0] == 'NUMBER':  # Number
        value = n_default
    elif data_type[0] == 'DATE':  # Date
        value = s_date
    elif data_type[0] == 'TIME':  # Time
        value = s_time
    elif data_type[0] == 'TIMESTAMP':  # Timestamp
        value = s_timestamp
    elif data_type[0] == 'INTERVAL':  # Interval
        value = i_default
    return value


def check_col_values(data_type, value, delimiter="|", lazy=False):
    """ Check and convert common data types from file to CSV.
    Needs a data type and the value of the column as arguments.
    """
    if data_type[0] in ('CHAR', 'CLOB', 'LONG', 'NVARCHAR2', 'VARCHAR2'):  # Character types and varchar
        value = value.replace(delimiter, '').strip()
        value = None if len(value) == 0 else value
    elif data_type[0] == 'NUMBER':  # Number
        # Strip blanks
        value = value.replace(' ', '')
        if len(value) > 0:
            # Convert comma to decimal point
            value = value.replace(',', '.')
            if 'E' in value or 'e' in value:
                # Convert string containing number with exponential representation to float representation
                try:
                    value = str(float(value))  # Try to convert exponential value
                except ValueError:
                    index = value.index('e')
                    value = value[0:index]  # If value is still inconvertible, set it to all numbers before e
            # Delete positive sign
            value = value.replace("+", "")
            if value[-1] == '-':
                # Convert sign in signed number to prefix if sign is a suffix
                value = "-{0}".format(value[:-1])
            if data_type[2] > 0 and "." not in value:
                # Set decimal point if missing and metadata has decimal places
                value = "{0}.{1}".format(value[:-int(data_type[2])], value[-int(data_type[2]):])
        elif len(value) == 0 and data_type[2] > 0:
            # Convert value with decimal places
            value = 0.0
        elif len(value) == 0 and not data_type[2] == 0:
            value = 0
    elif data_type[0] == 'DATE':  # Date
        dt_format = data_type[-1]
        if len(value.rstrip()) < 8:
            value = ""
        else:
            value = convert_date(value, dt_format, lazy=True) if lazy is True else convert_date(value, dt_format)
    elif data_type[0] == 'TIME':  # Time
        dt_format = data_type[-1]
        if len(value.rstrip()) != 6:
            value = ""
        else:
            value = convert_time(value, dt_format, lazy=True) if lazy is True else convert_time(value, dt_format)
    elif data_type[0] == 'TIMESTAMP':  # Timestamp
        dt_format = data_type[-1]
        if len(value.rstrip()) < 8:
            value = ""
        else:
            value = convert_timestamp(value, dt_format, lazy=True) if lazy is True else \
                convert_timestamp(value, dt_format)
    return value


# Classes
class FileObject(object):
    """ Klasse für ein Dateiobjekt.
    Enthaltene Attribute:
    name, string: Name des Fileobjekts. Sollte als Tabellenname in Metadatentabelle auflösbar sein.
    system, string: Quellsystem. Sollte als SAP_SID in Metadatentabelle auflösbar sein.
    date_format, string: Datumsformat als datetime Direktive
    Beispiel: YYYY-MM-DD : %Y-%m-%d
    time_format, string: Zeitformat als datetime Direktive
    Beispiel: HH24:MI:SS : %H:%M:%S
    timestamp_format, string: Zeitstempelformat als datetime Direktive
    Beispiel: YYYY-MM-DD HH24:MI:SSFF : %Y-%m-%d %H:%M:%S%f (%f: Microsekunden sind optional und dann 6-stellig)
    fixed_format, string: 0: CSV, 1: feste Länge
    Im Falle eines Formats mit fester Länge muss ein Eintrag in der Metadatentabelle existieren, wobei der Zugriff
    über den Parameter SAP_SID=system und TABELLE=name erfolgt.
    delimiter, string: Trennzeichen zwischen Datenfeldern, default ist "|"
    encoding, string: Encoding, default ist "utf8"
    """

    def __init__(self, name, system, date_format="%Y%m%d", time_format="%H%M%S", timestamp_format="%Y%m%d%H%M%S",
                 fixed_format=False, delimiter="|", encoding="utf8"):
        """

        @rtype: object
        """
        self.name = name.upper()
        self.system = system.upper()
        self.date_format = date_format
        self.time_format = time_format
        self.timestamp_format = timestamp_format
        self.fixed_format = fixed_format
        self.delimiter = delimiter
        self.encoding = encoding
        self.column_list = self.get_column_list()
        self.l_column_sizes = self.get_column_size_list()
        self.d_col_data_type = self.get_d_col_data_type()
        self.avro_schema = self.get_avro_schema()
        self.row_length = sum(x for x in self.l_column_sizes)
        self.d_col_position = dict(list(zip(self.column_list, range(len(self.column_list)))))

    def __str__(self):
        return str(self.__doc__)

    def get_column_list(self):
        query = "select feldname from obj_ddic_catalog where sap_sid='{0}' and tabelle='{1}' and infile=1 " \
                "order by tabpos;".format(self.system, self.name)
        set_result = oralib.sql_query("HASI", query)
        if set_result is False:
            log.info("Table {0} was not found in metadata table obj_ddic_catalog for SID {1}".format(
                self.system, self.name))
            return False
        self.column_list = list(x[0] for x in set_result)
        return self.column_list

    def get_column_size_list(self):
        query = "select ausgabelaenge from obj_ddic_catalog where sap_sid='{0}' and tabelle='{1}' " \
                "and feldname in ('{2}') order by tabpos;".format(self.system, self.name, "','".join(self.column_list))
        set_result = oralib.sql_query("HASI", query)
        if set_result is False:
            log.info("Table {0} was not found in metadata table obj_ddic_catalog for SID {1}".format(
                self.system, self.name))
            return False
        self.l_column_sizes = list(x[0] for x in set_result)
        return self.l_column_sizes

    def get_d_col_data_type(self):
        """ SQL Lookup auf Metadatentabelle obj_ddic_catalog in H.A.S.I.
            Erzeugtes Dictionary enthält:
            Feldname : [ Datentyp, Ausgabelänge, Anzahl Nachkommastellen, Nullable und optional Datetime Format ]
        """
        query = "select FELDNAME, " \
                "case when dynprotyp in ('CURR', 'DEC', 'FLTP', 'INT2', 'INT4', 'QUAN') then 'NUMBER' " \
                "when dynprotyp = 'DATS' then 'DATE' " \
                "when dynprotyp = 'TIMS' then 'TIME' " \
                "when dynprotyp = 'TIMESTAMP' then 'TIMESTAMP' " \
                "when dynprotyp in ('ACCP', 'CHAR', 'CLNT', 'CUKY', 'LANG', 'NUMC', 'RAW', 'UNIT') " \
                "then 'VARCHAR2' " \
                "end as data_type,  AUSGABELAENGE, DEZSTELLEN, " \
                "case when SCHLUESSELFELD = 'X' then 'N' else 'Y' end as nullable " \
                "from obj_ddic_catalog where sap_sid = '{0}' and tabelle = '{1}' and infile=1 " \
                "order by tabpos;".format(self.system, self.name)
        set_result = oralib.sql_query("HASI", query)
        if set_result is False:
            log.info("Table {0} was not found in metadata table obj_ddic_catalog for SID {1}".format(
                self.system, self.name))
            return False
        d_result = {}
        for row in set_result:
            column = list(row)
            if column[1] == "DATE":
                column.append(self.date_format)
            elif column[1] == "TIME":
                column.append(self.time_format)
            elif column[1] == "TIMESTAMP":
                column.append(self.timestamp_format)
            d_result[column[0]] = list(column[1:])
        self.d_col_data_type = d_result
        return self.d_col_data_type

    def get_avro_schema(self):
        """
            Build avro schema from file class
        """
        import fastavro
        l_descriptor = []
        l_avro_columns = []
        for item in self.column_list:
            if self.d_col_data_type.get(item)[0] == 'NUMBER' and self.d_col_data_type.get(item)[2] == 0:
                l_descriptor.append(("{0}".format(item), "long", "{0}".format(self.d_col_data_type.get(item)[3])))
            elif self.d_col_data_type.get(item)[0] == 'NUMBER' and self.d_col_data_type.get(item)[2] > 0:
                l_descriptor.append(("{0}".format(item), "float", "{0}".format(self.d_col_data_type.get(item)[3])))
            else:
                l_descriptor.append(("{0}".format(item), "string", "{0}".format(self.d_col_data_type.get(item)[3])))
        d_avro_schema = {"namespace": "aws.file.avro.{0}".format(self.system),
                         "type": "record",
                         "name": "{0}".format(self.name),
                         "fields": []}
        for item in l_descriptor:
            d_field_entry = {}
            """ Check nullable columns """
            if item[2] == 'Y':
                d_field_entry["name"] = "{0}".format(item[0])
                d_field_entry["type"] = ["{0}".format(item[1]), "null"]
            elif item[2] == 'N':
                d_field_entry["name"] = "{0}".format(item[0])
                d_field_entry["type"] = "{0}".format(item[1])
            log.debug(d_field_entry)
            l_avro_columns.append(d_field_entry.copy())
        d_avro_schema["fields"] = l_avro_columns
        self.avro_schema = fastavro.parse_schema(d_avro_schema)
        log.debug("AVRO Schema: {0}".format(self.avro_schema))
        return self.avro_schema

    def csv_export(self, directory, filename, extraction_timestamp, headline_exists,
                   metadata_columns=None, lazy_read=False):
        """
            Export file content into an CSV textfile.
        """
        import csv
        # Metadata columns
        headline = list(self.column_list)
        meta_values = []
        for i in metadata_columns:
            log.debug("Metadatenfeld: {0}".format(i))
            if i == "TA_CHDATE":
                headline.append("TA_CHDATE")
                meta_values.append(convert_date(extraction_timestamp[:8], date_format=self.date_format,
                                                lazy=lazy_read))
            elif i == "TA_CHTIME":
                headline.append("TA_CHTIME")
                meta_values.append(convert_time(extraction_timestamp[8:], time_format=self.time_format,
                                                lazy=lazy_read))
            elif i == "TA_EXTRAKTIONSDATUM":
                headline.append("TA_EXTRAKTIONSDATUM")
                meta_values.append(convert_timestamp(extraction_timestamp,
                                                     timestamp_format=self.timestamp_format, lazy=lazy_read))
            elif i == "TA_LADEDATUM":
                headline.append("TA_LADEDATUM")
                meta_values.append(convert_timestamp(time.strftime("%Y%m%d%H%M%S"),
                                                     timestamp_format=self.timestamp_format, lazy=lazy_read))
            elif i == "TA_LADE_DATUM":
                headline.append("TA_LADE_DATUM")
                meta_values.append(convert_timestamp(time.strftime("%Y%m%d%H%M%S"),
                                                     timestamp_format=self.timestamp_format, lazy=lazy_read))
            elif i == "TA_LADE_ID":
                headline.append("TA_LADE_ID")
                meta_values.append(0)
            elif i == "TA_QUELLSYSTEM":
                headline.append("TA_QUELLSYSTEM")
                meta_values.append(self.system)
            elif i == "TA_QUELLSTRUKTUR":
                headline.append("TA_QUELLSTRUKTUR")
                meta_values.append(self.name)
            elif i == "TA_QUELLSTRUKTUR_VERSION":
                headline.append("TA_QUELLSTRUKTUR_VERSION")
                meta_values.append(0)
            elif i == "TA_DWH_ID":
                headline.append("TA_DWH_ID")
                meta_values.append(0)
            elif i == "QUELLSYSTEM":
                headline.append("QUELLSYSTEM")
                meta_values.append(self.system)
            elif i == "TS_EXTRAKTION":
                headline.append("TS_EXTRAKTION")
                meta_values.append(extraction_timestamp)
        log.debug("Kopfzeile: {0}".format(headline))
        tgt_filename = "{0}.{1}.csv".format(time.strftime("%Y%m%d%H%M%S"), filename)
        err_filename = "{0}.{1}.err".format(time.strftime("%Y%m%d%H%M%S"), filename)
        log.info("Export {0}, file: {1}\nto CSV file {2}\nCodepage: {3}, \nDelimiter: {4}.".format(
            directory, filename, tgt_filename, self.encoding, self.delimiter))
        with open(tgt_filename, 'w') as out_file:
            with open(err_filename, 'w') as err_file:
                writer = csv.writer(out_file, dialect='excel', delimiter=self.delimiter,
                                    quoting=csv.QUOTE_ALL, quotechar='"')
                errorfile = csv.writer(err_file, dialect='excel', delimiter=self.delimiter,
                                       quoting=csv.QUOTE_ALL, quotechar='"')
                writer.writerow(headline)
                errorfile.writerow(headline)
                with open("{0}/{1}".format(directory, filename), "r", encoding=self.encoding) as in_file:
                    next(in_file) if headline_exists is True else None
                    if self.fixed_format is True:
                        for line in in_file:
                            l_row = list(get_columns_from_fixed_file(line, 0, *self.l_column_sizes))
                            l_csv_row = list(check_col_values(self.d_col_data_type.get(i),
                                                              l_row[self.d_col_position.get(i)].strip(),
                                                              delimiter=self.delimiter,
                                                              lazy=lazy_read)
                                             for i in self.column_list)
                            if "INVALID" in [x for x in l_csv_row]:
                                errorfile.writerow(l_csv_row)
                            else:
                                l_csv_row.extend(meta_values)
                                writer.writerow(l_csv_row)
                    else:
                        reader = csv.reader(in_file, dialect='excel', delimiter=self.delimiter,
                                            quoting=csv.QUOTE_ALL, quotechar='"')
                        for line in reader:
                            l_csv_row = list(check_col_values(self.d_col_data_type.get(i),
                                                              line[self.d_col_position.get(i)].strip(),
                                                              delimiter=self.delimiter,
                                                              lazy=lazy_read)
                                             for i in self.column_list)
                            if "INVALID" in [x for x in l_csv_row]:
                                errorfile.writerow(l_csv_row)
                            else:
                                l_csv_row.extend(meta_values)
                                writer.writerow(l_csv_row)
        return [True, tgt_filename]

    def avro_export(self, directory, filename, headline_exists, chunk_size=100000, lazy_read=False):
        """
            Export file content into an Avro binary file.
        """
        import fastavro
        tgt_filename = "{0}.{1}.avro".format(time.strftime("%Y%m%d%H%M%S"), filename)
        log.info("Export {0}, file: {1}\nto avro file {2}\nCodepage: {3}.".format(
            directory, filename, tgt_filename, self.encoding))
        with open("{0}/{1}".format(directory, tgt_filename), "a+b") as out_file:
            with open("{0}/{1}".format(directory, filename), "r", encoding=self.encoding) as in_file:
                next(in_file) if headline_exists is True else None
                if self.fixed_format is True:
                    for block in read_file(in_file, chunk_size):
                        l_record = []
                        lines = [x.replace("\n", "") for x in block]
                        for line in lines:
                            l_row = list(get_columns_from_fixed_file(line, 0, *self.l_column_sizes))
                            l_csv_row = list(check_col_values(self.d_col_data_type.get(i),
                                                              l_row[self.d_col_position.get(i)].strip(),
                                                              delimiter=self.delimiter,
                                                              lazy=lazy_read)
                                             for i in self.column_list)
                            record = dict(zip(self.column_list, l_csv_row))
                            l_record.append(record)
                        fastavro.writer(out_file, self.avro_schema, l_record, codec="deflate")
                else:
                    import csv
                    reader = csv.reader(in_file, dialect='excel', delimiter=self.delimiter,
                                        quoting=csv.QUOTE_ALL, quotechar='"')
                    for line in reader:
                        l_record = []
                        l_csv_row = list(check_col_values(self.d_col_data_type.get(i),
                                                          line[self.d_col_position.get(i)].strip(),
                                                          delimiter=self.delimiter,
                                                          lazy=lazy_read)
                                         for i in self.column_list)
                        record = dict(zip(self.column_list, l_csv_row))
                        l_record.append(record)
                        fastavro.writer(out_file, self.avro_schema, l_record, codec="deflate")

    @staticmethod
    def avro2csv(directory, filename, delimiter="|"):
        import csv
        import copy
        import fastavro
        import json
        with open("{0}/{1}".format(directory, filename), "rb") as s:
            with open("{0}/{1}.csv".format(directory, filename), "w") as t:
                reader = fastavro.reader(s)
                metadata = copy.deepcopy(reader.metadata)
                extracted_schema = json.loads(metadata["avro.schema"])
                log.debug("Schema: {0}".format(extracted_schema))
                writer = csv.writer(t, dialect='excel', delimiter=delimiter, quoting=csv.QUOTE_ALL,
                                    quotechar='"')
                writer.writerow(list(x.get("name") for x in extracted_schema.get("fields")))
                for record in reader:
                    writer.writerow(list(record.values()))


class MailObject(object):
    """
        Klasse für ein Mailobjekt.
        Enthaltene Attribute:
        project - Projektzuordnung
        subproject - Teilprojektzuordnung
        subject - Betreff der E-Mail
        body - Inhalt der E-Mail
        recipients - Empfänger
        attachment - Dateiname des Attachments
    """

    def __init__(self, project, subproject, subject, body, recipients, attachment=None):
        self.html_document = None
        self.recipients = recipients
        self.project = project
        self.subproject = subproject
        self.subject = subject
        self.body = body
        self.recipients = recipients
        self.attachment = attachment

    def __str__(self):
        return str(self.__doc__)

    def send_mail(self):
        """
        Sends an E-Mail via Popen command to sendmail. Returns 0 when sending was successful.
        """
        import smtplib
        if self.attachment:
            from email.mime.multipart import MIMEMultipart
            from email.mime.text import MIMEText
            from email.mime.application import MIMEApplication
            # generate a RFC 2822 message
            msg = MIMEMultipart("alternative")
            msg["From"] = os.environ.get("USER")
            msg["To"] = ",".join(self.recipients)
            msg["Subject"] = self.subject
            msg["Content-Type"] = "message/rfc822"
            msg["Content-Disposition"] = "inline"
            msg.attach(MIMEText(self.body, "html", "iso-8859-1"))
            for attached_file in self.attachment:
                with open(attached_file, "rb") as f:
                    attachment = MIMEApplication(f.read(), "subtype")
                    attachment["Content-Type"] = "application/octet-stream; name={}".format(attached_file)
                    attachment["Content-Disposition"] = "attachment; attached_file='%s';" % attached_file
                    msg.attach(attachment)
            message = msg.as_string()
        else:
            from email.mime.text import MIMEText
            # generate a RFC 2822 message
            msg = MIMEText(self.body)
            msg["From"] = os.environ.get("USER")
            msg["To"] = ",".join(self.recipients)
            msg["Subject"] = self.subject
            msg["Content-Type"] = "text/html; charset=en_US.ISO8859-1"
            message = msg.as_string()

        # Using installed MTA on localhost
        with smtplib.SMTP('localhost') as smtp:
            try:
                smtp.sendmail(os.environ.get("USER"), self.recipients, message)
            except smtplib.SMTPException as e:
                log.error(e)
                return False
            finally:
                smtp.close()
            return True
