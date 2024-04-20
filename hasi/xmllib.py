# -*- coding: utf8 -*-
###############################################################################
#                                 xmllib.py                                   #
###############################################################################
# Copyright (C) 2023  Jens Janzen

# Change history
# V1.0: 2019-04-26 - Initial Release

# Description:
# Library contains XML class with methods to parse, validate and conversion from csv files.

import itertools
import os
import time

from lxml import etree


class XMLObject(object):
    def __init__(self, name, xsdpath, column_list=None, treesize=1, treecolumns=None, customer_extensions=(),
                 delim="|", validation=True):
        """
            XML Class that holds methods for reading, parsing, writing xml objects.
        """
        self.name = name
        self.xsdpath = xsdpath
        self.delim = delim
        if column_list:
            self.column_list = column_list
        else:
            self.column_list = self.check_csv_header(self.delim)
        if self.column_list is None:
            raise ValueError
        self.treesize = treesize
        self.treecolumns = treecolumns
        self.customer_extensions = customer_extensions
        self.validation = validation
        self.record = []
        self.l_xmlfiles = []

    def check_csv_header(self, delim):
        try:
            with open("{0}.csv".format(self.name), "r") as fh_in:
                result = fh_in.readline().strip().split(delim)
        except FileNotFoundError:
            raise
        return result

    @staticmethod
    def read_next_n_lines(file_handler, number_of_lines):
        return [x.strip() for x in itertools.islice(file_handler, number_of_lines)]

    @staticmethod
    def compress_file(filename):
        import gzip
        import shutil
        with open(filename, "rb") as f_in:
            with gzip.open(filename + ".gz", "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(filename)
        print("{0}: File {1} compressed successfully.".format(time.strftime('%Y-%m-%d %H.%M.%S'), filename))

    def json2xml(self, csvfile, record_tag, headers=False, compression=False):
        """
        1. Create a xml structure from columns
        2. Read rows from CSV File
        3. Write records into one or more xml files according to size of source file
        """
        # Declarations
        record1 = None
        custom_tag = None
        global ce_exists

        # Obtain quantity structure
        # Blue Yonder restriction
        maxfilesize = 2000000000
        # Azure restriction 99 MB per file
        # maxfilesize = 99000000
        stat_csv_file = os.stat(csvfile)
        csv_filesize = stat_csv_file.st_size
        print("{0}: Size of CSV file is {1} bytes.".format(time.strftime('%Y-%m-%d %H.%M.%S'), csv_filesize))
        with open(csvfile, "r") as fh_rowcount:
            if headers:
                total_rows = sum(1 for _ in fh_rowcount) - 2
            else:
                total_rows = sum(1 for _ in fh_rowcount) - 1

        # XML fixed value length without records
        # 1. Header length in bytes
        # 2. Category name length + tags + linefeed in bytes
        # 3. Record closing tag length in bytes
        xml_header = 38
        xml_category = len("".join(self.name)) * 2 + 5 + 2
        xml_static_length = xml_header + xml_category

        print("{0}: Write sample XML file with one record.".format(time.strftime('%Y-%m-%d %H.%M.%S')))
        with open(csvfile, "r") as fh_csv_in:
            if headers:
                next(fh_csv_in)
            xmlfile = "{0}.sample".format(self.name)
            with open(xmlfile, "wb") as fh_xml_out:
                # Build XML Tree
                root = etree.Element("{0}".format(self.name))
                line = fh_csv_in.readline().strip()
                row = [x.strip() for x in line.split(self.delim)]
                record = etree.SubElement(root, record_tag)
                ce_exists = False
                for col, item in itertools.zip_longest(self.column_list, row):
                    if self.treesize > 1 and col in self.treecolumns:
                        # Hierarchy definition
                        pass
                    if item is None:
                        continue
                    elif str(item).strip() == "":
                        continue
                    elif str(item).strip() == "t":
                        item = "true"
                    elif str(item).strip() == "f":
                        item = "false"
                    if col in self.customer_extensions and ce_exists is True:
                        record1 = etree.SubElement(custom_tag, col)
                    elif col in self.customer_extensions and ce_exists is False:
                        custom_tag = etree.SubElement(record, "CustomerExtensions")
                        record1 = etree.SubElement(custom_tag, col)
                        ce_exists = True
                    elif col not in self.customer_extensions:
                        record1 = etree.SubElement(record, col)
                        ce_exists = False
                    record1.text = str(item)
                etree.ElementTree(root).write(fh_xml_out, pretty_print=True, xml_declaration=True,
                                              encoding="UTF-8")
            print("{0}: Sample XML file {1} written.".format(time.strftime('%Y-%m-%d %H.%M.%S'), xmlfile))
            if self.validation:
                result = self.validate_xml_with_xsd(xmlfile, "{0}/{1}.xsd".format(self.xsdpath, self.name))
                if result is False:
                    print(result)
                    return False
                print("{0}: Sample XML file {1} validated.".format(time.strftime('%Y-%m-%d %H.%M.%S'), xmlfile))
        stat_xml_file = os.stat(xmlfile)
        xml_sample_filesize = stat_xml_file.st_size
        record_size = xml_sample_filesize - xml_static_length
        xml_filesize = record_size * total_rows + xml_static_length
        numfiles = int(xml_filesize / maxfilesize) + 1
        numrows = int(total_rows / numfiles) + 1
        os.remove(xmlfile) if os.path.exists(xmlfile) else None
        print("{0}: Target XML data size is approximately {1} bytes.".format(
            time.strftime('%Y-%m-%d %H.%M.%S'), xml_filesize))
        print("{0}: Max filesize is set to {1} bytes.".format(time.strftime('%Y-%m-%d %H.%M.%S'), maxfilesize))
        print("{0}: CSV File will be written into {1} XML files.".format(time.strftime('%Y-%m-%d %H.%M.%S'), numfiles))

        if numfiles == 1:
            with open(csvfile, "r") as fh_csv_in:
                if headers:
                    next(fh_csv_in)
                xmlfile = "{0}.{1}.xml".format(time.strftime('%Y-%m-%d.%H.%M.%S'), self.name)
                self.l_xmlfiles.append(xmlfile + ".gz") if compression else self.l_xmlfiles.append(xmlfile)
                with open(xmlfile, "wb") as fh_xml_out:
                    # Build XML Tree
                    root = etree.Element("{0}".format(self.name))
                    print("{0}: Convert CSV file {1} to XML file {2}.".format(
                        time.strftime('%Y-%m-%d %H.%M.%S'), csvfile, xmlfile))
                    for line in fh_csv_in.readlines():
                        row = [x.strip() for x in line.split(self.delim)]
                        if row == ['']:
                            continue
                        record = etree.SubElement(root, record_tag)
                        ce_exists = False
                        for col, item in itertools.zip_longest(self.column_list, row):
                            if self.treesize > 1 and col in self.treecolumns:
                                # Hierarchy definition
                                pass
                            if item is None:
                                continue
                            elif str(item).strip() == "":
                                continue
                            elif str(item).strip() == "t":
                                item = "true"
                            elif str(item).strip() == "f":
                                item = "false"
                            if col in self.customer_extensions and ce_exists is True:
                                record1 = etree.SubElement(custom_tag, col)
                            elif col in self.customer_extensions and ce_exists is False:
                                custom_tag = etree.SubElement(record, "CustomerExtensions")
                                record1 = etree.SubElement(custom_tag, col)
                                ce_exists = True
                            elif col not in self.customer_extensions:
                                record1 = etree.SubElement(record, col)
                                ce_exists = False
                            record1.text = str(item)
                    etree.ElementTree(root).write(fh_xml_out, pretty_print=True, xml_declaration=True,
                                                  encoding="UTF-8")
                    print("{0}: XML file {1} written.".format(time.strftime('%Y-%m-%d %H.%M.%S'), xmlfile))
                # Compress file if flag is True
                if compression:
                    self.compress_file(xmlfile)
        elif numfiles > 1:
            filenumber = 1
            with open(csvfile, "r") as fh_csv_in:
                if headers:
                    next(fh_csv_in)
                while True:
                    lines = self.read_next_n_lines(fh_csv_in, numrows)
                    lines = list(filter(None, lines))
                    if len(lines) == 0:
                        break
                    # Set XML filename
                    xmlfile = "{0}.{1}_{2}.xml".format(time.strftime('%Y-%m-%d.%H.%M.%S'), self.name, filenumber)
                    self.l_xmlfiles.append(xmlfile + ".gz") if compression else self.l_xmlfiles.append(xmlfile)
                    with open(xmlfile, "wb") as fh_xml_out:
                        # Build XML Tree
                        root = etree.Element("{0}".format(self.name))
                        print("{0}: Convert CSV file {1} to XML file {2}.".format(
                            time.strftime('%Y-%m-%d %H.%M.%S'), csvfile, xmlfile))
                        for line in lines:
                            row = [x.strip() for x in line.split(self.delim)]
                            if row == ['']:
                                continue
                            record = etree.SubElement(root, record_tag)
                            ce_exists = False
                            for col, item in itertools.zip_longest(self.column_list, row):
                                if self.treesize > 1 and col in self.treecolumns:
                                    # Hierarchy definition
                                    pass
                                if item is None:
                                    continue
                                elif str(item).strip() == "":
                                    continue
                                elif str(item).strip() == "t":
                                    item = "true"
                                elif str(item).strip() == "f":
                                    item = "false"
                                if col in self.customer_extensions and ce_exists is True:
                                    record1 = etree.SubElement(custom_tag, col)
                                elif col in self.customer_extensions and ce_exists is False:
                                    custom_tag = etree.SubElement(record, "CustomerExtensions")
                                    record1 = etree.SubElement(custom_tag, col)
                                    ce_exists = True
                                elif col not in self.customer_extensions:
                                    record1 = etree.SubElement(record, col)
                                    ce_exists = False
                                record1.text = str(item)
                        etree.ElementTree(root).write(fh_xml_out, pretty_print=True, xml_declaration=True,
                                                      encoding="UTF-8")
                    print("{0}: XML file {1} written.".format(time.strftime('%Y-%m-%d %H.%M.%S'), xmlfile))
                    filenumber += 1
                    # Compress file if flag is True
                    if compression:
                        self.compress_file(xmlfile)
        os.remove(csvfile) if os.path.exists(csvfile) else None
        return True

    def validate_xml_with_xsd(self, xmlfile, xsdfile):
        parser = etree.XMLParser(ns_clean=True)
        xml_schema_doc = etree.parse(xsdfile, parser)
        xml_schema = etree.XMLSchema(xml_schema_doc)
        xml = etree.parse(xmlfile, parser)
        print("{0}: Validating XML file: {1}\nwith XSD declaration: {2}".format(
            time.strftime('%Y-%m-%d %H.%M.%S'), xmlfile, xsdfile))
        # xml_schema.assertValid(xml)
        # return True
        # log = xml_schema.error_log
        # error = log.last_error
        # print("domain: " + str(error.domain))
        # print("filename: " + error.filename)
        # print("level: " + str(error.level))
        # print("level_name: " + error.level_name)
        # print("line: " + str(error.line))
        # print("message: " + error.message)
        # print("type: " + str(error.type))
        # print("type_name: " + error.type_name)
        if not xml_schema.validate(xml):
            log = xml_schema.error_log
            error = log.last_error
            print("Category: {0}\nXML: {1}\nXSD: {2}\nError: {3}\nFirst record of xml: {4}.".format(
                self.name, xml, xsdfile, error, xmlfile.split('</Record>')[0][:1000]))
            return False
        else:
            return True
