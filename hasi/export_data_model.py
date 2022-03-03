# -*- coding: utf8 -*-
###############################################################################
#                             export_data_model.py                            #
###############################################################################
# Autor: Jens Janzen

# Change history
# V1.0: 2019-04-23 - Initial Release

import argparse
import oralib
import sys
import textwrap

# *******************
# ***** M A I N *****
# *******************
# Program runs standalone when called directly by operating system
if __name__ == "__main__":
    # Evaluate arguments
    desc = """ Konfigurierbare Parameter:
    """
    options = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                      description=textwrap.dedent(desc))
    options.add_argument("-fmt", "--format", dest="format", help="Format Option", metavar="Format", default="del")
    args = options.parse_args()
    export_type = args.format.lower()

    query = "select table_name from user_tables where table_name like 'OBJ%';"
    result = oralib.sql_query("HASI", query)
    l_tables = list(x[0] for x in result)
    print(l_tables)

    for table in l_tables:
        o_table = oralib.Table("HASI", table)
        o_table.csv_export(".", table, quoting=True)

sys.exit(0)
# M A I N End
