# -*- coding: utf8 -*-
###############################################################################
#                             export_data_model.py                            #
###############################################################################
# Copyright (C) 2023  Jens Janzen

# Change history
# V1.0: 2019-04-23 - Initial Release

import argparse
import logging
import oralib
import textwrap

""" Sets logging console handler for debugging """
log = logging.getLogger("SESSION")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

# *******************
# ***** M A I N *****
# *******************
# Program runs standalone when called directly by operating system
if __name__ == "__main__":
    # Evaluate arguments
    desc = """ Konfigurierbare Parameter:
    -fmt: Exportformate sind parquet, feather oder csv
    """
    options = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                      description=textwrap.dedent(desc))
    options.add_argument("-fmt", "--format", dest="format", help="Format Option", metavar="Format", default="parquet")
    args = options.parse_args()
    export_type = args.format.lower()

    query = "select table_name from user_tables where table_name like 'OBJ%' UNION " \
            "select table_name from user_tables where table_name like 'TDM_MDM%' UNION " \
            "select view_name from user_views where view_name like 'OBJ%'"
    result = oralib.sql_query("TDM_HASI", query)
    l_tables = list(x[0] for x in result)

    for table in l_tables:
        log.info("Exporting {0} to file with format {1}".format(table, export_type))
        o_table = oralib.Table("TDM_HASI", table)
        df = o_table.table2dataframe()
        if export_type.upper() == "PARQUET":
            df.to_parquet("EXPORT_DATAMODEL/{0}.pqt".format(table))
        elif export_type.upper() == "FEATHER":
            df.to_feather("EXPORT_DATAMODEL/{0}.ftr".format(table))
        elif export_type.upper() == "CSV":
            df.to_csv("EXPORT_DATAMODEL/{0}.csv".format(table), index=False,
                      header=True, sep=";", encoding="utf8", quoting=2)

log.info("Export finished.")
