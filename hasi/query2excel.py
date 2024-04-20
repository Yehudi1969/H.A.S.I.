# -*- coding: utf8 -*-
###############################################################################
#                              query2excel.py                                 #
###############################################################################
# Copyright (C) 2023  Jens Janzen
# Datum: 2021-07-01

import oralib
import os

conn = "P_RAW1"
HOME = os.environ['HOME'] = os.path.expanduser('~')
path = "{0}/data".format(HOME)
title = "Sätze mit SL_PD_NUTZUNG=0"
schema = "tdwhraw1"
table = "PAAN_PA01_233_DWH_VIEW_HS"
query = "select * from tdwhraw1.PAAN_PA01_233_DWH_VIEW_HS where SL_PD_NUTZUNG=0 order by pob, hs_lebt;"
o_table = oralib.Table(conn, table, schema)
o_table.excel_export(path, "{0}.xlsx".format(title), custom_query=query)
