# -*- coding: utf8 -*-
################################################################################
#                       ETL Mapping package for H.A.S.I.                       #
################################################################################
# Autor: Jens Janzen

# Change history
# v0.1: 2021-12-10 - Initial Release

import logging
import oralib as db
import tdm_rules

""" Sets logging console handler for debugging """
log = logging.getLogger("SESSION")


# Functions
def check_mapping(app_name, job_name, dsn):
    """ Check existence of mapping for job in OBJ_MAPPING """
    query = "select count(*) from obj_mapping where app_name='{0}' and job_name='{1}';".format(app_name, job_name)
    result = db.sql_query(dsn, query)[0][0]
    if result != 1:
        return False


def check_object(element, action):
    if action == "TABLE_EXISTS":
        if len(element.column_list) == 0:
            log.error("Table {0} does not exist.".format(element.table_name))
            return False
        else:
            return True
    else:
        log.error("Action {0} is not defined.".format(action))
        return False


def src_db_loader(db_type):
    if db_type == "DB2":
        import db2lib as src_db
    elif db_type == "HANA":
        import hanadblib as src_db
    elif db_type == "POSTGRES":
        import pglib as src_db
    elif db_type == "ORACLE":
        import oralib as src_db
    return src_db


def fil_db_loader(db_type):
    if db_type == "DB2":
        import db2lib as fil_db
    elif db_type == "HANA":
        import hanadblib as fil_db
    elif db_type == "POSTGRES":
        import pglib as fil_db
    elif db_type == "ORACLE":
        import oralib as fil_db
    return fil_db


def tgt_db_loader(db_type):
    if db_type == "DB2":
        import db2lib as tgt_db
    elif db_type == "HANA":
        import hanadblib as tgt_db
    elif db_type == "POSTGRES":
        import pglib as tgt_db
    elif db_type == "ORACLE":
        import oralib as tgt_db
    return tgt_db


def tdm_db_loader(db_type):
    if db_type == "DB2":
        import db2lib as tdm_db
    elif db_type == "HANA":
        import hanadblib as tdm_db
    elif db_type == "POSTGRES":
        import pglib as tdm_db
    elif db_type == "ORACLE":
        import oralib as tdm_db
    return tdm_db


def convert_dtypes(table_obj, df):
    d_conv = {}
    for col in table_obj.column_list:
        if table_obj.d_col_data_type[col][0] == "NUMBER" and table_obj.d_col_data_type[col][2] == 0:
            d_conv[col] = "int64"
        elif table_obj.d_col_data_type[col][0] == "NUMBER" and table_obj.d_col_data_type[col][2] > 0:
            d_conv[col] = "float64"
        elif table_obj.d_col_data_type[col][0] == "DATE" or table_obj.d_col_data_type[col][0][:9] == "TIMESTAMP":
            d_conv[col] = "datetime64"
        else:
            d_conv[col] = "object"
    return df.convert_dtypes(d_conv)


def inconverter(value):
    return int(value)


def inputtypehandler(cursor, value, numrows):
    import numpy as np
    if isinstance(value, np.int64):
        return cursor.var(int, arraysize=numrows, inconverter=inconverter)


# Classes
class Mapping(object):
    """ Klasse für ETL Mapping für H.A.S.I. """

    def __init__(self, app_name, job_name, dsn):
        """ Init map object """
        self.app_name = app_name
        self.job_name = job_name
        self.dsn = dsn
        self.d_status = {"EVENT_CODE": 0, "ROWS_WRITTEN": 0}
        """ Read mapping table from backend """
        query = "select" \
                "  a.src_type, a.src_dsn, a.src_schema, a.src_obj, a.src_business_key" \
                ", a.fil_type, a.fil_dsn, a.fil_schema, a.fil_obj, a.fil_business_key" \
                ", a.tgt_type, a.tgt_dsn, a.tgt_schema, a.tgt_obj, a.tgt_business_key" \
                ", a.ruleset_id, a.custom_query" \
                ", b.rule_name, b.rule_strategy, b.flg_mask_data, b.src_ruleset, b.tgt_ruleset, b.agg_cols " \
                "from obj_mapping a join obj_ruleset b " \
                "on a.ruleset_id = b.ruleset_id " \
                "where a.app_name='{0}' and a.job_name='{1}';".format(self.app_name, self.job_name)
        result = list(x for x in db.sql_query(self.dsn, query)[0])

        """ Initialize mapping attributes """
        self.src_type = result[0]
        self.src_dsn = result[1]
        self.src_schema = result[2]
        self.src_obj = result[3]
        self.src_business_key = result[4]
        self.fil_type = result[5]
        self.fil_dsn = result[6]
        self.fil_schema = result[7]
        self.fil_obj = result[8]
        self.fil_business_key = result[9]
        self.tgt_type = result[10]
        self.tgt_dsn = result[11]
        self.tgt_schema = result[12]
        self.tgt_obj = result[13]
        self.tgt_business_key = result[14]
        self.ruleset_id = result[15]
        self.custom_query = result[16]
        self.rule_name = result[17]
        self.rule_strategy = result[18]
        self.flg_mask_data = result[19]
        self.src_ruleset = result[20]
        self.tgt_ruleset = result[21]
        self.agg_cols = result[22]
        """ Initialize masking attributes """
        self.d_rules = {}
        self.replace_table_name = None

        """ Build mask rules if flg_mask_data is set to 1 """
        if self.flg_mask_data == 1:
            query = "with job as (" \
                    "select a.app_name, a.project, a.subproject, b.job_name, c.tgt_obj " \
                    "from obj_application a " \
                    "join obj_job b on a.app_name=b.app_name " \
                    "join obj_mapping c on a.app_name = c.app_name and b.job_name = c.job_name " \
                    "where a.app_name='{0}' and b.job_name='{1}') " \
                    "select a.anwendung, a.tabelle, a.attribut, a.rule_name, a.flg_rule_over" \
                    ", a.lkp_dsn, a.lkp_schema, a.lkp_obj, a.lkp_cols, a.lkp_id, a.translate_expression" \
                    ", a.default_value_1, a.default_value_2, a.default_value_3, a.format_string " \
                    "from obj_mask_data a join job b on a.anwendung=b.subproject and a.tabelle=b.tgt_obj " \
                    "where a.rule_name != 'R??';".format(self.app_name, self.job_name)
            rules = db.sql_query(dsn, query)
            for row in rules:
                self.d_rules[row[2]] = {"ANWENDUNG": row[0],
                                        "TABELLE": row[1],
                                        "ATTRIBUT": row[2],
                                        "RULE_NAME": row[3],
                                        "FLG_RULE_OVER": row[4],
                                        "LKP_DSN": row[5],
                                        "LKP_SCHEMA": row[6],
                                        "LKP_OBJ": row[7],
                                        "LKP_COLS": row[8],
                                        "LKP_ID": row[9],
                                        "TRANSLATE_EXPRESSION": row[10],
                                        "DEFAULT_VALUE_1": row[11],
                                        "DEFAULT_VALUE_2": row[12],
                                        "DEFAULT_VALUE_3": row[13],
                                        "FORMAT_STRING": row[14]}

            """ Reduce ruleset for multi attribute rules """
            s_rules = set(x["RULE_NAME"] for x in self.d_rules.values()
                          if x["FLG_RULE_OVER"] == 1 and x["LKP_COLS"] is not None)
            for rule in s_rules:
                l_keys = []
                l_attr = []
                l_lkp_cols = []
                for key, value in self.d_rules.items():
                    if value["RULE_NAME"] == rule:
                        l_keys.append(key)
                        l_attr.append(value["ATTRIBUT"])
                        l_lkp_cols.append(value["LKP_COLS"])
                self.d_rules[l_keys[0]]["ATTRIBUT"] = l_attr
                self.d_rules[l_keys[0]]["LKP_COLS"] = l_lkp_cols
                """ Lösche alle bis auf den ersten """
                for key in l_keys[1:]:
                    del self.d_rules[key]

        log.info("Mapping Übersicht")
        log.info("Object type Source: {0}".format(self.src_type))
        log.info("DB connection Source: {0}".format(self.src_dsn))
        log.info("Source schema: {0}".format(self.src_schema))
        log.info("Source object: {0}".format(self.src_obj))
        log.info("Business Key Source: {0}".format(self.src_business_key))
        log.info("Object type Filter: {0}".format(self.fil_type))
        log.info("DB connection Filter: {0}".format(self.fil_dsn))
        log.info("Filter schema: {0}".format(self.fil_schema))
        log.info("Filter object Name: {0}".format(self.fil_obj))
        log.info("Business Key Filter: {0}".format(self.fil_business_key))
        log.info("Object type Target: {0}".format(self.tgt_type))
        log.info("DB connection Target: {0}".format(self.tgt_dsn))
        log.info("Target schema: {0}".format(self.tgt_schema))
        log.info("Target object name: {0}".format(self.tgt_obj))
        log.info("Business Key Target: {0}".format(self.tgt_business_key))
        log.info("Rule ID: {0}".format(self.ruleset_id))
        log.info("Rule Name: {0}".format(self.rule_name))
        log.info("Rule Strategy: {0}".format(self.rule_strategy))
        log.info("Flag Maskierung: {0}".format(self.flg_mask_data))
        log.info("Source ruleset: {0}".format(self.src_ruleset))
        log.info("Target ruleset: {0}".format(self.tgt_ruleset))
        log.info("Custom query: {0}".format(self.custom_query))
        log.info("Aggregation columns: {0}".format(self.agg_cols))
        log.debug("Mask rules: {0}".format(self.d_rules))

        # Declaration of attributes
        self.o_src_table = None
        self.o_tgt_table = None
        self.o_fil_table = None
        self.l_src_cols = None
        self.l_src_pk = None
        self.l_fil_cols = None
        self.l_fil_pk = None
        self.l_tgt_cols = None
        self.l_tgt_pk = None
        self.l_tgt_attributes = None
        self.src_query = None
        self.tgt_query = None
        self.src_filter = ''
        self.successful_rows = 0

    def create_objects(self):
        """ Import database connectors and build table objects """
        src_db = src_db_loader(self.src_type)
        if self.fil_type:
            fil_db = fil_db_loader(self.fil_type)
        tgt_db = tgt_db_loader(self.tgt_type)

        if self.rule_strategy == "IGNORIEREN":
            log.info("Tabelle {0}.{1} should be ignored. Mapping quits.".format(
                self.src_schema, self.src_obj))
            self.d_status["EVENT_CODE"] = -1
            return self.d_status

        if self.src_type in ("DB2", "HANA", "POSTGRES", "ORACLE"):
            self.o_src_table = src_db.Table(self.src_dsn, self.src_obj, schema=self.src_schema)
            result = check_object(self.o_src_table, "TABLE_EXISTS")
            if result is False:
                self.d_status["EVENT_CODE"] = 2
                return self.d_status
            else:
                self.l_src_cols = self.o_src_table.column_list
                self.l_src_pk = self.o_src_table.primary_key_list
                if not self.l_src_pk:
                    log.warning("No primary key found on {0}. Try to use business key {1}.".format(
                        self.src_obj, self.src_business_key))
                    if not self.src_business_key:
                        log.warning("Error: No source business key found on {0} in mapping configuration.".format(
                            self.src_obj))
                        self.d_status["EVENT_CODE"] = 5
                        return self.d_status
                    self.l_src_pk = self.src_business_key.split(",")
                log.info("PK Source: {0}".format(", ".join(self.l_src_pk)))

        if self.fil_type in ("DB2", "HANA", "POSTGRES", "ORACLE"):
            if self.fil_obj:
                self.o_fil_table = fil_db.Table(self.fil_dsn, self.fil_obj, schema=self.fil_schema)
                """ Filtertabellen werden nur beim Reduzieren verwendet und sind nicht obligatorisch. """
                result = check_object(self.o_fil_table, "TABLE_EXISTS")
                if result is False:
                    self.o_fil_table = None
                else:
                    self.l_fil_cols = self.o_fil_table.column_list
                    self.l_fil_pk = self.o_fil_table.primary_key_list
                    if not self.l_fil_pk:
                        log.warning("No primary key found in {0}. Business Key {1} is used.".format(
                            self.fil_obj, self.fil_business_key))
                        self.l_fil_pk = self.fil_business_key.split(",")
                log.info("PK Filter: {0}".format(", ".join(self.l_fil_pk)))

        if self.tgt_type in ("DB2", "HANA", "POSTGRES", "ORACLE"):
            self.o_tgt_table = tgt_db.Table(self.tgt_dsn, self.tgt_obj, schema=self.tgt_schema)
            result = check_object(self.o_tgt_table, "TABLE_EXISTS")
            if result is False:
                self.d_status["EVENT_CODE"] = 3
                return self.d_status
            else:
                self.l_tgt_cols = self.o_tgt_table.column_list
                self.l_tgt_pk = self.o_tgt_table.primary_key_list
                if not self.l_tgt_pk:
                    log.warning("No primary key found on {0}. Try to use business key {1}.".format(
                        self.tgt_obj, self.tgt_business_key))
                    if not self.tgt_business_key:
                        log.warning("Error: No target business key found on {0} in mapping configuration.".format(
                            self.tgt_obj))
                        self.d_status["EVENT_CODE"] = 6
                        return self.d_status
                    self.l_tgt_pk = self.tgt_business_key.split(",")
                log.info("PK Target: {0}".format(", ".join(self.l_tgt_pk)))
                """ Nicht-Schlüsselattribute in der Targettabelle """
                self.l_tgt_attributes = list(x for x in self.l_tgt_cols if x not in self.l_tgt_pk)
        self.d_status["EVENT_CODE"] = 0
        return self.d_status

    def create_source_actions(self):
        if self.src_ruleset:
            for action in self.src_ruleset.split(","):
                log.info("Bearbeite Aktion {0}.".format(action))
                if action == "DEDUPLICATE":
                    result = self.deduplicate_rows()
                    if result is False:
                        self.d_status["EVENT_CODE"] = 1
                        return self.d_status
                elif action == "FILTER_INVALID":
                    self.filter_invalid_rows()
                elif action == "ERROR":
                    self.select_error_rows()
                elif action == "FILTER_JOIN":
                    self.create_join_filter()
        else:
            log.info("No source action defined. All rows will be copied from source {0}.".format(self.src_obj))
        log.info("Source Filter: {0}".format(self.src_filter))
        self.d_status["EVENT_CODE"] = 0
        return self.d_status

    def create_target_actions(self):
        if self.tgt_ruleset:
            for action in self.tgt_ruleset.split(","):
                log.info("Bearbeite Aktion {0}.".format(action))
                if action == "TRUNCATE":
                    result = self.truncate_object()
                    if result is False:
                        self.d_status["EVENT_CODE"] = 1
                        return self.d_status
                elif action == "INSERT":
                    # Selektion aller Felder der Source, welche namensgleich ebenfalls im Target sind
                    l_ins_cols = list("{0}.{1}.{2}".format(
                        self.src_schema, self.src_obj, x) for x in self.l_src_cols if x in self.l_tgt_cols)
                    if self.custom_query:
                        self.src_query = self.custom_query
                    else:
                        self.src_query = "SELECT {0} FROM {1}.{2} {3}".format(
                            ", ".join(l_ins_cols), self.src_schema, self.src_obj, self.src_filter)
                    log.info("Quellcursor: {0}".format(self.src_query))
                    self.tgt_query = "INSERT INTO {0}.{1} ({2}) VALUES ({3})".format(
                        self.tgt_schema, self.tgt_obj, ", ".join(self.l_tgt_cols),
                        ", ".join(list(":{0}".format(x) for x in range(1, len(self.l_tgt_cols) + 1))))
                    log.info("Targetcursor: {0}".format(self.tgt_query))
                elif action == "MERGE":
                    exp_join = " and ".join(["src." + x + "=tgt." + x for x in self.l_tgt_pk])
                    exp_merge_update = ", ".join(["tgt." + x + "=src." + x for x in self.l_tgt_attributes])
                    exp_merge_tgt_rows = ", ".join(["tgt." + x for x in self.l_tgt_cols])
                    if len(self.l_tgt_cols) - len(self.l_src_cols) == 1 and self.l_tgt_cols[-1] == "TA_FEHLER":
                        exp_merge_src_rows = ", ".join(["src." + x for x in self.l_tgt_cols])
                        self.tgt_query = "merge into {0}.{1} tgt using " \
                                         "(select {2}, 'Primärschlüssel unvollständig' as TA_FEHLER " \
                                         "from {3}.{4} {5}) src on ({6}) " \
                                         "when matched then update set {7} " \
                                         "when not matched then insert ({8}) values ({9});".format(
                                            self.tgt_schema, self.tgt_obj, ", ".join(self.l_src_cols),
                                            self.src_schema, self.src_obj, self.src_filter, exp_join,
                                            exp_merge_update, exp_merge_tgt_rows, exp_merge_src_rows)
                        self.l_src_cols.append("'Primärschlüssel unvollständig.' as TA_FEHLER")
                    else:
                        exp_merge_src_rows = ", ".join(["src." + x for x in self.l_src_cols])
                        self.tgt_query = "merge into {0}.{1} tgt using " \
                                         "(select {2} from {3}.{4} {5}) src on ({6}) " \
                                         "when matched then update set {7} " \
                                         "when not matched then insert ({8}) values ({9});".format(
                                            self.tgt_schema, self.tgt_obj, ", ".join(self.l_src_cols),
                                            self.src_schema, self.src_obj, self.src_filter, exp_join,
                                            exp_merge_update, exp_merge_tgt_rows, exp_merge_src_rows)
                    log.info("Query: {0}\n".format(self.tgt_query))
                elif action == "MASK":
                    l_ins_cols = list("{0}.{1}.{2}".format(
                        self.src_schema, self.src_obj, x) for x in self.l_src_cols if x in self.l_tgt_cols)
                    if self.custom_query:
                        self.src_query = self.custom_query
                    else:
                        self.src_query = "SELECT {0} FROM {1}.{2} {3}".format(
                            ", ".join(l_ins_cols), self.src_schema, self.src_obj, self.src_filter)
                    log.info("Quellcursor: {0}".format(self.src_query))
                    columns = []
                    for index, col in enumerate(self.l_tgt_cols):
                        if "TIMESTAMP" in self.o_tgt_table.d_col_data_type[col][0]:
                            columns.append("to_timestamp(:{0}, 'YYYY-MM-DD HH24:MI:SS.FF')".format(index + 1))
                        elif self.o_tgt_table.d_col_data_type[col][0] == "DATE":
                            columns.append("to_date(:{0}, 'YYYY-MM-DD HH24:MI:SS')".format(index + 1))
                        else:
                            columns.append(":{0}".format(index + 1))
                    self.tgt_query = "INSERT INTO {0}.{1} ({2}) VALUES ({3})".format(
                        self.tgt_schema, self.tgt_obj, ", ".join(self.l_tgt_cols), ", ".join(columns))
                    log.info("Targetcursor: {0}".format(self.tgt_query))
                else:
                    log.error("Target action {0} undefined.".format(action))
                    self.d_status["EVENT_CODE"] = 7
                    return self.d_status
        self.d_status["EVENT_CODE"] = 0
        return self.d_status

    def run_mapping(self, num_rows=100000):
        """
            Kopieren der Daten mit und ohne Pseudonymisierung
        """
        src_db = src_db_loader(self.src_type)
        tgt_db = tgt_db_loader(self.tgt_type)
        if self.tgt_ruleset:
            for action in self.tgt_ruleset.split(","):
                if action == "INSERT":
                    log.info("Kopiere Datensätze von {0}.{1} nach {2}.{3}.".format(
                        self.src_schema, self.src_obj, self.tgt_schema, self.tgt_obj))
                    try:
                        with tgt_db.get_connection(self.tgt_dsn) as tgt:
                            with tgt.cursor() as c2:
                                with src_db.get_connection(self.src_dsn) as src:
                                    with src.cursor() as c1:
                                        self.successful_rows = 0
                                        c1.execute(self.src_query)
                                        db_types = tuple(d[1] for d in c1.description)
                                        c2.setinputsizes(*db_types)
                                        while True:
                                            data = c1.fetchmany(num_rows)
                                            if len(data) == 0:
                                                break
                                            c2.executemany(self.tgt_query, data)
                                            self.successful_rows += c2.rowcount
                                        tgt.commit()
                    except Exception as err:
                        err, = err.args
                        log.error("Error-Code: {0}".format(err))
                        self.d_status["EVENT_CODE"] = 1
                        return self.d_status
                    log.info("Copied {0} rows to table {1}.{2}.".format(
                        self.successful_rows, self.tgt_schema, self.tgt_obj))
                elif action == "MASK":
                    log.info("Lese Datensätze aus {0}.{1} und schreibe anonymisierte Felder nach {2}.{3}.".format(
                        self.src_schema, self.src_obj, self.tgt_schema, self.tgt_obj))
                    try:
                        with src_db.get_connection(self.src_dsn) as src:
                            with tgt_db.get_connection(self.tgt_dsn) as tgt:
                                with src.cursor() as c1:
                                    self.successful_rows = 0
                                    c1.execute(self.src_query)
                                    # db_types = tuple(d[1] for d in c1.description)
                                    while True:
                                        data = c1.fetchmany(num_rows)
                                        if len(data) == 0:
                                            break
                                        log.info("Maskiere Daten mit Blocksize:{0}, Index: {1}".format(
                                            num_rows, self.successful_rows))
                                        data = self.mask_data(data)
                                        if data is False:
                                            self.d_status["EVENT_CODE"] = 1
                                            return self.d_status
                                        with tgt.cursor() as c2:
                                            for x, row in enumerate(data):
                                                try:
                                                    # c2.setinputsizes(*db_types)
                                                    # c2.inputtypehandler = inputtypehandler
                                                    c2.execute(self.tgt_query, row)
                                                except Exception as e:
                                                    print("Row: {0}, Error: {1}".format(x, e))
                                                    raise
                                                self.successful_rows += c2.rowcount
                                    tgt.commit()
                    except Exception as err:
                        log.error("Error-Code: {0}".format(err))
                        self.d_status["EVENT_CODE"] = 1
                        return self.d_status
                    log.info("Copied {0} rows to table {1}.{2}.".format(
                        self.successful_rows, self.tgt_schema, self.tgt_obj))
                elif action == "MERGE":
                    result = tgt_db.sql_query(self.tgt_dsn, self.tgt_query)
                    log.debug("Result: {0}".format(result))
                    if result is False:
                        self.d_status["EVENT_CODE"] = 1
                        return self.d_status
        else:
            log.info("Keine Regel gefunden für Targettabelle {0}.{1}. Tabelle wird ignoriert.".format(
                self.tgt_schema, self.tgt_obj))
        self.d_status["EVENT_CODE"] = 0
        self.d_status["ROWS_WRITTEN"] = self.successful_rows
        return self.d_status

    def mask_data(self, dataset):
        import pandas as pd
        tdm_db = tdm_db_loader(self.tgt_type)
        log.debug("Felder: {0}".format(self.l_tgt_cols))
        log.debug("Datensätze: {0}".format(dataset))
        table_rows = pd.DataFrame(dataset, columns=self.l_tgt_cols)
        orig_structure = table_rows.shape
        log.info("Original Datenblock enthält {0} Sätze in {1} Spalten.".format(
            orig_structure[0], orig_structure[1]))
        for d_rule in self.d_rules.values():
            function_call = eval("tdm_rules.rule_{0}".format(d_rule["RULE_NAME"].lower()))
            log.info("Verarbeite Regel {0} für Attribut {1}".format(d_rule["RULE_NAME"], d_rule["ATTRIBUT"]))
            if d_rule["LKP_OBJ"]:
                o_lkp_table = tdm_db.Table(d_rule["LKP_DSN"], d_rule["LKP_OBJ"], schema=d_rule["LKP_SCHEMA"])
                if d_rule["TRANSLATE_EXPRESSION"] is None:
                    stmt = "select {0} from {1}.{2};".format(
                        ", ".join(o_lkp_table.column_list), o_lkp_table.schema, o_lkp_table.table_name)
                    lkp_columns = o_lkp_table.column_list
                else:
                    stmt = "{0}".format(d_rule["TRANSLATE_EXPRESSION"])
                    lkp_columns = d_rule["LKP_COLS"].split(",")
                log.debug("Lookup Statement: {0}".format(stmt))
                raw_data = tdm_db.sql_query(d_rule["LKP_DSN"], stmt)
                lkp_cache = pd.DataFrame(raw_data, columns=lkp_columns)
                table_rows = function_call(d_rule, table_rows, lkp_cache)
            else:
                table_rows = function_call(d_rule, table_rows)
        mask_structure = table_rows.shape
        log.info("Zu schreibender Datenblock enthält {0} Sätze in {1} Spalten.".format(
            mask_structure[0], mask_structure[1]))

        """ Fix for timestamps to prevent loss of microseconds """
        for col in table_rows:
            """ Date and Timestamp conversion to string. """
            if "TIMESTAMP" in self.o_tgt_table.d_col_data_type[col][0] and table_rows[col].dtypes == "datetime64[ns]":
                table_rows[col] = table_rows[col].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                table_rows[col].fillna(value='', inplace=True)
            elif self.o_tgt_table.d_col_data_type[col][0] == "DATE" and table_rows[col].dtypes == "datetime64[ns]":
                table_rows[col] = table_rows[col].dt.strftime("%Y-%m-%d %H:%M:%S")
                table_rows[col].fillna(value='', inplace=True)

        """ Workaround for null values that database driver cannot understand. """
        table_rows.fillna('', axis=1, inplace=True)
        if orig_structure != mask_structure:
            log.error("Datenstrukturen zwischen Originalblock und maskiertem Block unterscheiden sich.")
            return False
        data = list(table_rows.itertuples(index=False, name=None))
        return data

    def deduplicate_rows(self):
        log.info("Bereinige Dubletten auf Tabelle {0}.{1}".format(
            self.src_schema, self.src_obj))
        self.l_src_pk = self.l_tgt_pk if not self.l_src_pk else self.l_src_pk
        return self.o_src_table.deduplicate_rows("MIN", self.l_src_pk)

    def filter_invalid_rows(self):
        log.info("Filtere Sätze aus {0}.{1} ohne vollständigen PK.".format(
            self.src_schema, self.src_obj))
        self.src_filter = "WHERE " + " AND ".join(list(x + " IS NOT NULL" for x in self.l_tgt_pk))

    def select_error_rows(self):
        log.info("Selektiere Fehlersätze in {0}.{1} ohne vollständigen PK.".format(
            self.src_schema, self.src_obj))
        self.src_filter = "WHERE " + " OR ".join(list(x + " IS NULL" for x in self.l_tgt_pk))

    def create_join_filter(self):
        log.info("Erstelle Joiner auf Filtertabelle.")
        l_join_cols = []
        for x, y in zip(self.src_business_key.split(","), self.fil_business_key.split(",")):
            l_join_cols.append(
                "{0}.{1}.{2} = {3}.{4}.{5}".format(self.src_schema, self.src_obj, x, self.fil_schema,
                                                   self.fil_obj, y))
        self.src_filter = "JOIN {0}.{1} ON ".format(self.fil_schema, self.fil_obj) + " AND ".join(
            l_join_cols)

    def truncate_object(self):
        log.info("Leere Targettabelle {0}.{1}.{2}.".format(self.tgt_dsn, self.tgt_schema, self.tgt_obj))
        return self.o_tgt_table.truncate_table()
