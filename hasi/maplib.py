# -*- coding: utf8 -*-
################################################################################
#                       ETL Mapping package for H.A.S.I.                       #
################################################################################
# Copyright (C) 2023  Jens Janzen

# Change history
# v0.1: 2021-12-10 - Initial Release

import logging
import oralib as meta_db

""" Sets logging console handler for debugging """
log = logging.getLogger("SESSION")


# Functions
def check_mapping(app_name, job_name, dsn):
    """ Check existence of mapping for job in OBJ_MAPPING """
    query = "select count(*) from obj_mapping where app_name='{0}' and job_name='{1}';".format(app_name, job_name)
    result = meta_db.sql_query(dsn, query)[0][0]
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
    src_db = None
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
    fil_db = None
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
    tgt_db = None
    if db_type == "DB2":
        import db2lib as tgt_db
    elif db_type == "HANA":
        import hanadblib as tgt_db
    elif db_type == "POSTGRES":
        import pglib as tgt_db
    elif db_type == "ORACLE":
        import oralib as tgt_db
    return tgt_db


def add_length_to_rule(d_col_data_type, d_rule):
    if isinstance(d_rule["ATTRIBUT"], list):
        d_rule["COLUMN_LENGTH"] = list(d_col_data_type[col][3] for col in d_rule["ATTRIBUT"])
    else:
        d_rule["COLUMN_LENGTH"] = d_col_data_type[d_rule["ATTRIBUT"]][3]
    return d_rule


def get_cache_dataframe(dsn, stmt):
    """
        Gibt ein DataFrame aus einem Select zurück.
        Die Spaltenköpfe entsprechen den selektierten Feldnamen.
        Wird keine Query übergeben, wird die Tabelle vollständig gelesen.
        Wird eine Query übergeben, werden die Angaben für Schema und Tabelle nicht verwendet.
    """
    import pandas as pd
    connection = meta_db.get_connection(dsn)
    cursor = connection.cursor()
    obj_exec = cursor.execute(stmt)
    header = list(x[0] for x in cursor.description)
    raw_data = obj_exec.fetchall()
    df = pd.DataFrame(raw_data, columns=header)
    log.debug("Header: {0}".format(header))
    return df


# Classes
class Mapping(object):
    """ Klasse für ETL Mapping für H.A.S.I. """

    def __init__(self, app_name, job_name, dsn):
        """ Set DSN for meta data """
        self.meta_dsn = "TDM_HASI"

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
        result = list(x for x in meta_db.sql_query(self.dsn, query)[0])

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
                    "where a.rule_name not like '%TODO';".format(self.app_name, self.job_name)
            rules = meta_db.sql_query(dsn, query)
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
        log.info("Objekttyp Quelle: {0}".format(self.src_type))
        log.info("Datenbankverbindung Quelle: {0}".format(self.src_dsn))
        log.info("Quellschema: {0}".format(self.src_schema))
        log.info("Quellobjekt: {0}".format(self.src_obj))
        log.info("Business Key Quelle: {0}".format(self.src_business_key))
        log.info("Objekttyp Filter: {0}".format(self.fil_type))
        log.info("Datenbankverbindung Filter: {0}".format(self.fil_dsn))
        log.info("Filterschema: {0}".format(self.fil_schema))
        log.info("Filterobjekt Name: {0}".format(self.fil_obj))
        log.info("Business Key Filter: {0}".format(self.fil_business_key))
        log.info("Objekttyp Ziel: {0}".format(self.tgt_type))
        log.info("Datenbankverbindung Ziel: {0}".format(self.tgt_dsn))
        log.info("Zielschema: {0}".format(self.tgt_schema))
        log.info("Zielobjekt Name: {0}".format(self.tgt_obj))
        log.info("Business Key Ziel: {0}".format(self.tgt_business_key))
        log.info("Regel ID: {0}".format(self.ruleset_id))
        log.info("Regel Name: {0}".format(self.rule_name))
        log.info("Regel Strategie: {0}".format(self.rule_strategy))
        log.info("Flag Maskierung: {0}".format(self.flg_mask_data))
        log.info("Quellregeln: {0}".format(self.src_ruleset))
        log.info("Zielregeln: {0}".format(self.tgt_ruleset))
        log.info("Custom query: {0}".format(self.custom_query))
        log.info("Aggregationsfelder: {0}".format(self.agg_cols))
        log.info("Maskierungsattribute: {0}".format(list(self.d_rules.keys())))
        log.debug("Maskierungsregeln: {0}".format(self.d_rules))

        """ Declaration of attributes """
        self.meta_db = None
        self.src_db = None
        self.fil_db = None
        self.tgt_db = None
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
        self.src_filter = None
        self.successful_rows = 0
        self.l_intersect = []
        self.xml_cols = []
        self.max_mem_proc = 104857600  # 100 MB pro Prozess
        self.d_cache = None
        log.debug("Verwendete Logging Instanzen: {0}".format(logging.Logger.manager.loggerDict))

    def create_objects(self):
        """ Import database connectors and build table objects """
        self.src_db = src_db_loader(self.src_type)
        self.fil_db = None
        if self.fil_type:
            self.fil_db = fil_db_loader(self.fil_type)
        self.tgt_db = tgt_db_loader(self.tgt_type)

        if self.rule_strategy == "IGNORIEREN":
            log.info("Tabelle {0}.{1} soll ignoriert werden. Mapping wird beendet.".format(
                self.src_schema, self.src_obj))
            self.d_status["EVENT_CODE"] = -1
            return self.d_status

        if self.src_type in ("DB2", "HANA", "POSTGRES", "ORACLE"):
            self.o_src_table = self.src_db.Table(self.src_dsn, self.src_obj, schema=self.src_schema)
            result = check_object(self.o_src_table, "TABLE_EXISTS")
            if result is False:
                self.d_status["EVENT_CODE"] = 2
                return self.d_status
            else:
                self.l_src_cols = self.o_src_table.column_list
                if self.src_business_key is not None:
                    self.l_src_pk = self.src_business_key.split(",")
                elif self.src_business_key is None and self.o_src_table.primary_key_list is not None:
                    self.l_src_pk = self.o_src_table.primary_key_list
                    log.info("PK Quelle: {0}".format(", ".join(self.l_src_pk)))
                else:
                    log.warning("Warning: No source business key found on {0} in mapping configuration.".format(
                        self.src_obj))
            """ Check for ruleset attribute exists in source table """
            for col in list(self.d_rules.keys()):
                if col not in self.l_src_cols:
                    log.warning("Zu maskierendes Attribut {0} ist nicht in {1} enthalten "
                                "und wird aus Regelsatz entfernt!".format(col, self.src_obj))
                    del self.d_rules[col]

        if self.fil_type in ("DB2", "HANA", "POSTGRES", "ORACLE"):
            if self.fil_obj:
                self.o_fil_table = self.fil_db.Table(self.fil_dsn, self.fil_obj, schema=self.fil_schema)
                """ Filtertabellen werden nur beim Reduzieren verwendet und sind nicht obligatorisch. """
                result = check_object(self.o_fil_table, "TABLE_EXISTS")
                if result is False:
                    self.o_fil_table = None
                else:
                    self.l_fil_cols = self.o_fil_table.column_list
                    if self.fil_business_key is not None:
                        self.l_fil_pk = self.fil_business_key.split(",")
                    elif self.fil_business_key is None and self.o_fil_table.primary_key_list is not None:
                        self.l_fil_pk = self.o_fil_table.primary_key_list
                        log.info("PK Filter: {0}".format(", ".join(self.l_fil_pk)))
                    else:
                        log.warning("Warning: No filter business key found on {0} in mapping configuration.".format(
                            self.fil_obj))

        if self.tgt_type in ("DB2", "HANA", "POSTGRES", "ORACLE"):
            self.o_tgt_table = self.tgt_db.Table(self.tgt_dsn, self.tgt_obj, schema=self.tgt_schema)
            result = check_object(self.o_tgt_table, "TABLE_EXISTS")
            if result is False:
                self.d_status["EVENT_CODE"] = 3
                return self.d_status
            else:
                self.l_tgt_cols = self.o_tgt_table.column_list
                if self.tgt_business_key is not None:
                    self.l_tgt_pk = self.tgt_business_key.split(",")
                elif self.tgt_business_key is None and self.o_tgt_table.primary_key_list is not None:
                    self.l_tgt_pk = self.o_tgt_table.primary_key_list
                    log.info("PK Ziel: {0}".format(", ".join(self.l_tgt_pk)))
                else:
                    log.warning("Warning: No target business key found on {0} in mapping configuration.".format(
                        self.tgt_obj))

                """ Nicht-Schlüsselattribute in der Zieltabelle """
                self.l_tgt_attributes = list(x for x in self.l_tgt_cols if x not in self.l_tgt_pk)

        self.d_status["EVENT_CODE"] = 0
        return self.d_status

    def create_source_actions(self):
        """ Collect source columns """
        self.l_intersect = list(x for x in self.l_src_cols if x in self.l_tgt_cols)
        """ Actions on source object """
        if self.src_ruleset:
            for action in self.src_ruleset.split(","):
                log.info("Bearbeite Aktion {0}.".format(action))
                if action == "SELECT":
                    if self.custom_query:
                        self.src_query = self.custom_query
                    else:
                        self.src_query = self.src_db.create_selector(self.l_intersect, self.o_src_table)
                elif action == "UNION":
                    if self.custom_query:
                        self.src_query = self.custom_query
                    else:
                        self.src_query = self.create_union_stmt()
                        if self.src_query is False:
                            return self.d_status
                elif action == "DEDUPLICATE":
                    result = self.deduplicate_rows()
                    if result is False:
                        self.d_status["EVENT_CODE"] = 1
                        return self.d_status
                elif action == "FILTER_INVALID":
                    self.src_filter = self.filter_invalid_rows()
                    self.src_query = self.src_query + " " + self.src_filter
                elif action == "ERROR":
                    self.src_filter = self.select_error_rows()
                    self.src_query = self.src_query + " " + self.src_filter
                elif action == "FILTER_JOIN":
                    self.src_filter = self.create_join_filter()
                    if self.src_filter is False:
                        return self.d_status
                    else:
                        self.src_query = self.src_query + " " + self.src_filter
                else:
                    log.error("Source action {0} not defined.".format(action))
                    self.d_status["EVENT_CODE"] = 7
                    return self.d_status
        log.info("Quellcursor: {0}".format(self.src_query))
        self.d_status["EVENT_CODE"] = 0
        return self.d_status

    def create_target_actions(self):
        """ Actions on target
        Es werden alle definierten Aktionen durchgegangen.
        Dabei werden abgearbeitete Aktionen aus dem Regelsatz entfernt.
        """
        if self.tgt_ruleset:
            l_actions = self.tgt_ruleset.split(",")
            for action in range(len(l_actions)):
                if l_actions[action] == "TRUNCATE":
                    result = self.truncate_object()
                    if result is False:
                        self.d_status["EVENT_CODE"] = 1
                        return self.d_status
                    else:
                        l_tgt_ruleset = self.tgt_ruleset.split(",")
                        l_tgt_ruleset.remove(l_actions[action])
                        self.tgt_ruleset = ",".join(l_tgt_ruleset)
                elif l_actions[action] in ("INSERT", "UPSERT", "MERGE", "MASK", "UPSERT_MASK"):
                    self.tgt_query = self.tgt_db.create_dml(self.l_intersect, self.o_src_table,
                                                            self.o_tgt_table, self.src_filter, l_actions[action])
                    log.info("Zielcursor: {0}".format(self.tgt_query))
                else:
                    log.error("Target action {0} undefined.".format(l_actions[action]))
                    self.d_status["EVENT_CODE"] = 7
                    return self.d_status
        self.d_status["EVENT_CODE"] = 0
        return self.d_status

    def run_mapping(self):
        """
            Mapping der Daten mit und ohne Pseudonymisierung
        """
        if self.tgt_ruleset:
            for action in self.tgt_ruleset.split(","):
                if action in ("INSERT", "UPSERT"):
                    log.info("Kopiere Datensätze von {0}.{1} nach {2}.{3}.".format(
                        self.src_schema, self.src_obj, self.tgt_schema, self.tgt_obj))
                    self.successful_rows = self.src_db.copy_data(self.src_dsn, self.src_schema, self.src_obj,
                                                                 self.src_query, self.tgt_dsn, self.tgt_schema,
                                                                 self.tgt_obj, self.tgt_query, self.max_mem_proc)
                    if self.successful_rows is False:
                        self.d_status["EVENT_CODE"] = 1
                        return self.d_status
                    else:
                        log.info("Copied {0} rows to table {1}.{2}.".format(
                            self.successful_rows, self.tgt_schema, self.tgt_obj))
                elif action in ("MASK", "UPSERT_MASK"):
                    log.info("Generiere Zuordnungsdaten für Regeln.")
                    """ Create cache dictionary """
                    d_names = {}
                    for item in self.d_rules.values():
                        d_names[item["RULE_NAME"] + "_cache"] = [item["LKP_DSN"], item["LKP_SCHEMA"], item["LKP_OBJ"],
                                                                 item["LKP_COLS"], item["LKP_ID"],
                                                                 item["TRANSLATE_EXPRESSION"]]
                    log.debug("Regel zu SQL statement: {0}".format(d_names))
                    self.d_cache = {}
                    for entry in d_names:
                        lkp_dsn = d_names[entry][0]
                        lkp_schema = d_names[entry][1]
                        lkp_obj = d_names[entry][2]
                        lkp_cols = d_names[entry][3]
                        lkp_id = d_names[entry][4]
                        stmt = d_names[entry][5]
                        if entry == "R12_cache":
                            """ Lese Zuers Zuordnungstabellen """
                            log.debug("Baue DataFrame für TDM_MT_ZUERS_ADR...")
                            stmt = "select adr_id, lower(org_adr) org_adr" \
                                   ", substr(mask_adr,3, substr(mask_adr,1,2)) as mask_str" \
                                   ", rtrim(substr(mask_adr, substr(mask_adr,1,2)+3)) as mask_hsn " \
                                   "from {0}.tdm_mt_zuers_adr".format(lkp_schema)
                            self.d_cache["R12_adr"] = get_cache_dataframe(d_names[entry][0], stmt)
                            log.debug("Ok.")
                            log.debug("Baue DataFrame für TDM_MT_ZUERS_HSN...")
                            stmt = "select adr_id, lower(org_adr) org_adr, rtrim(org_hsn) org_hsn" \
                                   ", substr(mask_adr,3, substr(mask_adr,1,2)) as mask_str" \
                                   ", rtrim(substr(mask_adr, substr(mask_adr,1,2)+3)) as mask_hsn " \
                                   "from {0}.tdm_mt_zuers_hsn".format(lkp_schema)
                            self.d_cache["R12_hsn"] = get_cache_dataframe(lkp_dsn, stmt)
                            log.debug("Ok.")
                        elif lkp_dsn is not None and lkp_schema is not None and lkp_obj is not None \
                                and lkp_cols is not None and stmt is None:
                            stmt = "select {0} from {1}.{2}".format(lkp_cols, lkp_schema, lkp_obj)
                            self.d_cache[entry] = get_cache_dataframe(lkp_dsn, stmt)
                        elif lkp_dsn is not None and stmt is not None:
                            if stmt.endswith(';'):
                                stmt = stmt.replace(";", "")
                            self.d_cache[entry] = get_cache_dataframe(lkp_dsn, stmt)
                    log.debug("Regel zu DataFrame: {0}".format(self.d_cache))

                    log.info("Lese Datensätze aus {0}.{1} und schreibe maskierte Felder "
                             "nach {2}.{3}.".format(
                                self.src_schema, self.src_obj, self.tgt_schema, self.tgt_obj))
                    self.successful_rows = self.src_db.mask_data(self.src_dsn, self.src_schema, self.src_obj,
                                                                 self.src_query, self.tgt_dsn, self.tgt_schema,
                                                                 self.tgt_obj, self.tgt_query, self.l_intersect,
                                                                 self.d_rules, self.max_mem_proc, self.d_cache)
                    log.debug(self.successful_rows)
                    if self.successful_rows is False:
                        self.d_status["EVENT_CODE"] = 1
                        return self.d_status
                    else:
                        log.info("{0} Datensätze erfolgreich in Tabelle {1}.{2} kopiert.".format(
                            self.successful_rows, self.tgt_schema, self.tgt_obj))
                elif action == "MERGE":
                    self.successful_rows = self.tgt_db.sql_query(self.tgt_dsn, self.tgt_query)
                    if self.successful_rows is False:
                        self.d_status["EVENT_CODE"] = 1
                        return self.d_status
                    else:
                        log.info("{0} Datensätze erfolgreich in Tabelle {1}.{2} kopiert.".format(
                            self.successful_rows, self.tgt_schema, self.tgt_obj))
                else:
                    log.info("Aktion {0} ist nicht definiert.".format(action))
                    self.d_status["EVENT_CODE"] = 7
                    return self.d_status
        else:
            log.info("Keine Regel gefunden für Zieltabelle {0}.{1}. Tabelle wird ignoriert.".format(
                self.tgt_schema, self.tgt_obj))
        self.d_status["EVENT_CODE"] = 0
        self.d_status["ROWS_WRITTEN"] = self.successful_rows
        return self.d_status

    def deduplicate_rows(self):
        log.info("Bereinige Dubletten auf Tabelle {0}.{1}".format(
            self.src_schema, self.src_obj))
        self.l_src_pk = self.l_tgt_pk if not self.l_src_pk else self.l_src_pk
        return self.o_src_table.deduplicate_rows("MIN", self.l_src_pk)

    def filter_invalid_rows(self):
        log.info("Filtere Sätze aus {0}.{1} ohne vollständigen PK.".format(
            self.src_schema, self.src_obj))
        return "WHERE " + " AND ".join(list(x + " IS NOT NULL" for x in self.l_tgt_pk))

    def select_error_rows(self):
        log.info("Selektiere Fehlersätze in {0}.{1} ohne vollständigen PK.".format(
            self.src_schema, self.src_obj))
        return "WHERE " + " OR ".join(list(x + " IS NULL" for x in self.l_tgt_pk))

    def create_join_filter(self):
        log.info("Erstelle Joiner auf Filtertabelle.")
        if self.src_business_key is None:
            self.d_status["EVENT_CODE"] = 5
            return False
        if self.fil_business_key is None:
            self.d_status["EVENT_CODE"] = 8
            return False
        l_join_cols = []
        for x, y in zip(self.src_business_key.split(","), self.fil_business_key.split(",")):
            l_join_cols.append(
                "{0}.\"{1}\".\"{2}\" = {3}.\"{4}\".\"{5}\"".format(self.src_schema, self.src_obj, x,
                                                                   self.fil_schema, self.fil_obj, y))
        return "JOIN {0}.\"{1}\" ON ".format(self.fil_schema, self.fil_obj) + " AND ".join(l_join_cols)

    def create_union_stmt(self):
        log.info("Erstelle UNION SQL für jedes Feld in Quell- und Filter BK.")
        if self.src_business_key is None:
            self.d_status["EVENT_CODE"] = 5
            return False
        if self.fil_business_key is None:
            self.d_status["EVENT_CODE"] = 8
            return False
        if self.src_business_key.count(",") != self.fil_business_key.count(","):
            self.d_status["EVENT_CODE"] = 9
            return False
        return self.src_db.create_union_stmt(self.o_src_table, self.o_fil_table,
                                             self.src_business_key, self.fil_business_key)

    def truncate_object(self):
        log.info("Leere Zieltabelle {0}.{1}.{2}.".format(self.tgt_dsn, self.tgt_schema, self.tgt_obj))
        return self.o_tgt_table.truncate_table()
