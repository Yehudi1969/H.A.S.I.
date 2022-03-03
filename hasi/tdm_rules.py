# -*- coding: utf8 -*-
################################################################################
#                                 tdm_rules.py                                 #
################################################################################
""" Regelmodul für TDM. Die Regeln wurden aus Kompatibilitätsgründen aus dem
    Regelset für TDM aus PowerCenter abgeleitet.

    Autor: Jens Janzen
    Version 0.1: 2022-01-10, JJ - Regeln R01-R03 implementiert.
    Version 0.2: 2022-02-08, JJ - Regeln R10, R16, R21, R46 implementiert.
    Version 0.3: 2022-02-22, JJ - Regel R12 implementiert.
"""
import logging
import pandas as pd


# Functions
def table2dataframe(dsn, schema, table, stmt=None):
    import oralib as db
    if not stmt:
        stmt = "select * from {0}.{1}".format(schema, table)
    connection = db.get_connection(dsn)
    cursor = connection.cursor()
    obj_exec = cursor.execute(stmt)
    cols = list(x[0] for x in cursor.description)
    raw_data = obj_exec.fetchall()
    df = pd.DataFrame(raw_data, columns=cols)
    log.info("Header: {0}".format(cols))
    return df


""" Sets logging console handler for debugging """
log = logging.getLogger("tdm_rules")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)


def rule_r01(ruleset, table_rows, lkp_cache):
    """ Rule ID R01->Nachname """
    src_col = ruleset["ATTRIBUT"]
    lkp_col = ruleset["LKP_COLS"].split(",")[0]
    mask_col = ruleset["LKP_COLS"].split(",")[1]
    default_value = ruleset["DEFAULT_VALUE_1"]
    """ Convert comparison column to lowercase """
    lkp_cache[lkp_col] = lkp_cache[lkp_col].str.lower()
    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[table_rows[src_col].isna()]
    df_mask = table_rows.loc[table_rows[src_col].notnull()]
    """ Add column WORT_ORIG as 10 first chars of NACHNAME lowercase """
    df_mask[lkp_col] = df_mask[src_col].str.slice(0, 10).str.lower().str.rstrip()
    """ Merge lookup rows to table dataframe """
    lkp_rows = pd.merge(df_mask, lkp_cache, how="left", on=lkp_col)
    """ Replace original value with masked value """
    del df_mask[lkp_col]
    """ Fill empty row values from lookup with default value """
    df_mask[src_col] = lkp_rows[mask_col].fillna(default_value)
    """ Replace original dataframe with concatenated two dataframes """
    table_rows = pd.concat([df_unchanged, df_mask])
    return table_rows


def rule_r02(ruleset, table_rows, lkp_cache):
    """ Rule ID R02->Vorname """
    src_col = ruleset["ATTRIBUT"]
    lkp_col = ruleset["LKP_COLS"].split(",")[0]
    mask_col = ruleset["LKP_COLS"].split(",")[1]
    default_value = ruleset["DEFAULT_VALUE_1"]
    """ Convert comparison column to lowercase """
    lkp_cache[lkp_col] = lkp_cache[lkp_col].str.lower()
    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[table_rows[src_col].isna()]
    df_mask = table_rows.loc[table_rows[src_col].notnull()]
    """ Add column WORT_ORIG as 10 first chars of NACHNAME lowercase """
    df_mask[lkp_col] = df_mask[src_col].str.slice(0, 10).str.lower().str.rstrip()
    """ Merge lookup rows to table dataframe """
    lkp_rows = pd.merge(df_mask, lkp_cache, how="left", on=lkp_col)
    """ Replace original value with masked value """
    del df_mask[lkp_col]
    """ Fill empty row values from lookup with default value """
    df_mask[src_col] = lkp_rows[mask_col].fillna(default_value)
    """ Replace original dataframe with concatenated two dataframes """
    table_rows = pd.concat([df_unchanged, df_mask])
    return table_rows


def rule_r03(ruleset, table_rows):
    """ Rule ID R03->Datum
    Wenn der Tag kleiner als der 15. eines Monats ist, setze 1. Ansonsten setze 2.
    Falls das Jahr 9999 ist, behalte den vorhandenen Wert bei.
    """

    def conditions(x):
        """ Replace all occurrences of day 1-14 with 1 and 15-eom with 15 if year is not 9999 """
        v1 = int(default_value_1)
        v2 = int(default_value_2)
        if x.year == 9999:
            return x
        elif x.year != 9999 and x.day < v2:
            return x.replace(day=v1)
        elif x.year != 9999 and x.day >= v2:
            return x.replace(day=v2)

    src_col = ruleset["ATTRIBUT"]
    data_type = table_rows[src_col].dtypes
    timestamp_format = ruleset["FORMAT_STRING"]
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    default_value_2 = ruleset["DEFAULT_VALUE_2"]
    log.debug("Maskiertes Attribut: {0}".format(src_col))
    log.debug("Datentyp Pandas Spalte: {0}".format(data_type))
    log.debug("Zeitstempel Format: {0}".format(timestamp_format))
    log.debug("Ersatzwert 1: {0}".format(default_value_1))
    log.debug("Ersatzwert 2: {0}".format(default_value_2))

    if timestamp_format == "DATE" and data_type == "object":
        table_rows["STRING_DATE"] = table_rows[src_col].astype(str)
        timestamp_format = "YYYY-MM-DD HH:MI:SS"
        pos_year = timestamp_format.index("YY")
        pos_day = timestamp_format.index("DD")
        len_year = timestamp_format.count("Y")
        len_day = timestamp_format.count("D")
        table_rows["DAY"] = table_rows.STRING_DATE.str[pos_day: pos_day + len_day]
        table_rows["YEAR"] = table_rows.STRING_DATE.str[pos_year: pos_year + len_year]
        table_rows.loc[(table_rows["DAY"] < default_value_2) &
                       (table_rows["YEAR"] != "9999"), ["DAY"]] = default_value_1
        table_rows.loc[(table_rows["DAY"] >= default_value_2) &
                       (table_rows["YEAR"] != "9999"), ["DAY"]] = default_value_2
        table_rows["STRING_DATE"] = table_rows.STRING_DATE.str[:pos_day] + \
                                    table_rows["DAY"] + \
                                    table_rows.STRING_DATE.str[pos_day + len_day:]
        table_rows[src_col] = table_rows["STRING_DATE"]
        del table_rows["YEAR"]
        del table_rows["DAY"]
        del table_rows["STRING_DATE"]
        return table_rows
    else:
        table_rows[src_col] = table_rows[src_col].apply(conditions)
    return table_rows


def rule_r10(ruleset, table_rows, lkp_cache):
    """ Rule ID R10->wohnhaft bei """
    src_col = ruleset["ATTRIBUT"]
    lkp_col = ruleset["LKP_COLS"].split(",")[0]
    mask_col = ruleset["LKP_COLS"].split(",")[1]
    default_value = ruleset["DEFAULT_VALUE_1"]
    """ Convert comparison column to lowercase """
    lkp_cache[lkp_col] = lkp_cache[lkp_col].str.lower()
    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[table_rows[src_col].isna()]
    df_mask = table_rows.loc[table_rows[src_col].notnull()]
    """ Add column WORT_ORIG as 10 first chars of NACHNAME lowercase """
    df_mask[lkp_col] = df_mask[src_col].str.slice(0, 1).str.lower()
    """ Merge lookup rows to table dataframe """
    lkp_rows = pd.merge(df_mask, lkp_cache, how="left", on=lkp_col)
    """ Replace original value with masked value """
    del df_mask[lkp_col]
    """ Fill empty row values from lookup with default value """
    df_mask[src_col] = lkp_rows[mask_col].fillna(default_value)
    """ Replace original dataframe with concatenated two dataframes """
    table_rows = pd.concat([df_unchanged, df_mask])
    return table_rows


def rule_r12(ruleset, data):
    """ Rule ID R12->Verfahren zur Adresse """
    """ Regelattribute """
    l_attributes = ruleset["ATTRIBUT"]
    l_lkp_cols = ruleset["LKP_COLS"]
    l_lkp_cols = list(filter(None, l_lkp_cols))
    d_map_cols = dict(zip(l_lkp_cols, l_attributes))
    d_map_cols = {k: v for k, v in d_map_cols.items() if v}
    null_check_col = ruleset["LKP_ID"]
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    default_value_2 = ruleset["DEFAULT_VALUE_2"]
    default_value_3 = ruleset["DEFAULT_VALUE_3"]
    """ Mappingattribute """
    log.debug("Mapping:\n{0}\n{1}\n{2}\n{3}\n{4}\n{5}".format(
        d_map_cols, null_check_col, default_value_1, default_value_2, default_value_3, l_attributes))
    """ Build lookup dataframes"""
    log.info("Baue DataFrame für TDM_MT_ZUERS_ADR...")
    query = "select adr_id, lower(org_adr) org_adr" \
            ", substr(mask_adr,3, substr(mask_adr,1,2)) as mask_str" \
            ", rtrim(substr(mask_adr, substr(mask_adr,1,2)+3)) as mask_hsn " \
            "from {0}.tdm_mt_zuers_adr".format(ruleset["LKP_SCHEMA"])
    adr = table2dataframe(ruleset["LKP_DSN"], ruleset["LKP_SCHEMA"], "TDM_MT_ZUERS_ADR", stmt=query)
    log.info("Ok.")
    log.info("Baue DataFrame für TDM_MT_ZUERS_HSN...")
    query = "select adr_id, lower(org_adr) org_adr, rtrim(org_hsn) org_hsn" \
            ", substr(mask_adr,3, substr(mask_adr,1,2)) as mask_str" \
            ", rtrim(substr(mask_adr, substr(mask_adr,1,2)+3)) as mask_hsn " \
            "from {0}.tdm_mt_zuers_hsn".format(ruleset["LKP_SCHEMA"])
    hsn = table2dataframe(ruleset["LKP_DSN"], ruleset["LKP_SCHEMA"], "TDM_MT_ZUERS_HSN", stmt=query)
    log.info("Ok.")
    """ Sichern des Datenblocks mit nicht zu bearbeitenden Datensätzen """
    df_unchanged = data.loc[data[null_check_col].isna()]
    """ Aufbau Adressschlüssel """
    data.loc[data[null_check_col].notnull(), "ORG_ADR"] = \
        data[d_map_cols["STRASSE"]].str.strip().str.lower() + \
        data[d_map_cols["PLZ"]].str.strip() + \
        data[d_map_cols["ORT"]].str.strip().str.lower()
    """ Merge auf Adresse """
    df_mask = pd.merge(data.loc[data[null_check_col].notna()],
                       adr[["ORG_ADR", "ADR_ID", "MASK_STR", "MASK_HSN"]], how="left", on="ORG_ADR")
    """ Änderung Datentyp ADR_ID auf integer """
    df_mask.loc[df_mask["ADR_ID"].isnull(), "ADR_ID"] = -2
    df_mask["ADR_ID"] = df_mask["ADR_ID"].astype('int')
    """ Übernahme Pseudonymisierung für Sätze ohne Hausnummer """
    """ Wenn keine Information zum Schlüssel gefunden wird, nehme den Ersatzwert """
    df_mask.loc[df_mask["ADR_ID"] == -2, "STRASSE"] = "Zürs-Straße"
    df_mask.loc[df_mask["ADR_ID"] == -2, "HAUSNUMMER"] = "1"
    """ Ansonsten nehme die Information aus adr """
    df_mask.loc[df_mask["ADR_ID"] == -1, "STRASSE"] = df_mask["MASK_STR"]
    df_mask.loc[df_mask["ADR_ID"] == -1, "HAUSNUMMER"] = df_mask["MASK_HSN"]
    df_hsn = pd.merge(df_mask.loc[df_mask["ADR_ID"] > -1], hsn, how="left",
                      left_on=["ORG_ADR", "ADR_ID", "HAUSNUMMER"],
                      right_on=["ORG_ADR", "ADR_ID", "ORG_HSN"])
    log.debug("\n{0}".format(df_hsn.head()))
    """ Übernahme Pseudonymisierung für Sätze mit Hausnummer """
    """ Wenn keine Information in hsn vorhanden ist, nehme die aus adr """
    df_hsn.loc[df_hsn["MASK_STR_y"].isna(), "STRASSE"] = df_hsn["MASK_STR_x"]
    df_hsn.loc[df_hsn["MASK_HSN_y"].isna(), "HAUSNUMMER"] = df_hsn["MASK_HSN_x"]
    """ Ansonsten nehme die Information aus hsn """
    df_hsn.loc[df_hsn["MASK_STR_y"].notnull(), "STRASSE"] = df_hsn["MASK_STR_y"]
    df_hsn.loc[df_hsn["MASK_STR_y"].notnull(), "HAUSNUMMER"] = df_hsn["MASK_HSN_y"]
    del df_hsn["ORG_ADR"]
    del df_hsn["ADR_ID"]
    del df_hsn["ORG_HSN"]
    del df_hsn["MASK_STR_x"]
    del df_hsn["MASK_HSN_x"]
    del df_hsn["MASK_STR_y"]
    del df_hsn["MASK_HSN_y"]
    del df_mask["MASK_STR"]
    del df_mask["MASK_HSN"]
    """ Zusammenfassen der Daten """
    """ In df_mask nur die Rows ohne Hausnummer behalten """
    df_mask = df_mask.loc[df_mask["ADR_ID"] < 0]
    """ Überzählige Spalten entfernen """
    del df_mask["ORG_ADR"]
    del df_mask["ADR_ID"]
    del data["ORG_ADR"]
    """ Die Dataframes wieder zum DataFrame data zusammenbauen """
    data = pd.concat([df_unchanged, df_mask, df_hsn])
    return data


def rule_r16(ruleset, table_rows):
    """ Rule ID R16->Löschen """
    src_col = ruleset["ATTRIBUT"]
    default_value = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[table_rows[src_col].notnull(), src_col] = default_value
    """ Return original dataframe """
    return table_rows


def rule_r21(ruleset, table_rows):
    """ Rule ID R21->Grundstück """
    src_col = ruleset["ATTRIBUT"]
    default_value = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[table_rows[src_col].notnull(), src_col] = default_value
    """ Return original dataframe """
    return table_rows


def rule_r46(ruleset, table_rows, lkp_cache):
    """ Rule ID R46->Ersetzen Nachname, Vorname """

    def mask_column(df, column, default):
        """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
        df_unchanged = df.loc[df[column].isna()]
        df_mask = df.loc[df[column].notnull()]
        """ Add column WORT_ORIG as 10 first chars of string lowercase """
        df_mask[lkp_col] = df_mask[column].str.slice(0, 10).str.lower().str.rstrip()
        """ Merge lookup rows to table dataframe """
        lkp_rows = pd.merge(df_mask, df_lkp, how="left", on=lkp_col)
        """ Replace original value with masked value """
        del df_mask[lkp_col]
        """ Fill empty row values from lookup with default value """
        df_mask[column] = lkp_rows[mask_col].fillna(default)
        """ Replace original dataframe with concatenated two dataframes """
        df = pd.concat([df_unchanged, df_mask])
        return df

    src_col = ruleset["ATTRIBUT"]
    lkp_col = ruleset["LKP_COLS"].split(",")[0]
    mask_col = ruleset["LKP_COLS"].split(",")[1]
    entities = ruleset["FORMAT_STRING"].split(",")
    defaults = ruleset["DEFAULT_VALUE_1"].split(",")
    """ Replace comma with blank in rows with different delimiters """
    table_rows[src_col] = table_rows[src_col].str.replace(",", " ")
    """ Convert comparison column to lowercase """
    lkp_cache[lkp_col] = lkp_cache[lkp_col].str.lower()
    """ Split name into forename and name """
    table_rows[entities] = table_rows[src_col].str.split(n=1, expand=True)
    for col, default_value in zip(entities, defaults):
        if col == "NACHNAME":
            df_lkp = lkp_cache.loc[lkp_cache["WORT_ART"].isin([1, 3])].sort_values(by="WORT_ART")
            df_lkp = df_lkp.drop_duplicates(subset=["WORT_ORIG", "WORT_MASK"], keep="first")
        elif col == "VORNAME":
            df_lkp = lkp_cache.loc[lkp_cache["WORT_ART"] == 5]
        table_rows = mask_column(table_rows, col, default_value)
    table_rows[src_col] = table_rows[entities[0]] + ", " + table_rows[entities[1]]
    for x in entities:
        del table_rows[x]
    return table_rows
