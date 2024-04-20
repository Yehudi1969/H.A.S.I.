# -*- coding: utf8 -*-
################################################################################
#                                 tdm_rules.py                                 #
################################################################################
# Copyright (C) 2023  Jens Janzen

""" Regelmodul für TDM. Die Regeln wurden aus Kompatibilitätsgründen aus dem
    Regelset für TDM aus PowerCenter abgeleitet.

    Autor: Jens Janzen
    Version 0.1: 2022-01-10, JJ - Regeln R01-R03 implementiert.
    Version 0.2: 2022-02-08, JJ - Regeln R10, R16, R21, R46 implementiert.
    Version 0.3: 2022-02-22, JJ - Regel R12 implementiert.
    Version 0.4: 2022-06-30, JJ - Regeln R11, R19, R35, R36, R37 und R69 implementiert.
    Version 0.5: 2022-07-07, JJ - Bugfixing SettingWithCopyWarning
                                  Explizites Kopieren statt Referenz implementiert.
    Version 0.7: 2022-07-19, JJ - Logging für eingehende und ausgehende DataFrames für Checks implementiert.
    Version 0.8: 2023-01-18, JJ - Regel R83 implementiert.
    Version 0.9: 2023-02-01, JJ - Regel R17 implementiert.
    Version 1.0: 2023-03-20, JJ - Regel R02 erweitert. Werte werden nun auf die maximale Länge in der Datenbank gekürzt.
                                  Regel R03 überarbeitet. Künstliche Spaltennamen auf Camelcase geändert,
                                  um Namensgleichheit mit vorhandenen Feldern auszuschließen.
    Version 1.1: 2023-04-05, JJ - Regel R41 implementiert.
    Version 1.2: 2023-04-12, JJ - Regel R14 und R18 implementiert.
    Version 1.3: 2023-07-06, JJ - Regeln R24, R55, R56, R58, R59 und R61 implementiert.
                                  R23 für data cleansing angepasst.
    Version 1.4: 2023-07-07, JJ - Regeln R49, R50, R57, R60, R62, R63, R64 und R65 implementiert.
    Version 1.5: 2024-02-14, JJ - Regel R47 implementiert.
"""
import logging
import pandas as pd
import numpy as np

""" Sets logging console handler for debugging """
log = logging.getLogger("SESSION")


# Functions
def rule_r01(ruleset, table_rows, lkp_cache):
    """ Rule ID R01->Nachname """
    src_col = ruleset["ATTRIBUT"]
    lkp_col = ruleset["LKP_COLS"].split(",")[0]
    mask_col = ruleset["LKP_COLS"].split(",")[1]
    default_value = ruleset["DEFAULT_VALUE_1"]
    max_length = ruleset["COLUMN_LENGTH"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[(table_rows[src_col].isna()) |
                                  (table_rows[src_col].str.strip() == '')].copy().reset_index(drop=True)
    df_mask = table_rows.loc[(table_rows[src_col].notnull()) &
                             (table_rows[src_col].str.strip() > '')].copy().reset_index(drop=True)
    del table_rows

    """ Check for rows to process """
    if df_mask.empty is True:
        log.info("No data found to mask.")
        return df_unchanged

    """ Add column WORT_ORIG as 10 first chars of NACHNAME lowercase """
    df_mask[lkp_col] = df_mask[src_col].str.slice(0, 10).str.lower().str.rstrip()
    """ Merge lookup rows to table dataframe """
    lkp_rows = pd.merge(df_mask, lkp_cache, how="left", on=lkp_col)
    """ Replace original value with masked value """
    del df_mask[lkp_col]
    """ Fill empty row values from lookup with default value """
    df_mask[src_col] = lkp_rows[mask_col].fillna(default_value)
    """ Truncate masked column if length exceeds column length in database """
    log.info("Kürze Feld {0} auf Länge {1}".format(src_col, max_length))
    df_mask["TruncColumn"] = df_mask[src_col].str.slice(0, max_length)
    df_mask.loc[df_mask[src_col].str.len() > max_length, src_col] = df_mask["TruncColumn"]
    del df_mask["TruncColumn"]
    """ Replace original dataframe with concatenated two dataframes """
    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r02(ruleset, table_rows, lkp_cache):
    """ Rule ID R02->Vorname """
    src_col = ruleset["ATTRIBUT"]
    lkp_col = ruleset["LKP_COLS"].split(",")[0]
    mask_col = ruleset["LKP_COLS"].split(",")[1]
    default_value = ruleset["DEFAULT_VALUE_1"]
    max_length = ruleset["COLUMN_LENGTH"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[(table_rows[src_col].isna()) |
                                  (table_rows[src_col].str.strip() == '')].copy().reset_index(drop=True)
    df_mask = table_rows.loc[(table_rows[src_col].notnull()) &
                             (table_rows[src_col].str.strip() > '')].copy().reset_index(drop=True)
    del table_rows

    """ Check for rows to process """
    if df_mask.empty is True:
        log.info("No data found to mask.")
        return df_unchanged

    """ Add column WORT_ORIG as 10 first chars of NACHNAME lowercase """
    df_mask[lkp_col] = df_mask[src_col].str.slice(0, 10).str.lower().str.rstrip()
    """ Merge lookup rows to table dataframe """
    lkp_rows = pd.merge(df_mask, lkp_cache, how="left", on=lkp_col)
    """ Replace original value with masked value """
    del df_mask[lkp_col]
    """ Fill empty row values from lookup with default value """
    df_mask[src_col] = lkp_rows[mask_col].fillna(default_value)
    """ Truncate masked column if length exceeds column length in database """
    log.info("Kürze Feld {0} auf Länge {1}".format(src_col, max_length))
    df_mask["TruncColumn"] = df_mask[src_col].str.slice(0, max_length)
    df_mask.loc[df_mask[src_col].str.len() > max_length, src_col] = df_mask["TruncColumn"]
    del df_mask["TruncColumn"]
    """ Replace original dataframe with concatenated two dataframes """
    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
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
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    if timestamp_format == "DATE":
        df_unchanged = table_rows.loc[(table_rows[src_col].isna()) |
                                      (table_rows[src_col] == "00000000")].copy().reset_index(drop=True)
        df_mask = table_rows.loc[(table_rows[src_col].notnull()) &
                                 (table_rows[src_col] != "00000000")].copy().reset_index(drop=True)
    else:
        df_unchanged = table_rows.loc[(table_rows[src_col].isna()) |
                                      (table_rows[src_col].str.strip() == '') |
                                      (table_rows[src_col] == "00000000")].copy().reset_index(drop=True)
        df_mask = table_rows.loc[(table_rows[src_col].notnull()) &
                                 (table_rows[src_col].str.strip() > '') &
                                 (table_rows[src_col] != "00000000")].copy().reset_index(drop=True)
    del table_rows

    """ Check for rows to process """
    if df_mask.empty is True:
        log.info("No data found to mask.")
        return df_unchanged

    if timestamp_format == "DATE":
        df_mask[src_col] = df_mask[src_col].apply(conditions)
    elif timestamp_format != "DATE":
        df_mask["StringDate"] = df_mask[src_col].astype(str)
        strfmt = timestamp_format
        pos_year = strfmt.index("YY")
        pos_day = strfmt.index("DD")
        len_year = strfmt.count("Y")
        len_day = strfmt.count("D")
        df_mask["DAY"] = df_mask["StringDate"].str[pos_day: pos_day + len_day]
        df_mask["YEAR"] = df_mask["StringDate"].str[pos_year: pos_year + len_year]
        df_mask.loc[(df_mask["DAY"] < default_value_2) &
                    (df_mask["YEAR"] != "9999"), ["DAY"]] = default_value_1
        df_mask.loc[(df_mask["DAY"] >= default_value_2) &
                    (df_mask["YEAR"] != "9999"), ["DAY"]] = default_value_2
        df_mask["StringDate"] = df_mask["StringDate"].str[:pos_day] + df_mask[
            "DAY"] + df_mask["StringDate"].str[pos_day + len_day:]
        df_mask[src_col] = df_mask["StringDate"]
        del df_mask["YEAR"]
        del df_mask["DAY"]
        del df_mask["StringDate"]

    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r04(ruleset, table_rows, lkp_cache):
    """ Rule ID R04->Geburtsort """
    src_col = ruleset["ATTRIBUT"]
    lkp_col = ruleset["LKP_COLS"].split(",")[0]
    mask_col = ruleset["LKP_COLS"].split(",")[1]
    default_value = ruleset["DEFAULT_VALUE_1"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[(table_rows[src_col].isna()) |
                                  (table_rows[src_col].str.strip() == '')].copy().reset_index(drop=True)
    df_mask = table_rows.loc[(table_rows[src_col].notnull()) &
                             (table_rows[src_col].str.strip() > '')].copy().reset_index(drop=True)
    del table_rows

    """ Add column WORT_ORIG as first char of source attribute lowercase """
    df_mask[lkp_col] = df_mask[src_col].str.slice(0, 1).str.lower().str.rstrip()
    """ Merge lookup rows to table dataframe """
    lkp_rows = pd.merge(df_mask, lkp_cache, how="left", on=lkp_col)
    """ Replace original value with masked value """
    del df_mask[lkp_col]
    """ Fill empty row values from lookup with default value """
    df_mask[src_col] = lkp_rows[mask_col].fillna(default_value)
    """ Replace original dataframe with concatenated two dataframes """
    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r05(ruleset, table_rows, lkp_cache):
    """ Rule ID R05->Geburtsname """
    src_col = ruleset["ATTRIBUT"]
    lkp_col = ruleset["LKP_COLS"].split(",")[0]
    mask_col = ruleset["LKP_COLS"].split(",")[1]
    default_value = ruleset["DEFAULT_VALUE_1"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[(table_rows[src_col].isna()) |
                                  (table_rows[src_col].str.strip() == '')].copy().reset_index(drop=True)
    df_mask = table_rows.loc[(table_rows[src_col].notnull()) &
                             (table_rows[src_col].str.strip() > '')].copy().reset_index(drop=True)
    del table_rows

    """ Add column WORT_ORIG as 1 first character of NACHNAME lowercase """
    df_mask[lkp_col] = df_mask[src_col].str.slice(0, 1).str.lower().str.rstrip()
    """ Merge lookup rows to table dataframe """
    lkp_rows = pd.merge(df_mask, lkp_cache, how="left", on=lkp_col)
    """ Replace original value with masked value """
    del df_mask[lkp_col]
    """ Fill empty row values from lookup with default value """
    df_mask[src_col] = lkp_rows[mask_col].fillna(default_value)
    """ Replace original dataframe with concatenated two dataframes """
    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r06(ruleset, table_rows, lkp_cache):
    """ Rule ID R06->Name Institution """
    src_col = ruleset["ATTRIBUT"]
    lkp_col = ruleset["LKP_COLS"].split(",")[0]
    mask_col = ruleset["LKP_COLS"].split(",")[1]
    default_value = ruleset["DEFAULT_VALUE_1"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[(table_rows[src_col].isna()) |
                                  (table_rows[src_col].str.strip() == '')].copy().reset_index(drop=True)
    df_mask = table_rows.loc[(table_rows[src_col].notnull()) &
                             (table_rows[src_col].str.strip() > '')].copy().reset_index(drop=True)
    del table_rows

    """ Add column WORT_ORIG as 10 first chars of NACHNAME lowercase """
    df_mask[lkp_col] = df_mask[src_col].str.slice(0, 10).str.lower().str.rstrip()
    """ Merge lookup rows to table dataframe """
    lkp_rows = pd.merge(df_mask, lkp_cache, how="left", on=lkp_col)
    """ Replace original value with masked value """
    del df_mask[lkp_col]
    """ Fill empty row values from lookup with default value """
    df_mask[src_col] = lkp_rows[mask_col].fillna(default_value)
    """ Replace original dataframe with concatenated two dataframes """
    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r10(ruleset, table_rows, lkp_cache):
    """ Rule ID R10->wohnhaft bei """
    src_col = ruleset["ATTRIBUT"]
    lkp_col = ruleset["LKP_COLS"].split(",")[0]
    mask_col = ruleset["LKP_COLS"].split(",")[1]
    default_value = ruleset["DEFAULT_VALUE_1"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[(table_rows[src_col].isna()) |
                                  (table_rows[src_col].str.strip() == '')].copy().reset_index(drop=True)
    df_mask = table_rows.loc[(table_rows[src_col].notnull()) &
                             (table_rows[src_col].str.strip() > '')].copy().reset_index(drop=True)
    del table_rows

    """ Check for rows to process """
    if df_mask.empty is True:
        log.info("No data found to mask.")
        return df_unchanged

    """ Add column WORT_ORIG as first char of source attribute lowercase """
    df_mask[lkp_col] = df_mask[src_col].str.slice(0, 1).str.lower().str.rstrip()
    """ Merge lookup rows to table dataframe """
    lkp_rows = pd.merge(df_mask, lkp_cache, how="left", on=lkp_col)
    """ Replace original value with masked value """
    del df_mask[lkp_col]
    """ Fill empty row values from lookup with default value """
    df_mask[src_col] = lkp_rows[mask_col].fillna(default_value)
    """ Replace original dataframe with concatenated two dataframes """
    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r11(ruleset, table_rows):
    """ Rule ID R11->Postfach """
    """ Vorhandenes Postfach wird nach der folgenden Regel maskiert:
        plz_mask = plz % 20 + 505000) 
    """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[(table_rows[src_col].isna()) |
                                  (table_rows[src_col].str.strip() == '')].copy().reset_index(drop=True)
    df_mask = table_rows.loc[(table_rows[src_col].notnull()) &
                             (table_rows[src_col].str.strip() > '')].copy().reset_index(drop=True)
    del table_rows

    """ Cleanse source column from all values that are not digits """
    df_mask[src_col] = df_mask[src_col].str.replace("[^0-9]", "", regex=True)
    df_mask[src_col] = pd.to_numeric(df_mask[src_col], downcast='signed')

    """ Check for rows to process """
    if df_mask.empty is True:
        log.info("No data found to mask.")
        return df_unchanged

    """ Calculate masked value for source column """
    df_mask[src_col] = df_mask[src_col] % 20 + 505000

    """ Return dataframe """
    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r12(ruleset, table_rows, adr, hsn, l_pk):
    """ Rule ID R12->Verfahren zur Adresse """
    """ Regelattribute """
    l_attributes = ruleset["ATTRIBUT"]
    l_lkp_cols = ruleset["LKP_COLS"]
    d_map_cols = {k: v for k, v in zip(l_lkp_cols, l_attributes) if k is not None}
    log.debug(d_map_cols)
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    default_value_2 = ruleset["DEFAULT_VALUE_2"]
    default_value_3 = ruleset["DEFAULT_VALUE_3"]
    format_string = ruleset["FORMAT_STRING"]
    max_length = None
    if "STRASSE_KURZ" in d_map_cols:
        ix_zusatz_attribut = l_lkp_cols.index("STRASSE_KURZ")
        max_length = ruleset["COLUMN_LENGTH"][ix_zusatz_attribut]
    entities = []
    delimiter = None
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    """ Mappingattribute """
    log.debug("Mapping:\n{0}\n{1}\n{2}\n{3}\n{4}".format(
        d_map_cols, l_attributes, default_value_1, default_value_2, default_value_3))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_mask = table_rows.loc[(table_rows[d_map_cols["STRASSE"]].notnull()) &
                             (table_rows[d_map_cols["STRASSE"]].str.strip() > '')].copy().reset_index(drop=True)

    df_done = table_rows.loc[(table_rows[d_map_cols["STRASSE"]].isna()) |
                             (table_rows[d_map_cols["STRASSE"]].str.strip() == '')].copy().reset_index(drop=True)
    del table_rows

    """ Check, ob es etwas zu maskieren gibt """
    if df_mask.empty is True:
        return df_done

    """ Check auf Formatanweisung """
    if format_string is not None:
        entities = format_string.split("|")[0].split(",")
        delimiter = format_string.split("|")[1]

    """ Wenn PLZ oder Ort im DataFrame fehlen, setze den Defaultwert ein und hänge die Daten an df_done. """
    rows = df_mask.loc[(df_mask[d_map_cols["PLZ"]].isna()) | (df_mask[d_map_cols["ORT"]].isna())].copy()
    if "HSN" in d_map_cols:
        """ Wenn es ein Attribut für die Hausnummer gibt, weise die Defaultwerte zu """
        rows[d_map_cols["STRASSE"]] = default_value_1
        rows[d_map_cols["HSN"]] = default_value_2
    elif len(entities) > 0 and delimiter is not None:
        """ Ansonsten schreibe Defaultwerte mit Trennzeichen in das Feld für die Straße """
        rows[d_map_cols["STRASSE"]] = default_value_1 + delimiter + default_value_2
    else:
        log.error("Kein Feld für Hausnummer vorhanden und Formatanweisung ist nicht gefüllt!")
        return False
    df_done = pd.concat([df_done, rows], ignore_index=True)
    df_mask.drop(rows.index, inplace=True)
    del rows

    """ Trennung von Strasse und Hausnummer, die in einem Feld stehen """
    if format_string is not None and "HSN" not in d_map_cols:
        df_mask[entities] = df_mask[d_map_cols["STRASSE"]].str.split(delimiter, n=1, expand=True)
        d_map_cols["HSN"] = entities[1]

    """ Aufbau Adressschlüssel """
    df_mask["ORG_ADR"] = df_mask[d_map_cols["STRASSE"]].str.strip().str.lower() + df_mask[
        d_map_cols["PLZ"]].str.strip() + df_mask[d_map_cols["ORT"]].str.strip().str.lower()

    """ Merge auf Adresse """
    df_mask = pd.merge(df_mask, adr[["ORG_ADR", "ADR_ID", "MASK_STR", "MASK_HSN"]], how="left", on="ORG_ADR")

    """ Änderung Datentyp ADR_ID auf integer """
    df_mask.loc[df_mask["ADR_ID"].isnull(), "ADR_ID"] = -2
    df_mask["ADR_ID"] = df_mask["ADR_ID"].astype(int)

    """ Übernahme Pseudonymisierung für Sätze ohne Hausnummer """
    """ Wenn keine Information zum Schlüssel gefunden wird, nehme den Ersatzwert """
    df_mask.loc[df_mask["ADR_ID"] == -2, d_map_cols["STRASSE"]] = default_value_1
    df_mask.loc[df_mask["ADR_ID"] == -2, d_map_cols["HSN"]] = default_value_2

    """ Ansonsten nehme die Information aus adr """
    df_mask.loc[df_mask["ADR_ID"] == -1, d_map_cols["STRASSE"]] = df_mask["MASK_STR"]
    df_mask.loc[df_mask["ADR_ID"] == -1, d_map_cols["HSN"]] = df_mask["MASK_HSN"]

    """ Zusammenfassen der Attribute und Anhängen an df_done """
    if d_map_cols["HSN"] not in l_attributes:
        df_mask.loc[df_mask["ADR_ID"] < 0, d_map_cols["STRASSE"]] = df_mask[entities[0]] + delimiter + df_mask[
            entities[1]]
    rows = df_mask.loc[df_mask["ADR_ID"] < 0].copy()
    del rows["ORG_ADR"]
    del rows["ADR_ID"]
    del rows["MASK_STR"]
    del rows["MASK_HSN"]
    if d_map_cols["HSN"] not in l_attributes:
        del rows[entities[1]]
    df_done = pd.concat([df_done, rows], ignore_index=True)
    df_mask.drop(rows.index, inplace=True)
    del rows

    """ Übernahme Pseudonymisierung für Sätze mit Hausnummer """
    df_mask = pd.merge(df_mask.loc[df_mask["ADR_ID"] > -1], hsn, how="left",
                       left_on=["ORG_ADR", "ADR_ID", d_map_cols["HSN"]],
                       right_on=["ORG_ADR", "ADR_ID", "ORG_HSN"])

    """ Check for duplicates """
    if l_pk:
        df_dup = df_mask.loc[df_mask.duplicated(subset=l_pk, keep='last')]
    else:
        df_dup = df_mask.loc[df_mask.duplicated(keep='last')]
    if df_dup.shape[0] > 0:
        log.warning("Duplicates found. Keep first row and delete last.")
        df_mask.drop(df_dup.index, inplace=True)

    """ Wenn keine Information in hsn vorhanden ist, nehme die aus adr """
    df_mask.loc[df_mask["MASK_STR_y"].isna(), d_map_cols["STRASSE"]] = df_mask["MASK_STR_x"]
    df_mask.loc[df_mask["MASK_HSN_y"].isna(), d_map_cols["HSN"]] = df_mask["MASK_HSN_x"]
    """ Ansonsten nehme die Information aus hsn """
    df_mask.loc[df_mask["MASK_STR_y"].notnull(), d_map_cols["STRASSE"]] = df_mask["MASK_STR_y"]
    df_mask.loc[df_mask["MASK_STR_y"].notnull(), d_map_cols["HSN"]] = df_mask["MASK_HSN_y"]
    del df_mask["ORG_ADR"]
    del df_mask["ADR_ID"]
    del df_mask["ORG_HSN"]
    del df_mask["MASK_STR_x"]
    del df_mask["MASK_HSN_x"]
    del df_mask["MASK_STR_y"]
    del df_mask["MASK_HSN_y"]

    """ Zusammenfügen von Strasse und Hausnummer """
    if d_map_cols["HSN"] not in l_attributes:
        df_mask[d_map_cols["STRASSE"]] = df_mask[entities[0]] + delimiter + " " + df_mask[entities[1]]
        del df_mask[entities[1]]

    """ Die Dataframes zum DataFrame table_rows zusammenbauen """
    table_rows = pd.concat([df_done, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))

    """ Falls eine Kurzform der Straße definiert ist, setze Substring der Straße in Großbuchstaben ein. """
    if "STRASSE_KURZ" in d_map_cols:
        log.info("Maskiere zusätzliches Attribut {0}".format(d_map_cols["STRASSE_KURZ"]))
        table_rows[d_map_cols["STRASSE_KURZ"]] = table_rows[d_map_cols["STRASSE"]].str.upper().str.slice(0, max_length)

    """ Return dataframe """
    return table_rows


def rule_r13(ruleset, table_rows):
    """ Rule ID R13->Telefon-/Faxnummern """
    """ Replace digits as follows: 3 --> 2 6 --> 5 7 --> 5 8 --> 9 """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[(table_rows[src_col].isnull()) |
                                  (table_rows[src_col] == '')].copy().reset_index(drop=True)
    df_mask = table_rows.loc[(table_rows[src_col].notnull()) &
                             (table_rows[src_col] > '')].copy().reset_index(drop=True)
    del table_rows

    """ Check for rows to process """
    if df_mask.empty is True:
        log.info("No data found to mask.")
        return df_unchanged

    df_mask[src_col] = df_mask[src_col].astype(str)
    df_mask[src_col] = df_mask[src_col].str.replace('3', '2').str.replace(
        '6', '5').str.replace('7', '5').str.replace('8', '9')
    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r14(ruleset, table_rows, lkp_cache):
    """ Rule ID R14->E-Mail-Adresse
    """
    src_col = ruleset["ATTRIBUT"]
    lkp_col = ruleset["LKP_COLS"].split(",")[0]
    mask_col = ruleset["LKP_COLS"].split(",")[1]
    wort_art = ruleset["LKP_COLS"].split(",")[2]
    default_value = ruleset["DEFAULT_VALUE_1"]
    max_length = ruleset["COLUMN_LENGTH"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[(table_rows[src_col].isna()) |
                                  (table_rows[src_col].str.strip() == '')].copy().reset_index(drop=True)
    df_mask = table_rows.loc[(table_rows[src_col].notnull()) &
                             (table_rows[src_col].str.strip() > '')].copy().reset_index(drop=True)
    del table_rows

    """ Check for rows to process """
    if df_mask.empty is True:
        log.info("No data found to mask.")
        return df_unchanged

    """ Separate email from url content """
    df_email = df_mask.loc[df_mask[src_col].str.contains("@")].copy().reset_index(drop=True)
    df_url = df_mask.loc[~df_mask[src_col].str.contains("@")].copy().reset_index(drop=True)
    del df_mask

    """ Add column WORT_ORIG as first char of source attribute lowercase """
    df_email[lkp_col] = df_email[src_col].str.slice(0, 1).str.lower().str.rstrip()
    df_url[lkp_col] = df_url[src_col].str.slice(0, 1).str.lower().str.rstrip()

    """ Merge lookup rows to table dataframe """
    lkp_email = pd.merge(df_email, lkp_cache.loc[lkp_cache[wort_art] == 7], how="left", on=lkp_col)
    lkp_url = pd.merge(df_url, lkp_cache.loc[lkp_cache[wort_art] == 8], how="left", on=lkp_col)

    """ Replace original value with masked value """
    del df_email[lkp_col]
    del df_url[lkp_col]

    """ Fill empty row values from lookup with default value """
    if df_email.empty is False:
        df_email[src_col] = lkp_email[mask_col].fillna(default_value)
    if df_url.empty is False:
        df_url[src_col] = lkp_url[mask_col].fillna(default_value)

    """ Replace original dataframe with concatenated two dataframes """
    df_mask = pd.concat([df_email, df_url], ignore_index=True)

    """ Truncate masked column if length exceeds column length in database """
    log.info("Kürze Feld {0} auf Länge {1}".format(src_col, max_length))
    df_mask["TruncColumn"] = df_mask[src_col].str.slice(0, max_length)
    df_mask.loc[df_mask[src_col].str.len() > max_length, src_col] = df_mask["TruncColumn"]
    del df_mask["TruncColumn"]

    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))

    """ Return dataframe """
    return table_rows


def rule_r16(ruleset, table_rows):
    """ Rule ID R16->Löschen """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r17(ruleset, table_rows):
    """ Rule ID R17->Sozialversicherungsnummer """

    def calc_pz(soznr, l_factor):
        l_val = []
        for x, y in zip(soznr, l_factor):
            l_val.append(str(int(x) * y))
        q_sum = 0
        for i in l_val:
            q_sum = q_sum + sum(list(int(x) for x in i))
        return str(q_sum % 10)

    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    default_value_2 = ruleset["DEFAULT_VALUE_2"]
    default_value_3 = ruleset["DEFAULT_VALUE_3"]
    l_letters = list(chr(x) for x in range(ord('A'), ord('Z') + 1))
    l_pos = list(x + 1 for x in range(26))
    d_pos = dict(zip(l_letters, l_pos))
    log.debug("Regelsatz: {0}".format(ruleset))
    log.debug("Maskiertes Attribut: {0}".format(src_col))
    log.debug("Ersatzwert Geburtsdatum 1: {0}".format(default_value_1))
    log.debug("Ersatzwert Geburtsdatum 2: {0}".format(default_value_2))
    log.debug("Ersatzwert Geburtsname Initialie: {0}".format(default_value_3))
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    log.debug("Position Initial im Alphabet: {0}".format(d_pos))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[(table_rows[src_col].isnull()) |
                                  (table_rows[src_col].str.strip() == '') |
                                  (table_rows[src_col].str.strip().str.len() < 12)].copy().reset_index(drop=True)
    df_mask = table_rows.loc[table_rows[src_col].str.len() == 12].copy().reset_index(drop=True)

    df_mask["BEREICHSNR"] = df_mask[src_col].str[0:2]
    df_mask["TAG"] = df_mask[src_col].str[2:4]
    df_mask["MON_JAHR"] = df_mask[src_col].str[4:8]
    df_mask["INI_POS"] = str(d_pos[default_value_3])
    df_mask["SERIENNR"] = df_mask[src_col].str[9:11]
    df_mask.loc[(df_mask["TAG"] < default_value_2), ["TAG"]] = default_value_1
    df_mask.loc[(df_mask["TAG"] >= default_value_2), ["TAG"]] = default_value_2
    df_mask["CHECK_PZ"] = df_mask["BEREICHSNR"] + df_mask["TAG"] + df_mask[
        "MON_JAHR"] + df_mask["INI_POS"] + df_mask["SERIENNR"]

    """ Cleanse rows that does not contain digits only """
    rows = df_mask.loc[~df_mask["CHECK_PZ"].str.isdigit()].copy()
    del rows["BEREICHSNR"]
    del rows["TAG"]
    del rows["MON_JAHR"]
    del rows["INI_POS"]
    del rows["SERIENNR"]
    del rows["CHECK_PZ"]
    df_unchanged = pd.concat([df_unchanged, rows], ignore_index=True)
    df_mask.drop(rows.index, inplace=True)

    """ Calculate check digit """
    l_gewichtung = [2, 1, 2, 5, 7, 1, 2, 1, 2, 1, 2, 1]
    df_mask["PZ"] = df_mask["CHECK_PZ"].apply(lambda x: calc_pz(x, l_gewichtung))
    df_mask[src_col] = df_mask["CHECK_PZ"].str[:8] + default_value_3 + df_mask[src_col].str[9:11] + df_mask["PZ"]

    """ Remove transformation columns """
    del df_mask["BEREICHSNR"]
    del df_mask["TAG"]
    del df_mask["MON_JAHR"]
    del df_mask["INI_POS"]
    del df_mask["SERIENNR"]
    del df_mask["CHECK_PZ"]
    del df_mask["PZ"]
    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r18(ruleset, table_rows):
    """ Rule ID R18->Arbeitgeber """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value_1
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r19(ruleset, table_rows):
    """ Rule ID R19->Dokumenten-ID """
    """ Regel in PowerCenter Expression:
    IIF($$TMMaskingFlag=0,
    inDOK_ID,
    DECODE(TRUE
    ,  ISNULL(inDOK_ID) OR LTRIM(inDOK_ID)='', inDOK_ID
    , TO_INTEGER(inART_DOKUMENT) = 26, '53220257B045'
    , TO_INTEGER(inART_DOKUMENT) = 31, '1111111111'
    , TO_INTEGER(inART_DOKUMENT) = 32, '92435680114'
    , '11111')
    )
    """
    src_col = ruleset["ATTRIBUT"]
    lkp_id = ruleset["LKP_ID"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    if lkp_id == 26:
        default_value = '53220257B045'
    elif lkp_id == 31:
        default_value = '1111111111'
    elif lkp_id == 32:
        default_value = '92435680114'
    else:
        default_value = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r21(ruleset, table_rows):
    """ Rule ID R21->Grundstück """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r23(ruleset, table_rows):
    """ Rule ID R23->KFZ AKZ """
    src_col = ruleset["ATTRIBUT"]
    l_cols = table_rows.columns.values.tolist()
    num_orig = "NUM_ORIG" if "NUM_ORIG" not in l_cols else "NUM_ORIGX"
    num_ord = "NUM_ORD" if "NUM_ORD" not in l_cols else "NUM_ORDX"
    num_mask = "NUM_MASK" if "NUM_MASK" not in l_cols else "NUM_MASKX"
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[(table_rows[src_col].isna()) |
                                  (table_rows[src_col].str.strip() == '')].copy().reset_index(drop=True)
    df_mask = table_rows.loc[(table_rows[src_col].notnull()) &
                             (table_rows[src_col].str.strip() > '')].copy().reset_index(drop=True)
    del table_rows

    """ Check for rows to process """
    if df_mask.empty is True:
        log.info("No data found to mask.")
        return df_unchanged

    """ Cleanse data for number plates without numbers and numberplates without characters """
    rows = df_mask.loc[(~df_mask[src_col].str.contains("\\d", regex=True))].copy()
    df_unchanged = pd.concat([df_unchanged, rows], ignore_index=True)
    df_mask.drop(rows.index, inplace=True)
    del rows
    rows = df_mask.loc[(~df_mask[src_col].str.contains("[A-Za-z]", regex=True))].copy()
    df_unchanged = pd.concat([df_unchanged, rows], ignore_index=True)
    df_mask.drop(rows.index, inplace=True)
    del rows

    """ Mask data """
    df_mask[src_col] = df_mask[src_col].astype(str)
    df_mask[num_orig] = df_mask[src_col].str.extract("(\\d+)")
    df_mask[src_col] = [a.replace(b, '') for a, b in zip(df_mask[src_col], df_mask[num_orig])]
    df_mask[num_orig] = df_mask[num_orig].astype(int)
    df_mask[num_ord] = df_mask[src_col].str.slice(0, 1).apply(lambda x: ord(x))
    df_mask[num_mask] = 0
    df_mask.loc[(df_mask[num_orig] > 0) & (df_mask[num_orig] < 100), num_mask] = \
        (df_mask[num_orig] * 137 + df_mask[num_ord]) % 99 + 1
    df_mask.loc[(df_mask[num_orig] >= 100) & (df_mask[num_orig] < 1000), num_mask] = \
        (df_mask[num_orig] * 1117 + df_mask[num_ord]) % 900 + 100
    df_mask.loc[(df_mask[num_orig] >= 1000) & (df_mask[num_orig] < 10000), num_mask] = \
        (df_mask[num_orig] * 3367 + df_mask[num_ord]) % 9000 + 1000
    df_mask[num_mask] = df_mask[num_mask].astype(str)
    df_mask[src_col] = df_mask[src_col] + df_mask[num_mask]
    del df_mask[num_orig]
    del df_mask[num_ord]
    del df_mask[num_mask]
    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r24(ruleset, table_rows):

    """ R24->KFZ Fahrzeugidentifikationsnummer (FIN)
        WAAUE31090E072401 => WXXUXX1XX0XX7XX0X
    """
    def convert_fin(value):
        l_value = list(value)
        if len(l_value) < 3:
            l_value = ['X', 'X']
            return "".join(l_value)
        for i in range(len(l_value)):
            if i in (1, 2, 4, 5, 7, 8, 10, 11, 13, 14, 16):
                l_value[i] = default_value
        return "".join(l_value)
    src_col = ruleset["ATTRIBUT"]
    default_value = ruleset["DEFAULT_VALUE_1"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[(table_rows[src_col].isna()) |
                                  (table_rows[src_col].str.strip() == '')].copy().reset_index(drop=True)
    df_mask = table_rows.loc[(table_rows[src_col].notnull()) &
                             (table_rows[src_col].str.strip() > '')].copy().reset_index(drop=True)
    del table_rows

    """ Check for rows to process """
    if df_mask.empty is True:
        log.info("No data found to mask.")
        return df_unchanged

    """ Replace original character with default value every 2nd and 3rd position """
    df_mask[src_col] = df_mask[src_col].apply(convert_fin)

    """ Return dataframe """
    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    return table_rows


def rule_r35(ruleset, table_rows):
    """ Rule ID R35->Freitextfeld """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value_1
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r36(ruleset, table_rows, lkp_cache):
    """ Rule ID R36->Risikoschätzung """
    """ [Output] = [Minwert] + ( [Input] - [Minwert] + [Cäsarschlüssel] ) modulo ([Maxwert] - [Minwert]+1) """
    src_col = ruleset["ATTRIBUT"]
    lkp_col = ruleset["LKP_COLS"].split(",")[0]
    mask_col = ruleset["LKP_COLS"].split(",")[1]
    default_value = ruleset["DEFAULT_VALUE_1"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[table_rows[src_col].isna()].copy().reset_index(drop=True)
    df_mask = table_rows.loc[table_rows[src_col].notnull()].copy().reset_index(drop=True)
    del table_rows

    """ Merge lookup rows to table dataframe """
    lkp_rows = pd.merge(df_mask, lkp_cache, how="left", on=lkp_col)

    """ Fill empty row values from lookup with default value """
    df_mask[src_col] = lkp_rows[mask_col].fillna(default_value)

    """ Replace original dataframe with concatenated two dataframes """
    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r37(ruleset, table_rows):
    """ R37->Einschluss - / Ausschlussklausel """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[table_rows[src_col].notnull(), src_col] = default_value
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r41(ruleset, table_rows):
    """ R41->Infofeld """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value_1
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r46(ruleset, table_rows, lkp_cache):
    """ Rule ID R46->Ersetzen Nachname, Vorname """

    def mask_column(df, column, default):
        """ Add column WORT_ORIG as 10 first chars of string lowercase """
        df[lkp_col] = df[column].str.slice(0, 10).str.lower().str.rstrip()
        """ Merge lookup rows to table dataframe """
        lkp_rows = pd.merge(df, df_lkp, how="left", on=lkp_col)
        """ Replace original value with masked value """
        del df[lkp_col]
        """ Fill empty row values from lookup with default value """
        df[column] = lkp_rows[mask_col].fillna(default)
        """ Return dataframe """
        return df

    src_col = ruleset["ATTRIBUT"]
    lkp_col = ruleset["LKP_COLS"].split(",")[1]
    mask_col = ruleset["LKP_COLS"].split(",")[2]
    format_string = ruleset["FORMAT_STRING"]
    defaults = ruleset["DEFAULT_VALUE_1"].split(",")
    entities = []
    delimiter = " "

    """ Check auf Formatanweisung """
    if format_string is not None:
        entities = format_string.split("|")[0].split(",")
        delimiter = format_string.split("|")[1]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[(table_rows[src_col].isna()) |
                                  (table_rows[src_col].str.strip() == '')].copy().reset_index(drop=True)
    df_mask = table_rows.loc[(table_rows[src_col].notnull()) &
                             (table_rows[src_col].str.strip() > '')].copy().reset_index(drop=True)
    del table_rows

    """ Check for rows to process """
    if df_mask.empty is True:
        log.info("No data found to mask.")
        return df_unchanged

    """ Replace comma with blank in rows with different delimiters """
    df_mask.loc[df_mask[src_col].str.contains(",", regex=False), src_col] = df_mask[src_col].str.replace(",", " ")

    """ Convert comparison column to lowercase """
    lkp_cache[lkp_col] = lkp_cache[lkp_col].str.lower()

    """ Split name into forename and name """
    df_mask[entities] = df_mask[src_col].str.split(pat=delimiter, n=1, expand=True)
    for col, default_value in zip(entities, defaults):
        if col == "NACHNAME":
            df_lkp = lkp_cache.loc[lkp_cache["WORT_ART"].isin([1, 3])].sort_values(by="WORT_ART")
            df_lkp = df_lkp.drop_duplicates(subset=["WORT_ORIG", "WORT_MASK"], keep="first")
        elif col == "VORNAME":
            df_lkp = lkp_cache.loc[lkp_cache["WORT_ART"] == 5]
        df_mask = mask_column(df_mask, col, default_value)
    df_mask[src_col] = df_mask[entities[0]] + delimiter + df_mask[entities[1]]

    """ Update empty values in masked dataframe """
    df_mask.loc[df_mask[src_col].str.strip() == "", src_col] = ruleset["DEFAULT_VALUE_1"]
    for x in entities:
        del df_mask[x]

    """ Replace original dataframe with concatenated two dataframes """
    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r47(ruleset, table_rows):
    """ R47->Aktenkennzeichen """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value_1
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r49(ruleset, table_rows):
    """ R49->versicherte Person """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value_1
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r50(ruleset, table_rows):
    """ R50->Ersetzen Adresse """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value_1
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r55(ruleset, table_rows):
    """ R55->Name des Meldenden """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value_1
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r56(ruleset, table_rows):
    """ R56->Ersetzen durch Leerzeichen """
    src_col = ruleset["ATTRIBUT"]
    default_value = ruleset["DEFAULT_VALUE_1"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[(table_rows[src_col].isna()) |
                                  (table_rows[src_col].str.strip() == '')].copy().reset_index(drop=True)
    df_mask = table_rows.loc[(table_rows[src_col].notnull()) &
                             (table_rows[src_col].str.strip() > '')].copy().reset_index(drop=True)
    del table_rows

    """ Check for rows to process """
    if df_mask.empty is True:
        log.info("No data found to mask.")
        return df_unchanged

    """ Truncate string after the first occurrence of blank """
    df_mask["X_SHORT"] = df_mask[src_col].str.split(" ").str.get(0) + default_value
    df_mask[src_col] = df_mask["X_SHORT"]
    del df_mask["X_SHORT"]

    """ Return dataframe """
    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    return table_rows


def rule_r57(ruleset, table_rows):
    """ R57->Kurzbezeichnung des Informanten """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value_1
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r58(ruleset, table_rows):
    """ R58->Tagebuchnummer """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value_1
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r59(ruleset, table_rows):
    """ R59->Bezeichnung der Aufgabe """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value_1
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r60(ruleset, table_rows):
    """ R60->Partnerbetrieb """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value_1
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r61(ruleset, table_rows):
    """ R61->Ansprechpartner GIS """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value_1
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r62(ruleset, table_rows):
    """ R62->Name Migrierter Zahlungsempfänger """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value_1
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r63(ruleset, table_rows):
    """ R63->Adresse Migrierter Zahlungsempfänger """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value_1
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r64(ruleset, table_rows):
    """ R64->Aktenzeichen der Klage bei Gericht """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    default_value_1 = ruleset["DEFAULT_VALUE_1"]
    """ Set fixed value for all values in column that are not null """
    table_rows.loc[(table_rows[src_col].notnull()) & (table_rows[src_col].str.strip() > ''), src_col] = default_value_1
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r65(ruleset, table_rows):
    """ R65->Bezeichnung des Deckungsbedenkens
        Wenn der Inhalt des Feldes ICLREF.REFERRAL = " " ist, wird der Inhalt des Feldes ICLREF.TEXT gelöscht,
        in allen anderen Fällen bleibt der Inhalt des Feldes unverändert erhalten.
    """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    table_rows.loc[table_rows["REFERRAL"] == " ", src_col] = np.nan

    """ Return dataframe """
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    return table_rows


def rule_r69(ruleset, table_rows):
    """ R69->Erkrankung """
    src_col = ruleset["ATTRIBUT"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    df_unchanged = table_rows.loc[(table_rows[src_col].isnull()) |
                                  (table_rows[src_col] == 0) |
                                  (table_rows[src_col] == -9999) |
                                  (table_rows[src_col] == 15000)].copy().reset_index(drop=True)
    df_mask = table_rows.loc[(table_rows[src_col].notnull()) &
                             (table_rows[src_col] != 0) &
                             (table_rows[src_col] != -9999) &
                             (table_rows[src_col] != 15000)].copy().reset_index(drop=True)
    del table_rows
    df_mask.loc[df_mask[src_col] % 3 == 0, src_col] = 15002
    df_mask.loc[df_mask[src_col] % 3 == 1, src_col] = 15012
    df_mask.loc[df_mask[src_col] % 3 == 2, src_col] = 15017
    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r82(ruleset, table_rows):
    """ R82 Process Parameters """
    src_col = ruleset["ATTRIBUT"]
    lkp_col = ruleset["LKP_COLS"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))

    """ Split dataframes into two copies of data. One stays unchanged and the other has to be masked """
    df_unchanged = table_rows.loc[
        ~table_rows[lkp_col].isin(['uv.ba.pr', 'uv.ja.pr', 'uv.jas.pr'])].copy().reset_index(drop=True)
    df_mask = table_rows.loc[
        table_rows[lkp_col].isin(['uv.ba.pr', 'uv.ja.pr', 'uv.jas.pr'])].copy().reset_index(drop=True)
    del table_rows

    """ When df_mask is empty, return unchanged rows """
    if df_mask.shape[0] == 0:
        return df_unchanged

    """ Extract separate fields from process parameter """
    df_mask["ORIG_NAME"] = df_mask[src_col].str.split(";").str.get(2)
    df_mask["LIST_ORIG"] = df_mask[src_col].str.split(";")
    df_mask["LEFT_PART"] = df_mask["LIST_ORIG"].str[0:2]
    df_mask["LEFT_PART"] = df_mask["LEFT_PART"].apply(lambda x: ";".join(x))
    df_mask["RIGHT_PART"] = df_mask["LIST_ORIG"].str[3:]
    df_mask["RIGHT_PART"] = df_mask["RIGHT_PART"].apply(lambda x: ";".join(x))

    """ Assign number of names in process parameters to new column """
    df_mask["NUM_NAMES"] = df_mask.loc[df_mask["ORIG_NAME"] != "", "ORIG_NAME"].str.split("|").str.len()

    """ Assign 0 to NULL values """
    df_mask.loc[df_mask["NUM_NAMES"].isnull(), "NUM_NAMES"] = 0

    """ Set datatype to integer """
    df_mask["NUM_NAMES"] = df_mask["NUM_NAMES"].astype(int)

    """ Mask names with string 'Anonym' according to number of names """
    df_mask["MASK_NAME"] = df_mask["NUM_NAMES"].apply(lambda x: "|".join(list("Anonym" for i in range(x))))

    """ Replace original name with masked name in src_col """
    df_mask[src_col] = df_mask["LEFT_PART"] + ";" + df_mask["MASK_NAME"] + ";" + df_mask["RIGHT_PART"]

    """ Remove temporary columns """
    del df_mask["ORIG_NAME"]
    del df_mask["LIST_ORIG"]
    del df_mask["LEFT_PART"]
    del df_mask["RIGHT_PART"]
    del df_mask["NUM_NAMES"]
    del df_mask["MASK_NAME"]

    """ Return dataframe """
    table_rows = pd.concat([df_unchanged, df_mask], ignore_index=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows


def rule_r83(ruleset, table_rows):
    """ Rule ID R83->Highdate """
    src_col = ruleset["ATTRIBUT"]
    datetime_format = ruleset["FORMAT_STRING"]
    default_value = ruleset["DEFAULT_VALUE_1"]
    log.debug("Input DataFrame: {0}".format(table_rows.shape))
    """ Set fixed value for all values in column that are not null """
    table_rows[src_col] = pd.to_datetime(table_rows[src_col], errors="coerce")
    table_rows[src_col] = table_rows[src_col].dt.strftime("{0}".format(datetime_format))
    table_rows[src_col].fillna(value='{0}'.format(default_value), inplace=True)
    log.debug("Output DataFrame: {0}".format(table_rows.shape))
    """ Return dataframe """
    return table_rows
