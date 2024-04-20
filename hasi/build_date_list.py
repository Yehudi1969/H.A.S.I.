# -*- coding: utf8 -*-
###############################################################################
#                              build_date_list.py                             #
###############################################################################
# Copyright (C) 2023  Jens Janzen
# Initial release: 2019-11-01

# Change history

# Description:
# Library contains helper functions for date time generation

# Shell access through subprocess module
import argparse
import calendar
import datetime
import logging
import sys
import textwrap

# Functions
d_isoweekday_text_d = {
    1: "Montag",
    2: "Dienstag",
    3: "Mittwoch",
    4: "Donnerstag",
    5: "Freitag",
    6: "Samstag",
    7: "Sonntag"
}

d_isoweekday_text_e = {
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
    7: "Sunday"
}

d_isoweekday_short_text_d = {
    1: "Mo",
    2: "Di",
    3: "Mi",
    4: "Do",
    5: "Fr",
    6: "Sa",
    7: "So"
}

d_isoweekday_short_text_e = {
    1: "Mon",
    2: "Tue",
    3: "Wed",
    4: "Thu",
    5: "Fri",
    6: "Sat",
    7: "Sun"
}

d_month_text_d = {
    "01": "Januar",
    "02": "Februar",
    "03": "März",
    "04": "April",
    "05": "Mai",
    "06": "Juni",
    "07": "Juli",
    "08": "August",
    "09": "September",
    "10": "Oktober",
    "11": "November",
    "12": "Dezember"
}

d_month_text_e = {
    "01": "January",
    "02": "February",
    "03": "March",
    "04": "April",
    "05": "Mai",
    "06": "June",
    "07": "Juli",
    "08": "August",
    "09": "September",
    "10": "October",
    "11": "November",
    "12": "December"
}

d_month_short_text_d = {
    "01": "Jan",
    "02": "Feb",
    "03": "Mrz",
    "04": "Apr",
    "05": "Mai",
    "06": "Jun",
    "07": "Jul",
    "08": "Aug",
    "09": "Sep",
    "10": "Okt",
    "11": "Nov",
    "12": "Dez"
}

d_month_short_text_e = {
    "01": "Jan",
    "02": "Feb",
    "03": "Mar",
    "04": "Apr",
    "05": "Mai",
    "06": "Jun",
    "07": "Jul",
    "08": "Aug",
    "09": "Sep",
    "10": "Oct",
    "11": "Nov",
    "12": "Dec"
}

d_land_txt = {
    "BW": "Baden-Württemberg",
    "BY": "Bayern",
    "BE": "Berlin",
    "BB": "Brandenburg",
    "HB": "Bremen",
    "HH": "Hamburg",
    "HE": "Hessen",
    "MV": "Mecklenburg-Vorpommern",
    "NI": "Niedersachsen",
    "NW": "Nordrhein-Westfalen",
    "RP": "Rheinland-Pfalz",
    "SL": "Saarland",
    "SN": "Sachsen",
    "ST": "Sachsen-Anhalt",
    "SH": "Schleswig-Holstein",
    "TH": "Thüringen",
    "DE": "Deutschland"
}


def f_get_easter(v_year):
    """ https://tondering.dk/claus/cal/easter.php """
    g = v_year % 19
    c = v_year / 100
    h = (c - int(c / 4) - int(((8 * c) + 13) / 25) + (19 * g) + 15) % 30
    i = h - int(h / 28) * (1 - int(29 / (h + 1)) * int((21 - g) / 11))
    j = (v_year + int(v_year / 4) + i + 2 - c + int(c / 4)) % 7
    v_l = i - j
    easter_month = 3 + int((v_l + 40) / 44)
    easter_day = int(v_l + 28 - (31 * int(easter_month / 4)))
    v_easter = "{0}-{1:02d}-{2:02d}".format(v_year, easter_month, easter_day)
    return v_easter


def check_celebday(v_date):
    """ Check auf bundeseinheitliche bewegliche, sowie feste Feiertage.
    Gibt True zurück, wenn es sich um einen Feiertag handelt.
    In der DEVK zählen Heiligabend und Sylvester als Feiertage. """
    easter = datetime.datetime.strptime(f_get_easter(v_date.year), '%Y-%m-%d').date()
    log.debug("Ostern am: {0}".format(easter))
    log.debug("Wochentag: {0}".format(v_date.isoweekday()))
    if v_date == easter:
        return True
    elif v_date == easter - datetime.timedelta(2):
        return True
    elif v_date == easter + datetime.timedelta(1):
        return True
    elif v_date == easter + datetime.timedelta(39):
        return True
    elif v_date == easter + datetime.timedelta(50):
        return True
    elif v_date.day == 1 and v_date.month == 1:
        return True
    elif v_date.day == 1 and v_date.month == 5:
        return True
    elif v_date.day == 3 and v_date.month == 10:
        return True
    elif v_date.day == 24 and v_date.month == 12:
        return True
    elif v_date.day == 25 and v_date.month == 12:
        return True
    elif v_date.day == 26 and v_date.month == 12:
        return True
    elif v_date.day == 31 and v_date.month == 12:
        return True
    else:
        return False


def fill_obj_date(v_date):
    now = datetime.datetime.now()
    iso_weekday = v_date.isoweekday()
    day = "{0:02d}".format(v_date.day)
    day_text_d = d_isoweekday_text_d[iso_weekday]
    day_text_e = d_isoweekday_text_e[iso_weekday]
    flg_celeb_day = 0
    celebday_text = ""
    land = None
    week = "{0:02d}".format(v_date.isocalendar()[1])
    month = "{0:02d}".format(v_date.month)
    year = v_date.year
    quarter = 0
    if int(month) < 4:
        quarter = 1
    elif 3 < int(month) < 7:
        quarter = 2
    elif 6 < int(month) < 10:
        quarter = 3
    elif int(month) > 9:
        quarter = 4
    d_fiscal = v_date
    m_fiscal = "{0:02d}".format(d_fiscal.month)
    y_fiscal = d_fiscal.year
    easter = datetime.datetime.strptime(f_get_easter(year), '%Y-%m-%d').date()
    # Moving celebration days
    if v_date == easter:
        celebday_text = "Ostersonntag"
        flg_celeb_day = 1
        land = ["DE"]
    elif v_date == easter - datetime.timedelta(48):
        celebday_text = "Rosenmontag"
        flg_celeb_day = 0
        land = ["DE"]
    elif v_date == easter - datetime.timedelta(47):
        celebday_text = "Fastnacht"
        flg_celeb_day = 0
        land = ["DE"]
    elif v_date == easter - datetime.timedelta(46):
        celebday_text = "Aschermittwoch"
        flg_celeb_day = 0
        land = ["DE"]
    elif v_date == easter - datetime.timedelta(2):
        celebday_text = "Karfreitag"
        flg_celeb_day = 1
        land = ["DE"]
    elif v_date == easter + datetime.timedelta(1):
        celebday_text = "Ostermontag"
        flg_celeb_day = 1
        land = ["DE"]
    elif v_date == easter + datetime.timedelta(39):
        celebday_text = "Christi Himmelfahrt"
        flg_celeb_day = 1
        land = ["DE"]
    elif v_date == easter + datetime.timedelta(49):
        celebday_text = "Pfingstsonntag"
        flg_celeb_day = 2
        land = ["BB", "HE"]
    elif v_date == easter + datetime.timedelta(50):
        celebday_text = "Pfingstmontag"
        flg_celeb_day = 1
        land = ["DE"]
    elif v_date == easter + datetime.timedelta(60):
        celebday_text = "Fronleichnam"
        flg_celeb_day = 2
        land = ["BW", "BY", "HE", "NW", "RP", "SL", "SN", "TH"]
    # Fixed celebration days
    elif day + month == "0101":
        celebday_text = "Neujahr"
        flg_celeb_day = 1
        land = ["DE"]
    elif day + month == "0601":
        celebday_text = "Epiphanias"
        flg_celeb_day = 2
        land = ["BW", "BY", "ST"]
    elif day + month == "0803" and year >= 2019:
        celebday_text = "Weltfrauentag"
        flg_celeb_day = 2
        land = ["BE"]
    elif day + month == "0105":
        celebday_text = "Maifeiertag"
        flg_celeb_day = 1
        land = ["DE"]
    elif day + month == "0805" and year == 2020:
        celebday_text = "Kapitulation der Wehrmacht und Kriegsende"
        flg_celeb_day = 2
        land = ["BE"]
    elif day + month == "1508":
        celebday_text = "Mariä Himmelfahrt"
        flg_celeb_day = 2
        land = ["BY", "SL"]
    elif day + month == "2009" and year >= 2019:
        celebday_text = "Internationaler Kindertag"
        flg_celeb_day = 2
        land = ["TH"]
    elif day + month == "0310":
        celebday_text = "Tag der deutschen Einheit"
        if year < 1990:
            flg_celeb_day = 0
            land = None
            celebday_text = None
        else:
            flg_celeb_day = 1
            land = ["DE"]
    elif day + month == "3110":
        celebday_text = "Reformationstag"
        flg_celeb_day = 2
        land = ["BB", "MV", "SN", "ST", "TH"]
        if year == 2017:
            flg_celeb_day = 1
            land = ["DE"]
        elif year >= 2018:
            land.extend(["HB", "HH", "NI", "SH"])
    elif day + month == "0111":
        celebday_text = "Allerheiligen"
        flg_celeb_day = 2
        land = ["BW", "BY", "NW", "RP", "SL"]
    elif iso_weekday == 3 and 1115 < v_date.month * 100 + v_date.day < 1123:
        celebday_text = "Buß- und Bettag"
        if year < 1934:
            flg_celeb_day = 0
            land = None
            celebday_text = None
        elif 1934 <= year < 1939:
            flg_celeb_day = 1
            land = ["DE"]
        elif 1939 <= year < 1945:
            flg_celeb_day = 0
            land = None
            celebday_text = None
        elif 1945 <= year < 1952:
            flg_celeb_day = 2
            land = ["BW", "BE", "HB", "HH", "HE", "NI", "NW", "RP", "SL", "SH"]
        elif 1952 <= year < 1990:
            flg_celeb_day = 2
            land = ["BW", "BY", "BE", "HB", "HH", "HE", "NI", "NW", "RP", "SL", "SH"]
        elif 1990 <= year < 1995:
            flg_celeb_day = 1
            land = ["DE"]
        elif year >= 1995:
            flg_celeb_day = 2
            land = ["SN"]
    elif day + month == "2512":
        celebday_text = "Erster Weihnachtsfeiertag"
        flg_celeb_day = 1
        land = ["DE"]
    elif day + month == "2612":
        celebday_text = "Zweiter Weihnachtsfeiertag"
        flg_celeb_day = 1
        land = ["DE"]
    # Nicht einheitliche Feiertage erst ab 1949 eintragen
    if flg_celeb_day == 2 and year < 1949:
        flg_celeb_day = 0
        celebday_text = ""
        land = None
    land_text = ", ".join([d_land_txt[x] for x in land]) if land else None
    region = ", ".join(land) if land else ''
    region_text = land_text if land_text else ''
    l_fields = [v_date.strftime("%Y-%m-%d"), day, day_text_d, day_text_e, flg_celeb_day, celebday_text,
                region, region_text, iso_weekday, week, month, d_month_text_d[month], d_month_text_e[month],
                quarter, year, m_fiscal, y_fiscal, now.strftime("%Y-%m-%d %H:%M:%S")]
    return l_fields


def fill_dim_datum(v_date, v_arbeitstag_nr, v_datenstand):
    id_datum = v_date.strftime("%Y%m%d")
    datum = v_date.strftime("%Y-%m-%d")
    id_jahr = v_date.year
    heiligabend = "{0}-12-24".format(v_date.year)
    sylvester = "{0}-12-31".format(v_date.year)
    id_halbjahr = "{0}1".format(v_date.year) if 0 < v_date.month <= 6 else "{0}2".format(v_date.year)
    if v_date.month < 4:
        id_quartal = "{0}1".format(v_date.year)
    elif 3 < v_date.month < 7:
        id_quartal = "{0}2".format(v_date.year)
    elif 6 < v_date.month < 10:
        id_quartal = "{0}3".format(v_date.year)
    elif v_date.month > 9:
        id_quartal = "{0}4".format(v_date.year)
    id_monat = "{0}{1:02d}".format(v_date.year, v_date.month)
    id_woche = "{0}{1:02d}".format(v_date.isocalendar()[0], v_date.isocalendar()[1])
    tag_kurz = d_isoweekday_short_text_d[v_date.isoweekday()].upper()
    tag = v_date.day
    tag_lang = d_isoweekday_text_d[v_date.isoweekday()]
    feiertag = "J" if check_celebday(v_date) else "N"
    werktag = 'J' if feiertag == 'N' and v_date.isoweekday() < 7 else 'N'
    if werktag == 'J' and v_date.isoweekday() != 6 and datum != heiligabend and datum != sylvester:
        # Arbeitstage hochzählen nur mo-fr, wenn kein Feiertag und auch kein Heiligabend oder Sylvester
        v_arbeitstag_nr += 1
    monatsultimo = "J" if v_date.day == calendar.monthrange(v_date.year, v_date.month)[1] else "N"
    quartalsultimo = "J" if v_date.month in (3, 6, 9, 12) and v_date.day == calendar.monthrange(
        v_date.year, v_date.month)[1] else "N"
    id_datum_jahresletzter = "{0}1231".format(v_date.year)
    id_datum_halbjahresletzter = "{0}0630".format(v_date.year) if 0 < v_date.month <= 6 \
        else "{0}1231".format(v_date.year)
    if v_date.month < 4:
        id_datum_quartalsletzter = "{0}0331".format(v_date.year)
    elif 3 < v_date.month < 7:
        id_datum_quartalsletzter = "{0}0630".format(v_date.year)
    elif 6 < v_date.month < 10:
        id_datum_quartalsletzter = "{0}0930".format(v_date.year)
    elif v_date.month > 9:
        id_datum_quartalsletzter = "{0}1231".format(v_date.year)
    id_datum_monatsletzter = "{0}{1:02d}{2}".format(v_date.year, v_date.month,
                                                    calendar.monthrange(v_date.year, v_date.month)[1])
    days_till_sunday = 7 - v_date.isoweekday()
    id_datum_wochenletzter = (v_date + datetime.timedelta(days=days_till_sunday)).strftime("%Y%m%d")
    sortierung_lfd = id_datum
    sequenz = v_date - v_datenstand
    l_fields = [id_datum, datum, id_jahr, id_halbjahr, id_quartal, id_monat, id_woche, tag_kurz, tag,
                tag_lang, feiertag, werktag, v_arbeitstag_nr, monatsultimo, quartalsultimo,
                id_datum_jahresletzter, id_datum_halbjahresletzter, id_datum_quartalsletzter,
                id_datum_monatsletzter, id_datum_wochenletzter, sortierung_lfd, sequenz.days - 1]
    return l_fields, v_arbeitstag_nr


def fill_dim_halbjahr(v_date, v_datenstand):
    i_halbjahr = 1 if 0 < v_date.month <= 6 else 2
    i_halbjahr_datenstand = 1 if 0 < v_datenstand.month <= 6 else 2
    id_halbjahr = "{0}{1}".format(v_date.year, i_halbjahr)
    id_jahr = v_date.year
    halbjahr_kurz = "H{0} {1}".format(i_halbjahr, v_date.year)
    halbjahr = id_halbjahr
    halbjahr_lang = "{0}. Halbjahr {1}".format(i_halbjahr, v_date.year)
    sortierung_lfd = id_halbjahr
    sequenz = (v_date.year - v_datenstand.year) * 2 + (i_halbjahr - i_halbjahr_datenstand)
    l_fields = [id_halbjahr, id_jahr, halbjahr_kurz, halbjahr, halbjahr_lang, sortierung_lfd, sequenz]
    return l_fields


def fill_dim_jahr(v_date, v_datenstand):
    id_jahr = v_date.year
    jahr = v_date.year
    sortierung_lfd = v_date.year
    sequenz = v_date.year - v_datenstand.year
    l_fields = [id_jahr, jahr, sortierung_lfd, sequenz]
    return l_fields


def fill_dim_monat(v_date, v_datenstand):
    mon = "{0:02d}".format(v_date.month)
    id_monat = "{0}{1}".format(v_date.year, mon)
    if v_date.month < 4:
        id_quartal = "{0}1".format(v_date.year)
    elif 3 < v_date.month < 7:
        id_quartal = "{0}2".format(v_date.year)
    elif 6 < v_date.month < 10:
        id_quartal = "{0}3".format(v_date.year)
    elif v_date.month > 9:
        id_quartal = "{0}4".format(v_date.year)
    monat_kurz = "{0} {1}".format(d_month_short_text_d[mon], v_date.year)
    monat = v_date.month
    monat_lang = "{0} {1}".format(d_month_text_d[mon], v_date.year)
    sortierung_lfd = id_monat
    sequenz = (v_date.year - v_datenstand.year) * 12 + (v_date.month - v_datenstand.month)
    l_fields = [id_monat, id_quartal, monat_kurz, monat, monat_lang, sortierung_lfd, sequenz]
    return l_fields


def fill_dim_quartal(v_date, v_datenstand):
    if v_date.month < 4:
        i_quartal = 1
    elif 3 < v_date.month < 7:
        i_quartal = 2
    elif 6 < v_date.month < 10:
        i_quartal = 3
    elif v_date.month > 9:
        i_quartal = 4
    if v_datenstand.month < 4:
        i_quartal_datenstand = 1
    elif 3 < v_datenstand.month < 7:
        i_quartal_datenstand = 2
    elif 6 < v_datenstand.month < 10:
        i_quartal_datenstand = 3
    elif v_datenstand.month > 9:
        i_quartal_datenstand = 4
    id_quartal = "{0}{1}".format(v_date.year, i_quartal)
    i_halbjahr = "1" if 0 < v_date.month <= 6 else "2"
    id_halbjahr = "{0}{1}".format(v_date.year, i_halbjahr)
    quartal_kurz = "Q{0} {1}".format(i_quartal, v_date.year)
    quartal = id_quartal
    quartal_lang = "{0}. Quartal {1}".format(i_quartal, v_date.year)
    sortierung_lfd = id_quartal
    sequenz = (v_date.year - v_datenstand.year) * 4 + (i_quartal - i_quartal_datenstand)
    l_fields = [id_quartal, id_halbjahr, quartal_kurz, quartal, quartal_lang, sortierung_lfd, sequenz]
    return l_fields


def fill_dim_woche(v_date, v_datenstand):
    i_woche = "{0}".format(v_date.isocalendar()[1])
    id_woche = "{0}{1:02d}".format(v_date.isocalendar()[0], v_date.isocalendar()[1])
    woche_kurz = "KW{0:02d} {1}".format(v_date.isocalendar()[1], v_date.isocalendar()[0])
    woche = i_woche
    woche_lang = "Kalenderwoche {0:02d} {1}".format(v_date.isocalendar()[1], v_date.isocalendar()[0])
    sortierung_lfd = id_woche
    sequenz = v_date - v_datenstand
    l_fields = [id_woche, woche_kurz, woche, woche_lang, sortierung_lfd, int(sequenz.days / 7)]
    return l_fields


# Logging
log = logging.getLogger("build_date_list")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

# *******************
# ***** M A I N *****
# *******************
if __name__ == "__main__":
    # Evaluate arguments
    desc = """Datumsgenerator:
    """
    options = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                      description=textwrap.dedent(desc))
    options.add_argument("-b", "--begin", dest="begin", help="Erstes Datum im Format YYYYMMDD", type=int, required=True)
    options.add_argument("-e", "--end", dest="end", help="Letztes Datum im Format YYYYMMDD", type=int, required=True)
    options.add_argument("-ds", "--datenstand", dest="datenstand",
                         help="Datum des letzten Ladelaufs im Format YYYYMMDD", type=int, default=None)
    options.add_argument("-d", "--delimiter", dest="delimiter", help="Trennzeichen, default=|", type=str, default="|")
    options.add_argument("-f", "--filename", dest="filename", help="Output filename", required=True)
    args = options.parse_args()
    d_begin = datetime.datetime.strptime(str(args.begin), '%Y%m%d').date()
    d_end = datetime.datetime.strptime(str(args.end), '%Y%m%d').date()
    if not args.datenstand:
        d_datenstand = datetime.datetime.now()
    else:
        d_datenstand = datetime.datetime.strptime(str(args.datenstand), '%Y%m%d').date()
    delimiter = args.delimiter
    filename = args.filename
    log.debug("Date Begin: {0}:".format(d_begin))
    log.debug("Date End: {0}:".format(d_end))
    log.debug("Datenstand: {0}:".format(d_datenstand))
    log.debug(("Delimiter: {0}".format(delimiter)))
    log.debug("Filename: {0}".format(filename))
    x_date = d_begin  # Variable für Datumsobjekt
    arbeitstag_nr = 0  # Globaler Zähler für den Arbeitstag
    l_obj_date = ['D_DATE', 'S_DAY', 'DAY_TEXT_D', 'DAY_TEXT_E', 'FLG_CELEBDAY', 'CELEBDAY_TEXT', 'REGION',
                  'REGION_DESC', 'ISO_WEEKDAY', 'S_WEEK', 'S_MONTH', 'MONTH_TEXT_D', 'MONTH_TEXT_E', 'S_QUARTER',
                  'S_YEAR', 'S_FISCAL_MONTH', 'S_FISCAL_YEAR', 'TS_INS']
    l_hd_tag = ['ID_DATUM', 'DATUM', 'JAHR', 'ID_HALBJAHR', 'ID_QUARTAL', 'ID_MONAT', 'ID_WOCHE',
                'TAG_KURZ', 'TAG', 'TAG_LANG', 'FEIERTAG', 'WERKTAG', 'ARBEITSTAG_NR', 'MONATSULTIMO',
                'QUARTALSULTIMO', 'ID_DATUM_JAHRESLETZTER', 'ID_DATUM_HALBJAHRESLETZTER',
                'ID_DATUM_QUARTALSLETZTER', 'ID_DATUM_MONATSLETZTER', 'ID_DATUM_WOCHENLETZTER',
                'SORTIERUNG_LFD', 'SEQUENZ']
    l_hd_halbjahr = ['ID_HALBJAHR', 'ID_JAHR', 'HALBJAHR_KURZ', 'HALBJAHR', 'HALBJAHR_LANG',
                     'SORTIERUNG_LFD', 'SEQUENZ']
    l_hd_jahr = ['ID_JAHR', 'JAHR', 'SORTIERUNG_LFD', 'SEQUENZ']
    l_hd_monat = ['ID_MONAT', 'ID_QUARTAL', 'MONAT_KURZ', 'MONAT', 'MONAT_LANG', 'SORTIERUNG_LFD', 'SEQUENZ']
    l_hd_quartal = ['ID_QUARTAL', 'ID_HALBJAHR', 'QUARTAL_KURZ', 'QUARTAL', 'QUARTAL_LANG', 'SORTIERUNG_LFD', 'SEQUENZ']
    l_hd_woche = ['ID_WOCHE', 'WOCHE_KURZ', 'WOCHE', 'WOCHE_LANG', 'SORTIERUNG_LFD', 'SEQUENZ']
    l_filenames = ['lu_datum_org.csv', 'lu_halbjahr_org.csv', 'lu_jahr_org.csv', 'lu_monat_org.csv',
                   'lu_quartal_org.csv', 'lu_woche_org.csv', 'obj_date.csv']

    with open(filename, "w") as fh:
        if filename == "lu_datum_org.csv":
            headline = delimiter.join(list(str(x).upper() for x in l_hd_tag))
            l_ersatzwerte = ['-1', '9999-12-31 23:59:59', '0', '-1', '-1', '-1', '-1', 'n.r.',
                             '0', 'n.r.', '-', '-', '0', '-', '-', '-1', '-1', '-1', '-1', '-1', '9999999999', '999999']
            add_column = delimiter.join(list(str(x).upper() for x in l_ersatzwerte))
        elif filename == "lu_halbjahr_org.csv":
            headline = delimiter.join(list(str(x).upper() for x in l_hd_halbjahr))
            l_ersatzwerte = ['-1', '-1', 'n.r.', '0', 'n.r.', '9999999999', '999999']
            add_column = delimiter.join(list(str(x).upper() for x in l_ersatzwerte))
        elif filename == "lu_jahr_org.csv":
            headline = delimiter.join(list(str(x).upper() for x in l_hd_jahr))
            l_ersatzwerte = ['-1', '0', '9999999999', '999999']
            add_column = delimiter.join(list(str(x).upper() for x in l_ersatzwerte))
        elif filename == "lu_monat_org.csv":
            headline = delimiter.join(list(str(x).upper() for x in l_hd_monat))
            l_ersatzwerte = ['-1', '-1', 'n.r.', '0', 'n.r.', '9999999999', '999999']
            add_column = delimiter.join(list(str(x).upper() for x in l_ersatzwerte))
        elif filename == "lu_quartal_org.csv":
            headline = delimiter.join(list(str(x).upper() for x in l_hd_quartal))
            l_ersatzwerte = ['-1', '-1', 'n.r.', '0', 'n.r.', '9999999999', '999999']
            add_column = delimiter.join(list(str(x).upper() for x in l_ersatzwerte))
        elif filename == "lu_woche_org.csv":
            headline = delimiter.join(list(str(x).upper() for x in l_hd_woche))
            l_ersatzwerte = ['-1', 'n.r.', '0', 'n.r.', '9999999999', '999999']
            add_column = delimiter.join(list(str(x).upper() for x in l_ersatzwerte))
        elif filename == "obj_date.csv":
            headline = delimiter.join(list(str(x).upper() for x in l_obj_date))
            add_column = None
        else:
            log.error("Filename not in list.\nUse one of the following filenames:\n{0}".format("\n".join(l_filenames)))
            sys.exit(1)
        fh.write(headline + "\n")
        fh.write(add_column + "\n") if add_column else None
        while x_date <= d_end:
            b_write_action = False
            if filename == "lu_datum_org.csv":
                l_result = fill_dim_datum(x_date, arbeitstag_nr, d_datenstand)
                row = delimiter.join(list(str(x) for x in l_result[0]))
                arbeitstag_nr = l_result[1]
                b_write_action = True
            elif filename == "lu_halbjahr_org.csv":
                if x_date.month in (1, 7) and x_date.day == 1:
                    l_result = fill_dim_halbjahr(x_date, d_datenstand)
                    row = delimiter.join(list(str(x) for x in l_result))
                    b_write_action = True
            elif filename == "lu_jahr_org.csv":
                if x_date.month == 1 and x_date.day == 1:
                    l_result = fill_dim_jahr(x_date, d_datenstand)
                    row = delimiter.join(list(str(x) for x in l_result))
                    b_write_action = True
            elif filename == "lu_monat_org.csv":
                if x_date.day == 1:
                    l_result = fill_dim_monat(x_date, d_datenstand)
                    row = delimiter.join(list(str(x) for x in l_result))
                    b_write_action = True
            elif filename == "lu_quartal_org.csv":
                if x_date.month in (1, 4, 7, 10) and x_date.day == 1:
                    l_result = fill_dim_quartal(x_date, d_datenstand)
                    row = delimiter.join(list(str(x) for x in l_result))
                    b_write_action = True
            elif filename == "lu_woche_org.csv":
                if x_date.isoweekday() == 1:
                    l_result = fill_dim_woche(x_date, d_datenstand)
                    row = delimiter.join(list(str(x) for x in l_result))
                    b_write_action = True
            elif filename == "obj_date.csv":
                l_result = fill_obj_date(x_date)
                row = delimiter.join(list(str(x) for x in l_result))
                b_write_action = True
            fh.write(row + "\n") if b_write_action is True else None
            x_date = x_date + datetime.timedelta(days=1)
