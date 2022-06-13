import json
import shutil

import pandas as pd
import datetime
import numpy as np

from .utils import is_nan


def read_json(path, nrows=None):
    with open(path, encoding='utf-8-sig') as f:
        if nrows is None:
            selected_rows = pd.DataFrame([json.loads(l) for l in f.readlines()])
        else:
            nrows_read = 0
            selected_rows = []
            while nrows_read < nrows:
                line = f.readline()
                if not line:
                    break
                else:
                    selected_rows.append(json.loads(line))
                nrows_read += 1
        return pd.DataFrame(selected_rows)


def read_csv(path, delimiter, escape='\\', nrows=None, low_memory=True, remove_breaks=False,
             dates_from_julian_to_gregorian=None):
    print("Low memory param: ", low_memory)
    parse_dates = None
    date_parser = None
    df = None
    if dates_from_julian_to_gregorian:
        parse_dates = dates_from_julian_to_gregorian
        date_parser = from_julian_to_greogorian
    try:
        df = pd.read_csv(path, sep=delimiter, nrows=nrows,
                         escapechar=escape,
                         parse_dates=parse_dates,
                         date_parser=date_parser, low_memory=low_memory)

    except UnicodeDecodeError:
        df = pd.read_csv(path, sep=delimiter, nrows=nrows,
                         encoding='cp1252', escapechar=escape,
                         parse_dates=parse_dates,
                         date_parser=date_parser, low_memory=low_memory)

    if remove_breaks:
        df = csv_remove_break(df)
    return df


def csv_remove_break(df):
    if df is not None:
        return df[df.columns].replace({'\r': ''}, regex=True).replace({'\n': ''}, regex=True)
    else:
        return None


def calculate_julian_single_date(julian_date_sing):
    if is_nan(julian_date_sing) or str(int(float(julian_date_sing))) == '0':
        return np.nan
    julian_date = str(int(float(julian_date_sing)))
    return datetime.datetime.strptime("" + str(1900 + int(julian_date[:3])) + str(julian_date[-3:]), '%Y%j')


def from_julian_to_greogorian(julian_date):
    try:
        parsed = calculate_julian_single_date(julian_date)
    except Exception as e:
        parsed = [calculate_julian_single_date(d) for d in julian_date]
    return parsed


def move_file(source_path, target_path):
    shutil.move(source_path, target_path)


def copy_file(source_path, target_path):
    shutil.copy(source_path, target_path)
