import os
from os import listdir
import numpy as np
from .file_utils import read_json, csv_remove_break,read_csv
from .utils import is_int, is_float, is_date_loc
from get_date_format import infer
from .classes import DataType
from .constants import TESTROW, SEPARATORS
from itertools import chain, combinations
import pandas as pd


def read_first_lines(file_path, max_rows):
    rows = []
    count_dq = None
    try:
        with open(file_path, encoding="utf8") as f:
            for idx, line in enumerate(f):
                if count_dq is not None and (count_dq % 2 == 1 or count_dq == 0):
                    rows[-1] = rows[-1] + line
                else:
                    rows.append(line)
                count_dq = len([a for a in line if a == '"'])
                if idx > max_rows:
                    break
    except:
        rows = []
    if not rows:
        with open(file_path, encoding="ISO-8859-1") as f:
            for idx, line in enumerate(f):
                if count_dq and (count_dq % 2 == 1 or count_dq == 0):
                    rows[-1] = rows[-1] + line
                else:
                    rows.append(line)
                if idx > max_rows:
                    break
    return rows


def get_delimiter(file_path):
    sep_counts = [0] * len(SEPARATORS)
    rows = read_first_lines(get_file_path(file_path), 10)
    for row in rows:
        for y, val in enumerate(SEPARATORS):
            sep_counts[y] = row.count(val) if row.count(val) > sep_counts[y] else sep_counts[y]
    return SEPARATORS[sep_counts.index(max(sep_counts))]


def get_metadata_json(file_path, nrows=TESTROW):
    data_frame = read_json(get_file_path(file_path), nrows=nrows)

    return extract_types_from_data(data_frame)


def get_metadata_csv(file_path, delimiter, remove_breaks=False, low_memory=True, escape='\\',
                     dates_from_julian_to_gregorian=None, nrows=TESTROW):
    data_frame = read_csv(get_file_path(file_path), delimiter, low_memory=low_memory, escape=escape, nrows=nrows,
                          dates_from_julian_to_gregorian=dates_from_julian_to_gregorian)
    if remove_breaks:
        data_frame = csv_remove_break(data_frame)
    return extract_types_from_data(data_frame)


def extract_types_from_data(data_frame):
    rows = [[c for c in data_frame.columns]]
    rows.extend(data_frame.values.tolist())
    cols = list(map(list, zip(*rows)))
    res = []
    col_headers = []
    for col in cols:
        app = get_data_type(col)
        if str(app.column).lower() in col_headers:
            app.column = "{0}I".format(app.column)
        col_headers.append(str(app.column).lower())
        res.append(app)
    return res


def get_data_type(col):
    col_types = list(set([cell_type(col[i]) for i in range(1, len(col)) if col[i] != "" and col[i] is not None]))
    if len(col_types) == 1:
        col_type = col_types[0]
        if col_type == "int":
            col_type = "bigint"
    elif len(col_types) == 2 and "float" in col_types and "int" in col_types:
        col_type = 'float'
    elif len(col_types) == 2 and "timestamp" in col_types and True in [x==np.datetime64('NaT') for x in col_types]:
        col_type = 'timestamp'
    else:
        col_type = 'string'
    datatype = DataType(format_col_name(col[0]), col[0], False, col_type, "")
    if col_type == 'date':
        datatype.date_format = get_date_format(col)
    return datatype


def get_len(col):
    app = [len(col[i]) for i in range(1, len(col))]
    if len(app) > 0:
        return max(app)
    else:
        return 50


def get_date_format(col):
    try:
        res = infer([col[i] for i in range(1, len(col))])
        return res
    except:
        return '%Y-%m-%d %H:%M:%S'


def cell_type(cell):
    if cell == 'None' or cell == '':
        return 'none'
    elif isinstance(cell, bool):
        return 'boolean'
    elif is_int(str(cell)):
        return 'int'
    elif is_float(str(cell)):
        return 'float'
    elif is_date_loc(str(cell)):
        return 'timestamp'
    else:
        return 'string'


def format_col_name(col_name):
    return "".join(
        [x for x in str(col_name).strip() if
         ord(x) > 32 and ord(x) != 65279 and x not in ['.', '/', '\\', '#', '@', '%']])


def key_options(items):
    return chain.from_iterable(combinations(items, r) for r in range(1, len(items) + 1))


def find_primary_keys(rows):
    res = None
    cols = list(map(list, zip(*rows)))
    if len(cols) > 7:
        cols = cols[:7]
    test_rows = list(map(list, zip(*cols)))

    df = pd.DataFrame(test_rows[1:], columns=test_rows[0])
    for candidate in key_options(test_rows[0]):
        ded = df.drop_duplicates(candidate)
        if len(ded.index) == len(df.index):
            res = candidate
            break
    return res


def get_file_path(file_path):
    if os.path.isdir(file_path):
        return file_path + "/" + listdir(file_path)[0]
    return file_path
