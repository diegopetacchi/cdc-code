from string import Template

import os
import datetime




def get_history_path(source_path, source, load_id, spark_style):
    return get_path('history', source_path, source, load_id, spark_style)


def get_working_path(source_path, source, load_id, spark_style):
    try:
        if spark_style:
            source_path = get_spark_path(source_path)
        else:
            source_path = get_normal_path(source_path)
        paths = source_path.split("/" + source + "/")
        ris = paths[0] + "/" + "working/" + str(load_id) + "/" + source + "/"
        if str(paths[1]).lower().endswith('.csv'):
            pos = paths[1].rfind('/')
            return ris + paths[1][:pos] + paths[1][pos:]
        else:
            return ris + paths[1]
    except:
        return None


def get_error_path(source_path, source, load_id, spark_style):
    return get_path('error', source_path, source, load_id, spark_style)


def get_path(path_type, source_path, source, load_id, spark_style):
    try:
        if spark_style:
            source_path = get_spark_path(source_path)
        else:
            source_path = get_normal_path(source_path)
        paths = source_path.split("/" + source + "/")
        ris = paths[0] + "/" + path_type + "/" + source + "/"
        if str(paths[1]).lower().endswith('.csv'):
            pos = paths[1].rfind('/')
            return ris + paths[1][:pos] + "/" + str(load_id) + paths[1][pos:]
        else:
            return ris + paths[1] + "/" + str(load_id)
    except:
        return None


def mkdir(path):
    file_path = ""
    for folder in path.split("/"):
        if folder != '':
            file_path = file_path + "/" + folder
            try:
                if not os.path.exists(file_path):
                    os.mkdir(file_path)
            except:
                pass


def get_folder_list(path):
    source_path = path.replace("/dbfs/mnt", "").replace("dbfs:/mnt/", "")
    file_path = "/dbfs/mnt"
    folder_list = []
    for folder in source_path.split("/"):
        if folder != '':
            file_path = file_path + "/" + folder
            folder_list.append(file_path)
    return folder_list


def java_style_date(date_format):
    mapping = {'Y': 'yyyy', 'm': 'MM', 'd': 'dd', 'H': 'HH', 'M': 'mm', 'S': 'ss', 'f': 'SSS'}
    return Template(date_format.replace('%', '$')).substitute(**mapping)


def get_spark_path(path):
    return path.replace('/dbfs/mnt/', 'dbfs:/mnt/')


def get_normal_path(path):
    return path.replace('dbfs:/mnt/', '/dbfs/mnt/')


def is_float(s):
    return str(s).replace('.', '', 1).isdigit() or (
            str(s).startswith("-") and str(s).replace('.', '', 1).replace('-', '', 1).isdigit())


def is_int(s):
    return str(s).isdigit() or (str(s).startswith("-") and str(s).replace('-', '', 1).isdigit())





def is_enclosed(rows):
    count = 0
    for row in rows:
        count += row.count("\"")
    return True if count > len(rows) * 2 else False


def get_columns_from_metadata(metadata):
    columns = ''
    date_format = None
    for idx, meta in enumerate(metadata):
        if meta.data_type == 'date':
            date_format = meta.date_format
        columns = columns + meta.column + " " + meta.data_type + ","
    columns = columns[:-1]
    return columns, date_format


def get_columns_from_json_cdc_metadata(json_metadata,dates_from_julian_to_gregorian=None,cdc=None):
    columns = ''
    if is_nan(cdc) is False:
        columns += 'header__change_seq STRING,header__change_oper STRING,header__change_mask STRING,header__stream_position STRING,header__operation STRING,header__transaction_id STRING,header__timestamp STRING,'

    for idx, meta in enumerate(json_metadata.dataInfo['columns']):
        if isinstance(dates_from_julian_to_gregorian,list) and meta['name'] in dates_from_julian_to_gregorian:
            columns = columns + meta['name'] + " " + "TIMESTAMP ,"
        elif 'string' in str(meta['type']).lower():
            columns = columns + meta['name'] + " " + "STRING ,"
        elif 'int' in str(meta['type']).lower():
            columns = columns + meta['name'] + " " + "BIGINT ,"
        elif 'numeric' in str(meta['type']).lower():
            columns = columns + meta['name'] + " " + "NUMERIC ,"
        else:
            columns = columns + meta['name'] + " " + "STRING ,"
    columns = columns[:-1]

    return columns


def get_primary_key_from_json_cdc_metadata(json_metadata):
    primary_key = ''
    for idx, meta in enumerate(json_metadata.dataInfo['columns']):

        if int(meta['primaryKeyPos']) > 0:
            primary_key += meta['name'] + ','
    primary_key = primary_key[:-1]
    return primary_key


def is_nan(num):
    return num is None or num != num or num == ''
