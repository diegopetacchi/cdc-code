from .sql_utils import with_clause
from .utils import java_style_date, get_columns_from_metadata, get_spark_path




def get_configuration(source, table_name, file_path, metadata, separator):
    res = {}
    keys = []
    date_column = ''
    date_format = ''
    modified_column = ''
    all_columns = []
    for meta in metadata:
        all_columns.append(meta.column)
        if meta.key:
            keys.append(meta.column)
        if (date_column == '' and meta.data_type == 'timestamp') or meta.column == 'CreatedDate':
            date_column = meta.column
            date_format = meta.date_format
        if meta.data_type == 'timestamp' and "modif" in str(meta.column).lower():
            modified_column = meta.column

    res["table_name"] = table_name
    res["source"] = source
    res["file_exp"] = get_spark_path(file_path)
    res["primary_key"] = ",".join(keys)
    res["partition_column"] = date_column
    res["date_format"] = date_format
    res["z_order_cols"] = ",".join(keys)
    res["modified_column"] = modified_column
    res["delimiter"] = separator
    res["data_quality_type"] = 'none'
    if len(keys) == 0:
        res["load_type"] = "truncate"
    else:
        res["load_type"] = "merge"
    return res



def get_staging_table_script_json(schema, affiliate, category, original_table_name, file_path, metadata):
    file_path = get_spark_path(file_path)
    columns, _ = get_columns_from_metadata(metadata)
    create_staging_table = 'create table ' + schema + '.TO_' + affiliate + '_' + category + "_" + original_table_name
    create_staging_table = create_staging_table + ' (' + columns + ')'
    create_staging_table = create_staging_table.replace(",", ",\n\t").replace('(', '(\n\t')
    create_staging_table = create_staging_table + 'USING JSON'
    create_staging_table = create_staging_table + " location '" + file_path + "';"

    return create_staging_table


def get_staging_table_script_csv(schema, affiliate, category, original_table_name, file_path, delimiter, escape,
                                 date_format, metadata, mode="PERMISSIVE"):
    file_path = get_spark_path(file_path)
    columns, _ = get_columns_from_metadata(metadata)
    create_staging_table = 'create table ' + schema + '.TO_' + affiliate + '_' + category + "_" + original_table_name
    create_staging_table = create_staging_table + ' (' + columns + ')'
    create_staging_table = create_staging_table.replace(",", ",\n\t").replace('(', '(\n\t')
    create_staging_table = create_staging_table + 'USING com.databricks.spark.csv OPTIONS (\n\theader "true",'
    create_staging_table = create_staging_table + '\n\tmode "' + mode + '" ,'
    create_staging_table = create_staging_table + '\n\tdelimiter "' + delimiter + '",'
    create_staging_table = create_staging_table + '\n\tescape \'' + escape + '\',\n\tinferSchema "false"'
    if date_format:
        create_staging_table = create_staging_table + ',\n\tdateFormat "' + java_style_date(date_format) + '")'
    else:
        create_staging_table = create_staging_table + ')'
    create_staging_table = create_staging_table + " location '" + file_path + "';"

    return create_staging_table



def get_delta_table_script(schema, affiliate, category, original_table_name, location, metadata, partition_column):
    columns, _ = get_columns_from_metadata(metadata)
    create_table = 'create table ' + schema + '.TS_' + affiliate + '_' + category + "_" + original_table_name
    if partition_column != '':
        columns = columns + ',partition_key_yearmonth int'
    columns = columns + ',aud_creationdate timestamp,aud_modifieddate timestamp,aud_load_id string,aud_operation string'
    create_table = create_table + ' (' + columns + ') USING delta'
    if partition_column != '':
        create_table = create_table + " partitioned by (partition_key_yearmonth)"

    create_table = create_table + " location '" + get_spark_path(location) + "';"

    return create_table.replace(",", ",\n\t").replace('(', '(\n\t')


def alter_staging_table_location_script(schema, table, new_location):
    return "ALTER TABLE " + schema + "." + table + " SET LOCATION '" + get_spark_path(new_location) + "';"


def get_insert_script(desc_staging, schema, source, affiliate, configuration, load_id):
    partition_column = str(configuration["partition_column"])
    columns = ''
    partition_type = ''
    for idx, meta in enumerate(desc_staging):
        columns = columns + str(meta.col_name) + ','
        if str(meta.col_name) == partition_column:
            partition_type = str(meta.data_type)
    columns = columns[:-1]
    source_columns = columns
    if partition_type == 'timestamp':
        columns = columns + ",partition_key_yearmonth"
        source_columns = source_columns + ",date_format(" + partition_column + ",'yyyyMM') "
    columns = columns + ",aud_creationdate,aud_modifieddate,aud_load_id,aud_operation"
    source_columns = source_columns + ",current_timestamp(),current_timestamp(),'" + str(load_id) + "','I'"
    sql_insert = 'insert into ' + schema + '.TS_' + affiliate + '_' + source + '_' + configuration[
        "table_name"] + '(' + columns + ') '
    sql_insert = sql_insert + 'select ' + source_columns + ' from ' + schema + '.TO_' + affiliate + '_' + source + '_' + \
                 configuration["table_name"] + ";"
    return sql_insert


def get_merge_script(desc_staging, metadata, schema, source, affiliate, load_id):
    table_name_op = 'TO_' + affiliate + '_' + source + '_' + metadata['table_name']
    table_name_delta = 'TS_' + affiliate + '_' + source + '_' + metadata['table_name']
    primary_keys = []
    primary_keys.extend(str(metadata["primary_key"]).split(","))
    print("pks: ", primary_keys)
    partition_column = str(metadata["partition_column"])
    merge_columns = ''
    source_columns = ''
    join_columns = ''
    columns = ''
    partion_type = ''
    for idx, meta in enumerate(desc_staging):
        print(meta)
        source_columns = source_columns + 'source.' + str(meta.col_name) + ','
        columns = columns + str(meta.col_name) + ','
        if str(meta.col_name) in primary_keys:
            join_columns = join_columns + 'target.' + str(meta.col_name) + '=source.' + str(meta.col_name) + ' and '
        else:
            merge_columns = merge_columns + str(meta.col_name) + '=source.' + str(meta.col_name) + ','
        if str(meta.col_name) == partition_column:
            partion_type = meta.data_type
    columns = columns[:-1]
    merge_columns = merge_columns[:-1]
    source_columns = source_columns[:-1]
    join_columns = join_columns[:-4]
    print("join: ", join_columns)
    print("partition type: ", partion_type)

    if partion_type == 'timestamp':
        columns = columns + ",partition_key_yearmonth"
        join_columns = join_columns + " and partition_key_yearmonth=date_format(source." + partition_column + ",'yyyyMM') "
        source_columns = source_columns + ",date_format(source." + partition_column + ",'yyyyMM') "
    elif partion_type != '' and partition_column not in primary_keys:
        join_columns = join_columns + " and target." + partition_column + "=source." + partition_column + " "
    columns = columns + ",aud_creationdate,aud_modifieddate,aud_load_id,aud_operation"
    merge_columns = merge_columns + ",aud_modifieddate=current_timestamp(),aud_load_id='" + str(
        load_id) + "',aud_operation='U'"
    source_columns = source_columns + ",current_timestamp(),current_timestamp(),'" + str(load_id) + "','I'"
    sql_merge = with_clause(metadata, schema, table_name_op) + ' \n'
    sql_merge = sql_merge + 'MERGE INTO ' + schema + '.' + table_name_delta + ' as target USING source ON '
    sql_merge = sql_merge + join_columns
    sql_merge = sql_merge + ' WHEN MATCHED THEN UPDATE SET ' + merge_columns
    sql_merge = sql_merge + " WHEN NOT MATCHED THEN INSERT (" + columns + ") values (" + source_columns + ");"
    return sql_merge






def get_delete_check_script(desc_staging, metadata, schema, source, affiliate, load_id):
    table_name_op = 'TO_' + affiliate + '_' + source + '_' + metadata['table_name']
    table_name_delta = 'TS_' + affiliate + '_' + source + '_' + metadata['table_name']
    primary_keys = []
    primary_keys.extend(str(metadata["primary_key"]).split(","))
    join_columns = ''
    columns = ''
    select_columns = ''
    for idx, meta in enumerate(desc_staging):
        if str(meta.col_name) in primary_keys:
            join_columns = join_columns + 'target.' + str(meta.col_name) + '=source.' + str(meta.col_name) + ' and '
            columns = columns + str(meta.col_name) + ','
            select_columns = select_columns + 'target.' + str(meta.col_name) + ','
    join_columns = join_columns[:-4]
    columns = columns[:-1]
    select_columns = select_columns[:-1]
    sql = f'select {select_columns} from (select {columns} from {schema}.{table_name_delta}) target left join (select {columns}, 1 flag_deleted from {schema}.{table_name_op}) source on {join_columns} where source.flag_deleted is null'
    return sql


def get_merge_delete_script(desc_staging, metadata, schema, source, affiliate, load_id):
    table_name_op = 'to_delete_' + affiliate + '_' + source + '_' + metadata['table_name']
    table_name_delta = 'TS_' + affiliate + '_' + source + '_' + metadata['table_name']
    primary_keys = []
    primary_keys.extend(str(metadata["primary_key"]).split(","))
    join_columns = ''
    columns = ''
    select_columns = ''
    for idx, meta in enumerate(desc_staging):
        if str(meta.col_name) in primary_keys:
            join_columns = join_columns + 'target.' + str(meta.col_name) + '=source.' + str(meta.col_name) + ' and '
            columns = columns + str(meta.col_name) + ','
            select_columns = select_columns + 'target.' + str(meta.col_name) + ','
    join_columns = join_columns[:-4]
    columns = columns[:-1]
    select_columns = select_columns[:-1]
    sql = f'MERGE INTO {schema}.{table_name_delta} as target USING {table_name_op} source ON {join_columns} WHEN MATCHED THEN DELETE'
    return sql
