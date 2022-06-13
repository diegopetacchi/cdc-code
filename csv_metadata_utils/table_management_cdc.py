import pandas as pd
from .sql_utils import with_clause
from .utils import get_columns_from_json_cdc_metadata, get_spark_path, get_normal_path, is_nan


def get_staging_table_script_cdc(schema, affiliate, category, original_table_name, file_path,
                                 json_file_metadata, delimiter=',', escape='\\\\', mode="PERMISSIVE", cdc=None):
    json_metadata = pd.read_json(get_normal_path(json_file_metadata))
    columns = get_columns_from_json_cdc_metadata(json_metadata, cdc=cdc)
    create_staging_table = 'create table ' + schema + '.TO_' + affiliate + '_' + category + "_" + original_table_name
    create_staging_table = create_staging_table + ' (' + columns + ')'
    create_staging_table = create_staging_table.replace(",", ",\n\t").replace('(', '(\n\t')
    create_staging_table = create_staging_table + 'USING com.databricks.spark.csv OPTIONS (\n\theader "true",'
    create_staging_table = create_staging_table + '\n\tmode "' + mode + '" ,'
    create_staging_table = create_staging_table + '\n\tdelimiter "' + delimiter + '",'
    create_staging_table = create_staging_table + '\n\tescape \'' + escape + '\',\n\tinferSchema "false"'
    create_staging_table = create_staging_table + ')'
    create_staging_table = create_staging_table + " location '" + get_spark_path(file_path) + "';"

    return create_staging_table.replace('#', '_').replace('$', '_')


def get_delta_table_script_cdc(schema, affiliate, category, original_table_name, location, json_file_metadata,
                               partition_column, dates_from_julian_to_gregorian=None):
    json_metadata = pd.read_json(get_normal_path(json_file_metadata))
    columns = get_columns_from_json_cdc_metadata(json_metadata, dates_from_julian_to_gregorian, cdc=True)
    create_table = 'create table ' + schema + '.TS_' + affiliate + '_' + category + "_" + original_table_name
    if not is_nan(partition_column):
        columns = columns + ',partition_key_yearmonth int'
    create_table = create_table + ' (' + columns + ') USING delta'
    if not is_nan(partition_column):
        create_table = create_table + " partitioned by (partition_key_yearmonth)"

    create_table = create_table + " location '" + get_spark_path(location) + "';"

    return create_table.replace('#', '_').replace('$', '_').replace(",", ",\n\t").replace('(', '(\n\t')


def get_insert_script_cdc(desc_staging, schema, source, original_table_name, affiliate, partition_column=None):
    columns = ''
    source_columns = ''
    partition_type = ''
    for idx, meta in enumerate(desc_staging):
        col = str(meta.col_name)
        columns += col + ','
        source_columns += col + ','
        if not is_nan(partition_column) and col == partition_column:
            partition_type = str(meta.data_type)
    columns = columns[:-1]
    source_columns = source_columns[:-1]
    if partition_type == 'timestamp':
        columns = columns + ",partition_key_yearmonth"
        source_columns = source_columns + ",date_format(" + partition_column + ",'yyyyMM') "
    source_columns = "'0','I','0','0','INSERT','0',current_timestamp()," + source_columns
    sql_insert = 'insert into ' + schema + '.TS_' + affiliate + '_' + source + '_' + original_table_name  # + '(' + columns + ') '
    sql_insert = sql_insert + ' select ' + source_columns + ' from ' + schema + '.TO_' + affiliate + '_' + source + '_' + original_table_name + " as source;"
    return sql_insert.replace('#', '_').replace('$', '_')


def get_merge_script_cdc(desc_staging, schema, source, affiliate, metadata, insert=False):
    table_name_op = 'TO_' + affiliate + '_' + source + '_' + metadata['table_name'] + '__ct'
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
        col = str(meta.col_name)
        source_col = 'source.' + col
        source_columns = source_columns + source_col + ','
        columns = columns + col + ','
        if str(meta.col_name) in primary_keys:
          join_columns = join_columns + 'target.' + col + '=' + source_col + ' and '
        else:
          merge_columns = merge_columns + col + '=' + source_col + ','

        if not is_nan(partition_column) and col == partition_column:
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
    metadata["modified_column"] = 'header__change_seq'

    sql_merge = with_clause(metadata, schema, table_name_op, insert) + ' \n'
    sql_merge = sql_merge + 'MERGE INTO ' + schema + '.' + table_name_delta + ' as target USING source ON '
    sql_merge = sql_merge + join_columns
    sql_merge = sql_merge + ' WHEN MATCHED THEN UPDATE SET ' + merge_columns
    sql_merge = sql_merge + " WHEN NOT MATCHED THEN INSERT (" + columns + ") values (" + source_columns + ");"
    return sql_merge.replace('#', '_').replace('$', '_')


