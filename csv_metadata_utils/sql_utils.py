from .utils import is_nan


def with_clause(metadata, schema, table_name,insert=False):
    cl1=""
    cl2=""
    if insert==True:
        cl1=" where header__change_oper='I'"
        cl2="and header__change_oper='I'"
    if is_nan(metadata["modified_column"]) or is_nan(metadata["primary_key"]):
        return 'with source as (select distinct * from ' + schema + '.' + table_name + ')'
    else:
        with_cl = "with source as (select distinct a.* from " + schema + "."
        with_cl = with_cl + table_name
        with_cl = with_cl + ' a inner join (select ' + str(metadata["primary_key"])
        with_cl = with_cl + ', max(' + str(
            metadata["modified_column"]) + ') maxdate from ' + schema + '.' + table_name + cl1+' group by ' + str(
            metadata["primary_key"]) + ') b on '
        join_key = " and ".join(['a.' + k + '=b.' + k for k in str(metadata["primary_key"]).split(",")])
        return with_cl + join_key + ' and a.' + str(metadata["modified_column"]) + '=maxdate '+cl2+' )'

