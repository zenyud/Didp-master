# -*- coding: UTF-8 -*-
################################################################################
# Date Time     : 2018-11-03
# Write By      : adtec(xiazhy,zhaogx)
# Function Desc : ORACLE库卸数,加载插件
#
# History       :
#                 20181103  zgx     Create
#
# Remarks       :
################################################################################
import os
import re
import math
import codecs
import traceback
import jaydebeapi

from utils.didp_logger import Logger 
from utils.didp_tools import check_path, generate_ddl_file, generate_ctrl_file
from utils.didp_ddl_operator import DdlOperator
from utils.didp_db_operator import DbOperator

# 日志输出实例
LOG = Logger()

# 指定jdbc类
JDBC_CLASS  = "oracle.jdbc.driver.OracleDriver"


class Exporter(object):
    """ ORACLE卸数插件
    
    Attributes:
       __args           : 参数
       __target_db_info : 目标库信息(字典)
       __db_oper        : 数据库连接类实例
    """
    def __init__(self, args, target_db_info):
        self.__args = args         
        self.__target_db_info = target_db_info
        self.__export_lines = 0

        self.__db_oper = DbOperator(self.__target_db_info['db_user'],
                                    self.__target_db_info['db_pwd'],
                                    JDBC_CLASS,
                                    self.__target_db_info['jdbc_url'])

    def __get_table_struct(self):
        """ 获取目标表表结构

        Args:

        Returns:
            [0, 结构信息] : 成功 | [-1, []] : 失败
        Raise:

        """
        sql = ""          # sql语句
        columns_info = [] # 字段信息
        result_info = []  # 结果信息

        LOG.info("获取表[{0}]的结构".format(self.__args.table))
        sql = ("SELECT"
               "\n    UPPER(TRIM(T1.COLUMN_NAME)) AS COLUMN_NAME,"
               "\n    UPPER(TRIM(T1.DATA_TYPE)) AS DATA_TYPE,"
               "\n    T1.DATA_SCALE AS DATA_SCALE,"
               "\n    T1.DATA_PRECISION AS DATA_PRECISION,"
               "\n    T1.CHAR_LENGTH AS CHAR_LENGTH,"
               "\n    T1.DATA_LENGTH AS DATA_LENGTH,"
               "\n    (CASE"
               "\n        WHEN T1.NULLABLE = 'N' THEN '0'"
               "\n        ELSE '1'"
               "\n    END) AS IS_NULL,"
               "\n    (CASE"
               "\n        WHEN T2.KEY_COL IS NULL THEN '0'"
               "\n        ELSE '1'"
               "\n    END) AS IS_PK,"
               "\n    T3.COLUMN_DESC AS COLUMN_DESC,"
               "\n    T4.TABLE_DESC AS TABLE_DESC"
               "\nFROM ALL_TAB_COLUMNS T1 "
               "\nLEFT JOIN ( "
               "\n    SELECT A.COLUMN_NAME AS KEY_COL "
               "\n    FROM USER_CONS_COLUMNS A, USER_CONSTRAINTS B"
               "\n    WHERE A.CONSTRAINT_NAME = B.CONSTRAINT_NAME"
               "\n      AND B.CONSTRAINT_TYPE = 'P' "
               "\n      AND UPPER(A.OWNER) = '{0}' "
               "\n      AND UPPER(A.TABLE_NAME) = '{1}' "
               "\n) T2 "
               "\nON T1.COLUMN_NAME = T2.KEY_COL "
               "\nLEFT JOIN ( "
               "\n    SELECT COLUMN_NAME,Comments AS COLUMN_DESC "
               "\n    FROM ALL_COL_COMMENTS "
               "\n    WHERE UPPER(OWNER) = '{0}' "
               "\n      AND UPPER(TABLE_NAME) = '{1}' "
               "\n) T3 "
               "\nON T1.COLUMN_NAME = T3.COLUMN_NAME "
               "\nLEFT JOIN ( "
               "\n    SELECT COMMENTS AS TABLE_DESC "
               "\n    FROM ALL_TAB_COMMENTS "
               "\n    WHERE UPPER(OWNER) = '{0}' "
               "\n      AND UPPER(TABLE_NAME) = '{1}' "
               "\n) T4 "
               "\nON 1 = 1 "
               "\nWHERE UPPER(OWNER) = '{0}' "
               "\n  AND UPPER(TABLE_NAME) = '{1}' "
               "\nORDER BY COLUMN_ID ").format(
                   self.__target_db_info['db_schema'].upper(),
                   self.__args.table.upper())

        LOG.info("SQL:\n{0}".format(sql))
        try:
            result_info = self.__db_oper.fetchall_direct(sql)
        except Exception as e:
            LOG.error("获取表[{0}]的结构失败".format(self.__args.table))
            return -1, []

        if len(result_info) == 0:
            LOG.error(("无法查询到目标表的表结构,请检查表是否存在,"
                      "或者配置是否正确"))
            return -1, []

        LOG.debug("表结构查询结果:\n{0}".format(result_info))

        for i in range(len(result_info)):
            column_info = {}                # 字段信息
            column_name = result_info[i][0] # 字段名
            data_type = result_info[i][1]   # 字段类型
            column_define_length = 0        # 字段类型中定义的长度
             
            # None结果区分,取整
            if result_info[i][2] != None:
                data_scale = int(result_info[i][2])
                column_scale = data_scale
            else:
                data_scale = result_info[i][2]
                column_scale = 0
          
            if result_info[i][3] != None:
                data_precision = int(result_info[i][3])
            else:
                data_precision = result_info[i][3]
 
            if result_info[i][4] != None:
                char_length = int(result_info[i][4])
            else:
                char_length = result_info[i][4]

            if result_info[i][5] != None:
                data_length = int(result_info[i][5])
            else:
                data_length = result_info[i][5]
           
            is_null = result_info[i][6]     # 是否可空
            is_pk = result_info[i][7]       # 是否主键
            if result_info[i][8] == None:
                column_desc = ''
            else:
                column_desc = result_info[i][9]  # 字段描述

            if result_info[i][9] == None:
                table_desc = ''
            else:
                table_desc = result_info[i][9]  # 表描述

            # 调整类型,生成通用类型
            if data_type == "CHAR" or data_type == "VARCHAR2":
                column_base_type = "VARCHAR"
                column_length = data_length
                column_define_length = data_length
                column_type = "{0}({1})".format(data_type,
                                  data_length)
                if data_type == "CHAR":
                    column_std_type = "{0}!anc".format(column_length)
                else:
                    column_std_type = "anc..{0}".format(column_length)
            elif data_type == "NCHAR" or data_type == "NVARCHAR2":
                column_base_type = "NVARCHAR"
                column_define_length = data_length
                column_type = "{0}({1})".format(data_type,
                                  char_length)
                if self.__args.charset == "1":
                    column_length = char_length * 3
                else:
                    column_length = char_length * 2
                if data_type == "NCHAR":
                    column_std_type = "{0}!anc".format(column_length)
                else:
                    column_std_type = "anc..{0}".format(column_length)
            elif data_type == "NUMBER":
                if data_scale != None:
                    column_base_type = "NUMERIC"
                    if data_scale == 0:
                        if data_precision == None:
                            column_length = 38 + 1
                            column_std_type = "19n"
                            column_type = "NUMBER"
                            column_define_length = data_length
                        else:
                            column_length = data_precision + 1
                            column_std_type = "{0}n".format(data_precision)
                            column_type = "NUMBER({0})".format(data_precision)
                            column_define_length = data_precision
                    else:
                        column_length = data_precision + 2
                        column_define_length = data_precision
                        column_std_type = "{0}n({1})".format(data_precision+1,
                                                             data_scale)
                        column_type = "NUMBER({0},{1})".format(data_precision,
                                          data_scale)
                else:
                    column_base_type = "NUMERIC0"
                    column_length = 38 + 2
                    column_std_type = "19n"
                    column_type = "NUMBER(38)"
            elif data_type == "FLOAT":
                column_base_type = "FLOAT"
                if data_precision == None:
                   data_precision = 38
                else:
                   column_define_length = data_precision

                column_length = math.ceil(data_precision * math.log(2) 
                                          / math.log(10)) + 2
                column_std_type = "39n(10)"
                column_type = "FLOAT"
            elif data_type == "BINARY_FLOAT":
                column_base_type = "FLOAT"
                column_length = 38 + 2;
                column_std_type = "39n(10)"
                column_type = "BINARY_FLOAT"
            elif data_type == "BINARY_DOUBLE":
                column_base_type = "FLOAT"
                column_length = 38 + 2;
                column_std_type = "39n(10)"
                column_type = "BINARY_DOUBLE"
            elif data_type == "DATE":
                column_base_type = "DATE"
                column_length = 10
                column_std_type = "YYYY-MM-DD"
                if self.__args.dtfmt == "YYYY-MM-DDTHH:MI:SS":
                    column_length = 19
                    column_std_type = "YYYY-MM-DDTHH:MM:SS"
                column_type = data_type
            elif data_type == "CLOB" or data_type == "BLOB":
                column_base_type = data_type
                column_length = 4000
                column_std_type = data_type
                column_type = data_type
            elif data_type == "UROWID" or data_type == "ROWID":
                column_base_type = data_type
                column_length = 18
                column_std_type = "anc..18"
                column_type = data_type
            else:
                match_obj = re.match(r"TIMESTAMP\((.*?)\)", data_type)
                if match_obj:
                    tmp_val = match_obj.group(1)
                    if tmp_val == "0":
                        column_length = 19 
                        column_std_type = "YYYY-MM-DDTHH:MM:SS"
                        column_type = "TIMESTAMP(0)"
                    elif tmp_val == "3":
                        column_length = 23
                        column_std_type = "YYYY-MM-DDTHH:MM:SS.NNN"
                        column_type = "TIMESTAMP(3)"
                    elif tmp_val == "6":
                        column_length = 26
                        column_std_type = "YYYY-MM-DDTHH:MM:SS.NNNNNN"
                        column_type = "TIMESTAMP(6)"
                    else:
                        data_type = "TIMESTAMP(6)"
                        column_type = "TIMESTAMP(6)"
                    column_base_type = data_type
                    column_scale = 0
                    data_type = "TIMESTAMP"
                    column_define_length = int(tmp_val)
                else:
                    column_type = data_type

                    match_obj = re.match(r"^LONG", data_type)
                    if match_obj:
                        column_base_type = "LONG"
                        column_std_type = "anc..10"
                        column_length = 10
                    else:
                        match_obj = re.match(r"^RAW", data_type)
                        if match_obj:
                            column_base_type = "RAW"
                            column_std_type = "anc..10"
                            column_length = 10
                        else:
                            column_base_type = data_type
                            column_std_type = data_type
                            column_length = data_length

            column_info['column_name'] = column_name
            column_info['column_base_type'] = column_base_type
            column_info['data_type'] = data_type
            column_info['column_type'] = column_type
            column_info['column_length'] = column_length
            column_info['column_define_length'] = column_define_length
            column_info['column_scale'] = column_scale
            column_info['column_std_type'] = column_std_type
            column_info['is_null'] = is_null
            column_info['is_pk'] = is_pk
            column_info['partition_flag'] = "0"
            column_info['bucket_flag'] = "0"
            column_info['column_desc'] = column_desc
            column_info['table_desc'] = table_desc
            column_info['fixed'] = self.__args.fixed
            column_info['rcdelim'] = self.__args.rcdelim
            column_info['delim'] = self.__args.delim
            column_info['table_name'] = self.__args.table
            column_info['quote_type'] = "0"

            if self.__args.enddel == "N" and i == len(result_info)-1:
                column_info['delim'] = ""

            columns_info.append(column_info)

        LOG.debug("调整后的结果:\n{0}".format(columns_info))
        return 0, columns_info

    def __get_export_sql(self, columns_info):
        """ 获取卸数SQL

        Args:
            columns_info : 字段信息
        Returns:

        Raise:

        """
        LOG.info("生成卸数的SQL")

        def_date = "'1900-01-01'" # 默认日期值
        def_time = "'00:00:00'" # 默认时间值
        # 默认时间戳(6位毫秒)
        def_timestamp6 = "'1900-01-01 00:00:00.000000'"
        # 默认时间戳(3位毫秒)
        def_timestamp3 = "'1900-01-01 00:00:00.000'"
        # 默认时间戳(无毫秒)
        def_timestamp = "'1900-01-01 00:00:00'"
        def_string = "' '" # 默认字符串
        def_number = "'0'" # 默认数值
        def_time_format = "'HH24:MI:SS'" # 默认时间格式
        def_date_format = "'YYYY-MM-DD'" # 默认日期格式
        # 默认时间戳格式(无毫秒)
        def_timestamp_format = "'YYYY-MM-DD HH24:MI:SS'"
        # 默认时间戳格式(3位毫秒)
        def_timestamp_format3 = "'YYYY-MM-DD HH24:MI:SS.FF3'"
        # 默认时间戳格式(6位毫秒)
        def_timestamp_format6 = "'YYYY-MM-DD HH24:MI:SS.FF6'"

        if self.__args.dtfmt == "YYYY-MM-DDTHH:MM:SS":
            def_date_format = def_timestamp_format

        use_data_type = ""    # 默认转换字符类型
        use_data_type_n = ""  # 默认转换Unicode字符类型
        default_null_val = "" # 默认空值

        export_sql = "" # 卸数sql
        export_columns_list = []    # 卸数字段列表
        export_column_sql_list = [] # 卸数字段级sql

        # 设定分隔符替换的字符串
        # 每个字符替换成一个空格
        replace_delim = re.sub(r"\S", "#", self.__args.delim)

        # 定长不定长判断
        if self.__args.fixed == "1":
            use_data_type = "CHAR"
            use_data_type_n = "NCHAR"
            default_null_val = "' '"
        else:
            use_data_type = "VARCHAR2"
            use_data_type_n = "NVARCHAR2"
            default_null_val = "''"

        # 指定字段卸数
        if self.__args.selcol:
            select_columns_list = self.__args.selcol.upper()
            LOG.info("校验用户指定的卸数字段")
            for select_column in select_columns_list.split(","):
                is_include = "0"
                for column_info in columns_info:
                    if select_column == column_info["column_name"]:
                        is_include = "1"
                        break
                if is_include == "0":
                    LOG.error("指定卸载的字段不存在:[{0}]".format(select_column)) 
                    return -1, ''
                else:
                    export_columns_list.append(select_column)
        else:
            for column_info in columns_info:
                export_columns_list.append(column_info["column_name"]) 
 
        LOG.info("卸数字段列表:{0}".format(export_columns_list))

        # 根据字段类型生成每个字段对应的sql
        for export_column in export_columns_list:
            for src_column_info in columns_info:
                if export_column != src_column_info["column_name"]:
                    continue

                # 定长卸载时不支持长度大于2000
                if (self.__args.fixed == "1"
                   and src_column_info["column_length"] > 2000):
                    LOG.error(("ORACLE卸数格式为定长且字段长度超过2000,"
                              "字段:{0}").format(column_name))
                    return -1, ''
                
                if self.__args.setdef == "Y":
                    if src_column_info['column_base_type'] == "DATE":
                        export_column_sql_list.append(
                            ("CAST( CASE WHEN {0} IS NULL THEN {1} "
                             "ELSE TO_CHAR({0},{2}) END AS {3}({4}) )").format(
                                export_column, def_date,
                                def_date_format, use_data_type, 
                                src_column_info["column_length"]))
                    elif src_column_info['column_base_type'] == "FLOAT":
                        export_column_sql_list.append(
                            ("CASE WHEN {0} IS NULL OR {0} = 0 THEN '0.00' "
                             "ELSE CASE WHEN TRUNC({0}) = 0 THEN "
                             "CASE WHEN {0} < 0 THEN '-0' || "
                             "SUBSTR({0}, INSTR({0}, '.')) "
                             "ELSE '0' || CAST({0} AS VARCHAR(40)) END "
                             "ELSE CAST({0} AS VARCHAR(40)) END END").format(
                                 export_column))
                    elif src_column_info['column_base_type'] == "TIMESTAMP(3)":
                        export_column_sql_list.append(
                            ("CAST(CASE WHEN {0} IS NULL THEN {1} "
                             "ELSE TO_CHAR({0},{2}) END AS {3}({4}))").format(
                                export_column, def_timestamp3, 
                                def_timestamp_format3, use_data_type, 
                                src_column_info["column_length"]))
                    elif src_column_info['column_base_type'] == "TIMESTAMP(6)":
                        export_column_sql_list.append(
                             ("CAST(CASE WHEN {0} IS NULL THEN {1} "
                              "ELSE TO_CHAR({0},{2}) END AS {3}({4}))").format(
                                export_column, def_timestamp6,
                                def_timestamp_format6, use_data_type, 
                                src_column_info["column_length"]))
                    elif src_column_info['column_base_type'] == "TIMESTAMP(0)":
                        export_column_sql_list.append(
                             ("CAST(CASE WHEN {0} IS NULL THEN {1} "
                              "ELSE TO_CHAR({0},{2}) END AS {3}({4}))").format(
                                export_column, def_timestamp,
                                def_timestamp_format, use_data_type, 
                                src_column_info["column_length"]))
                    elif (src_column_info['column_base_type'] == "NCHAR"
                          or src_column_info['column_base_type'] == "NVARCHAR"
                          or src_column_info['column_base_type'] == "NVARCHAR2"):
                        if self.__args.repdel == "Y":
                            tmp_str = ("REPLACE({0}, '{1}', '{2}')").format(
                                          export_column, self.__args.delim,
                                          replace_delim)
                        else:
                            tmp_str = export_column

                        if self.__args.trimflg == "0":
                            tmp_str = "TRIM({0})".format(tmp_str)
                        elif self.__args.trimflg == "1":
                            tmp_str = "RTRIM({0})".format(tmp_str)

                        if self.__args.repflg == "Y":
                            tmp_str = ("REPLACE(REPLACE({0}, CHR(10),' ')"
                                       ", CHR(13), ' ')").format(tmp_str)
                        export_column_sql_list.append(
                            ("CASE WHEN {0} IS NULL OR {0} ='' THEN "
                             "CAST({1} AS NVARCHAR2(10)) ELSE {0} END").format(
                                tmp_str, def_string))
                    elif (src_column_info['column_base_type'] == "NUMERIC"
                          or src_column_info['column_base_type'] == "NUMERIC0"):
                        if self.__args.trimflg == "0":
                            export_column_sql_list.append(
                                ("CASE WHEN {0} IS NULL OR {0} = 0 THEN "
                                 "CAST({1} AS {2}({3})) ELSE CASE WHEN {0} = 0 "
                                 "THEN  CAST(TRIM("
                                 "CAST({0} AS {2}({3}))) AS {2}({3})) ELSE "
                                 "CASE WHEN TRUNC({0}) = 0 THEN "
                                 "CASE WHEN {0} < 0 THEN '-0' || "
                                 "SUBSTR({0}, INSTR({0}, '.')) WHEN {0} = 0 "
                                 "THEN CAST(TRIM("
                                 "CAST({0} AS {2}({3}))) AS {2}({3})) "
                                 "ELSE '0' || CAST(TRIM("
                                 "CAST({0} AS {2}({3}))) AS {2}({3})) END ELSE "
                                 "CAST(TRIM(CAST({0} AS {2}({3}))) AS {2}({3})) "
                                 "END END END").format(export_column,
                                     def_number, use_data_type,
                                     src_column_info["column_length"]))
                        elif self.__args.trimflg == "1":
                            export_column_sql_list.append(
                                ("CASE WHEN {0} IS NULL OR {0} = 0 THEN "
                                 "CAST({1} AS {2}({3})) ELSE CASE WHEN {0} = 0 "
                                 "THEN  CAST(RTRIM("
                                 "CAST({0} AS {2}({3}))) AS {2}({3})) ELSE "
                                 "CASE WHEN TRUNC({0}) = 0 THEN "
                                 "CASE WHEN {0} < 0 THEN '-0' || "
                                 "SUBSTR({0}, INSTR({0}, '.')) WHEN {0} = 0 "
                                 "THEN CAST(RTRIM("
                                 "CAST({0} AS {2}({3}))) AS {2}({3})) "
                                 "ELSE '0' || CAST(RTRIM("
                                 "CAST({0} AS {2}({3}))) AS {2}({3})) END ELSE "
                                 "CAST(RTRIM(CAST({0} AS {2}({3}))) AS {2}({3})) "
                                 "END END END").format(export_column,
                                     def_number, use_data_type,
                                     src_column_info["column_length"]))
                        else:
                            export_column_sql_list.append(
                                ("CASE WHEN {0} IS NULL OR {0} = 0 THEN "
                                 "CAST({1} AS {2}({3})) ELSE CASE WHEN {0} = 0 "
                                 "THEN  CAST("
                                 "CAST({0} AS {2}({3})) AS {2}({3})) ELSE "
                                 "CASE WHEN TRUNC({0}) = 0 THEN "
                                 "CASE WHEN {0} < 0 THEN '-0' || "
                                 "SUBSTR({0}, INSTR({0}, '.')) WHEN {0} = 0 "
                                 "THEN CAST("
                                 "CAST({0} AS {2}({3})) AS {2}({3})) "
                                 "ELSE '0' || CAST("
                                 "CAST({0} AS {2}({3})) AS {2}({3})) END ELSE "
                                 "CAST(CAST({0} AS {2}({3})) AS {2}({3})) "
                                 "END END END").format(export_column,
                                     def_number, use_data_type,
                                     src_column_info["column_length"]))
                    elif (src_column_info['column_base_type'] == "CHAR"
                          or src_column_info['column_base_type'] == "VARCHAR"
                          or src_column_info['column_base_type'] == "VARCHAR2"):
                        if self.__args.repdel == "Y":
                            tmp_str = ("REPLACE({0}, '{1}', '{2}')").format(
                                          export_column, self.__args.delim,
                                          replace_delim)
                        else:
                            tmp_str = export_column

                        if self.__args.trimflg == "0":
                            tmp_str = "TRIM({0})".format(tmp_str)
                        elif self.__args.trimflg == "1":
                            tmp_str = "RTRIM({0})".format(tmp_str)
 
                        if self.__args.repflg == "Y":
                            tmp_str = ("REPLACE(REPLACE({0}, CHR(10),' ')"
                                       ", CHR(13), ' ')").format(tmp_str)

                        export_column_sql_list.append(
                            ("CASE WHEN NVL({0},'\|\@\|') = '\|\@\|' "
                             "THEN CAST({1} AS {2}({3})) ELSE "
                             "CAST({0} AS {2}({3})) END").format(tmp_str,
                                def_string, use_data_type, 
                                src_column_info["column_length"]))
                    elif (src_column_info['column_base_type'] == "BLOB"
                          or src_column_info['column_base_type'] == "LONG"
                          or src_column_info['column_base_type'] == "RAW"):
                        export_column_sql_list.append(
                            "CAST ( '' AS VARCHAR(10) )")
                    else:
                        export_column_sql_list.append(
                            ("CASE WHEN {0} IS NULL THEN "
                             "CAST({1} AS {2}({3})) ELSE "
                             "CAST({0} AS {2}({3})) END").format(export_column,
                                 def_string, use_data_type, 
                                 src_column_info["column_length"]))
                else:
                    if src_column_info['column_base_type'] == "DATE":
                        export_column_sql_list.append(
                            "CAST( TO_CHAR({0}, {1}) AS {2}({3}) )".format(
                                export_column, def_date_format, use_data_type,
                                src_column_info["column_length"]))
                    elif src_column_info['column_base_type'] == "FLOAT":
                        export_column_sql_list.append(
                            ("CASE WHEN {0} IS NULL THEN {1} ELSE "
                             "CASE WHEN {0} = 0 THEN "
                             "CAST(TO_CHAR({0}) AS VARCHAR(40)) ELSE "
                             "CASE WHEN TRUNC({0}) = 0 THEN "
                             "CASE WHEN {0} < 0 THEN '-0' || "
                             "SUBSTR({0}, INSTR({0}, '.')) WHEN {0} = 0 THEN "
                             "CAST(TO_CHAR({0}) AS VARCHAR(40)) ELSE '0' || "
                             "CAST(TO_CHAR({0}) AS VARCHAR(40)) END ELSE "
                             "CAST(TO_CHAR({0}) AS VARCHAR(40)) "
                             "END END END").format(export_column,
                                 default_null_val))
                    elif src_column_info['column_base_type'] == "TIMESTAMP(3)":
                        export_column_sql_list.append(
                            "CAST( TO_CHAR({0},{1}) AS {2}({3}))".format(
                                export_column, def_timestamp_format3,
                                use_data_type, 
                                src_column_info["column_length"]))
                    elif src_column_info['column_base_type'] == "TIMESTAMP(6)":
                        export_column_sql_list.append(
                            "CAST( TO_CHAR({0},{1}) AS {2}({3}))".format(
                                export_column, def_timestamp_format6,
                                use_data_type, 
                                src_column_info["column_length"]))
                    elif src_column_info['column_base_type'] == "TIMESTAMP(0)":
                        export_column_sql_list.append(
                            "CAST( TO_CHAR({0},{1}) AS {2}({3}))".format(
                                export_column, def_timestamp_format,
                                use_data_type, 
                                src_column_info["column_length"]))
                    elif (src_column_info['column_base_type'] == "NCHAR"
                          or src_column_info['column_base_type'] == "NVARCHAR"
                          or src_column_info['column_base_type'] == "NVARCHAR2"):
                        if self.__args.repdel == "Y":
                            tmp_str = ("REPLACE({0}, '{1}', '{2}')").format(
                                          export_column, self.__args.delim,
                                          replace_delim)
                        else:
                            tmp_str = export_column

                        if self.__args.trimflg == "0":
                            tmp_str = "TRIM({0})".format(tmp_str)
                        elif self.__args.trimflg == "1":
                            tmp_str = "RTRIM({0})".format(tmp_str)

                        if self.__args.repflg == "Y":
                            tmp_str = ("REPLACE(REPLACE({0}, CHR(10),' ')"
                                       ", CHR(13), ' ')").format(tmp_str)
                        export_column_sql_list.append(
                            "{0}".format(tmp_str, def_string))
                    elif (src_column_info['column_base_type'] == "NUMERIC"
                          or src_column_info['column_base_type'] == "NUMERIC0"):
                        if self.__args.trimflg == "0":
                            export_column_sql_list.append(
                                ("CASE WHEN {0} IS NULL THEN "
                                 "CAST({1} AS {2}({3})) ELSE CASE WHEN {0} = 0 "
                                 "THEN  CAST(TRIM("
                                 "CAST({0} AS {2}({3}))) AS {2}({3})) ELSE "
                                 "CASE WHEN TRUNC({0}) = 0 THEN "
                                 "CASE WHEN {0} < 0 THEN '-0' || "
                                 "SUBSTR({0}, INSTR({0}, '.')) WHEN {0} = 0 "
                                 "THEN CAST(TRIM("
                                 "CAST({0} AS {2}({3}))) AS {2}({3})) "
                                 "ELSE '0' || CAST(TRIM("
                                 "CAST({0} AS {2}({3}))) AS {2}({3})) END ELSE "
                                 "CAST(TRIM(CAST({0} AS {2}({3}))) AS {2}({3})) "
                                 "END END END").format(export_column,
                                     default_null_val, use_data_type,
                                     src_column_info["column_length"]))
                        elif self.__args.trimflg == "1":
                            export_column_sql_list.append(
                                ("CASE WHEN {0} IS NULL THEN "
                                 "CAST({1} AS {2}({3})) ELSE CASE WHEN {0} = 0 "
                                 "THEN  CAST(RTRIM("
                                 "CAST({0} AS {2}({3}))) AS {2}({3})) ELSE "
                                 "CASE WHEN TRUNC({0}) = 0 THEN "
                                 "CASE WHEN {0} < 0 THEN '-0' || "
                                 "SUBSTR({0}, INSTR({0}, '.')) WHEN {0} = 0 "
                                 "THEN CAST(RTRIM("
                                 "CAST({0} AS {2}({3}))) AS {2}({3})) "
                                 "ELSE '0' || CAST(RTRIM("
                                 "CAST({0} AS {2}({3}))) AS {2}({3})) END ELSE "
                                 "CAST(RTRIM(CAST({0} AS {2}({3}))) AS {2}({3})) "
                                 "END END END").format(export_column,
                                     default_null_val, use_data_type,
                                     src_column_info["column_length"]))
                        else:
                            export_column_sql_list.append(
                                ("CASE WHEN {0} IS NULL THEN "
                                 "CAST({1} AS {2}({3})) ELSE CASE WHEN {0} = 0 "
                                 "THEN  CAST("
                                 "CAST({0} AS {2}({3})) AS {2}({3})) ELSE "
                                 "CASE WHEN TRUNC({0}) = 0 THEN "
                                 "CASE WHEN {0} < 0 THEN '-0' || "
                                 "SUBSTR({0}, INSTR({0}, '.')) WHEN {0} = 0 "
                                 "THEN CAST("
                                 "CAST({0} AS {2}({3})) AS {2}({3})) "
                                 "ELSE '0' || CAST("
                                 "CAST({0} AS {2}({3})) AS {2}({3})) END ELSE "
                                 "CAST(CAST({0} AS {2}({3})) AS {2}({3})) "
                                 "END END END").format(export_column,
                                     default_null_val, use_data_type,
                                     src_column_info["column_length"]))
                    elif (src_column_info['column_base_type'] == "CHAR"
                          or src_column_info['column_base_type'] == "VARCHAR"
                          or src_column_info['column_base_type'] == "VARCHAR2"):
                        if self.__args.repdel == "Y":
                            tmp_str = ("REPLACE({0}, '{1}', '{2}')").format(
                                          export_column, self.__args.delim,
                                          replace_delim)
                        else:
                            tmp_str = export_column

                        if self.__args.trimflg == "0":
                            tmp_str = "TRIM({0})".format(tmp_str)
                        elif self.__args.trimflg == "1":
                            tmp_str = "RTRIM({0})".format(tmp_str)
 
                        if self.__args.repflg == "Y":
                            tmp_str = ("REPLACE(REPLACE({0}, CHR(10),' ')"
                                       ", CHR(13), ' ')").format(tmp_str)

                        export_column_sql_list.append(
                            "CAST({0} AS {1}({2}))".format(
                                tmp_str, use_data_type, 
                                src_column_info["column_length"]))
                    elif (src_column_info['column_base_type'] == "BLOB"
                          or src_column_info['column_base_type'] == "LONG"
                          or src_column_info['column_base_type'] == "RAW"):
                        export_column_sql_list.append(
                            "CAST ( '' AS VARCHAR(10) )")
                    else:
                        export_column_sql_list.append(
                            "CAST({0} AS {1}({2}))".format(export_column,
                                use_data_type, 
                                src_column_info["column_length"]))

        export_column_sql = "\n,".join(export_column_sql_list)
        export_sql = ("SELECT "
                      "\n{0}"
                      "\nFROM {1}.{2}"
                      "\nWHERE {3}").format(export_column_sql,
                          self.__target_db_info['db_schema'],
                          self.__args.table,
                          self.__args.filt)

        return 0, export_sql

    def __get_table_records(self):
        """ 获取表记录数

        Args:

        Returns:
            [0, 记录数] : 成功 | [-1, ''] : 失败
        Raise:

        """
        export_table_records = 0        # 表记录数
        filt = self.__args.filt.upper() # 过滤条件

        LOG.info("获取目标表记录数")

        # 根据过滤条件生成获取记录数的sql
        match_obj = re.match(r"\sROWNUM", filt, re.I)
        if match_obj:
            sql = ("SELECT COUNT(1) AS CNT"
                   "\nFROM (SELECT 1 AS NAME FROM {0}.{1} "
                   "\nWHERE {2}) TMP_TAB").format(
                       self.__target_db_info['db_schema'],
                       self.__args.table, filt)
        else:
            sql = ("SELECT COUNT(*) AS CNT"
                   "\nFROM {0}.{1}"
                   "\nWHERE {2}").format(
                       self.__target_db_info['db_schema'],
                       self.__args.table, filt)

        LOG.info("SQL:\n{0}".format(sql))
        try:
            result_info = self.__db_oper.fetchall_direct(sql)
        except Exception as e:
            LOG.error("获取表[{0}]的记录数失败".format(self.__args.table))
            return -1, []

        export_table_records = int(result_info[0][0])

        LOG.info("目标表的记录数为:{0}".format(export_table_records))

        return 0, export_table_records

    def __generate_export_file(self, export_sql, columns_info,
                               export_table_records):
        """ 生成卸数目标文件

        Args:
            export_sql           : 卸数sql
            columns_info         : 字段信息
            export_table_records : 卸数表记录数 
        Returns:
            [0, 字段数] : 成功 | [-1, ''] : 失败
        Raise:

        """
        LOG.info("执行卸数SQL")

        # 检查文件
        ret = check_path(self.__args.outfile)
        if ret == -1:
            return -1

        charset = ""
        try:
            if self.__args.charset == "1":
                charset = "UTF-8"
            else:
                charset = "GBK"
            DATA_FILE = codecs.open(self.__args.outfile, "w", charset)
        except Exception as e:
            traceback.print_exc()
            LOG.error("打开数据文件失败")
            return -1

        # 执行卸数sql,写输出文件
        LOG.info("SQL:\n{0}".format(export_sql))
        try:
            self.__db_oper.connect()

            while 1:
                line_result_info = self.__db_oper.fetchone(export_sql)
                if line_result_info:    
                    line_str = ""
                    for i in range(len(line_result_info)):
                        if line_result_info[i] != None:
                            column_value = line_result_info[i]
                        else:
                            column_value = ""

                        if i != len(line_result_info) - 1:
                            line_str = "{0}{1}{2}".format(line_str,
                                        column_value, self.__args.delim)
                        else:
                            line_str = "{0}{1}".format(line_str,
                                        column_value)

                    if self.__args.enddel == "Y":
                        line_str = "{0}{1}{2}".format(
                                       line_str, self.__args.delim,
                                       self.__args.rcdelim) 
                    else:
                        line_str = "{0}{1}".format(
                                       line_str, self.__args.rcdelim) 
                    DATA_FILE.write(line_str)
                    self.__export_lines = self.__export_lines + 1
                else:
                    break

            self.__db_oper.close()
        except Exception as e:
            traceback.print_exc()
            LOG.error("获取表[{0}]的记录数失败".format(self.__args.table))
            self.__db_oper.close()
            DATA_FILE.close() 
            return -1, []

        DATA_FILE.close() 

        LOG.info("生成目标文件:{0}".format(self.__args.outfile))
        LOG.info("卸载文件记录数:{0}".format(self.__export_lines))

        if self.__args.ddlfile:
            LOG.info("生成目标DDL文件:{0}".format(self.__args.ddlfile))
            version = "V1.0"

            ret = generate_ddl_file(self.__args.ddlfile, version,
                                    self.__args.table, self.__args.fixed, 
                                    columns_info, charset, "ORACLE",
                                    self.__args.selcol, self.__args.rcdelim)
            if ret != 0:
                return -1
            

        if self.__args.ctlfile:
            LOG.info("生成目标CTRL文件:{0}".format(self.__args.ctlfile))
            ret = generate_ctrl_file(self.__args.ctlfile,
                                    self.__args.outfile, export_table_records,
                                    "ORACLE", self.__args.charset)
            if ret != 0:
                return -1

        return 0

    def run(self):
        """ 卸数主函数

        Args:

        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        ret = 0 # 状态变量
        export_table_records = 0 # 卸数表记录数
        columns_info = [] # 字段信息

        # 获取表结构
        ret, columns_info = self.__get_table_struct()
        if ret != 0:
            return -1, 0, 0

        # 获取表记录数
        ret, export_table_records = self.__get_table_records()
        if ret != 0:
            return -1, 0, 0

        # 获取卸数SQL
        ret, export_sql = self.__get_export_sql(columns_info)
        if ret != 0:
            return -1, 0, 0

        # 执行卸数sql,生成目标卸数文件
        ret = self.__generate_export_file(export_sql, columns_info,
                                          export_table_records)
        if ret != 0:
            return -1, 0, 0

        # 更新DDL信息(通用类型DDL表和元数据表)
        try:
            ddl_operator = DdlOperator()
        except Exception as e:
            return -1, 0, 0

        ret = ddl_operator.load_ddl_direct(self.__args.proid,
                                           self.__args.tableid,
                                           columns_info)
        if ret != 0:
            return -1, 0, 0

        return 0, self.__export_lines, export_table_records

# 加载类 TODO
#class Loader:
#     pass
