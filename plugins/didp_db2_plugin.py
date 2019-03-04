# -*- coding: UTF-8 -*-
################################################################################
# Date Time     : 2018-11-03
# Write By      : adtec(xiazhy,zhaogx)
# Function Desc : DB2库卸数,加载插件
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
from utils.didp_tools import generate_uuid, write_file
from utils.didp_ddl_operator import DdlOperator
from utils.didp_db_operator import DbOperator

DIDP_HOME = os.environ["DIDP_HOME"]

# 日志输出实例
LOG = Logger()

# 指定jdbc类
JDBC_CLASS  = "com.ibm.db2.jcc.DB2Driver"


class Exporter(object):
    """ DB2卸数插件
       使用export卸载    
    ATTributes:
       __args           : 参数
       __target_db_info : 目标库信息(字典)
       __db_oper        : 数据连接类实例
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
               "\n    UPPER(T1.COLNAME) AS COLUMN_NAME,"
               "\n    UPPER(T1.TYPENAME) AS TYPE_NAME,"
               "\n    T1.SCALE AS SCALE,"
               "\n    T1.LENGTH AS LENGTH,"
               "\n    (CASE "
               "\n        WHEN T1.NULLS = 'N' THEN '0' "
               "\n        ELSE '1' "
               "\n      END) AS IS_NULL,"
               "\n    (CASE "
               "\n        WHEN T2.COLNAME IS NULL THEN '0' "
               "\n        ELSE '1' "
               "\n      END) AS IS_PK,"
               "\n    T1.REMARKS AS COLUMN_DESC, "
               "\n    T1.TABLE_REMARKS AS TABLE_DESC "
               "\nFROM "
               "\n    ( "
               "\n      SELECT TT1.*, TT2.REMARKS AS TABLE_REMARKS, TT2.OWNER "
               "\n      FROM SYSCAT.COLUMNS TT1, SYSCAT.TABLES TT2  "
               "\n      WHERE TT1.TABSCHEMA = TT2.TABSCHEMA "
               "\n        AND TT1.TABNAME=TT2.TABNAME "
               "\n        AND UPPER(TT1.TABSCHEMA) = '{0}' "
               "\n        AND UPPER(TT1.TABNAME) = '{1}' "
               "\n    )T1"
               "\nLEFT JOIN"
               "\n    ("
               "\n      SELECT TT1.TABSCHEMA, TT1.TABNAME, TT2.COLNAME "
               "\n      FROM SYSCAT.INDEXES TT1 , SYSCAT.INDEXCOLUSE TT2 "
               "\n      WHERE TT1.INDSCHEMA = TT2.INDSCHEMA "
               "\n        AND TT1.INDNAME=TT2.INDNAME AND TT1.UNIQUERULE='P'"
               "\n        AND UPPER(TT1.TABSCHEMA)='{0}'"
               "\n        AND UPPER(TT1.TABNAME) = '{1}' "
               "\n    )T2"
               "\nON UPPER(T1.TABNAME) = UPPER(T2.TABNAME) "
               "\n        AND T1.TABSCHEMA = T2.TABSCHEMA"
               "\n        AND T1.COLNAME = T2.COLNAME"
               "\nWHERE UPPER(T1.TABSCHEMA) = '{0}' "
               "\n    AND UPPER(T1.TABNAME) = '{1}' "
               "\n    ORDER BY T1.TABNAME, T1.COLNO").format(
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
            data_type = result_info[i][1]   # 数据类型(不带长度)
            column_define_length = 0        # 字段类型中定义的长度
             
            # None结果区分,取整
            if result_info[i][2] != None:
                data_scale = int(result_info[i][2])
            else:
                data_scale = 0
 
            if result_info[i][3] != None:
                data_length = int(result_info[i][3])
            else:
                data_length = 0
           
            is_null = result_info[i][4]     # 是否可空
            is_pk = result_info[i][5]       # 是否主键
            column_desc = result_info[i][6] # 字段描述
            table_desc = result_info[i][7]  # 表描述

            # 调整类型,生成通用类型
            if data_type == "CHARACTER":
                column_base_type = "VARCHAR"
                column_length = data_length
                column_type = "CHARACTER({0})".format(data_length)
                column_std_type = "{0}!anc".format(data_length)
                column_define_length = data_length
            elif data_type == "VARCHAR":
                column_base_type = "VARCHAR"
                column_type = "VARCHAR({0})".format(data_length)
                column_length = data_length
                column_std_type = "anc..{0}".format(data_length)
                column_define_length = data_length
            elif data_type == "SMALLINT":
                column_base_type = "NUMERIC0"
                column_type = "SMALLINT"
                column_length = 5 + 1
                column_std_type = "5n"
            elif data_type == "INTEGER":
                column_base_type = "NUMERIC0"
                column_type = "INTEGER"
                column_length = 10 + 1
                column_std_type = "10n"
            elif data_type == "BIGINT":
                column_base_type = "NUMERIC0"
                column_type = "BIGINT"
                column_length = 19 + 1
                column_std_type = "19n"
            elif (data_type == "REAL" or data_type == "DECFLOAT"
                  or data_type == "DOUBLE"):
                column_base_type = "FLOAT"
                column_type = data_type
                column_length = 38 + 2
                column_std_type = "31n(10)"
            elif data_type == "DECIMAL":
                column_base_type = "NUMERIC"
                if data_scale > 0:
                    column_type = "DECIMAL({0},{1})".format(data_length, 
                                                            data_scale)
                    column_length = data_length + 2
                    column_std_type = "{0}n({1})".format(data_length+1,
                                                         data_scale)
                else:
                    column_type = "DECIMAL({0})".format(data_length)
                    column_length = data_length + 1
                    column_std_type = "{0}n".format(data_length)

                column_define_length = data_length
            elif data_type == "DATE":
                column_base_type = "DATE"
                column_type = "DATE"
                column_length = 10
                column_std_type = "YYYY-MM-DD"
            elif data_type == "TIME":
                column_base_type = "TIME"
                column_type = "TIME"
                column_length = 8
                column_std_type = "HH:MM:SS"
            elif data_type == "TIMESTAMP":
                column_base_type = "TIMESTAMP({0})".format(data_scale)
                column_type = "TIMESTAMP({0})".format(data_scale)
                if data_scale > 0:
                    column_length = 19 + 1 + data_scale
                    if data_scale == 3:
                        column_std_type = "YYYY-MM-DDTHH:MM:SS.NNN"
                    elif data_scale == 6:
                        column_std_type = "YYYY-MM-DDTHH:MM:SS.NNNNNN"
                else:
                    column_length = 19
                    column_std_type = "YYYY-MM-DDTHH:MM:SS"
                column_define_length = data_scale
            elif data_type == "CLOB" or data_type == "BLOB":
                column_base_type = data_type
                column_type = data_type
                column_length = 4000
                column_std_type = data_type
            elif (data_type == "LONG VARCHAR" or data_type == "LONG VARGRAPHIC"
                  or data_type == "VARGRAPHIC" or data_type == "GRAPHIC"):
                column_base_type = data_type
                column_type = data_type
                column_length = 4000
                column_std_type = "anc..4000"
            else:
                column_base_type = data_type
                column_type = data_type
                column_length = data_length
                column_std_type = data_type

            column_info['column_name'] = column_name
            column_info['column_base_type'] = column_base_type
            column_info['data_type'] = data_type
            column_info['column_type'] = column_type
            column_info['column_length'] = column_length
            column_info['column_define_length'] = column_define_length
            column_info['column_scale'] = data_scale
            column_info['column_std_type'] = column_std_type
            column_info['is_null'] = is_null
            column_info['is_pk'] = is_pk
            column_info['partition_flag'] = "0"
            column_info['bucket_flag'] = "0"
            column_info['table_desc'] = table_desc
            column_info['fixed'] = self.__args.fixed
            column_info['rcdelim'] = self.__args.rcdelim
            column_info['delim'] = self.__args.delim
            column_info['table_name'] = self.__args.table
            column_info['quote_type'] = "0"
            if column_desc != None:
                column_info['column_desc'] = column_desc
            else:
                column_info['column_desc'] = "" 

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
        def_time = "'00:00:00'"   # 默认时间值
        # 默认时间戳(6位毫秒)
        def_timestamp6 = "'1900-01-01 00:00:00.000000'" 
        # 默认时间戳(3位毫秒)
        def_timestamp3 = "'1900-01-01 00:00:00.000'"
        # 默认时间戳格式(6位毫秒)
        def_timestamp = "'1900-01-01 00:00:00'"
        def_string = "' '" # 默认字符串
        def_number = "'0'" # 默认数值
        def_date_format = "'YYYY-MM-DD'" # 默认日期格式
        def_time_format = "'HH24:MI:SS'" # 默认时间格式
        # 默认时间戳格式(无毫秒)
        def_timestamp_format = "'YYYY-MM-DD HH24:MI:SS'"
        # 默认时间戳格式(3位毫秒)
        def_timestamp_format3 = "'YYYY-MM-DD HH24:MI:SS.FF3'"
        # 默认时间戳格式(6位毫秒)
        def_timestamp_format6 = "'YYYY-MM-DD HH24:MI:SS.FF6'"

        use_data_type = ""    # 默认转换字符类型
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
            default_null_val = "' '"
        else:
            use_data_type = "VARCHAR"
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
                if self.__args.setdef == "Y":
                    if src_column_info['column_base_type'] == "DATE":
                        export_column_sql_list.append(
                            ("CAST( CASE WHEN {0} IS NULL THEN {1} "
                             "ELSE TO_CHAR({0},{2}) END AS {3}({4}) )").format(
                                 export_column, def_date,
                                 def_date_format, use_data_type, 
                                 src_column_info["column_length"]))
                    elif src_column_info['column_base_type'] == "TIME":
                        export_column_sql_list.append(
                            ("CAST(CASE WHEN {0} IS NULL THEN {1} "
                             "ELSE REPLACE(TO_CHAR(HOUR({0}),'00')"
                             "||':'||TO_CHAR(MINUTE({0}),'00')||':'"
                             "||TO_CHAR(SECOND({0}),'00'),' ','') "
                             "END AS {2}({3}))").format(
                                 export_column, def_time, use_data_type, 
                                 src_column_info["column_length"]))
                    elif (src_column_info['column_base_type'] == "FLOAT"
                          or src_column_info['column_base_type'] == "REAL"
                          or src_column_info['column_base_type'] == "DOUBLE"):
                        export_column_sql_list.append(
                            ("CASE WHEN {0} IS NULL OR {0} = 0 THEN '0.00' "
                             "ELSE CASE WHEN TRUNC({0}) = 0 THEN "
                             "CASE WHEN {0} < 0 THEN '-0' || "
                             "SUBSTR(CAST(CAST({0} AS DECIMAL(31,10)) "
                             "AS VARCHAR(33)) , INSTR(CAST(CAST({0} "
                             "AS DECIMAL(31,10)) AS VARCHAR(33)) , '.')) "
                             "ELSE '0' || CAST(CAST({0} AS DECIMAL(31,10)) "
                             "AS VARCHAR(33)) END ELSE "
                             "CAST(CAST({0} AS DECIMAL(31,10)) AS VARCHAR(33))"
                             " END END").format(export_column))
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
                    elif (src_column_info['column_base_type'] == "NUMERIC"
                          or src_column_info['column_base_type'] == "NUMERIC0"):
                        if self.__args.trimflg == "0":
                            export_column_sql_list.append(
                                ("CASE WHEN {0} IS NULL OR {0} = 0 THEN "
                                 "CAST({1} AS {2}({3})) ELSE CASE WHEN "
                                 "TRUNC({0}) = 0 THEN CASE WHEN {0} < 0 "
                                 "THEN '-0' || SUBSTR(CAST(TRIM(CAST({0} "
                                 "AS {2}({3}))) AS {2}({3})), INSTR("
                                 "CAST(TRIM(CAST({0} AS {2}({3}))) AS "
                                 "{2}({3})), '.')) ELSE '0' || CAST(TRIM("
                                 "CAST({0} AS {2}({3}))) AS {2}({3})) END "
                                 "ELSE CAST(TRIM(CAST({0} AS {2}({3}))) "
                                 "AS {2}({3})) END END").format( 
                                     export_column, def_number, use_data_type, 
                                     src_column_info["column_length"]))
                        elif self.__args.trimflg == "1":
                            export_column_sql_list.append(
                                ("CASE WHEN {0} IS NULL OR {0} = 0 THEN "
                                 "CAST({1} AS {2}({3})) ELSE CASE WHEN "
                                 "TRUNC({0}) = 0 THEN CASE WHEN {0} < 0 "
                                 "THEN '-0' || SUBSTR(CAST(RTRIM(CAST({0} "
                                 "AS {2}({3}))) AS {2}({3})), INSTR("
                                 "CAST(RTRIM(CAST({0} AS {2}({3}))) AS "
                                 "{2}({3})), '.')) ELSE '0' || CAST(RTRIM("
                                 "CAST({0} AS {2}({3}))) AS {2}({3})) END "
                                 "ELSE CAST(RTRIM(CAST({0} AS {2}({3}))) "
                                 "AS {2}({3})) END END").format( 
                                     export_column, def_number, use_data_type, 
                                     src_column_info["column_length"]))
                        else:
                            export_column_sql_list.append(
                                ("CASE WHEN {0} IS NULL OR {0} = 0 THEN "
                                 "CAST({1} AS {2}({3})) ELSE CASE WHEN "
                                 "TRUNC({0}) = 0 THEN CASE WHEN {0} < 0 "
                                 "THEN '-0' || SUBSTR(CAST(CAST({0} "
                                 "AS {2}({3})) AS {2}({3})), INSTR("
                                 "CAST(CAST({0} AS {2}({3})) AS "
                                 "{2}({3})), '.')) ELSE '0' || CAST("
                                 "CAST({0} AS {2}({3})) AS {2}({3})) END "
                                 "ELSE CAST(CAST({0} AS {2}({3})) "
                                 "AS {2}({3})) END END").format( 
                                     export_column, def_number, use_data_type, 
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
                          or src_column_info['column_base_type'] == "VARGRAPHIC"
                          or src_column_info['column_base_type'] == "LONG VARGRAPHIC"
                          or src_column_info['column_base_type'] == "GRAPHIC"):
                        export_column_sql_list.append(
                            "CAST ( '' AS VARCHAR(10) )")
                    elif src_column_info['column_base_type'] == "LONG VARCHAR":
                        if self.__args.repdel == "Y":
                            tmp_str = ("REPLACE({0}, '{1}', '{2}')").format(
                                          export_column, self.__args.delim,
                                          replace_delim)
                        else:
                            tmp_str = export_column

                        if self.__args.repflg == "Y":
                            tmp_str = ("REPLACE(REPLACE({0}, CHR(10),' ')"
                                       ", CHR(13), ' ')").format(tmp_str)

                        export_column_sql_list.append(
                            ("CASE WHEN {0} IS NULL "
                             "THEN CAST({1} AS {2}({3})) ELSE "
                             "{0} END").format(tmp_str,
                                 def_string, use_data_type, 
                                 src_column_info["column_length"]))
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
                            ("CAST( CASE WHEN {0} IS NULL THEN {1} "
                             "ELSE TO_CHAR({0},{2}) END AS {3}({4}) )").format(
                                export_column, default_null_val,
                                def_date_format, use_data_type, 
                                src_column_info["column_length"]))
                    elif src_column_info['column_base_type'] == "TIME":
                        export_column_sql_list.append(
                            ("CAST(CASE WHEN {0} IS NULL THEN {1} "
                             "ELSE REPLACE(TO_CHAR(HOUR({0}),'00')"
                             "||':'||TO_CHAR(MINUTE({0}),'00')||':'"
                             "||TO_CHAR(SECOND({0}),'00'),' ','') "
                             "END AS {2}({3}))").format(
                                 export_column, default_null_val, use_data_type,
                                 src_column_info["column_length"]))
                    elif (src_column_info['column_base_type'] == "FLOAT"
                          or src_column_info['column_base_type'] == "REAL"
                          or src_column_info['column_base_type'] == "DOUBLE"):
                        export_column_sql_list.append(
                            ("CASE WHEN {0} IS NULL THEN '' "
                             "ELSE CASE WHEN TRUNC({0}) = 0 THEN "
                             "CASE WHEN {0} < 0 THEN '-0' || "
                             "SUBSTR(CAST(CAST({0} AS DECIMAL(31,10)) "
                             "AS VARCHAR(33)) , INSTR(CAST(CAST({0} "
                             "AS DECIMAL(31,10)) AS VARCHAR(33)) , '.')) "
                             "ELSE '0' || CAST(CAST({0} AS DECIMAL(31,10)) "
                             "AS VARCHAR(33)) END ELSE "
                             "CAST(CAST({0} AS DECIMAL(31,10)) AS VARCHAR(33))"
                             " END END").format(export_column))
                    elif src_column_info['column_base_type'] == "TIMESTAMP(3)":
                        export_column_sql_list.append(
                            ("CAST(CASE WHEN {0} IS NULL THEN {1} "
                             "ELSE TO_CHAR({0},{2}) END AS {3}({4}))").format(
                                export_column, default_null_val, 
                                def_timestamp_format3, use_data_type, 
                                src_column_info["column_length"]))
                    elif src_column_info['column_base_type'] == "TIMESTAMP(6)":
                        export_column_sql_list.append(
                             ("CAST(CASE WHEN {0} IS NULL THEN {1} "
                              "ELSE TO_CHAR({0},{2}) END AS {3}({4}))").format(
                                export_column, default_null_val,
                                def_timestamp_format6, use_data_type, 
                                src_column_info["column_length"]))
                    elif src_column_info['column_base_type'] == "TIMESTAMP(0)":
                        export_column_sql_list.append(
                             ("CAST(CASE WHEN {0} IS NULL THEN {1} "
                              "ELSE TO_CHAR({0},{2}) END AS {3}({4}))").format(
                                export_column, def_timestamp,
                                def_timestamp_format, use_data_type, 
                                src_column_info["column_length"]))
                    elif (src_column_info['column_base_type'] == "NUMERIC"
                          or src_column_info['column_base_type'] == "NUMERIC0"):
                        if self.__args.trimflg == "0":
                            export_column_sql_list.append(
                                ("CASE WHEN {0} IS NULL THEN "
                                 "CAST({1} AS {2}({3})) ELSE "
                                 "CASE WHEN {0} = 0 THEN "
                                 "'0' "
                                 "ELSE CASE WHEN TRUNC({0}) = 0 THEN "
                                 "CASE WHEN {0} < 0 THEN '-0' || "
                                 "SUBSTR(CAST(TRIM(CAST({0} AS {2}({3}))) "
                                 "AS {2}({3})), INSTR(CAST(TRIM(CAST({0} "
                                 "AS {2}({3}))) AS {2}({3})), '.')) ELSE '0' "
                                 "|| CAST(TRIM(CAST({0} AS {2}({3}))) "
                                 "AS {2}({3})) END ELSE CAST(TRIM(CAST({0} "
                                 "AS {2}({3}))) AS {2}({3})) "
                                 "END END END").format( 
                                     export_column, default_null_val, 
                                     use_data_type, 
                                     src_column_info["column_length"]))
                        elif self.__args.trimflg == "1":
                            export_column_sql_list.append(
                                ("CASE WHEN {0} IS NULL THEN "
                                 "CAST({1} AS {2}({3})) ELSE "
                                 "CASE WHEN {0} = 0 THEN "
                                 "'0' "
                                 "ELSE CASE WHEN TRUNC({0}) = 0 THEN "
                                 "CASE WHEN {0} < 0 THEN '-0' || "
                                 "SUBSTR(CAST(RTRIM(CAST({0} AS {2}({3}))) "
                                 "AS {2}({3})), INSTR(CAST(RTRIM(CAST({0} "
                                 "AS {2}({3}))) AS {2}({3})), '.')) ELSE '0' "
                                 "|| CAST(RTRIM(CAST({0} AS {2}({3}))) "
                                 "AS {2}({3})) END ELSE CAST(RTRIM(CAST({0} "
                                 "AS {2}({3}))) AS {2}({3})) "
                                 "END END END").format( 
                                     export_column, default_null_val, 
                                     use_data_type, 
                                     src_column_info["column_length"]))
                        else:
                            export_column_sql_list.append(
                                ("CASE WHEN {0} IS NULL THEN "
                                 "CAST({1} AS {2}({3})) ELSE "
                                 "CASE WHEN {0} = 0 THEN "
                                 "'0' "
                                 "ELSE CASE WHEN TRUNC({0}) = 0 THEN "
                                 "CASE WHEN {0} < 0 THEN '-0' || "
                                 "SUBSTR(CAST(CAST({0} AS {2}({3})) "
                                 "AS {2}({3})), INSTR(CAST(CAST({0} "
                                 "AS {2}({3})) AS {2}({3})), '.')) ELSE '0' "
                                 "|| CAST(CAST({0} AS {2}({3})) "
                                 "AS {2}({3})) END ELSE CAST(CAST({0} "
                                 "AS {2}({3})) AS {2}({3})) "
                                 "END END END").format( 
                                     export_column, default_null_val, 
                                     use_data_type, 
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
                                 default_null_val, use_data_type, 
                                 src_column_info["column_length"]))
                    elif (src_column_info['column_base_type'] == "BLOB"
                         or src_column_info['column_base_type'] == "VARGRAPHIC"
                         or src_column_info['column_base_type'] == "LONG VARGRAPHIC"
                         or src_column_info['column_base_type'] == "GRAPHIC"):
                        export_column_sql_list.append(
                            "CAST ( '' AS VARCHAR(10) )")
                    elif src_column_info['column_base_type'] == "LONG VARCHAR":
                        if self.__args.repdel == "Y":
                            tmp_str = ("REPLACE({0}, '{1}', '{2}')").format(
                                          export_column, self.__args.delim,
                                          replace_delim)
                        else:
                            tmp_str = export_column
 
                        if self.__args.repflg == "Y":
                            tmp_str = ("REPLACE(REPLACE({0}, CHR(10),' ')"
                                       ", CHR(13), ' ')").format(tmp_str)

                        export_column_sql_list.append(
                            ("CASE WHEN {0} IS NULL "
                             "THEN CAST({1} AS {2}({3})) ELSE "
                             "{0} END").format(tmp_str,
                                 default_null_val, use_data_type, 
                                 src_column_info["column_length"]))
                    else:
                        export_column_sql_list.append(
                            ("CASE WHEN {0} IS NULL THEN "
                             "CAST({1} AS {2}({3})) ELSE "
                             "CAST({0} AS {2}({3})) END").format(export_column,
                                 default_null_val, use_data_type, 
                                 src_column_info["column_length"]))

        # 分隔符长度大于1的时候，先拼接前几个分隔符
        delim_len = len(self.__args.delim)
        if delim_len > 1:
            delim = self.__args.delim[0:-1]
            export_column_sql = " || '{0}'\n,".format(delim).join(
                                    export_column_sql_list)
        else:
            export_column_sql = "\n,".join(export_column_sql_list)

        if self.__args.enddel == "Y":
            export_column_sql += " || CAST('{0}' AS {1}({2}))".format(
                                     self.__args.delim, use_data_type,
                                     delim_len)

        # export工具默认的换行符为\n, 去掉sql上拼接的记录分隔符末的\n
        rcdelim = self.__args.rcdelim
        match_obj = re.match(".*?\n$", rcdelim)
        if not match_obj:
            LOG.error(r"不支持非\n结尾的记录结束符") 
            return -1, ''

        rcdelim = re.sub("(.*?)\n$", r"\1", rcdelim)
        rcdelim_len = len(self.__args.rcdelim) 

        export_column_sql += " || CAST('{0}' AS {1}({2}))".format(
                                 rcdelim, use_data_type, rcdelim_len)

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
        match_obj = re.match(r"\s+fetch\s+first\s+\d+rows\s+only\s+$", filt, re.I)
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
        self.__export_lines = export_table_records

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

        # 字符集转换
        charset = ""
        if self.__args.charset == "1":
            charset = "UTF-8"
        else:
            charset = "GBK"

        # 包含export的sql文件
        cfg_file = "{0}/tmp/.{1}.sql".format(DIDP_HOME, generate_uuid())

        # 检查文件目录是否存在
        ret = check_path(self.__args.outfile)
        if ret == -1:
            return -1

        # 检查文件是否存在
        ret = check_path(cfg_file)
        if ret == -1:
            return -1

        # 取分隔符最后一个字节,转成十六进制
        # export支持单个字节分隔
        delim = self.__args.delim
        if len(self.__args.delim) > 1:
            delim = "{:#X}".format(ord(self.__args.delim[-1]))

        cfg_txt = ("CONNECT TO {0} USER {1} USING {2};\n"
                   "EXPORT TO {3} OF DEL MODIFIED BY COLDEL{4} "
                   "NOCHARDEL CODEPAGE=1208 {5};\n"
                   "DISCONNECT CURRENT;\n"
                   "QUIT;\n").format(self.__target_db_info["db_name"],
                                     self.__target_db_info["db_user"],
                                     self.__target_db_info["db_pwd"],
                                     self.__args.outfile,
                                     delim,
                                     export_sql)

        LOG.info("使用export命令卸载数据,Command:\n{0}".format(cfg_txt))

        # 写配置文件
        ret = write_file(cfg_file, cfg_txt, "utf8")
        if ret != 0:
            return -1

        # 执行export
        ret = os.system("db2 -stvf {0}".format(cfg_file))
        ret = ret >> 8
        if ret != 0 and ret != 2:
            LOG.info("执行exprot失败:{0}".format(ret))
            return -1

        LOG.info("生成目标文件:{0}".format(self.__args.outfile))
        LOG.info("卸载文件记录数:{0}".format(self.__export_lines))

        if self.__args.ddlfile:
            LOG.info("生成目标DDL文件:{0}".format(self.__args.ddlfile))
            version = "V1.0"

            ret = generate_ddl_file(self.__args.ddlfile, version,
                                    self.__args.table, self.__args.fixed, 
                                    columns_info, charset, "DB2",
                                    self.__args.selcol, self.__args.rcdelim)
            if ret != 0:
                return -1
            

        if self.__args.ctlfile:
            LOG.info("生成目标CTRL文件:{0}".format(self.__args.ctlfile))
            ret = generate_ctrl_file(self.__args.ctlfile,
                                    self.__args.outfile, export_table_records,
                                    "DB2", self.__args.charset)
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
