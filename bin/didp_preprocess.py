#-*- coding: UTF-8 -*-  
################################################################################
# Date Time     : 2018-10-26
# Write By      : adtec(zhaogx)
# Function Desc : 预处理组件主程序
#
# History       :
#                 20181110  zhaogx     Create
#
# Remarks       :
################################################################################
import os
import re
import sys
import glob
import argparse
import traceback
import importlib
import jaydebeapi

reload(sys)
sys.setdefaultencoding('utf8')

sys.path.append("{0}".format(os.environ["DIDP_HOME"]))

from datetime import datetime
from utils.didp_logger import Logger
from utils.didp_db_operator import DbOperator
from utils.didp_log_recorder import LogRecorder
from utils.didp_tools import check_path
from utils.didp_tools import generate_common_ddl_file, write_file
from utils.didp_tools import generate_common_ddl_type, generate_schema_file

# 全局变量
LOG = Logger()     
DIDP_HOME = os.environ["DIDP_HOME"]

# 配置库用户
DIDP_CFG_DB_USER = os.environ["DIDP_CFG_DB_USER"]
# 配置库用户密码
DIDP_CFG_DB_PWD = os.environ["DIDP_CFG_DB_PWD"]

DIDP_CFG_DB_DATA_SOURCE = os.environ["DIDP_CFG_DB_DATA_SOURCE"]
# JDBC信息
DIDP_CFG_DB_JDBC_CLASS = os.environ["DIDP_CFG_DB_JDBC_CLASS"]
DIDP_CFG_DB_JDBC_URL = os.environ["DIDP_CFG_DB_JDBC_URL"]


class PreProcess(object):
    """ 数据预处理类
    
    Attributes:
       __args : 参数
    """
    def __init__(self, args):
        self.__args             = args # 参数
        self.__pre_info         = []   # 预处理信息
        self.__check_info       = []   # 检核信息
        self.__split_info       = []   # 拆分信息
        self.__trans_info       = []   # 转换信息
        self.__select_info      = []   # 筛选信息
        self.__decompose_info   = []   # 分解信息
        self.__final_ddl_info   = []   # 最终DDL信息
        self.__osh_script       = ""   # Osh脚本路径
        self.__stage_level      = 1    # Osh Stage层级,用于拼接Stage
        self.__src_delim        = ""   # 源分隔符
        self.__src_final_delim  = ""   # 源末尾字段分隔符
        self.__src_record_delim = ""   # 源记录结束符
        self.__quote            = ""   # 定界符
        self.__dump_delim       = ""   # 检核落地文件格式
        self.__line_break       = ""   # 文件记录结束符 
        self.__fixed            = ""   # 文件定长标识

        self.__input_lines = 0  # 输入文件记录数
        self.__output_lines = 0 # 输出文件记录数
        self.__reject_lines = 0 # 拒绝文件记录数

        # Osh文件
        self.__osh_file = "{0}/tmp/.{1}.osh".format(DIDP_HOME,
                                                    self.__args.preid)

        # 检核Schema文件
        self.__schema_file = "{0}/tmp/.{1}.xml".format(DIDP_HOME,
                                                       self.__args.preid)

        # Log文件
        self.__log_file = "{0}/tmp/.{1}.log".format(DIDP_HOME,
                                                    self.__args.preid)

        # 数据库操作类实例
        self.__db_oper = DbOperator(DIDP_CFG_DB_USER,
                                    DIDP_CFG_DB_PWD,
                                    DIDP_CFG_DB_JDBC_CLASS,
                                    DIDP_CFG_DB_JDBC_URL)

        # apt文件未指定时使用默认的文件
        if self.__args.aptfile == None:
            self.__args.aptfile = os.environ["APT_CONFIG_FILE"]

        self.__log_recorder = LogRecorder(process_id=self.__args.proid,
                                          system_key=self.__args.system,
                                          branch_no=self.__args.org,
                                          biz_date=self.__args.bizdate,
                                          batch_no=self.__args.batch,
                                          table_name=self.__args.table,
                                          table_id=self.__args.tableid,
                                          job_type="3")
        
        # 打印参数
        self.__print_arguments()

    def __print_arguments(self):
        """ 参数格式化输出

        Args:

        Returns:

        Raise:

        """
        LOG.debug("-------------------参数清单-------------------")
        LOG.debug("流程ID               : {0}".format(self.__args.proid))
        LOG.debug("预处理ID             : {0}".format(self.__args.preid))
        LOG.debug("系统标识             : {0}".format(self.__args.system))
        LOG.debug("批次号               : {0}".format(self.__args.batch))
        LOG.debug("机构号               : {0}".format(self.__args.org))
        LOG.debug("业务日期             : {0}".format(self.__args.bizdate))
        LOG.debug("表名                 : {0}".format(self.__args.table))
        LOG.debug("表ID                 : {0}".format(self.__args.tableid))
        LOG.debug("输入文件(全路径)     : {0}".format(self.__args.infile))
        LOG.debug("输出文件(全路径)     : {0}".format(self.__args.outfile))
        LOG.debug("APT_CONFIG_FILE文件  : {0}".format(self.__args.aptfile))
        LOG.debug("允许的拒绝记录百分比 : {0}".format(self.__args.rjlimit))
        if self.__args.macro:        
            LOG.debug("默认值宏             : {0}".format(self.__args.macro))
        LOG.debug("----------------------------------------------")

    def __get_cfg_info(self):
        """ 从配置库中查询预处理的各项配置信息

        Args:

        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        result_info = [] # 查询结果

        LOG.info("获取预处理的配置信息")

        sql = ("SELECT "
              "\n TRANSCODING_FLAG,"
              "\n TAR_TABLE_ID"
              "\n FROM DIDP_PRE_PROCESS_INFO"
              "\n WHERE PRE_PROCESS_ID='{0}'").format(self.__args.preid)

        LOG.info("SQL:\n{0}".format(sql))

        try:
            result_info = self.__db_oper.fetchall_direct(sql)
        except Exception as e:
            traceback.print_exc()
            return -1

        if len(result_info) == 0:
            LOG.error(("未获取到预处理ID为[{0}]"
                      "的数据预处理信息").format(self.__args.preid))
            return -1

        self.__pre_info = result_info

        LOG.info("获取待处理表的DDL信息")

        sql = ("SELECT "
              "\n T4.COLUMN_ID,"
              "\n T4.PROJECT_VERSION_ID,"
              "\n T4.COL_SEQ,"
              "\n T4.COL_NAME,"
              "\n T4.COL_DESC,"
              "\n T5.DDL_FIELDTYPE,"
              "\n T4.COL_TYPE,"
              "\n T4.COL_LENGTH,"
              "\n T4.COL_SCALE,"
              "\n T4.COL_DEFAULT,"
              "\n T4.NULL_FLAG,"
              "\n T4.PK_FLAG,"
              "\n T4.PARTITION_FLAG,"
              "\n T4.BUCKET_FLAG,"
              "\n T4.DESCRIPTION,"
              "\n T5.DDL_DELIM,"
              "\n T5.ISFIXED,"
              "\n T5.RECORD_DELIM,"
              "\n T5.DDL_LENGTH,"
              "\n T5.QUOTE_TYPE"
              "\n FROM "
              "\n DIDP_DATA_ACCESS_INFO T1,"
              "\n DIDP_COLLECTION_INFO T2,"
              "\n DIDP_PRE_PROCESS_INFO T3,"
              "\n DIDP_META_COLUMN_INFO T4,"
              "\n DIDP_COMMON_DDL_INFO T5"
              "\n WHERE T3.COLLECTION_ID = T2.COLLECTION_ID"
              "\n AND T1.DATA_ACCESS_ID = T2.DATA_ACCESS_ID"
              "\n AND T1.TABLE_ID = T4.TABLE_ID"
              "\n AND T4.TABLE_ID = T5.TABLE_ID"
              "\n AND T4.COL_NAME = T5.DDL_FIELDNAME"
              "\n AND T3.PRE_PROCESS_ID = '{0}'"
              "\n ORDER BY T4.COL_SEQ").format(self.__args.preid)

        LOG.info("SQL:\n{0}".format(sql))

        try:
            result_info = self.__db_oper.fetchall_direct(sql)
        except Exception as e:
            traceback.print_exc()
            return -1

        if len(result_info) == 0:
            LOG.error("未找到待处理表的DDL信息")
            return -1
        else:
            self.__ddl_info = result_info
            self.__fixed = result_info[0][16]

        LOG.info("获取字段检核预处理配置")

        sql = ("SELECT "
              "\n T2.COL_NAME,"
              "\n T1.CH_FLAG,"
              "\n T1.NUM_FLAG,"
              "\n T1.CHAR_FLAG"
              "\n FROM "
              "\n DIDP_PRE_CHECK_CONFIG T1,"
              "\n DIDP_META_COLUMN_INFO T2"
              "\n WHERE T1.SRC_COL_ID = T2.COLUMN_ID"
              "\n AND PRE_PROCESS_ID = '{0}'").format(self.__args.preid)

        LOG.info("SQL:\n{0}".format(sql))

        try:
            result_info = self.__db_oper.fetchall_direct(sql)
        except Exception as e:
            traceback.print_exc()
            return -1

        self.__check_info = result_info

        LOG.info("获取字段转换预处理配置")

        sql = ("SELECT "
               "\nT1.COL_NAME, "
               "\nT2.TRANS_RULE_TYPE, "
               "\nT2.POSITION_FROM, "
               "\nT2.POSITION_TO "
               "\nFROM DIDP_PRE_TRANS_CONFIG T1, DIDP_TRANS_RULES_INFO T2 "
               "\nWHERE T1.TRANS_RULE_ID = T2.TRANS_RULE_ID"
               "\nAND T1.PRE_PROCESS_ID = '{0}'").format(self.__args.preid)

        LOG.info("SQL:\n{0}".format(sql))

        try:
            result_info = self.__db_oper.fetchall_direct(sql)
        except Exception as e:
            traceback.print_exc()
            return -1

        self.__trans_info = result_info

        LOG.info("获取字段拆分预处理配置")

        sql = ("SELECT "
              "\n T2.COL_NAME,"
              "\n T1.TABLE_ID"
              "\n FROM "
              "\n DIDP_PRE_SPLIT_CONFIG T1,"
              "\n DIDP_META_COLUMN_INFO T2"
              "\n WHERE T1.SRC_COL_ID = T2.COLUMN_ID"
              "\n AND PRE_PROCESS_ID = '{0}'").format(self.__args.preid)

        LOG.info("SQL:\n{0}".format(sql))

        try:
            result_info = self.__db_oper.fetchall_direct(sql)
        except Exception as e:
            traceback.print_exc()
            return -1

        self.__split_info = result_info

        LOG.info("获取字段分解预处理配置")

        sql = ("SELECT "
              "\n T2.COL_NAME"
              "\n FROM "
              "\n DIDP_PRE_DECOMPOSE_CONFIG T1,"
              "\n DIDP_META_COLUMN_INFO T2"
              "\n WHERE T1.SRC_COL_ID = T2.COLUMN_ID"
              "\n AND PRE_PROCESS_ID = '{0}'").format(self.__args.preid)

        LOG.info("SQL:\n{0}".format(sql))

        try:
            result_info = self.__db_oper.fetchall_direct(sql)
        except Exception as e:
            traceback.print_exc()
            return -1

        self.__decompose_info = result_info

        LOG.info("获取字段筛选预处理配置")

        sql = ("SELECT "
              "\n A.TABLE_ID,"
              "\n A.SRC_COL_ID,"
              "\n A.SRC_COL_NAME,"
              "\n A.CUR_COL_NAME,"
              "\n A.COL_DESC,"
              "\n A.SRC_COL_TYPE,"
              "\n A.CUR_COL_TYPE,"
              "\n A.COL_LENGTH,"
              "\n A.COL_SCALE,"
              "\n A.COL_DEFAULT,"
              "\n A.NULL_FLAG,"
              "\n A.PK_FLAG,"
              "\n A.PARTITION_FLAG,"
              "\n A.BUCKET_FLAG,"
              "\n A.DESCRIPTION,"
              "\n A.COL_SEQ,"
              "\n B.DDL_FIELDTYPE FROM ("
              "\nSELECT"
              "\n T2.TABLE_ID,"
              "\n T1.SRC_COL_ID,"
              "\n T2.COL_NAME AS SRC_COL_NAME,"
              "\n T1.COL_NAME AS CUR_COL_NAME,"
              "\n T1.COL_DESC,"
              "\n T2.COL_TYPE AS SRC_COL_TYPE,"
              "\n T1.COL_TYPE AS CUR_COL_TYPE,"
              "\n T1.COL_LENGTH,"
              "\n T1.COL_SCALE,"
              "\n T1.COL_DEFAULT,"
              "\n T1.NULL_FLAG,"
              "\n T1.PK_FLAG,"
              "\n T1.PARTITION_FLAG,"
              "\n T1.BUCKET_FLAG,"
              "\n T1.DESCRIPTION,"
              "\n T1.COL_SEQ"
              "\n FROM"
              "\n DIDP_PRE_SELECT_CONFIG T1"
              "\n LEFT JOIN"
              "\n DIDP_META_COLUMN_INFO T2"
              "\n ON T1.SRC_COL_ID = T2.COLUMN_ID"
              "\n WHERE T1.PRE_PROCESS_ID = '{0}') A"
              "\n LEFT JOIN DIDP_COMMON_DDL_INFO B"
              "\n ON A.TABLE_ID = B.TABLE_ID"
              "\n AND A.SRC_COL_NAME = B.DDL_FIELDNAME"
              "\n ORDER BY A.COL_SEQ").format(self.__args.preid)

        LOG.info("SQL:\n{0}".format(sql))

        try:
            result_info = self.__db_oper.fetchall_direct(sql)
        except Exception as e:
            traceback.print_exc()
            return -1

        self.__select_info = result_info

        return 0

    def __generate_import_stage(self):
        """ 生成import stage

        Args:

        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        LOG.info("创建文件读取 Stage...")

        record_length = 0
        self.__src_delim = self.__ddl_info[0][15]        # 分隔符
        self.__src_final_delim = self.__ddl_info[-1][15] # 末尾分隔符
        if self.__ddl_info[0][19] == "0":
            self.__quote = "none"
        elif self.__ddl_info[0][19] == "1":
            self.__quote = "double"
        elif self.__ddl_info[0][19] == "2":
            self.__quote = "single"
        else:
            LOG.error("不支持的定界符类型:{0}".format(self.__ddl_info[0][19]))
            return -1

        # 取消转义,目前只考虑回车换行
        # 其他不可见字符可以写成十六进制形式
        self.__src_record_delim = re.sub("\n", r"\\n", self.__ddl_info[0][17])
        self.__src_record_delim = re.sub("\r", r"\\r", self.__src_record_delim)
        
        # 拼接stage
        import_stage = ("##########################################\n"
                         "#### STAGE: 文件读取\n"
                         "## Operator\n"
                         "import\n"
                         "## Operator options\n"
                         "-schema record\n"
                         " {{record_delim_string='{0}', delim_string='{1}',"
                         " final_delim=none, quote={2}").format(
                             self.__src_delim+self.__src_record_delim, 
                             self.__src_final_delim, self.__quote)

        # 拼接字段
        tmp_col_info = ""
        for i in range(len(self.__ddl_info)):
            column_name = self.__ddl_info[i][3]
            record_length += (int(self.__ddl_info[i][18]) 
                              + len(self.__ddl_info[i][15]))
            tmp_col_info += "    {0}:string;\n".format(column_name)

        # 定长增加记录长度
        if self.__fixed == "1":
            import_stage += ", record_length={0}".format(record_length)

        import_stage += "}}\n  (\n{0}".format(tmp_col_info)

        import_stage += ("  )\n"
                         "-file {0}\n"
                         "-rejects FAIL\n"
                         "-reportProgress yes\n"
                         "-recordNumberField 'rowNumberColumn'\n"
                         "## Outputs\n"
                         "0> [] 'Lnk{1}.v';\n\n").format(self.__args.infile,
                                                         self.__stage_level)

        self.__osh_script = import_stage
        return 0

    def __generate_split_stage(self):
        """ 生成字段拆分 stage

        Args:

        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        LOG.info("创建字段拆分 Stage...")
        result_info = "" # 查询结果
        split_stage = "" # 拆分stage

        # 判断是否配置了字段拆分
        split_info_num = len(self.__split_info) # 拆分配置信息记录数
        if split_info_num <= 0:
            LOG.info("无大字段拆分步骤的配置,不进行大字段拆分处理")
            return 0

        for i in range(split_info_num):
            split_field = self.__split_info[i][0]    # 拆分字段 
            split_table_id = self.__split_info[i][1] # 拆分表表ID

            LOG.info("获取大字段拆分映射的目标表DDL")
            sql = ("SELECT \n"
                   " COL_NAME,\n"
                   " COL_LENGTH\n"
                   "FROM DIDP_META_COLUMN_INFO\n"
                   "WHERE TABLE_ID = '{0}'\n"
                   "ORDER BY COL_SEQ").format(split_table_id)

            LOG.info("SQL:\n{0}".format(sql))

            try:
                result_info = self.__db_oper.fetchall_direct(sql)
            except Exception as e:
                traceback.print_exc()
                return -1

            if len(result_info) == 0:
                LOG.error("元数据表中没有找到拆分目标表的字段信息")
                return -1

            # 拼接大字段拆分stage
            split_stage = ("##########################################\n"
                           "#### STAGE: 大字段拆分\n"
                           "## Operator\n"
                           "field_import\n"
                           "## Operator options\n"
                           "-field {0}\n"
                           "-schema record\n"
                           " {{ delim=none }}\n"
                           "   (\n").format(split_field)

            for i in range(len(result_info)):
                split_stage += "    {0}:string[{1}];\n".format(
                                   result_info[i][0],
                                   result_info[i][1])

            # 处理输入输出的link连接
            last_stage_level = self.__stage_level
            current_stage_level = self.__stage_level + 1
            self.__stage_level = current_stage_level
            split_stage += ("   )\n"
                            "-failRejects\n"
                            "## Outputs\n"
                            "0< [] 'Lnk{0}.v'\n"
                            "0> [] 'Lnk{1}.v';\n\n").format(last_stage_level,
                                                            current_stage_level)

            self.__osh_script += split_stage

        return 0

    def __generate_select_stage(self):
        """ 生成字段筛选 stage

        Args:

        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        LOG.info("创建字段筛选 Stage...")

        select_stage = ""  # 字段筛选stage
        default_macro = {} # 默认值宏

        # 判断是否配置了字段筛选
        select_info_num = len(self.__select_info)
        if select_info_num <= 0:
            LOG.info("无字段筛选步骤的配置,不进行字段筛选处理")
            if len(self.__split_info) > 0:
                LOG.error("存在字段拆分时,字段筛选表没有记录")
                return -1

            for i in range(len(self.__ddl_info)):
                tmp_col_info = {}
                tmp_col_info["column_name"] = self.__ddl_info[i][3]
                tmp_col_info["is_null"] = self.__ddl_info[i][10]
                tmp_col_info["is_pk"] = self.__ddl_info[i][11]
                tmp_col_info["default_value"] = self.__ddl_info[i][9]
                tmp_col_info["column_desc"] = self.__ddl_info[i][4]
                tmp_col_info["column_std_type"] = self.__ddl_info[i][5]
                tmp_col_info["column_type"] = self.__ddl_info[i][6]
                tmp_col_info["column_length"] = self.__ddl_info[i][7]
                tmp_col_info["column_scale"] = self.__ddl_info[i][8]
                self.__final_ddl_info.append(tmp_col_info)
            
            return 0

        new_columns = []     # 新增字段
        delete_columns = ""  # 删除字段
        renamed_columns = [] # 改名字段

        # 解析参数传入的默认值宏
        if self.__args.macro:
            tmp_val1 = self.__args.macro.split(",")
            for i in range(len(tmp_val1)):
                tmp_val2 = tmp_val1[i].split("=")
                default_macro[tmp_val2[0]] = tmp_val2[1]

        # 找出删除的字段
        for i in range(len(self.__ddl_info)):
            delete_flag = 0
            src_column_id = self.__ddl_info[i][0]
            src_column_name = self.__ddl_info[i][3]
            for j in range(select_info_num):
                cur_column_id = self.__select_info[j][1]
                if src_column_id == cur_column_id:
                    delete_flag = 1
                    break
            if delete_flag == 0:
                delete_columns += "{0},".format(src_column_name)

        # 找出改名和新增的字段
        for i in range(select_info_num):
            column_id = self.__select_info[i][1]
            src_column_name = self.__select_info[i][2]
            cur_column_name = self.__select_info[i][3]
            src_column_type = self.__select_info[i][5]
            cur_column_type = self.__select_info[i][6]
            cur_column_length = int(self.__select_info[i][7])
            if (self.__select_info[i][8] == "" 
                or self.__select_info[i][8] == None):
                cur_column_scale = 0
            else:
                cur_column_scale = int(self.__select_info[i][8])
            default_value = self.__select_info[i][9]

            tmp_col_info = {}
            tmp_renamed_columns = {}

            tmp_col_info["column_name"] = cur_column_name
            tmp_col_info["is_null"] = self.__select_info[i][10]
            tmp_col_info["is_pk"] = self.__select_info[i][11]
            tmp_col_info["column_length"] = cur_column_length
            tmp_col_info["column_scale"] = cur_column_scale
            tmp_col_info["default_value"] = default_value
            tmp_col_info["column_desc"] = ""

            # 源ID字段为空的既是新增的字段
            if column_id == None or column_id == "":
                if default_value == None or default_value == "":
                    LOG.error("新增字段[{0}]默认值为空".format(cur_column_name))
                    return -1

                default_value = default_value[1:-1]
                if default_macro.has_key(default_value):
                    tmp_col_info["default_value"] = default_macro[default_value]
                else:
                    LOG.error("新增字段[{0}]指定的默认值未设定值".format(
                                  cur_column_name))
                    return -1

                tmp_col_info["column_std_type"] = generate_common_ddl_type(
                                                     cur_column_type,
                                                     cur_column_length,
                                                     cur_column_scale)
                new_columns.append(tmp_col_info)
                tmp_col_info["column_type"] = cur_column_type
                self.__final_ddl_info.append(tmp_col_info)
                continue
            else:
                # 字段名不一样的时候需要修改字段名
                # 源字段为空时为删除的字段,自适应,该字段不处理
                if src_column_name != None:
                    if src_column_name != cur_column_name:
                        tmp_col_info["column_std_type"] = generate_common_ddl_type(
                                                              cur_column_type,
                                                              cur_column_length,
                                                              cur_column_scale)
                        
                        tmp_renamed_columns["src_column_name"] = src_column_name
                        tmp_renamed_columns["cur_column_name"] = cur_column_name
                        renamed_columns.append(tmp_renamed_columns)
                        tmp_col_info["column_type"] = cur_column_type
                    else:
                        # 字段名一样但是通用类型为空的时候需要生成通用类型
                        if self.__select_info[i][16] == None:
                            tmp_col_info["column_std_type"] = generate_common_ddl_type(
                                                                  cur_column_type,
                                                                  cur_column_length,
                                                                  cur_column_scale)
                        else:
                            tmp_col_info["column_std_type"] = self.__select_info[i][16]
                        tmp_col_info["column_type"] = src_column_type

                    self.__final_ddl_info.append(tmp_col_info)


        # 处理新增的字段
        new_columns_num = len(new_columns)
        if new_columns_num > 0:
            tmp_stage_info = ("##########################################\n"
                              "#### STAGE: 筛选(字段新增)\n"
                              "## Operator\n"
                              "generator\n"
                              "## Operator options\n"
                              "-schema record\n"
                              " (\n")

            for i in range(new_columns_num):
                tmp_stage_info += ("   {0}:string {{ cycle = "
                                   "{{ value = '{1}' }} }};\n").format(
                                       new_columns[i]["column_name"],
                                       new_columns[i]["default_value"])
        
            # 处理输入输出的link连接
            last_stage_level = self.__stage_level
            current_stage_level = self.__stage_level + 1
            self.__stage_level = current_stage_level
            tmp_stage_info += (" )\n"
                               "## Inputs\n"
                               "0< [] 'Lnk{0}.v'\n"
                               "## Outputs\n"
                               "0> [] 'Lnk{1}.v';\n\n").format(
                                  last_stage_level,
                                  current_stage_level)

            select_stage += tmp_stage_info

        # 处理改名字段
        renamed_columns_num = len(renamed_columns)
        if renamed_columns_num > 0 or delete_columns != "":
            # 处理输入输出的link连接
            last_stage_level = self.__stage_level
            current_stage_level = self.__stage_level + 1
            self.__stage_level = current_stage_level

            tmp_stage_info = ("##########################################\n"
                              "#### STAGE: 筛选(修改字段名)\n"
                              "## Operator\n"
                              "modify '")

            if delete_columns != "":
                delete_columns = re.sub(",$", "", delete_columns)
                tmp_stage_info += "drop {0};".format(delete_columns)

            for i in range(renamed_columns_num):
                tmp_stage_info += "{0}={1}; ".format(
                                     renamed_columns[i]["cur_column_name"],
                                     renamed_columns[i]["src_column_name"])

            tmp_stage_info += ("'\n"
                               "## Inputs\n"
                               "0< [] 'Lnk{0}.v'\n"
                               "## Outputs\n"
                               "0> [] 'Lnk{1}.v';\n\n").format(
                                   last_stage_level,
                                   current_stage_level)

            select_stage += tmp_stage_info

        self.__osh_script += select_stage

        return 0

    def __generate_decompose_stage(self):
        """ 生成字段分解 stage

        Args:

        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        LOG.info("生成字段分解的OSH代码段")

        if len(self.__decompose_info) <= 0:
            LOG.info("无字段分解步骤的配置,不进行字段分解处理")
            return 0

        decompose_stage = ("##########################################\n"
                           "#### STAGE: 分解\n"
                           "## Operator\n"
                           "FieldDecompose\n"
                           "## Operator options\n"
                           "-decomposefield {0}\n"
                           "-outfile {1}\n"
                           "-fielddelim \'{2}\'\n"
                           "-linedelim \'{3}\'\n").format(
                               self.__decompose_info[0][0],
                               self.__args.outfile,
                               self.__dump_delim,
                               self.__line_break)

        last_stage_level = self.__stage_level
        decompose_stage += ("## Inputs\n"
                            "0< [] 'Lnk{0}.v';\n\n").format(
                              last_stage_level)

        self.__osh_script += decompose_stage

        return 0

    def __generate_check_osh(self):
        """ 生成检核 stage

        Args:

        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        LOG.info("创建检核 Stage...")

        # 处理字段类型修改
        # 获取检核配置的ddl个数和最终的ddl个数
        check_ddl_num = len(self.__check_info)
        final_ddl_num = len(self.__final_ddl_info)
        if check_ddl_num > 0:
            for i in range(final_ddl_num):
                new_common_type = ""
                column_name = self.__final_ddl_info[i]["column_name"]
                column_type = self.__final_ddl_info[i]["column_std_type"]
                column_length = self.__final_ddl_info[i]["column_length"]
                for j in range(check_ddl_num):
                    check_column_name = self.__check_info[j][0]
                    alph_flag = self.__check_info[j][1]
                    num_flag = self.__check_info[j][2]
                    char_flag = self.__check_info[j][3]
                    if column_name == check_column_name:
                        if alph_flag == "1":
                            new_common_type += "a"
                        elif num_flag == "1":
                            new_common_type += "n"
                        elif char_flag == "1":
                            new_common_type += "c"

                        match_obj = re.match("\.\.", column_type)
                        if match_obj:
                            new_common_type = "{0}..{1}".format(new_common_type,
                                                                column_length)
                        else:
                            new_common_type = "{0}!{1}".format(column_length,
                                                               new_common_type)
                        break
                # 更新通用类型
                if new_common_type != "":
                    self.__final_ddl_info[i]["column_std_type"] = new_common_type

        # 合并字段(在未改动现有检核组件的前提下,需要将字段拼接)
        check_stage = ("##########################################\n"
                       "#### STAGE: 检核(记录读取)\n"
                       "## Operator\n"
                       "field_export\n"
                       "-field 'rec'\n"
                       "-type string\n"
                       "-schema record\n"
                       " {{final_delim=none, delim_string='{0}', quote=none}}\n"
                       "  (\n").format(self.__src_delim)

        for i in range(final_ddl_num):
            check_stage += "   {0}:string;\n".format(
                               self.__final_ddl_info[i]["column_name"])

        last_stage_level = self.__stage_level
        current_stage_level = self.__stage_level + 1
        self.__stage_level = current_stage_level
        check_stage += "  )\n"\
                       "## Inputs\n"\
                       "0< [] 'Lnk{0}.v'\n"\
                       "## Outputs\n"\
                       "0> [] 'Lnk{1}.v';\n\n".format(
                          last_stage_level,
                          current_stage_level)

        # Check
        transcoding_flag = "True"
        if self.__pre_info[0][0] == "0":
            transcoding_flag = "False"

        LOG.info("生成schema文件")
        ret = generate_schema_file(self.__schema_file, self.__args.table,
                                   self.__fixed, self.__src_delim, self.__final_ddl_info)
        if ret != 0:
            LOG.info("生成schema文件失败")
            return -1

        in_charset = "UTF8"
        if self.__args.inc == "0":
            in_charset = "GBK"
        
        out_charset = "UTF8"
        if self.__args.outc == "0":
            out_charset = "GBK"
 
        check_stage += ("##########################################\n"
                        "#### STAGE: 检核(Check)\n"
                        "## Operator\n"
                        "Check\n"
                        "## Operator options\n"
                        "-systemid '{0}'\n"
                        "-logicaldate '{1}'\n"
                        "-tablenm '{2}'\n"
                        "-orgid '{3}'\n"
                        "-checkCharset {4}\n"
                        "-charsetFrom '{5}'\n"
                        "-charsetTo '{6}'\n"
                        "-schemafile '{7}'\n"
                        "-allowKeyNull True\n"
                        "-doSort False\n"
                        "-tableDefinitionType 'DDL'\n"
                        "-saveReject True\n"
                        "-saveWarning True\n"
                        "-saveRejectData True\n").format(self.__args.system,
                                                         self.__args.bizdate,
                                                         self.__args.table,
                                                         self.__args.org,
                                                         transcoding_flag,
                                                         in_charset,
                                                         out_charset,
                                                         self.__schema_file)

        # 从项目规范表中获取项目分隔符
        LOG.info("从项目规范表中获取项目约定分隔符")
        sql = ("SELECT "
               "\n T1.SEPARATE_CHAR,"
               "\n T1.LINE_BREAK FROM"
               "\n DIDP_PROJECT_STANDARD_INFO T1,"
               "\n DIDP_PROJECT_VERSION_INFO T2,"
               "\n DIDP_PRE_PROCESS_INFO T3"
               "\n WHERE T1.PROJECT_ID = T2.PROJECT_ID"
               "\n AND T2.PROJECT_VERSION_ID = T3.PROJECT_VERSION_ID"
               "\n AND T3.PRE_PROCESS_ID = '{0}'").format(self.__args.preid)

        LOG.info("SQL:\n{0}".format(sql))
        try:
            result_info = self.__db_oper.fetchall_direct(sql)
        except Exception as e:
            traceback.print_exc()
            return -1

        if len(result_info) == 0:
            LOG.error("项目规范表中没有找到对应的信息")
            return -1

        self.__dump_delim = result_info[0][0]
        self.__line_break = result_info[0][1]

        check_stage += "-dumpFileFieldDelim '{0}'\n".format(
                           self.__dump_delim)

        if self.__args.repdel == "Y":
            replace_str = re.sub(r"\S", "#", self.__dump_delim) 
            check_stage += ("-replaceDelimFlag True\n"
                            "-replaceDelimStr '{0}'\n").format(replace_str)
        else:
            check_stage += "-replaceDelimFlag False\n"

        trans_info_num = len(self.__trans_info)
        decompose_info_num = len(self.__decompose_info)
        if trans_info_num == 0 and decompose_info_num == 0:
            check_stage += ("-saveOutput False\n"
                            "-dumpfile {0}\n").format(self.__args.outfile)
        else:
            check_stage += "-saveOutput True\n"

        last_stage_level = current_stage_level
        check_stage += ("## Inputs\n"
                        "0< [] 'Lnk{0}.v'\n"
                        "## Outputs\n").format(last_stage_level)

        current_stage_level = self.__stage_level + 1
        self.__stage_level = current_stage_level
        if trans_info_num <= 0 and decompose_info_num <= 0:
            check_stage += ("0> [] 'Lnk_check_rj.v'\n"
                            "1> [] 'Lnk_check_wr.v'\n"
                            "2> [] 'Lnk_check_rjd.v';\n")
        else:
            check_stage += ("0> [] 'Lnk{0}.v'\n"
                            "1> [] 'Lnk_check_rj.v'\n"
                            "2> [] 'Lnk_check_wr.v'\n"
                            "3> [] 'Lnk_check_rjd.v';\n").format(
                                current_stage_level)

        self.__osh_script += check_stage

        # 生成rj,rjd,wr文件输出的stage
        self.__generate_export_osh("rj", "0")
        self.__generate_export_osh("rjd", "0")
        self.__generate_export_osh("wr", "0")

        return 0

    def __generate_trans_osh(self):
        """ 生成代码转换 stage

        Args:

        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        LOG.info("创建转换 Stage...")

        trans_info_num = len(self.__trans_info)
        if trans_info_num <= 0:
            LOG.info("无字段转换步骤的配置,不进行字段筛选处理")
            return 0

        trans_statement = ""
        for i in range(trans_info_num):
            if self.__trans_info[i][1] != "1":
                LOG.error(("不支持的转换规则类型[{0}],"
                           "目前只支持字段截取").format(
                               self.__trans_info[i][1]))
                return -1

            trans_statement += "{0} = substring[{1},{2}]({0}); ".format(
                                   self.__trans_info[i][0],
                                   self.__trans_info[i][2],
                                   self.__trans_info[i][3])

        # 首先拼接查询数据库的odbcread operator
        trans_stage = ("##########################################\n"
                       "#### STAGE: 转换\n"
                       "## Operator\n"
                       "modify\n"
                       "## Operator options\n"
                       "  '{0}'\n").format(trans_statement)

        last_stage_level = self.__stage_level
        current_stage_level = self.__stage_level + 1
        self.__stage_level = current_stage_level
        decompose_num = len(self.__decompose_info)
        if decompose_num > 0:
            trans_stage += ("## Inputs\n"
                            "0< [] 'Lnk{0}.v'\n"
                            "## Outputs\n"
                            "0> [] 'Lnk{1}.v';\n\n").format(
                                last_stage_level,
                                current_stage_level)
        else:
            trans_stage += ("## Inputs\n"
                            "0< [] 'Lnk{0}.v'\n"
                            "## Outputs\n"
                            "0> [] 'Lnk_trans_out.v';\n\n").format(
                                last_stage_level,
                                current_stage_level)
         
        self.__osh_script += trans_stage
        
        # 生成输出文件的stage
        if decompose_num <= 0:
            self.__generate_export_osh("trans", "0")

        return 0

    def __generate_export_osh(self, mode, wr_flag):
        """ 生成文件输出 stage
            根据参数生成对应的stage
        Args:
            mode    : 处理模式(rj|rjd|wr|trans)
            wr_flag : 代码转换时使用,区分wr和数据文件输出
        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        LOG.info("生成文件输出 Stage...")

        export_stage = ("##########################################\n"
                        "#### STAGE: 文件输出({0})\n"
                        "## Operator\n"
                        "export\n"
                        "## Operator options\n"
                        "-schema record\n").format(mode)

        # mode不同,输出文件的格式不用
        if mode == "rjd":
            if self.__src_final_delim == "":
                export_stage += (" {{ record_delim_string='{0}', delim=none,"
                                 "quote={1}, "
                                 "null_field='' }}\n").format(
                                     self.__src_record_delim,
                                     self.__quote)
            else:
                export_stage += (" {{ record_delim_string='{0}', "
                                 "delim_string='{1}', "
                                 "quote={2}, "
                                 "null_field='' }}\n").format(
                                     self.__src_record_delim,
                                     self.__src_final_delim,
                                     self.__quote)
        elif mode == "trans":
            export_stage += (" {{ record_delim_string='{0}', delim_string='{1}',"
                             " quote={2}, null_field='' }}\n").format(
                                 self.__line_break, self.__dump_delim,
                                 self.__quote)
        else:
            export_stage += (" { record_delim_string='\\n', delim_string='|@|',"
                             " quote=none, null_field='' }\n")

        export_stage += "  (\n"
        # 代码转换时拼接输出的schema
        if mode == "trans" and wr_flag == "0":
            for final_ddl_info in self.__final_ddl_info:
                if final_ddl_info["is_null"] == "1":
                    export_stage += "   {0}:nullable ".format(
                                        final_ddl_info["column_name"])
                else:
                    export_stage += "   {0}: ".format(
                                        final_ddl_info["column_name"])

                if re.match("^.*?\.\..*?", final_ddl_info["column_std_type"])\
                    or re.match("^.*?!.*?$", final_ddl_info["column_std_type"]):
                    export_stage += "string;\n"
                elif re.match(".*?n\(.*?\)", final_ddl_info["column_std_type"]):
                    export_stage += "decimal[{0},{1}];\n".format(
                                        final_ddl_info["column_length"],
                                        final_ddl_info["column_scale"])
                elif re.match(".*?n$", final_ddl_info["column_std_type"]):
                    export_stage += "int64;\n"
                elif final_ddl_info["column_std_type"] == "YYYY-MM-DD":
                    export_stage += "date {date_format='%yyyy-%mm-%dd'};\n"
                elif final_ddl_info["column_std_type"] == "YYYY-MM-DDTHH:MM:SS":
                    export_stage += "timestamp {timestamp_format='%yyyy-%mm-%dd %hh:%nn:%ss'};\n"
                elif final_ddl_info["column_std_type"] == "YYYY-MM-DDTHH:MM:SS.NNN":
                    export_stage += "timestamp {timestamp_format='%yyyy-%mm-%dd %hh:%nn:%ss.3'};\n"
                elif final_ddl_info["column_std_type"] == "YYYY-MM-DDTHH:MM:SS.NNNNNN":
                    export_stage += "timestamp {timestamp_format='%yyyy-%mm-%dd %hh:%nn:%ss.6'};\n"
                else:
                    export_stage += "string;\n"

        export_stage += "  )\n"

        if mode == "rj":
            export_stage += "-file {0}.rj\n".format(self.__args.outfile)
        elif mode == "wr":
            export_stage += "-file {0}.wr\n".format(self.__args.outfile)
        elif mode == "rjd":
            export_stage += "-file {0}.rjd\n".format(self.__args.outfile)
        elif mode == "trans":
            if wr_flag == "0":
                export_stage += "-file {0}\n".format(self.__args.outfile)
            else:
                export_stage += "-file {0}.trans.wr\n".format(
                                    self.__args.outfile)

        export_stage += ("-rejects Fail\n"
                         "-overwrite\n"
                         "## Inputs\n")

        if mode == "trans":
            if wr_flag == "0":
                export_stage += "0< [] 'Lnk_trans_out.v';\n"
        else:
            export_stage += "0< [] 'Lnk_check_{0}.v';\n".format(mode)

        self.__osh_script += export_stage

    def __generate_osh_script(self):
        """ 生成OSH文件

        Args:

        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        LOG.info("生成预处理的OSH脚本:{0}".format(self.__osh_file))

        # import
        ret = self.__generate_import_stage()
        if ret != 0:
            return -1

        # 1 拆分
        ret = self.__generate_split_stage()
        if ret != 0:
            return -1

        # 2 字段筛选
        ret = self.__generate_select_stage()
        if ret != 0:
            return -1

        # 3 检核转码
        ret = self.__generate_check_osh()
        if ret != 0:
            return -1

        # 4 代码转换
        ret = self.__generate_trans_osh()
        if ret != 0:
            return -1

        # 5 分解
        ret = self.__generate_decompose_stage()
        if ret != 0:
            return -1

        LOG.info("OSH:\n{0}".format(self.__osh_script))

        ret = write_file(self.__osh_file, self.__osh_script, "UTF-8")
        if ret != 0:
            LOG.error("生成OSH脚本失败")
            return -1

        LOG.info("生成OSH脚本完成")
       
        return 0

    def __check_history_file(self, file_name):
        """ 检查并删除历史遗留文件

        Args:
            file_name : 文件名
        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        # 处理输出文件
        # 运行前先删除历史运行遗留的文件
        # 当存在分解配置时,文件名上必须存在#ORG#的宏
        match_obj = re.match(".*?#ORG#.*?", file_name)
        if match_obj:
            regexp_file_name = re.sub("#ORG#", "*", file_name)

            history_files = []
            history_files = glob.glob(regexp_file_name)

            for i in range(len(history_files)):
                if os.path.exists(history_files[i]):
                    os.remove(history_files[i])    
        else:  
            if len(self.__decompose_info) > 0:
                LOG.error("配置了字段分解时,字段名中需要使用#ORG#宏")
                return -1

            if os.path.exists(file_name):
                os.remove(file_name)    
        return 0

    def run(self):
        """ 运行预处理

        Args:

        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        # 记录开始时间
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start_time_sec = datetime.now()
        error_message = "" # 错误信息

        # 读取配置信息
        ret = self.__get_cfg_info()        
        if ret != 0:
            error_message = "获取预处理配置信息失败"
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return -1

        # 检查输出目录是否存在
        ret = check_path(self.__args.outfile)
        if ret == -1:
            error_message = "检查输出文件目录是否存在失败"
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return -1

        # 检查是否已经存在的历史文件,先删除
        # 处理字段分解时的#ORG#宏
        ret = self.__check_history_file(self.__args.outfile)
        if ret != 0:
            error_message = "检查并清理历史数据文件失败"
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return -1

        ret = self.__check_history_file(self.__args.ddlfile)
        if ret != 0:
            error_message = "检查并清理历史DDL文件失败"
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return -1
        # 拼接OSH脚本
        ret = self.__generate_osh_script()
        if ret != 0:
            error_message = "创建OSh脚本失败"
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return -1

        # 执行osh文件
        LOG.info("执行OSH脚本")
        command = "osh -f {0}".format(self.__osh_file)
        LOG.info("COMMAND: {0}".format(command))
        ret = os.system("{0} 1>{1} 2>&1".format(command, self.__log_file))
        os.system("cat {0}".format(self.__log_file))
        if ret != 0:
            error_message = "执行OSH脚本失败"
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return -1
        else:
            try: 
                FILE = open(self.__log_file, 'r')
            except Exception as e:
                error_message = "打开日志文件失败"
                LOG.error(error_message)
                self.__log_recorder.record(job_starttime=start_time,
                                           job_status=1,
                                           error_message=error_message)
                return -1

            for line in FILE.readlines():
                line = line.strip()
                m = re.match((r"^.*?CheckBasic.*input,(.*?);"
                              " output,(.*?);.*?$"), line) 
                if m:
                    self.__input_lines = int(m.group(1)) + self.__input_lines
                    self.__output_lines = int(m.group(2)) + self.__output_lines


            self.__reject_lines = self.__input_lines - self.__output_lines
            FILE.close()
            LOG.info("执行OSH脚本完成")
            LOG.info("输入记录数:{0},输出记录数:{1},拒绝记录数:{2}".format(
                         self.__input_lines, self.__output_lines, 
                         self.__reject_lines))

            if self.__input_lines != 0:
                reject_percentage = int(self.__reject_lines * 100 / self.__input_lines)
                if reject_percentage > self.__args.rjlimit:
                    error_message = "拒绝记录数的百分比[{0}]超过了最大值[{1}]".format(
                                      reject_percentage, self.__args.rjlimit)
                    LOG.error(error_message)
                    self.__log_recorder.record(job_starttime=start_time,
                                               job_status=1,
                                               error_message=error_message)
                    return -1

            # 删除中间文件
            os.remove(self.__schema_file)
            os.remove(self.__osh_file)
            os.remove(self.__log_file)

        # 生成ddl文件
        # 版本和字符编码默认
        match_obj = re.match(".*?#ORG#.*?", self.__args.outfile)
        if match_obj:
            match_obj = re.match(".*?#ORG#.*?", self.__args.ddlfile)
            if match_obj:
                regexp_file_name = re.sub("#ORG#", "*", self.__args.outfile)

                output_files = []
                output_files = glob.glob(regexp_file_name)
                 
                LOG.info("生成的DAT和DDL文件:")
                for i in range(len(output_files)):
                    LOG.info(output_files[i])
                    regexp_str = re.sub("#ORG#", "(.*?)", self.__args.outfile)
                    match_obj = re.match(r"^{0}$".format(regexp_str),
                                         output_files[i])
                    if match_obj:
                        ddl_file = re.sub("#ORG#", match_obj.group(1),
                                          self.__args.ddlfile)

                        LOG.info(ddl_file)

                        ret = generate_common_ddl_file(ddl_file,
                                                       "V1.0",
                                                       self.__args.table,
                                                       self.__fixed,
                                                       self.__final_ddl_info,
                                                       self.__args.inc,
                                                       self.__args.outc)
                        if ret != 0:
                            error_message = "生成DDL文件失败:{0}".format(
                                              self.__args.ddlfile)
                            LOG.error(error_message)
                            self.__log_recorder.record(job_starttime=start_time,
                                                       job_status=1,
                                                       error_message=error_message)
                            return -1
            else:
                error_message = ("指定了字段分解时,"
                               "ddl文件名中没有指定机构的宏#ORG#:{0}").format(
                                   self.__args.ddlfile)
                LOG.error(error_message)
                self.__log_recorder.record(job_starttime=start_time,
                                           job_status=1,
                                           error_message=error_message)
                return -1
        else:
            LOG.info("生成DAT文件:{0}".format(self.__args.outfile))
            LOG.info("生成DDL文件:{0}".format(self.__args.ddlfile))

            ret = generate_common_ddl_file(self.__args.ddlfile, "V1.0",
                                           self.__args.table, self.__fixed,
                                           self.__final_ddl_info,
                                           self.__args.inc,
                                           self.__args.outc)
            if ret != 0:
                error_message = "生成DDL文件失败:{0}".format(self.__args.ddlfile)
                LOG.error(error_message)
                self.__log_recorder.record(job_starttime=start_time,
                                           job_status=1,
                                           error_message=error_message)
                return -1

        ret = self.__log_recorder.record(job_starttime=start_time,
                                   job_status=0,
                                   input_lines=self.__input_lines,
                                   output_lines=self.__output_lines,
                                   reject_lines=self.__reject_lines)
        if ret != 0:
            LOG.error("记录采集日志失败")
            return -1

        end_time_sec = datetime.now()

        # 计算耗时
        cost_time = (end_time_sec - start_time_sec).seconds
        LOG.info("预处理耗时:{0}s".format(cost_time))

        return 0

# main
if __name__ == "__main__":
    ret = 0 # 状态变量

    # 参数解析
    parser = argparse.ArgumentParser(description="数据预处理组件")
    parser.add_argument("-proid",   required=True, help="流程ID")
    parser.add_argument("-preid",   required=True, help="预处理ID")
    parser.add_argument("-system",  required=True, help="系统名")
    parser.add_argument("-batch",   required=True, help="批次号")
    parser.add_argument("-org",     required=True, help="机构号")
    parser.add_argument("-bizdate", required=True, help="业务日期(YYYYMMDD)")
    parser.add_argument("-table",   required=True, help="表名")
    parser.add_argument("-tableid", required=True, help="表ID")
    parser.add_argument("-infile",  required=True, help="输入文件(全路径)")
    parser.add_argument("-outfile", required=True, help="输出文件(全路径)")
    parser.add_argument("-ddlfile", default="", help="[可选]输出DDL文件(全路径)")
    parser.add_argument("-inc",     choices=["0", "1"], default="1",
                        help="[可选]输入文件字符集(1:UTF-8|0:GBK),默认1")
    parser.add_argument("-outc",    choices=["0", "1"], default="1",
                        help="[可选]输出文件字符集(1:UTF-8|0:GBK),默认1")
    parser.add_argument("-macro",   help=("[可选]新增字段时使用的默认值宏,"
                                          "格式为:宏名1=值1|宏名2=值2..."))
    parser.add_argument("-aptfile", help=("[可选]APT_CONFIG_FILE文件(全路径),"
                                          "默认使用DS自带的APT文件"))
    parser.add_argument("-rjlimit", default=0, type=int,
                        help="[可选]预处理允许的reject比例,默认0")
    parser.add_argument("-repdel",  choices=["Y", "N"], default="Y",
                        help="[可选]是否替换字符串字段中的分隔符,默认:Y")

    args = parser.parse_args()

    # 调用卸数类
    pre_process = PreProcess(args)
    LOG.info("数据预处理开始")
    ret = pre_process.run()
    if ret == 0:
        LOG.info("数据预处理完成")
        exit(0)
    else:
        LOG.error("数据预处理失败")
        exit(-1)

