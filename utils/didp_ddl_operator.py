#-*- coding: UTF-8 -*-  
################################################################################
# Date Time     : 2018-10-26
# Write By      : adtec(zhaogx)
# Function Desc : ddl操作类
#
# History       :
#                 20181104  zgx     Create
#
# Remarks       :
################################################################################
import os
import sys
import argparse
import traceback
import jaydebeapi

reload(sys)
sys.setdefaultencoding('utf8')

sys.path.append("{0}".format(os.environ["DIDP_HOME"]))

from datetime import datetime
from didp_logger import Logger
from didp_tools import generate_uuid
from didp_db_operator import DbOperator

# 全局变量
LOG = Logger()

# 配置库用户
DIDP_CFG_DB_USER = os.environ["DIDP_CFG_DB_USER"]
# 配置库用户密码
DIDP_CFG_DB_PWD = os.environ["DIDP_CFG_DB_PWD"]
# JDBC信息
DIDP_CFG_DB_JDBC_CLASS = os.environ["DIDP_CFG_DB_JDBC_CLASS"]
DIDP_CFG_DB_JDBC_URL = os.environ["DIDP_CFG_DB_JDBC_URL"]


class DdlOperator(object):
    """ DDL操作类
    
    Attributes:
    """
    def __init__(self):
        self.__db_oper = DbOperator(DIDP_CFG_DB_USER, DIDP_CFG_DB_PWD,
                                    DIDP_CFG_DB_JDBC_CLASS, DIDP_CFG_DB_JDBC_URL)

    def __get_table_id(self, schema_id, project_version_id, table_name):
        """ 获取TABLE_ID

        Args:
            schema_id : 表ID
            project_version_id : 项目版本ID
            table_name : 表名
        Returns:
            [0, 表ID] : 成功 | [-1, ''] : 失败
        Raise:

        """
        LOG.info("获取TABLE_ID")

        sql = ("SELECT"
               "\n T1.TABLE_ID "
               "\nFROM DIDP_META_TABLE_INFO T1"
               "\nWHERE T1.SCHEMA_ID = '{0}'"
               "\nAND T1.PROJECT_VERSION_ID = '{1}' "
               "\nAND T1.TABLE_NAME = '{2}' ").format(schema_id,
                                                     project_version_id,
                                                     table_name.upper())
        LOG.info("SQL:\n{0}".format(sql))

        try:
            result_info = self.__db_oper.fetchall_direct(sql)
        except Exception as e:
            traceback.print_exc()
            return -1, ''

        if len(result_info) != 1:
            LOG.error("获取TABLE_ID失败,元数据表中没有此表的配置")
            return -1, ''
        else:
            return 0, result_info[0][0]

    def get_meta_ddl_info(self, table_id, project_version_id):
        """ 获取元数据字段信息

        Args:
            project_version_id : 项目版本ID
        Returns:
            [0, 字段信息] : 成功 | [-1, []] : 失败
        Raise:

        """
        sql = ""         # SQL
        retult_info = [] # 查询结果

        LOG.info("查询元数据DDL信息")

        LOG.info("从DIDP_META_COLUMN_INFO表中获取元数据DDL信息")

        sql = ("SELECT"
               "\n T1.COLUMN_ID,"
               "\n T1.COL_NAME,"
               "\n T1.COL_DESC,"
               "\n T1.COL_TYPE,"
               "\n T1.COL_LENGTH,"
               "\n T1.COL_SCALE,"
               "\n T1.NULL_FLAG,"
               "\n T1.PK_FLAG,"
               "\n T1.PARTITION_FLAG,"
               "\n T1.BUCKET_FLAG"
               "\n FROM DIDP_META_COLUMN_INFO T1 "
               "\n WHERE T1.TABLE_ID = '{0}'"
               "\n AND T1.PROJECT_VERSION_ID = '{1}' "
               "\nORDER BY T1.COL_SEQ").format(table_id, project_version_id)

        LOG.info("SQL:\n{0}".format(sql))

        try:
            result_info = self.__db_oper.fetchall_direct(sql)
        except Exception as e:
            traceback.print_exc()
            return -1,  []

        return 0, result_info

    def __insert_meta_ddl_info(self, table_id, project_version_id, ddl_info,
                               column_id_flag):
        """ 写入元数据字段信息表

        Args:
            project_version_id : 项目版本ID
            ddl_info : 字段信息
            column_id_flag : column_id生成标识(初次写入的时候才用新的column_id)
        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        LOG.info("插入记录到表DIDP_META_COLUMN_INFO")

        update_user = "SYSTEM"  # 更新用户，默认SYSTEM
        # 更新时间
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S') 

        for i in range(len(ddl_info)):
            if column_id_flag == '0':
                column_id = generate_uuid()
            else:
                column_id = ddl_info[i]["column_id"]
            
            sql = ("INSERT INTO DIDP_META_COLUMN_INFO ( "
                   "\n COLUMN_ID,"
                   "\n TABLE_ID,"
                   "\n PROJECT_VERSION_ID,"
                   "\n LAST_UPDATE_TIME,"
                   "\n LAST_UPDATE_USER,"
                   "\n COL_SEQ,"
                   "\n COL_NAME,"
                   "\n COL_DESC,"
                   "\n COL_TYPE,"
                   "\n COL_LENGTH,"
                   "\n COL_SCALE,"
                   "\n COL_DEFAULT,"
                   "\n NULL_FLAG,"
                   "\n PK_FLAG,"
                   "\n PARTITION_FLAG,"
                   "\n BUCKET_FLAG,"
                   "\n DESCRIPTION ) VALUES ( "
                   "\n '{0}',"
                   "\n '{1}',"
                   "\n '{2}',"
                   "\n '{3}',"
                   "\n '{4}',"
                   "\n {5},"
                   "\n '{6}',"
                   "\n '{7}',"
                   "\n '{8}',"
                   "\n {9},"
                   "\n {10},"
                   "\n '{11}',"
                   "\n '{12}',"
                   "\n '{13}',"
                   "\n '{14}',"
                   "\n '{15}',"
                   "\n '{16}')").format(column_id, table_id, 
                                       project_version_id,
                                       update_time, update_user, i+1,
                                       ddl_info[i]["column_name"],
                                       ddl_info[i]["column_desc"],
                                       ddl_info[i]["data_type"],
                                       ddl_info[i]["column_define_length"],
                                       ddl_info[i]["column_scale"],
                                       '',
                                       ddl_info[i]["is_null"],
                                       ddl_info[i]["is_pk"],
                                       ddl_info[i]["partition_flag"],
                                       ddl_info[i]["bucket_flag"],
                                       '')

            LOG.debug("SQL:\n{0}".format(sql))
            try:
                self.__db_oper.execute(sql)
            except Exception as e:
                traceback.print_exc()
                LOG.error(("执行插入表DIDP_META_COLUMN_INFO的语句失败,"
                          "SQL:\n{0}").format(sql))
                return -1

        return 0

    def __compare_meta_ddl_info(self, ddl_info, neweast_meta_ddl_info):
        """ 比较元数据字段信息与当前获取的字段信息是否一致

        Args:
            ddl_info : 字段信息
            neweast_meta_ddl_info : 项目版本ID
        Returns:
            0 : 一致 | 1 : 不一致
        Raise:

        """
        change_flag = 0  # 更新标识

        LOG.info("比较当前DDL和库中最新的元数据DDL是否一致")

        ddl_num = len(ddl_info) # ddl中字段个数
        neweast_meta_ddl_num = len(neweast_meta_ddl_info)
        final_ddl_info = [] # 最终ddl信息

        for i in range(ddl_num):
            ret = 0 # 状态值
            find_column_flag = 0
            for j in range(neweast_meta_ddl_num):
                meta_column_name = neweast_meta_ddl_info[j][1]
                meta_column_desc = neweast_meta_ddl_info[j][2]
                meta_column_type = neweast_meta_ddl_info[j][3]
                meta_column_length = neweast_meta_ddl_info[j][4]
                meta_column_scale = neweast_meta_ddl_info[j][5]
                meta_column_isnull = neweast_meta_ddl_info[j][6]
                meta_column_iskey = neweast_meta_ddl_info[j][7]
                meta_column_ispartition = neweast_meta_ddl_info[j][8]
                meta_column_isbucket = neweast_meta_ddl_info[j][9]
                
                if ddl_info[i]["column_name"] == meta_column_name:
                    find_column_flag = 1
                    meta_column_id = neweast_meta_ddl_info[j][0]
                    if ddl_info[i]["column_desc"] != meta_column_desc:
                        LOG.info("字段{0}的说明不一致:{1}|{2}".format(
                                                    ddl_info[i]["column_name"],
                                                    ddl_info[i]["column_desc"],
                                                    meta_column_desc))
                        ret = 1
                        # 建表的时候很可能没有写字段描述，但是元数据中有配置
                        # 以元数据为准
                        #ddl_info[i]["column_desc"] = meta_column_desc

                    if ddl_info[i]["data_type"] != meta_column_type:
                        LOG.info("字段{0}的类型不一致:{1}|{2}".format(
                                                    ddl_info[i]["column_name"],
                                                    ddl_info[i]["data_type"],
                                                    meta_column_type))
                        ret = 1

                    if ddl_info[i]["column_define_length"] != meta_column_length:
                        LOG.info("字段{0}的长度不一致:{1}|{2}|".format(
                                                    ddl_info[i]["column_name"],
                                                    ddl_info[i]["column_define_length"],
                                                    meta_column_length))
                        ret = 1

                    if ddl_info[i]["column_scale"] != meta_column_scale:
                        LOG.info("字段{0}的精度不一致:{1}|{2}".format(
                                                    ddl_info[i]["column_name"],
                                                    ddl_info[i]["column_scale"],
                                                    meta_column_scale))
                        ret = 1

                    if ddl_info[i]["is_null"] != meta_column_isnull:
                        LOG.info("字段{0}的是否可空标识不一致:{1}|{2}".format(
                                                    ddl_info[i]["column_name"],
                                                    ddl_info[i]["is_null"],
                                                    meta_column_isnull))
                        ret = 1

                    if ddl_info[i]["is_pk"] != meta_column_iskey:
                        LOG.info("字段{0}的主键不一致:{1}|{2}".format(
                                                    ddl_info[i]["column_name"],
                                                    ddl_info[i]["is_pk"],
                                                    meta_column_iskey))
           
                        ret = 1

                    if ddl_info[i]["partition_flag"]:
                        if ddl_info[i]["partition_flag"] != meta_column_ispartition:
                            LOG.info("字段{0}的分布键不一致:{1}|{2}".format(
                                                        ddl_info[i]["column_name"],
                                                        ddl_info[i]["partition_flag"],
                                                        meta_column_ispartition))
                            ret = 1
                    else:
                        ddl_info[i]["partition_flag"] = meta_column_ispartition

                    if ddl_info[i]["bucket_flag"]:
                        if ddl_info[i]["bucket_flag"] != meta_column_isbucket: 
                            LOG.info("字段{0}的分桶键不一致:{1}|{2}".format(
                                                        ddl_info[i]["column_name"],
                                                        ddl_info[i]["bucket_flag"],
                                                        meta_column_isbucket))
                            ret = 1
                    else:
                        ddl_info[i]["bucket_flag"] = meta_column_isbucket
                    break

            if ret == 1:
                change_flag = 1

            if find_column_flag == 0:
                meta_column_id = generate_uuid()

            tmp_list = ddl_info[i]
            tmp_list["column_id"] = meta_column_id

            final_ddl_info.append(tmp_list)
        if change_flag == 1:
            return 1, final_ddl_info
        else:
            LOG.info("当前DDL和库中最新的元数据DDL一致,无需更新")
            return 0, ''

    def __update_meta_ddl_info(self, table_id, project_version_id, ddl_info):
        """ 更新元数据字段信息

        Args:
            table_id : 表ID
            project_version_id : 项目版本ID
            ddl_info : 字段信息
        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        LOG.info("更新元数据DDL信息")

        LOG.info("备份当前元数据字段信息到历史表")
        his_table_id = generate_uuid() # 历史表ID
        update_user = "SYSTEM" # 更新用户
        # 更新时间
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  

        sql = ("INSERT INTO DIDP_META_COLUMN_INFO_HIS ("
               "\n TABLE_HIS_ID,"
               "\n COLUMN_ID,"
               "\n TABLE_ID,"
               "\n PROJECT_VERSION_ID,"
               "\n LAST_UPDATE_TIME,"
               "\n LAST_UPDATE_USER,"
               "\n COL_SEQ,"
               "\n COL_NAME,"
               "\n COL_DESC,"
               "\n COL_TYPE,"
               "\n COL_LENGTH,"
               "\n COL_SCALE,"
               "\n COL_DEFAULT,"
               "\n NULL_FLAG,"
               "\n PK_FLAG,"
               "\n PARTITION_FLAG,"
               "\n BUCKET_FLAG,"
               "\n DESCRIPTION)"
               "\n SELECT "
               "\n '{0}',"
               "\n T1.COLUMN_ID,"
               "\n T1.TABLE_ID,"
               "\n T1.PROJECT_VERSION_ID,"
               "\n T1.LAST_UPDATE_TIME,"
               "\n T1.LAST_UPDATE_USER,"
               "\n T1.COL_SEQ,"
               "\n T1.COL_NAME,"
               "\n T1.COL_DESC,"
               "\n T1.COL_TYPE,"
               "\n T1.COL_LENGTH,"
               "\n T1.COL_SCALE,"
               "\n T1.COL_DEFAULT,"
               "\n T1.NULL_FLAG,"
               "\n T1.PK_FLAG,"
               "\n T1.PARTITION_FLAG,"
               "\n T1.BUCKET_FLAG,"
               "\n T1.DESCRIPTION"
               "\n FROM DIDP_META_COLUMN_INFO T1 "
               "\n WHERE T1.TABLE_ID = '{1}'"
               "\n AND T1.PROJECT_VERSION_ID = '{2}'").format(his_table_id,
                                                              table_id,
                                                              project_version_id)

        LOG.info("SQL:\n{0}".format(sql))
        try:
            self.__db_oper.execute(sql)
        except Exception as e:
            traceback.print_exc()
            LOG.error("执行备份元数据字段信息的语句失败")
            return -1

        LOG.info("删除元数据字段信息表中当前的数据")
        sql = ("DELETE FROM DIDP_META_COLUMN_INFO "
               "\n WHERE TABLE_ID = '{0}'"
               "\n AND PROJECT_VERSION_ID = '{1}'").format(table_id,
                                                         project_version_id)

        LOG.info("SQL:\n{0}".format(sql))
        try:
            self.__db_oper.execute(sql)
        except Exception as e:
            traceback.print_exc()
            LOG.error("执行删除元数据字段信息表中当前数据的语句失败")
            return -1

        ret = self.__insert_meta_ddl_info(table_id, project_version_id,
                                          ddl_info, '1')
        if ret != 0:
            return -1

        LOG.info("备份当前元数据表基本信息到历史表")
        his_column_id = generate_uuid() # 历史字段ID
        #his_table_id = generate_uuid()

        sql = ("INSERT INTO DIDP_META_TABLE_INFO_HIS ("
               "\n TABLE_HIS_ID,"
               "\n TABLE_ID,"
               "\n SCHEMA_ID,"
               "\n PROJECT_VERSION_ID,"
               "\n LAST_UPDATE_TIME,"
               "\n LAST_UPDATE_USER,"
               "\n TABLE_NAME,"
               "\n TABLE_NAME_CN,"
               "\n BUCKET_NUM,"
               "\n DESCRIPTION,"
               "\n RELEASE_DATE)"
               "\n SELECT "
               "\n '{0}',"
               "\n T1.TABLE_ID,"
               "\n T1.SCHEMA_ID,"
               "\n T1.PROJECT_VERSION_ID,"
               "\n T1.LAST_UPDATE_TIME,"
               "\n T1.LAST_UPDATE_USER,"
               "\n T1.TABLE_NAME,"
               "\n T1.TABLE_NAME_CN,"
               "\n T1.BUCKET_NUM,"
               "\n T1.DESCRIPTION,"
               "\n T1.RELEASE_DATE"
               "\n FROM DIDP_META_TABLE_INFO T1 "
               "\n WHERE T1.TABLE_ID = '{1}'"
               "\n AND T1.PROJECT_VERSION_ID = '{2}'").format(his_table_id,
                                                              table_id,
                                                              project_version_id)

        LOG.info("SQL:\n{0}".format(sql))
        try:
            self.__db_oper.execute(sql)
        except Exception as e:
            traceback.print_exc()
            LOG.error("执行备份元数据字段信息的语句失败")
            return -1

        LOG.info("更新元数据DDL信息完成")
        return 0

    def get_common_ddl_info(self, table_id):
        """ 获取通用DDL信息

        Args:
        Returns:
            [0, 字段信息] : 成功 | [-1, []] : 失败
        Raise:

        """
        sql = "" # SQL
        retult_info = [] # 查询结果
        
        LOG.info("查询通用DDL信息")

        LOG.info("从DIDP_COMMON_DDL_INFO表中获取DDL信息")

        sql = ("SELECT"
               "\n T1.DDL_TABLENAME,"
               "\n T1.DDL_FIELDNAME,"
               "\n T1.DDL_FIELDTYPE,"
               "\n T1.DDL_ISKEY,"
               "\n T1.DDL_NULLABLE,"
               "\n T1.DDL_DELIM,"
               "\n T1.DDL_TYPE,"
               "\n T1.DDL_CHINAME,"
               "\n T1.DDL_ORDER,"
               "\n T1.ISFIXED,"
               "\n T1.RECORD_DELIM"
               "\n FROM DIDP_COMMON_DDL_INFO T1 "
               "\n WHERE T1.TABLE_ID = '{0}'"
               "\nORDER BY T1.DDL_ORDER").format(table_id)

        LOG.info("SQL:\n{0}".format(sql))

        try:
            result_info = self.__db_oper.fetchall_direct(sql)
        except Exception as e:
            traceback.print_exc()
            return -1, []

        return 0, result_info

    def __insert_common_ddl_info(self, table_id, common_ddl_info):
        """ 写入通用类型DDL信息

        Args:
            table_id : 表名
            common_ddl_info : 通用DDL信息
        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        LOG.info("插入记录到表DIDP_COMMON_DDL_INFO")

        for i in range(len(common_ddl_info)):
            sql = ("INSERT INTO DIDP_COMMON_DDL_INFO ( "
                   "\n TABLE_ID,"
                   "\n DDL_TABLENAME,"
                   "\n DDL_FIELDNAME,"
                   "\n DDL_FIELDTYPE,"
                   "\n ISFIXED,"
                   "\n DDL_ISKEY,"
                   "\n DDL_NULLABLE,"
                   "\n DDL_DELIM,"
                   "\n DDL_ORDER,"
                   "\n DDL_CHINAME,"
                   "\n DDL_TYPE,"
                   "\n DDL_LENGTH,"
                   "\n DDL_SCALE,"
                   "\n RECORD_DELIM,"
                   "\n QUOTE_TYPE) VALUES ( "
                   "\n '{0}',"
                   "\n '{1}',"
                   "\n '{2}',"
                   "\n '{3}',"
                   "\n '{4}',"
                   "\n '{5}',"
                   "\n '{6}',"
                   "\n '{7}',"
                   "\n '{8}',"
                   "\n '{9}',"
                   "\n '{10}',"
                   "\n '{11}',"
                   "\n '{12}',"
                   "\n '{13}',"
                   "\n '{14}')").format(table_id,
                       common_ddl_info[i]["table_name"],
                       common_ddl_info[i]["column_name"],
                       common_ddl_info[i]["column_std_type"],
                       common_ddl_info[i]["fixed"],
                       common_ddl_info[i]["is_pk"],
                       common_ddl_info[i]["is_null"],
                       common_ddl_info[i]["delim"],
                       i+1,
                       common_ddl_info[i]["column_desc"],
                       common_ddl_info[i]["data_type"],
                       common_ddl_info[i]["column_define_length"],
                       common_ddl_info[i]["column_scale"],
                       common_ddl_info[i]["rcdelim"],
                       common_ddl_info[i]["quote_type"])

            LOG.debug("SQL:\n{0}".format(sql))
            try:
                self.__db_oper.execute(sql)
            except Exception as e:
                traceback.print_exc()
                LOG.error(("执行插入表DIDP_COMMON_DDL_INFO的语句失败,"
                           "SQL:\n{0}".format(sql)))
                return -1

        return 0

    def __compare_common_ddl_info(self, common_ddl_info, 
                                  neweast_common_ddl_info):
        """ 比较通用类型的DDL和当前获取的DDL是否一致

        Args:
            common_ddl_info : 当前获取的DDL信息
            neweast_common_ddl_info : 库中最新的DDL信息
        Returns:
            0 : 一致 | 1 : 不一致
        Raise:

        """
        ret = 0 # 状态值
        LOG.info("比较当前DDL和库中最新的DDL是否一致")

        common_ddl_num = len(common_ddl_info)
        neweast_common_ddl_num = len(neweast_common_ddl_info)
        if common_ddl_num != neweast_common_ddl_num:
            LOG.info("字段数不一致:{0}|{1}".format(common_ddl_num,
                                                   neweast_common_ddl_num))
            return 1

        common_table_name = neweast_common_ddl_info[0][0]
        common_fixed = neweast_common_ddl_info[0][9]
        common_rcdelim = neweast_common_ddl_info[0][10]
        for i in range(common_ddl_num):
            common_column_name = neweast_common_ddl_info[i][1]
            common_column_std_type = neweast_common_ddl_info[i][2]
            common_column_iskey = neweast_common_ddl_info[i][3]
            common_column_isnull = neweast_common_ddl_info[i][4]
            common_column_delim = neweast_common_ddl_info[i][5]
            common_column_type = neweast_common_ddl_info[i][6]
            common_column_desc = neweast_common_ddl_info[i][7]
            if common_ddl_info[i]["table_name"] != common_table_name:
                LOG.info("表名不一致:{0}|{1}".format(common_ddl_info[i]["table_name"],
                                                   common_table_name))
                return 1

            if common_ddl_info[i]["fixed"] != common_fixed:
                LOG.info("定长标识不一致:{0}|{1}".format(common_ddl_info[i]["fixed"],
                                                   common_fixed))
                return 1

            if common_ddl_info[i]["rcdelim"] != common_rcdelim:
                LOG.info("记录分隔符不一致:{0}|{1}".format(common_ddl_info[i]["rcdelim"],
                                                   common_rcdelim))
                return 1

            if common_ddl_info[i]["column_name"] != common_column_name:
                LOG.info("字段名不一致:{0}|{1}".format(
                                            common_ddl_info[i]["column_name"],
                                            common_column_name))
                ret = 1

            if common_ddl_info[i]["column_std_type"] != common_column_std_type:
                LOG.info("字段{0}的通用类型不一致:{1}|{2}".format(
                                            common_ddl_info[i]["column_name"],
                                            common_ddl_info[i]["column_std_type"],
                                            common_column_std_type))
                ret = 1

            if common_ddl_info[i]["is_pk"] != common_column_iskey:
                LOG.info("字段{0}的主键不一致:{1}|{2}".format(
                                            common_ddl_info[i]["column_name"],
                                            common_ddl_info[i]["is_pk"],
                                            common_column_iskey))
                ret = 1

            if common_ddl_info[i]["is_null"] != common_column_isnull:
                LOG.info("字段{0}的是否可空标识不一致:{1}|{2}".format(
                                            common_ddl_info[i]["column_name"],
                                            common_ddl_info[i]["is_null"],
                                            common_column_isnull))
                ret = 1

            if common_ddl_info[i]["delim"] != common_column_delim:
                LOG.info("字段{0}的分隔符不一致:{1}|{2}".format(
                                            common_ddl_info[i]["column_name"],
                                            common_ddl_info[i]["delim"],
                                            common_column_delim))
                ret = 1

            if common_ddl_info[i]["data_type"] != common_column_type:
                LOG.info("字段{0}的类型不一致:{1}|{2}".format(
                                            common_ddl_info[i]["column_name"],
                                            common_ddl_info[i]["data_type"],
                                            common_column_type))
                ret = 1

            if common_ddl_info[i]["column_desc"] != common_column_desc:
                LOG.info("字段{0}的说明不一致:{1}|{2}|".format(
                                            common_ddl_info[i]["column_name"],
                                            common_ddl_info[i]["column_desc"],
                                            common_column_desc))
                ret = 1

        if ret == 1:
            return ret
        else:
            LOG.info("当前通用DDL和库中最新的DDL一致,无需更新")
            return 0

    def __update_common_ddl_info(self, table_id, common_ddl_info):
        """ 更新通用类型的DDL

        Args:
            table_id : 表ID
            common_ddl_info : 当前获取的DDL信息
        Returns:
            0 : 一致 | 1 : 不一致
        Raise:

        """
        LOG.info("更新DDL信息")

        LOG.info("备份当前DDL信息到历史表")
        his_table_id = generate_uuid() # 历史表ID

        sql = ("INSERT INTO DIDP_COMMON_DDL_INFO_HIS ("
               "\n TABLE_HIS_ID,"
               "\n TABLE_ID,"
               "\n DDL_TABLENAME,"
               "\n DDL_FIELDNAME,"
               "\n DDL_FIELDTYPE,"
               "\n DDL_ISKEY,"
               "\n DDL_NULLABLE,"
               "\n DDL_CHINAME,"
               "\n DDL_TYPE,"
               "\n DDL_LENGTH,"
               "\n DDL_SCALE,"
               "\n DDL_DELIM,"
               "\n DDL_ORDER,"
               "\n ISFIXED,"
               "\n RECORD_DELIM,"
               "\n RECORD_TIME)"
               "\n SELECT"
               "\n '{0}',"
               "\n T1.TABLE_ID,"
               "\n T1.DDL_TABLENAME,"
               "\n T1.DDL_FIELDNAME,"
               "\n T1.DDL_FIELDTYPE,"
               "\n T1.DDL_ISKEY,"
               "\n T1.DDL_NULLABLE,"
               "\n T1.DDL_CHINAME,"
               "\n T1.DDL_TYPE,"
               "\n T1.DDL_LENGTH,"
               "\n T1.DDL_SCALE,"
               "\n T1.DDL_DELIM,"
               "\n T1.DDL_ORDER,"
               "\n T1.ISFIXED,"
               "\n T1.RECORD_DELIM,"
               "\n T1.RECORD_TIME"
               "\n FROM DIDP_COMMON_DDL_INFO T1 "
               "\n WHERE T1.TABLE_ID = '{1}'").format(his_table_id,
                                                    table_id)

        LOG.info("SQL:\n{0}".format(sql))
        try:
            self.__db_oper.execute(sql)
        except Exception as e:
            traceback.print_exc()
            LOG.error("执行备份DDL信息的语句失败")
            return -1

        LOG.info("删除DDL表中当前的数据")
        sql = ("DELETE FROM DIDP_COMMON_DDL_INFO "
               "\n WHERE TABLE_ID = '{0}'").format(table_id)

        LOG.info("SQL:\n{0}".format(sql))
        try:
            self.__db_oper.execute(sql)
        except Exception as e:
            traceback.print_exc()
            LOG.error("执行删除DDL表中当前数据的语句失败")
            return -1

        ret = self.__insert_common_ddl_info(table_id, common_ddl_info)
        if ret != 0:
            return -1

        LOG.info("更新DDL信息完成")
        return 0

    def load_ddl_direct(self, project_version_id, table_id, ddl_info):
        """ 直接DDL信息
            包括通用DDL信息和元数据信息
        Args:
            project_version_id : 项目版本ID 
            table_id : 表ID
            ddl_info : DDL信息的结构
        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """

        ret = self.load_common_ddl_direct(table_id, ddl_info)
        if ret != 0:
            return -1

        ret = self.load_meta_ddl_direct(table_id, project_version_id, ddl_info)
        if ret != 0:
            return -1

        return 0

    def load_common_ddl_direct(self, table_id, common_ddl_info):
        """ 直接加载通用DDL信息

        Args:
            table_id : 表ID
            common_ddl_info : DDL信息的结构
        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        LOG.info("加载通用DDL信息")

        neweast_common_ddl_info = [] # 最新的ddl信息

        ret, neweast_common_ddl_info = self.get_common_ddl_info(table_id)
        if ret != 0:
            return -1

        if len(neweast_common_ddl_info) == 0:
            # 没有记录，直接插入
            LOG.info("表DIDP_COMMON_DDL_INFO中无记录,直接插入新记录")

            ret = self.__insert_common_ddl_info(table_id, common_ddl_info)
            if ret != 0:
                return -1
        else:
            # 如果有，则进行比对
            ret = self.__compare_common_ddl_info(common_ddl_info,
                                                 neweast_common_ddl_info)
            if ret == -1:
                return -1
            elif ret == 1:
                ret = self.__update_common_ddl_info(table_id, common_ddl_info)
                if ret != 0:
                    return -1
        return 0

    def load_meta_ddl_direct(self, table_id, project_version_id, ddl_info):
        """ 直接DDL信息加载

        Args:
            table_id : 表ID
            poject_version_id : 项目版本ID
            ddl_info : DDL信息的结构
        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        # 获取元数据ddl信息
        ret, neweast_meta_ddl_info = self.get_meta_ddl_info(table_id,
                                                            project_version_id)
        if ret != 0:
            return -1

        if len(neweast_meta_ddl_info) == 0:
            # 没有记录，直接插入
            LOG.info("DIDP_META_COLUMN_INFO表中无记录,直接插入新记录")

            ret = self.__insert_meta_ddl_info(table_id, project_version_id,
                                              ddl_info, '0')
            if ret != 0:
                return -1
        else:
            # 如果有，则进行比对
            ret, final_ddl_info = self.__compare_meta_ddl_info(ddl_info,
                                      neweast_meta_ddl_info)
            if ret == -1:
                return -1
            elif ret == 1:
                # 比对发现不一致的时候更新
                ret = self.__update_meta_ddl_info(table_id, project_version_id,
                                                  final_ddl_info)
                if ret != 0:
                    return -1
        return 0

    def load_ddl_from_file(self, schema_id, project_version_id, table_name,
                           delim, ddl_flag, ddl_file):
        """ 通过ddl文件加载
            TODO
        Args:
            ddl_file : ddl文件
        Returns:
            0 : 成功 | -1 : 失败
        Raise:
            None
        """
        pass

