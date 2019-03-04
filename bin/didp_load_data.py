#-*- coding: UTF-8 -*-  
################################################################################
# Date Time     : 2018-10-26
# Write By      : adtec(xiazhy,zhaogx)
# Function Desc : 加载组件主程序
#
# History       :
#                 20181115  xiazhy     Create
#
# Remarks       :
################################################################################
import os
import sys
import argparse
import traceback
import importlib
import jaydebeapi

reload(sys)
sys.setdefaultencoding('utf8')

from datetime import datetime
sys.path.append("{0}".format(os.environ["DIDP_HOME"]))

from utils.didp_logger import Logger
from utils.didp_tools import get_db_login_info,stat_file_record,stat_table_record
from utils.didp_ddl_operator import DdlOperator
from utils.didp_ddlfile_parser import DDLFileParser
from utils.didp_log_recorder import LogRecorder

# 全局变量
LOG = Logger()

# 配置库用户
DIDP_CFG_DB_USER = os.environ["DIDP_CFG_DB_USER"]
# 配置库用户密码
DIDP_CFG_DB_PWD = os.environ["DIDP_CFG_DB_PWD"]
# JDBC信息python
DIDP_CFG_DB_JDBC_CLASS = os.environ["DIDP_CFG_DB_JDBC_CLASS"]
DIDP_CFG_DB_JDBC_URL = os.environ["DIDP_CFG_DB_JDBC_URL"]

class DataLoader:
    """ 数据加载类
    
    Attributes:
       __args : 参数
    """
    def __init__(self, args):
        self.__args = args

        self.__log_recorder = LogRecorder(process_id=self.__args.proid,
                                          system_key=self.__args.system,
                                          branch_no=self.__args.org,
                                          biz_date=self.__args.bizdate,
                                          batch_no=self.__args.batch,
                                          table_name=self.__args.table,
                                          table_id=self.__args.tableid,
                                          job_type="4")

        # self.__check_params()
        self.__print_arguments()

    def __print_arguments(self):
        """ 参数格式化输出
        Args:
            None
        Returns:
            None    
        Raise:
            None
        """
        LOG.debug("-------------------参数清单-------------------")
        LOG.debug("流程ID                     : {0}".format(self.__args.proid))
        LOG.debug("项目版本ID                 : {0}".format(self.__args.proveid))
        LOG.debug("SCHEMA ID                  : {0}".format(self.__args.schid))
        LOG.debug("TABLE ID                   : {0}".format(self.__args.tableid))
        LOG.debug("系统标识                   : {0}".format(self.__args.system))
        LOG.debug("批次号                     : {0}".format(self.__args.batch))
        LOG.debug("机构号                     : {0}".format(self.__args.org))
        LOG.debug("业务日期                   : {0}".format(self.__args.bizdate))
        LOG.debug("数据文件                   : {0}".format(self.__args.srcfile))
        LOG.debug("DDL文件                    : {0}".format(self.__args.ddlfile))
        LOG.debug("导入模式                   : {0}".format(self.__args.mode))
        LOG.debug("ktuser(大数据平台)         : {0}".format(self.__args.ktuser))
        LOG.debug("ktfile(大数据平台)         : {0}".format(self.__args.ktfile))
        LOG.debug("krb5file(大数据平台)       : {0}".format(self.__args.krbfile))
        LOG.debug("目标目录(大数据平台)       : {0}".format(self.__args.loaddir))
        LOG.debug("目标字符集                 : {0}".format(self.__args.charset))
        LOG.debug("加载允许的reject比例       : {0}".format(self.__args.rjlimit))
        LOG.debug("加载目标表表名             : {0}".format(self.__args.table))
        LOG.debug("数据文件分隔符             : {0}".format(self.__args.delim))
        LOG.debug("加载方式                   : {0}".format(self.__args.loadway))
        LOG.debug("加载文件的字符编码         : {0}".format(self.__args.inc))
        LOG.debug("----------------------------------------------")

    def fetch_source_info(self):
        """ 解析文件名获取表信息
        Args:
            None
        Returns:
            0, 目标库信息 - 成功 | -1, '' - 失败
        Raise:
            None
        """
        LOG.info("解析文件名获取表信息")

        # 不需要解析文件名获取，直接通过传入的tableid获取
        return 0

    def run(self):
        """ 运行加载

        Args:
            None
        Returns:
            0 - 成功 | -1 - 失败
        Raise:
            None
        """
        error_message = "" # 错误信息
        target_db_info = {} # 卸数目标库连接信息
        # 记录开始时间
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start_time_sec = datetime.now()

        # 判断DAT文件是否存在
        if not os.path.isfile(self.__args.srcfile):
            error_message = "DAT文件不存在[{0}]".format(self.__args.srcfile)
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return -1

        # 判断DDL文件是否存在
        if not os.path.isfile(self.__args.ddlfile):
            error_message = "DDL文件不存在[{0}]".format(self.__args.ddlfile)
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return -1

        # 获取目标表连接信息
        ret, target_db_info = get_db_login_info(self.__args.schid)
        if ret != 0:
            error_message = "获取目标表连接信息失败"
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return ret

        # 解析DDL文件
        dfp = DDLFileParser(self.__args.ddlfile, "STD")
        dfp.set_target_db_type(target_db_info['db_type'])
        ret, ddl_info = dfp.get_ddl_info()
        if ret != 0:
            error_message = "获取表的DDL信息失败"
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return ret

        LOG.info("加载{0}库的建表类创建目标表".format(target_db_info['db_type']))
        try:
            target_module = importlib.import_module(
                "plugins.didp_{0}_plugin".format(
                     target_db_info['db_type'].lower()))

            ## 加载（前处理：创建/清空目标表、加载）
            loader_class = getattr(target_module, "Loader")
            loader_instance = loader_class(self.__args, ddl_info, target_db_info, "")
            ret = loader_instance.run()
            if ret != 0:
                error_message = "加载过程运行失败"
                LOG.error(error_message)
                self.__log_recorder.record(job_starttime=start_time,
                                           job_status=1,
                                           error_message=error_message)
                return ret

        except Exception as e:
            traceback.print_exc()
            error_message = "动态加载指定库失败"
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return -1

        # 校验加载的记录数
        file_row = stat_file_record(self.__args.srcfile)
        table_row = stat_table_record(target_db_info, self.__args.table)
        if file_row != 0 and (file_row - table_row)/file_row > self.__args.rjlimit:
            error_message = ("加载错误超过预设reject比例,"
                           "文件记录数[{0}]加载记录数[{1}]").format(file_row, table_row)
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return -1
        

        # 更新元数据信息
        ret = DdlOperator().load_meta_ddl_direct(self.__args.tableid, self.__args.proveid, ddl_info)
        if ret != 0:
            error_message = "解析DDL文件有误"
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return ret


        ret = self.__log_recorder.record(job_starttime=start_time,
                                   job_status=0,
                                   input_lines=file_row,
                                   output_lines=table_row)
        if ret != 0:
            LOG.error("记录采集日志失败")
            return -1

        end_time_sec = datetime.now()  # 记录结束时间
        cost_time = (end_time_sec - start_time_sec).seconds # 计算耗费时间
        LOG.info("加载总耗时:{0}s".format(cost_time))


        return 0

# main
if __name__ == "__main__":
    ret = 0 # 状态变量

    # 参数解析
    parser = argparse.ArgumentParser(description="数据加载组件")
    parser.add_argument("-proid",   required=True, help="流程ID")
    parser.add_argument("-proveid", required=True, help="项目版本ID")
    parser.add_argument("-schid",   required=True, help="SCHEMA ID")
    parser.add_argument("-tableid", required=True, help="目标表ID标识")
    parser.add_argument("-system",  required=True, help="系统标识")                                                                          
    parser.add_argument("-batch",   required=True, help="批次号")
    parser.add_argument("-org",     required=True, help="机构号")
    parser.add_argument("-bizdate", required=True, help="业务日期，格式YYYYMMDD")
    parser.add_argument("-srcfile", required=True, help="导入文件(绝对路径)")
    parser.add_argument("-ddlfile", required=True, help="DDL文件")
    parser.add_argument("-table",   required=True, help="加载目标表表名")
    parser.add_argument("-mode",    required=False, default="CREATE", help="导入模式[CREATE 或 TRUNCATE 或 APPEND]")
    parser.add_argument("-ktuser",  required=False, help="[加载到Hive时必填]keytab用户名")
    parser.add_argument("-ktfile",  required=False, help="[加载到Hive时必填]keytab文件")
    parser.add_argument("-krbfile", required=False, help="[加载到Hive时必填]krb5文件")
    parser.add_argument("-loaddir", required=False, help="[加载到Hive时必填]加载的HDFS目录")
    parser.add_argument("-delim",   required=False, default="\x01", help="数据文件分隔符，[选填,默认\x01]")
    parser.add_argument("-rjlimit", required=False, default="0", help="加载允许的reject比例[选填,默认0]")
    parser.add_argument("-loadway", required=False, default="0", help="加载方式:0:日常加载作业 1:指定表名加载 2:指定字段加载[选填：默认为0]")
    parser.add_argument("-charset", required=False, default="NULL", help="建表时指定目标字符集(LATIN 或 UNICODE,非TD数据库：NULL)")
    parser.add_argument("-inc",     required=False, default="UTF8", help="加载文件的字符编码(UTF8/GBK)")

    args = parser.parse_args()

    # 调用数据加载类
    loader = DataLoader(args)
    LOG.info("数据加载开始")
    ret = loader.run()
    if ret == 0:
        LOG.info("数据加载完成")
        exit(0)
    else:
        LOG.error("数据加载失败")
        exit(-1)

