#-*- coding: UTF-8 -*-  
################################################################################
# Date Time     : 2018-10-26
# Write By      : adtec(xiazhy,zhaogx)
# Function Desc : 卸载组件主程序
#
# History       :
#                 20181026  xiazhy     Create
#
# Remarks       :
################################################################################
import os
import re
import sys
import argparse
import traceback
import importlib
import jaydebeapi

reload(sys)
sys.setdefaultencoding('utf8')

sys.path.append("{0}".format(os.environ["DIDP_HOME"]))

from datetime import datetime
from utils.didp_logger import Logger
from utils.didp_tools import get_db_login_info
from utils.didp_log_recorder import LogRecorder

# 全局变量
LOG = Logger()


class ExportData(object):
    """ 数据卸载类
    
    Attributes:
       __args : 参数
    """
    def __init__(self, args):
        self.__args = args

        # argparse接收到的参数没有转义，后续使用的时候需要将其替换成对应的值
        self.__args.rcdelim = re.sub(r"\\n", "\n", self.__args.rcdelim)
        self.__args.rcdelim = re.sub(r"\\r", "\r", self.__args.rcdelim)


        self.__log_recorder = LogRecorder(process_id=self.__args.proid,
                                          system_key=self.__args.system,
                                          branch_no=self.__args.org,
                                          biz_date=self.__args.bizdate,
                                          batch_no=self.__args.batch,
                                          table_name=self.__args.table,
                                          table_id=self.__args.tableid,
                                          job_type="1")

        self.__print_arguments()

    def __print_arguments(self):
        """ 参数格式化输出

        Args:

        Returns:

        Raise:

        """
        LOG.debug("-------------------参数清单-------------------")
        LOG.debug("流程ID                     : {0}".format(self.__args.proid))
        LOG.debug("SCHEMA ID                  : {0}".format(self.__args.schid))
        LOG.debug("项目版本ID                 : {0}".format(self.__args.proid))
        LOG.debug("系统标识                   : {0}".format(self.__args.system))
        LOG.debug("批次号                     : {0}".format(self.__args.batch))
        LOG.debug("机构号                     : {0}".format(self.__args.org))
        LOG.debug("业务日期                   : {0}".format(self.__args.bizdate))
        LOG.debug("导出文件(全路径)           : {0}".format(self.__args.outfile))
        LOG.debug("卸数目标表名               : {0}".format(self.__args.table))
        LOG.debug("卸数目标表名ID             : {0}".format(self.__args.tableid))
        LOG.debug("过滤条件                   : {0}".format(self.__args.filt))
        LOG.debug("定长标志                   : {0}".format(self.__args.fixed))
        LOG.debug("分隔符                     : {0}".format(self.__args.delim))
        LOG.debug("是否含末尾分隔符           : {0}".format(self.__args.enddel))
        LOG.debug("记录分隔格式               : {0}".format(self.__args.rcdelim))
        LOG.debug("字符集                     : {0}".format(self.__args.charset))
        LOG.debug("是否是视图                 : {0}".format(self.__args.isview))
        LOG.debug("视图分布键                 : {0}".format(self.__args.viewkey))
        LOG.debug("筛选字段                   : {0}".format(self.__args.selcol))
        LOG.debug("是否设置默认值             : {0}".format(self.__args.setdef))
        LOG.debug("字符字段是否作TRIM         : {0}".format(self.__args.trimflg))
        LOG.debug("是否替换字段中包含的换行符 : {0}".format(self.__args.repflg))
        LOG.debug("----------------------------------------------")

    def run(self):
        """ 运行卸数

        Args:

        Returns:
            0 - 成功 | -1 - 失败
        Raise:

        """
        error_message = "" # 错误信息
        target_db_info = {} # 卸数目标库连接信息
        # 采集开始时间
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start_time_sec = datetime.now()

        # 获取目标表连接信息
        ret, target_db_info = get_db_login_info(self.__args.schid)
        if ret != 0:
            error_message = "获取目标表连接信息失败"
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return ret 
        
        # 动态加载指定库的卸数类
        LOG.info("加载{0}库的卸数类,并调用对应的卸数方法".format(
            target_db_info['db_type']))
        try:         
            export_module = importlib.import_module(
                "plugins.didp_{0}_plugin".format(
                     target_db_info['db_type'].lower()))
            export_class = getattr(export_module, "Exporter")
        except Exception as e:
            traceback.print_exc()
            error_message = "动态加载指定库的卸数类失败"
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return -1

        export_instance = export_class(self.__args, target_db_info)
        ret, export_lines, table_lines = export_instance.run()
        if ret != 0:
            error_message = ("调用{0}库的卸数类执行卸数失败".format(
                              target_db_info['db_type']))
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return ret 

        ret = self.__log_recorder.record(job_starttime=start_time,
                                   job_status=0,
                                   input_lines=table_lines,
                                   output_lines=export_lines)
        if ret != 0:
            LOG.error("记录采集日志失败")
            return -1

        end_time_sec = datetime.now()

        # 计算耗时
        cost_time = (end_time_sec - start_time_sec).seconds
        LOG.info("采集耗时:{0}s".format(cost_time))

        return 0

# main
if __name__ == "__main__":
    ret = 0 # 状态变量

    # 参数解析
    parser = argparse.ArgumentParser(description="数据卸载组件")
    parser.add_argument("-proid",   required=True, help="流程ID")
    parser.add_argument("-proveid", required=True, help="项目版本ID")
    parser.add_argument("-schid",   required=True, help="SCHEMA ID")
    parser.add_argument("-system",  required=True, help="系统标识")
    parser.add_argument("-batch",   required=True, help="批次号")
    parser.add_argument("-org",     required=True, help="机构号")
    parser.add_argument("-bizdate", required=True, help="业务日期(YYYYMMDD)")
    parser.add_argument("-table",   required=True, help="卸数目标表")
    parser.add_argument("-tableid", required=True, help="卸数目标表ID")
    parser.add_argument("-outfile", required=True, help="卸数文件(全路径)")
    parser.add_argument("-filt",    required=True, help="过滤条件")
    parser.add_argument("-fixed",   choices=["0", "1"], default="0",
                        help="[可选]定长标识(1:定长|0:不定长),默认:0")
    parser.add_argument("-delim",   default="|@|",
                        help="[可选]分隔符,默认:|@|")
    parser.add_argument("-enddel",  choices=["Y", "N"], default="Y",
                        help="[可选]是否含末尾分隔符,默认:Y")
    parser.add_argument("-rcdelim", default="\n",
                        help="[可选]记录分隔格式,默认UNIX格式(\\n结束)")
    parser.add_argument("-charset", default="1",
                        help="[可选]字符集(1:UTF-8|0:GBK),默认:1")
    parser.add_argument("-ddlfile", default="", help="[可选]DDL文件(全路径)")
    parser.add_argument("-ctlfile", default="", help="[可选]CTRL文件(全路径)")
    parser.add_argument("-isview",  choices=["Y", "N"], default="N",
                        help="[可选]是否是视图,默认:N")
    parser.add_argument("-viewkey", default="",
                        help="[可选]视图分布键,当isview为Y时给出")
    parser.add_argument("-selcol",  default="", 
                        help="[可选]按字段卸数,逗号分隔")
    parser.add_argument("-setdef",  choices=["Y", "N"], default="N",
                        help="[可选]是否设置默认值,默认:N")
    parser.add_argument("-trimflg", choices=["0", "1", "2"], default="1",
                        help=("[可选]字符字段TRIM选择"
                              "(0 - 两侧TRIM,1 - 右侧TRIM,2 - 不做TRIM),默认:1"))
    parser.add_argument("-repflg",  choices=["Y", "N"], default="N",
                        help="[可选]是否替换回车,换行符,默认:Y")
    parser.add_argument("-dtfmt",   choices=["YYYY-MM-DD", "YYYY-MM-DDTHH:MI:SS"],
                        default="YYYY-MM-DDTHH:MI:SS", help=("[可选]DATE类型格式,"
                        "针对oralce(YYYY-MM-DD|YYYY-MM-DDTHH:MI:SS),"
                        "默认:YYYY-MM-DDTHH:MI:SS"))
    parser.add_argument("-repdel",  choices=["Y", "N"], default="Y",
                        help="[可选]是否替换字符串字段中的分隔符,默认:Y")

    args = parser.parse_args()

    # 调用卸数类
    export = ExportData(args)
    LOG.info("数据卸载开始")
    ret = export.run()
    if ret == 0:
        LOG.info("数据卸载完成")
        exit(0)
    else:
        LOG.error("数据卸载失败")
        exit(-1)

