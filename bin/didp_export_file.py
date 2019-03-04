#-*- coding: UTF-8 -*-  
################################################################################
# Date Time     : 2018-11-27
# Write By      : adtec(xiazhy,zhaogx)
# Function Desc : 文件采集组件
#
# History       :
#                 20181127  xiazhy     Create
#
# Remarks       :
################################################################################
import os
import sys
import argparse
import traceback
import importlib
import jaydebeapi
import ftplib

from datetime import datetime
sys.path.append("{0}".format(os.environ["DIDP_HOME"]))

from utils.didp_logger import Logger
from utils.didp_tools import stat_file_record
from utils.didp_db_operator import DbOperator
from utils.didp_ddl_operator import DdlOperator
from utils.didp_log_recorder import LogRecorder
from utils.didp_ddlfile_parser import DDLFileParser
from utils.didp_ctlfile_parser import CtlFileParser
from utils.didp_tools import get_db_login_info,generate_common_ddl_type

# 全局变量
LOG = Logger()
JDBC_DRIVER_PATH = os.getenv("DIDP_JDBC_DRIVER_PATH")
# 配置库用户
DIDP_CFG_DB_USER = os.environ["DIDP_CFG_DB_USER"]
# 配置库用户密码
DIDP_CFG_DB_PWD = os.environ["DIDP_CFG_DB_PWD"]
# JDBC信息python
DIDP_CFG_DB_JDBC_CLASS = os.environ["DIDP_CFG_DB_JDBC_CLASS"]
DIDP_CFG_DB_JDBC_URL = os.environ["DIDP_CFG_DB_JDBC_URL"]


class FileCollector:
    """ 文件采集类

        Attributes:
           __args : 参数
    """

    __conInfo = {}

    def __init__(self, args):
        self.__args = args
        self.__log_recorder = LogRecorder(provess_id=self.__args.proid,
                                          system_key=self.__args.system,
                                          branch_no=self.__args.org,
                                          biz_date=self.__args.bizdate,
                                          batch_no=self.__args.batch,
                                          table_name=self.__args.table,
                                          table_id=self.__args.tableid,
                                          job_type="2")

        self.__print_arguments()

    def __print_arguments(self):
        """ 参数格式化输出
        Args:

        Returns:

        Raise:

        """
        LOG.debug("-------------------参数清单-------------------")
        LOG.debug("流程ID                     : {0}".format(self.__args.proid))
        LOG.debug("项目版本ID                 : {0}".format(self.__args.proveid))
        LOG.debug("SCHEMA ID                  : {0}".format(self.__args.schid))
        LOG.debug("系统标识                   : {0}".format(self.__args.system))
        LOG.debug("批次号                     : {0}".format(self.__args.batch))
        LOG.debug("机构号                     : {0}".format(self.__args.org))
        LOG.debug("业务日期                   : {0}".format(self.__args.bizdate))
        LOG.debug("表名                       : {0}".format(self.__args.table))
        LOG.debug("表ID                       : {0}".format(self.__args.tableid))
        LOG.debug("业务日期                   : {0}".format(self.__args.bizdate))
        LOG.debug("源文件全路径               : {0}".format(self.__args.srcfile))
        LOG.debug("目标文件全路径             : {0}".format(self.__args.tarfile))
        LOG.debug("源控制文件全路径           : {0}".format(self.__args.scfile))
        LOG.debug("目标控制文件全路径         : {0}".format(self.__args.tcfile))
        LOG.debug("源DDL文件全路径            : {0}".format(self.__args.sdfile))
        LOG.debug("目标DDL文件全路径          : {0}".format(self.__args.tdfile))
        LOG.debug("备份目录                   : {0}".format(self.__args.bakdir))
        LOG.debug("----------------------------------------------")

    def __get_srcfile_conf_info(self, schemaid):
        """ 从配置库中获取目标库的信息
        Args:
            schema_id - SCHAME ID
        Returns:
            0, 目标库信息 - 成功 | -1, '' - 失败
        Raise:

        """
        LOG.info("获取文件数据源配置信息")
        result_info = []  # 结果信息
        target_con_info = {}  # 目标库连接信息

        sql = "SELECT" \
              "\n T1.SCHEMA_NAME," \
              "\n T2.FILE_TYPE," \
              "\n T2.SERVER_TYPE," \
              "\n T2.PROTOCOL_TYPE," \
              "\n T1.ROOT_PATH," \
              "\n T3.IP_ADDRESS," \
              "\n T3.PORT," \
              "\n T2.USER_NAME," \
              "\n T2.USER_PWD," \
              "\n T2.SEPARATE_CHAR," \
              "\n T2.LINE_BREAK," \
              "\n T2.QUOTE_TYPE" \
              "\n FROM DIDP_META_SCHEMA_INFO T1," \
              "\n      DIDP_META_SOURCE_FILE_CONFIG T2," \
              "\n      DIDP_META_DATA_SOURCE_INFO T3" \
              "\n WHERE T1.SOURCE_ID = T3.SOURCE_ID" \
              "\n   AND T3.CONFIG_ID = T2.CONFIG_ID" \
              "\n   AND T1.SCHEMA_ID = '{0}'".format(schemaid)

        LOG.info("SQL:\n{0}".format(sql))

        db_oper = DbOperator(DIDP_CFG_DB_USER, DIDP_CFG_DB_PWD,
                             DIDP_CFG_DB_JDBC_CLASS, DIDP_CFG_DB_JDBC_URL,
                             "{0}/mysql.jar".format(JDBC_DRIVER_PATH))

        try:
            result_info = db_oper.fetchall_direct(sql)
        except Exception as e:
            LOG.error("获取文件源连接信息失败")
            return -1, None

        if result_info:
            LOG.info("------------文件源信息------------")
            LOG.info("SCHEMA名     : {0}".format(result_info[0][0]))
            LOG.info("文件类型     : {0}".format(result_info[0][1]))
            LOG.info("服务器类型   : {0}".format(result_info[0][2]))
            LOG.info("传输协议     : {0}".format(result_info[0][3]))
            LOG.info("存储路径     : {0}".format(result_info[0][4]))
            LOG.info("IP地址       : {0}".format(result_info[0][5]))
            LOG.info("端口         : {0}".format(result_info[0][6]))
            LOG.info("用户名       : {0}".format(result_info[0][7]))
            LOG.info("密码         : {0}".format(result_info[0][8]))
            LOG.info("字段分隔符   : {0}".format(result_info[0][9]))
            LOG.info("行分隔符     : {0}".format(result_info[0][10]))
            LOG.info("定界符类型   : {0}".format(result_info[0][11]))
            LOG.info("----------------------------------------------")

            target_con_info['file_type'] = result_info[0][1]
            target_con_info['server_type'] = result_info[0][2]
            target_con_info['protocol_type'] = result_info[0][3]
            target_con_info['ip_addr'] = result_info[0][5]
            target_con_info['port'] = result_info[0][6]
            target_con_info['user'] = result_info[0][7]
            target_con_info['password'] = result_info[0][8]
            target_con_info['delim'] = result_info[0][9]
            target_con_info['rcdelim'] = result_info[0][10]
            target_con_info['quote_type'] = result_info[0][11]

            return 0, target_con_info
        else:
            LOG.error("未找到SCHEMA ID为[{0}]的连接信息".format(schemaid))
            return -1, None

    def dir_check(self, file):
        try:
            tardir = os.path.dirname(file)

            # 目标目录不存在则创建
            if not os.path.exists(tardir):
                os.makedirs(tardir)

            # 目标文件存在则删除
            if os.path.exists(file):
                LOG.info("目标文件已存在，删除[{0}]".format(file))
                os.remove(file)

        except Exception as e:
            traceback.print_exc()
            LOG.error("复制文件失败")
            raise


    def copy_file(self, srcfile, tarfile):

        try:

            self.dir_check(tarfile)

            # 复制文件
            LOG.info("复制文件[{0}]->[{1}]".format(srcfile, tarfile))
            open(tarfile, "wb").write(open(srcfile, "rb").read()) 
        except Exception as e:
            traceback.print_exc()
            LOG.error("复制文件失败")
            return -1
        
        return 0

    def link_file(self, srcfile, tarfile):

        try:

            self.dir_check(tarfile)

            # 复制文件
            #LOG.info("复制文件[{0}]->[{1}]".format(srcfile, tarfile))
            #open(tarfile, "wb").write(open(srcfile, "rb").read()) 
            os.symlink(srcfile, tarfile)
        except Exception as e:
            traceback.print_exc()
            LOG.error("复制文件失败")
            return -1
        
        return 0

    def __check_data_file(self):

        cfp = CtlFileParser(self.__args.scfile, "XML")
        ret, ctl_info = cfp.get_ctl_info()
        if ret != 0:
            LOG.error("解析控制文件失败")
            return ret

        # 1.校验文件大小是否正确
        ssize = os.path.getsize(self.__args.srcfile)
        if ssize != int(ctl_info['filesize']):
            LOG.error("文件不小校验不通过：预期[{0}]实际[{1}]".format(ctl_info['filesize'], ssize))
            return -1

        # 2.校验记录数（资源消耗大）

        # 3.校验文件编码
        
        return 0
    
    def __collect_file_from_local(self):

        LOG.info("开始采集本地文件")

        if self.__args.srcfile == "":
            LOG.error("采集源DAT文件全路径未给出")
            return -1

        # 控制文件存在则先检验
        if os.path.isfile(self.__args.srcfile) and os.path.isfile(self.__args.scfile):
            ret = self.__check_data_file()
            if ret != 0:
                LOG.error("源文件校验失败")
                return ret
            else:
                LOG.info("源文件校验通过")

        # 判断源文件是否存在
        if not os.path.isfile(self.__args.srcfile):
            LOG.error("采集源文件不存在[{0}]".format(self.__args.srcfile))
            return -1
        else:
            ret = self.link_file(self.__args.srcfile, self.__args.tarfile)
            if ret != 0:
                LOG.error("复制数据文件失败")
                return -1

        # 判断控制文件是否存在
        if not os.path.isfile(self.__args.scfile):
            LOG.info("源数据文件无控制文件")
        else:
            ret = self.link_file(self.__args.scfile, self.__args.tcfile)
            if ret != 0:
                LOG.error("复制控制文件失败")
                return -1

        # 判断DDL文件是否存在
        if not os.path.isfile(self.__args.sdfile):
            LOG.info("源数据文件无DDL文件")
        else:
            ret = self.link_file(self.__args.sdfile, self.__args.tdfile)
            if ret != 0:
                LOG.error("复制DDL文件失败")
                return -1

        # 校验源文件和目标文件的大小
        ssize = os.path.getsize(self.__args.srcfile)
        tsize = os.path.getsize(self.__args.tarfile)
        LOG.debug("源数据文件大小[{0}]目标数据文件大小[{1}]".format(ssize, tsize))
        if ssize - tsize != 0:
            LOG.error("源数据文件大小[{0}]和目标数据文件大小[{1}]不一致".format(ssize, tsize))
            return -1

        # 备份文件
        if self.__args.bakdir != "":

            for f in [self.__args.tarfile, self.__args.tcfile, self.__args.tdfile]:

                # 备份文件
                fname = os.path.basename(f)
                tar = "{0}/{1}".format(self.__args.bakdir, fname)

                LOG.info("备份文件[{0}]->[{1}]".format(f, tar))

                ret = self.copy_file(f, tar)
                if ret != 0:
                    LOG.error("备份文件失败")
                    return -1

        return 0

    def __collect_file_from_ftp(self):

        LOG.info("开始采集SFTP服务器数据文件")

        if self.__conInfo['ip_addr'] == "":
            LOG.error("FTP数据源的ip_addr未配置")
            return -1;

        if self.__conInfo['port'] == "":
            LOG.error("FTP数据源的port未配置")
            return -1;

        if self.__conInfo['user'] == "":
            LOG.error("FTP数据源的user未配置")
            return -1;

        if self.__conInfo['password'] == "":
            LOG.error("FTP数据源的password未配置")
            return -1;

        if self.__args.srcfile == "":
            LOG.error("采集源数据文件全路径未给出")
            return -1

        sf = paramiko.Transport(self.__conInfo['ip_addr'], self.__conInfo['port'])
        sf.connect(username=self.__conInfo['user'], password=self.__conInfo['password'])
        sftp = paramiko.SFTPClient.from_transport(sf)

        # 判断源文件是否存在
        if not os.path.isfile(self.__args.srcfile):
            LOG.error("采集源文件不存在[{0}]".format(self.__args.srcfile))
            return -1
        else:
            self.dir_check(self.__args.tarfile)
            sftp.get(self.__args.srcfile, self.__args.tarfile)

        # 判断控制文件是否存在
        if not os.path.isfile(self.__args.scfile):
            LOG.info("源数据文件无控制文件")
        else:
            self.dir_check(self.__args.tarfile)
            sftp.get(self.__args.scfile, self.__args.tcfile)

        # 判断DDL文件是否存在
        if not os.path.isfile(self.__args.sdfile):
            LOG.info("源数据文件无DDL文件")
        else:
            self.dir_check(self.__args.tarfile)
            sftp.get(self.__args.sdfile, self.__args.tdfile)


        # 校验文件大小
        ssize = f.size(self.__args.srcfile)
        tsize = os.path.getsize(self.__args.tarfile)
        LOG.debug("源数据文件大小[{0}]目标数据文件大小[{1}]".format(ssize, tsize))
        if ssize - tsize != 0:
            LOG.error("源数据文件大小[{0}]和目标数据文件大小[{1}]不一致".format(ssize, tsize))
            return -1

        return 0

    def __collect_file_from_sftp(self):
        pass
        ## 暂不支持，后续再开发
        return -1

    def __ddl_to_stdddl(self, col_info, conF):
        out_col_info = []
        for i in col_info:
            col = {}
            col['table_name'] = i['tablename']
            col['fixed'] = i['fixed']
            col['rcdelim'] = conF['rcdelim']
            col['column_name'] = i['column_name']
            col['column_define_length'] = i['column_define_length']
            col['column_scale'] = i['column_precision']
            col['column_std_type'] = generate_common_ddl_type(i['data_type'], i['column_define_length'], i['column_precision'])
            col['is_pk'] = i['is_pk']
            col['is_null'] = "Y"
            col['delim'] = conF['delim']
            col['data_type'] = i['data_type']
            col['column_desc'] = i['column_desc']
            col['quote_type'] = i['quote_type']

            out_col_info.append(col)

        return out_col_info

    def run(self):
        """ 运行文件加载程序
        Args:
            None
        Returns:
            0 - 成功 | -1 - 失败
        Raise:
            None
        """
        start_time = datetime.now() # 记录开始时间
        error_message = "" # 错误信息

        # 获取文件数据源配置信息
        ret, self.__conInfo = self.__get_srcfile_conf_info(self.__args.schid)
        if ret != 0:
            error_message = "获取文件数据源信息失败"
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return ret

        # 根据数据源类型分别处理 server_type:1-本地 2-远程；protocol_type:1-ftp 2-sftp
        LOG.debug("server_type[{0}]protocol_type[{1}]".format(self.__conInfo['server_type'], self.__conInfo['protocol_type']))
        if self.__conInfo['server_type'] == '1':
            ret = self.__collect_file_from_local()
            if ret != 0:
                error_message = "本地文件采集失败"
                LOG.error(error_message)
                self.__log_recorder.record(job_starttime=start_time,
                                           job_status=1,
                                           error_message=error_message)
                return -1
        elif self.__conInfo['server_type'] == '2' or self.__conInfo['protocol_type'] == '1':
            ret = self.__collect_file_from_ftp()
            if ret != 0:
                error_message = "FTP文件采集失败"
                LOG.error(error_message)
                self.__log_recorder.record(job_starttime=start_time,
                                           job_status=1,
                                           error_message=error_message)
                return -1
        elif self.__conInfo['server_type'] == '2' or self.__conInfo['protocol_type'] == '2':
            ret = self.__collect_file_from_sftp()
            if ret != 0:
                error_message = "SFTP文件采集失败"
                LOG.error(error_message)
                self.__log_recorder.record(job_starttime=start_time,
                                           job_status=1,
                                           error_message=error_message)
                return -1


        # DDL比对并更新
        LOG.info("-----------比对DDL-----------")
        dfp = DDLFileParser(self.__args.sdfile, "XML")
        ret, ddl_info = dfp.get_ddl_info()
        if ret != 0:
            error_message = "解析DDL文件有误"
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return ret
        #LOG.debug(ddl_info)
        
        std_ddl_info = self.__ddl_to_stdddl(ddl_info, self.__conInfo)
        ret = DdlOperator().load_common_ddl_direct(self.__args.tableid, std_ddl_info)
        if ret != 0:
            error_message = "更新通用DDL失败"
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return ret

        ret = DdlOperator().load_meta_ddl_direct(self.__args.tableid, self.__args.proveid, ddl_info)
        if ret != 0:
            error_message = "更新元数据DDL失败"
            LOG.error(error_message)
            self.__log_recorder.record(job_starttime=start_time,
                                       job_status=1,
                                       error_message=error_message)
            return ret

        # 获取源和目标文件的记录数
        src_file_lines = stat_file_record(self.__args.srcfile)
        tar_file_lines = stat_file_record(self.__args.tarfile)
        
        self.__log_recorder.record(job_starttime=start_time,
                                   job_status=0,
                                   input_lines=src_file_lines,
                                   output_lines=tar_file_lines)

        end_time = datetime.now()  # 记录结束时间
        cost_time = (end_time - start_time).seconds # 计算耗费时间
        LOG.info("卸数总耗时:{0}s".format(cost_time))

        return 0

# main
if __name__ == "__main__":
    ret = 0 # 状态变量

    # 参数解析
    parser = argparse.ArgumentParser(description="数据卸载组件")
    parser.add_argument("-proid",   required=True, help="流程ID")
    parser.add_argument("-proveid",   required=True, help="项目版本ID")
    parser.add_argument("-schid",   required=True, help="SCHEMA ID")
    parser.add_argument("-table",   required=True, help="目标表名")
    parser.add_argument("-tableid", required=True, help="目标表ID标识")
    parser.add_argument("-system",  required=True, help="系统标识")
    parser.add_argument("-batch",   required=True, help="批次号")
    parser.add_argument("-org",     required=True, help="机构号")
    parser.add_argument("-bizdate", required=True, help="业务日期(YYYYMMDD)")
    parser.add_argument("-srcfile", required=True, help="源文件全路径")
    parser.add_argument("-tarfile", required=True, help="目标文件全路径")
    parser.add_argument("-scfile",  required=False, help="源控制文件全路径")
    parser.add_argument("-tcfile",  required=False, help="目标控制文件全路径")
    parser.add_argument("-sdfile",  required=False, help="源DDL文件全路径")
    parser.add_argument("-tdfile",  required=False, help="目标DDL文件全路径")
    parser.add_argument("-bakdir",  required=False, help="备份目录")

    args = parser.parse_args()

    # 调用数据加载类
    fcol = FileCollector(args)
    LOG.info("文件采集开始")
    ret = fcol.run()
    if ret == 0:
        LOG.info("数据采集完成")
        exit(0)
    else:
        LOG.error("数据采集失败")
        exit(-1)

