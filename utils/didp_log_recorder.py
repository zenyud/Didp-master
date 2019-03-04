# -*- coding: UTF-8 -*-                                                                                                                      
################################################################################
# Date Time     : 2018-10-26
# Write By      : adtec(zhaogx)
# Function Desc : 日志记录模块
#
# History       :
#                 20181110  zhaogx     Create
#
# Remarks       :
################################################################################
import os
import datetime
import traceback

from didp_logger import Logger
from didp_db_operator import DbOperator

LOG = Logger()

# 配置库用户
DIDP_CFG_DB_USER = os.environ["DIDP_CFG_DB_USER"]
# 配置库用户密码
DIDP_CFG_DB_PWD = os.environ["DIDP_CFG_DB_PWD"]
# JDBC信息
DIDP_CFG_DB_JDBC_CLASS = os.environ["DIDP_CFG_DB_JDBC_CLASS"]
DIDP_CFG_DB_JDBC_URL = os.environ["DIDP_CFG_DB_JDBC_URL"]


class LogRecorder(object):
    """ 记录执行日志
        支持所有组件的日志记录
    Attributes:
       __process_id: 流程ID
       __system_key: 系统标识
       __branch_no: 机构
       __biz_date: 业务日期(YYYYMMDD)
       __batch_no: 批次号
       __table_name: 当前处理的表名
       __table_id: 当前处理的表名
       __job_type: 加工作业类型(1:采集作业,2:预处理作业,3:装载作业,4:归档作业)
    """
    def __init__(self, process_id, system_key, branch_no, biz_date, batch_no,
                 table_name, table_id, job_type):
        self.__process_id = process_id
        self.__system_key = system_key
        self.__branch_no = branch_no
        self.__biz_date = biz_date
        self.__batch_no = batch_no
        self.__table_name = table_name
        self.__table_id = table_id
        self.__job_type = job_type

        self.__db_oper = DbOperator(DIDP_CFG_DB_USER, DIDP_CFG_DB_PWD,
                                    DIDP_CFG_DB_JDBC_CLASS, DIDP_CFG_DB_JDBC_URL)

    def __if_record_exist(self):
        """ 判断记录是否存在

        Args:

        Returns:
            0 : 存在 | 1 : 不存在 | -1 : 异常
        Raise:
            jaydebeapi的异常
        """
        LOG.debug("检查当前记录是否已经存在历史记录")

        sql = ("SELECT COUNT(1) FROM DIDP_MON_RUN_LOG"
               "\n WHERE PROCESS_ID = '{0}' AND SYSTEM_KEY = '{1}'"
               "\n AND BRANCH_NO = '{2}' AND BIZ_DATE = '{3}'"
               "\n AND BATCH_NO = '{4}'").format(self.__process_id, self.__system_key,
                                                 self.__branch_no, self.__biz_date,
                                                 self.__batch_no)

        LOG.debug("SQL:\n{0}".format(sql))

        try:
            result_debug = self.__db_oper.fetchall_direct(sql)
        except Exception as e:
            LOG.error("查找当前记录是否已经存在历史失败")
            return -1

        if result_debug[0][0] >= 1:
            LOG.debug("存在历史记录")
            return 0
        else:
            LOG.debug("不存在历史记录")
            return 1

    def __bakup_current_record(self):
        """ 备份当前记录到历史表

        Args:

        Returns:
            0 : 成功 | -1 : 失败
        Raise:
            jaydebeapi的异常
        """
        LOG.debug("备份记录到历史表")
        sql = ("INSERT INTO DIDP_MON_RUN_LOG_HIS "
               "\n SELECT * FROM DIDP_MON_RUN_LOG"
               "\n WHERE PROCESS_ID = '{0}' AND SYSTEM_KEY = '{1}'"
               "\n AND BRANCH_NO = '{2}' AND BIZ_DATE = '{3}'"
               "\n AND BATCH_NO = '{4}'").format(self.__process_id, self.__system_key,
                                                 self.__branch_no, self.__biz_date,
                                                 self.__batch_no)

        LOG.debug("SQL:\n{0}".format(sql))
        try:
            self.__db_oper.execute(sql)
        except Exception as e:
            LOG.error("备份记录到历史表失败")
            return -1

        sql = ("DELETE FROM DIDP_MON_RUN_LOG"
               "\n WHERE PROCESS_ID = '{0}' AND SYSTEM_KEY = '{1}'"
               "\n AND BRANCH_NO = '{2}' AND BIZ_DATE = '{3}'"
               "\n AND BATCH_NO = '{4}'").format(self.__process_id, self.__system_key,
                                                 self.__branch_no, self.__biz_date,
                                                 self.__batch_no)

        LOG.debug("SQL:\n{0}".format(sql))
        try:
            self.__db_oper.execute(sql)
        except Exception as e:
            LOG.error("删除当前表中的历史记录失败")
            return -1

        return 0

    def __insert_current_record(self, job_starttime, job_endtime, job_status,
                                input_lines, output_lines, reject_lines,
                                error_message):
        """ 写入最新记录 

        Args:

        Returns:
            0 : 成功 | -1 : 失败
        Raise:
            jaydebeapi的异常
        """
        LOG.debug("写入记录到作业日志表")

        sql = ("INSERT INTO DIDP_MON_RUN_LOG ("
               "\n PROCESS_ID, "
               "\n SYSTEM_KEY, "
               "\n BRANCH_NO, "
               "\n BIZ_DATE, "
               "\n BATCH_NO, "
               "\n TABLE_NAME, "
               "\n TABLE_ID, "
               "\n JOB_TYPE, "
               "\n JOB_STARTTIME, "
               "\n JOB_ENDTIME, "
               "\n JOB_STATUS, "
               "\n INPUT_LINES, "
               "\n OUTPUT_LINES, "
               "\n REJECT_LINES, "
               "\n ERR_MESSAGE) VALUES ("
               "\n '{0}', "
               "\n '{1}', "
               "\n '{2}', "
               "\n '{3}', "
               "\n '{4}', "
               "\n '{5}', "
               "\n '{6}', "
               "\n '{7}', "
               "\n '{8}', "
               "\n '{9}', "
               "\n '{10}', "
               "\n {11}, "
               "\n {12}, "
               "\n {13}, "
               "\n '{14}') ").format(self.__process_id, self.__system_key,
                                     self.__branch_no, self.__biz_date,
                                     self.__batch_no, self.__table_name,
                                     self.__table_id, self.__job_type,
                                     job_starttime, job_endtime, job_status,
                                     input_lines, output_lines,
                                     reject_lines, error_message)
        LOG.debug("SQL:\n{0}".format(sql))
        try:
            self.__db_oper.execute(sql)
        except Exception as e:
            LOG.error("写入记录到作业日志表失败")
            return -1

        return 0

    def record(self, job_starttime, job_status, input_lines=0,
               output_lines=0, reject_lines=0, error_message=""):
        """ 记录执行日志

        Args:
            job_starttime: 加工开始时间
            job_endtime: 加工结束时间
            job_status: 作业状态(0:成功,1:失败,其他类型:TODO)
            input_lines: 输入的记录数
            output_lines: 输出的记录数
            reject_lines: 拒绝的记录数
            error_message: 错误信息
        Returns:
            0 : 成功 | -1 : 失败
        Raise:

        """
        job_endtime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        LOG.info("记录执行日志")

        # 检查当前记录是否已经存在
        ret = self.__if_record_exist()
        if ret == -1:
            return -1
        elif ret == 0:
            # 备份当前记录到历史表
            if self.__bakup_current_record():
                return -1
            # 写入当前记录
            if self.__insert_current_record(job_starttime, job_endtime,
                                            job_status, input_lines, output_lines,
                                            reject_lines, error_message):
                return -1
        elif ret == 1:
            # 写入当前记录
            if self.__insert_current_record(job_starttime, job_endtime,
                                            job_status, input_lines, output_lines,
                                            reject_lines, error_message):
                return -1

        LOG.debug("记录执行日志完成")
        return 0

