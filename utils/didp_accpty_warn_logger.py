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


class AccPtyWarnLogger(object):
    """ 记录账号关联执行日志

    Attributes:
       __bank_id: 机构号
       __batch_dt: 系统标识
       __db_name: 数据库
       __table_name: 表名
       __col_name: 字段名
       __err_num: 错误行数

    """

    def __init__(self, bank_id, batch_dt, db_name, table_name, col_name, err_num):
        self.__bank_id = bank_id
        self.__batch_dt = batch_dt
        self.__db_name = db_name
        self.__table_name = table_name
        self.__col_name = col_name
        self.__err_num = err_num
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

        sql = ("SELECT COUNT(1) FROM DIDP_ACCT_PTY_WARN_LOG"
               "\n WHERE DB_NAME = '{0}' AND TABLE_NAME = '{1}'"
               "\n AND BANK_ID = '{2}' AND BATCH_DT = '{3}'"
               "\n ").format(self.__db_name, self.__table_name,
                             self.__bank_id, self.__batch_dt,
                             )

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

    # def __bakup_current_record(self):
    #     """ 备份当前记录到历史表
    #
    #     Args:
    #
    #     Returns:
    #         0 : 成功 | -1 : 失败
    #     Raise:
    #         jaydebeapi的异常
    #     """
    #     LOG.debug("备份记录到历史表")
    #     sql = ("INSERT INTO DIDP_ACCT_PTY_WARN_LOG "
    #            "\n SELECT * FROM DIDP_MON_RUN_LOG"
    #            "\n WHERE PROCESS_ID = '{0}' AND SYSTEM_KEY = '{1}'"
    #            "\n AND BRANCH_NO = '{2}' AND BIZ_DATE = '{3}'"
    #            "\n AND BATCH_NO = '{4}'").format(self.__process_id, self.__system_key,
    #                                              self.__branch_no, self.__biz_date,
    #                                              self.__batch_no)
    #
    #     LOG.debug("SQL:\n{0}".format(sql))
    #     try:
    #         self.__db_oper.execute(sql)
    #     except Exception as e:
    #         LOG.error("备份记录到历史表失败")
    #         return -1
    #
    #     sql = ("DELETE FROM DIDP_MON_RUN_LOG"
    #            "\n WHERE PROCESS_ID = '{0}' AND SYSTEM_KEY = '{1}'"
    #            "\n AND BRANCH_NO = '{2}' AND BIZ_DATE = '{3}'"
    #            "\n AND BATCH_NO = '{4}'").format(self.__process_id, self.__system_key,
    #                                              self.__branch_no, self.__biz_date,
    #                                              self.__batch_no)
    #
    #     LOG.debug("SQL:\n{0}".format(sql))
    #     try:
    #         self.__db_oper.execute(sql)
    #     except Exception as e:
    #         LOG.error("删除当前表中的历史记录失败")
    #         return -1
    #
    #     return 0

    def insert_current_record(self):
        """ 写入最新记录 

        Args:

        Returns:
            0 : 成功 | -1 : 失败
        Raise:
            jaydebeapi的异常
        """
        LOG.debug("写入记录到作业日志表")

        sql = ("INSERT INTO  DIDP_ACCT_PTY_WARN_LOG("
               "\n BANK_ID, "
               "\n BATCH_DT, "
               "\n DB_NAME, "
               "\n TABLE_NAME, "
               "\n COL_NAME, "
               "\n ERR_NUM "
               "\n ) VALUES ("
               "\n '{0}', "
               "\n '{1}', "
               "\n '{2}', "
               "\n '{3}', "
               "\n '{4}', "
               "\n '{5}'"
               ") ").format(self.__bank_id, self.__batch_dt, self.__db_name, self.__table_name, self.__col_name,
                            self.__err_num)
        LOG.debug("SQL:\n{0}".format(sql))
        try:
            self.__db_oper.execute(sql)
        except Exception as e:
            LOG.error("写入记录到作业日志表失败")
            return -1

        return 0

    # def record(self):
    #     """ 记录执行日志
    #
    #     Args:
    #         job_starttime: 加工开始时间
    #         job_endtime: 加工结束时间
    #         job_status: 作业状态(0:成功,1:失败,其他类型:TODO)
    #         input_lines: 输入的记录数
    #         output_lines: 输出的记录数
    #         reject_lines: 拒绝的记录数
    #         error_message: 错误信息
    #     Returns:
    #         0 : 成功 | -1 : 失败
    #     Raise:
    #
    #     """
    #     job_endtime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #     LOG.info("记录执行日志")
    #
    #     # 检查当前记录是否已经存在
    #     ret = self.__if_record_exist()
    #     if ret == -1:
    #         return -1
    #     elif ret == 0:
    #         # 备份当前记录到历史表
    #         if self.__bakup_current_record():
    #             return -1
    #         # 写入当前记录
    #         if self.__insert_current_record(job_starttime, job_endtime,
    #                                         job_status, input_lines, output_lines,
    #                                         reject_lines, error_message):
    #             return -1
    #     elif ret == 1:
    #         # 写入当前记录
    #         if self.__insert_current_record(job_starttime, job_endtime,
    #                                         job_status, input_lines, output_lines,
    #                                         reject_lines, error_message):
    #             return -1
    #
    #     LOG.debug("记录执行日志完成")
    #     return 0
