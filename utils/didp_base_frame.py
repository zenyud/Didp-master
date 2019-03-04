#-*- coding: UTF-8 -*-
################################################################################
# Date Time     : 2018-12-26
# Write By      : zhangwg
# Function Desc : 仓库加工作业主框架程序
#
# History       :
#                 20181226   zhangwg     Create
#
# Remarks       :
################################################################################

import os
import sys
import types
import importlib
import traceback

reload(sys)
sys.setdefaultencoding( "utf-8" )


sys.path.append("{0}".format(os.environ["DIDP_HOME"]))

from datetime import datetime
from utils.didp_process_tools import Logger, DateOper, ProcessDbOper, FileOper

LOG = Logger()

# 脚本存放路径
JOB_PATH = "/home/moiaagent/job"
RUNLOCKDIR = "/tmp/didp_run_lock"
#RUNLOCKDIR = "F:/lock/didp_run_lock"

class BaseFrame(object):

    def __init__(self, batch_type=None):
        LOG.info("Init BaseFrame")
        self.bank_id  = sys.argv[1]
        self.batch_dt = sys.argv[2]

        self.area_name = "ADS"
        self.db_handle = None  # 数据库连接句柄
        self.batch_type = batch_type;
        LOG.debug("Init Bank ID    : {0}".format(self.bank_id))
        LOG.debug("Init Batch Date : {0}".format(self.batch_dt))

    # 前处理函数：判断作业标志位，确定是否数据回滚
    def before_deal(self):

        LOG.debug("父类: 前处理")

        # 创建数据库连接
        self.db_handle = ProcessDbOper(self.area_name)
        self.db_handle.connect()

        # 初始化变量
        self.yesterday = DateOper.getYesterday(self.batch_dt)

        '''
        if self.batch_type!=None:
            LOG.info("父类: 特殊前处理，批量类别：{0}".format(self.batch_type))
        else:
            LOG.info("父类: 通用前处理")
        '''

    # 后处理函数
    def after_deal(self):
        LOG.debug("父类: 后处理")
        # 断开数据库连接
        if self.db_handle:
            self.db_handle.close()

    # 根据标志文件判断是否需要进行数据清理
    def clean_judge(self):

        lck_file = "{0}/{1}/{2}.lck".\
            format(RUNLOCKDIR, self.area_name, os.path.basename(sys.argv[0]))
        if not os.path.exists(lck_file):
            FileOper.touch_file(lck_file)
            LOG.info("作业当日首次运行,创建作业标志文件[" + lck_file + "]")
            return False
        else:
            LOG.info("作业运行过,需要进行清理")
            return True

    def run(self):

        try:

            start_time = datetime.now()  # 记录开始时间

            LOG.info("Run[{0}]".format(self.__class__.__name__))

            self.before_deal()

            if self.clean_judge():
                self.rollback_deal()

            self.positive_deal()

            self.after_deal()

            end_time = datetime.now()  # 记录结束时间
            cost_time = (end_time - start_time).seconds  # 计算耗费时间
            LOG.info("作业运行总耗时:{0}s".format(cost_time))

        except Exception as ex:
            LOG.error("Exception:{0}".format(ex))
            return -1

        return 0


