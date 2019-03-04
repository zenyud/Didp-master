#-*-coding: utf-8 -*-
################################################################################
# Date Time     : 2018-12-10
# Write By      : adtec(xiazhy)
# Function Desc : 数据加工的公共模块
#
# History       :
#                 20181210  xiazhy     Create
#
# Remarks       :
################################################################################

import os
import sys
import time
import datetime

#字符集配置
reload(sys)
sys.setdefaultencoding('utf8')

sys.path.append("{0}".format(os.environ["DIDP_HOME"]))

from utils.didp_db_operator import DbOperator
from utils.didp_logger import Logger

DIDP_HOME = os.environ["DIDP_HOME"]

LOG = Logger()

class FileOper(object):
    @staticmethod
    def touch_file(file):

        dir = os.path.dirname(file)

        if not os.path.exists(dir):
            os.makedirs(dir)

        if not os.path.exists(dir):
            return -1

        f = open(file, 'w')
        f.close()

        return 0

class DateOper(object):

    @staticmethod
    def isValidDate(date_string):
        """ 校验8位日期字段是否合法
        
        Args:
            date_string : 日期字符串YYYYMMDD
        Returns:
            True: 成功
            False: 失败
        Raise:
    
        """
        try:
            time.strptime(date_string, "%Y%m%d")
            return True
        except Exception as e:
            return False 
    
    
    @staticmethod
    def getYesterday(date_string):
        """ 计算昨天日期
        
        Args:
            date_string : 日期字符串YYYYMMDD
        Returns:
            yesterday_string : 日期字符串YYYYMMDD
        Raise:
    
        """
        try:
            now = datetime.datetime.strptime(date_string, "%Y%m%d")
            yesterday = now - datetime.timedelta(days=1)
            yesterday_str = yesterday.strftime('%Y%m%d')
            return yesterday_str
        except Exception as e:
            LOG.error(e)
            return False 
    
class ProcessDbOper(DbOperator):

    # 数据库连接串配置目录
    __LOGIN_CONF_DIR = DIDP_HOME + "/etc"

    __db_user = ""
    __db_passwd = ""
    __db_url = ""

    def __init__(self, whichStore):
        LOG.debug("Init ProcessDbOper:" + whichStore)
        if whichStore == "SDS":
            (self.__db_user, self.__db_passwd, self.__db_url ) \
                = self.__get_db_config(self.__LOGIN_CONF_DIR
                                       + "/SDS_LOGON")

        elif whichStore == "FDS":
            (self.__db_user, self.__db_passwd, self.__db_url) \
                = self.__get_db_config(self.__LOGIN_CONF_DIR
                                       + "/FDS_LOGON")

        super(ProcessDbOper, self).__init__(
            self.__db_user, self.__db_passwd,
            "org.apache.hive.jdbc.HiveDriver", self.__db_url,
            "{0}/drivers/inceptor-driver-6.0.0.jar".format(DIDP_HOME))

    def connect(self):

        if self.__db_user == None:
            raise("数据库用户配置为空")
        if self.__db_passwd == None:
            raise("数据库密码配置为空")
        if self.__db_url == None:
            raise("数据库URL配置为空")

        super(ProcessDbOper, self).connect();

    def __get_db_config(self, login_file):
        """ 计算昨天日期

        Args:
            login_file : 数据库连接信息配置文件
        Returns:
            db_user : 数据库用户
            db_passwd : 数据库密码
            db_name : 数据库名
        Raise:

        """
        try:
            f = open(login_file, 'r')
            login_str = f.read().strip("\n")
            login_arr = login_str.split("|")
            db_user = login_arr[0]
            db_passwd = login_arr[1]
            db_url = login_arr[2]

        except Exception as e:
            LOG.error(e)
            return ( None, None, None)

        finally:
            if f:
                f.close()
        return ( db_user, db_passwd, db_url)
