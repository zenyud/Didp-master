# -*- coding: UTF-8 -*-
################################################################################
# Date Time     : 2018-10-29
# Write By      : adtec(zhaogx)
# Function Desc : 数据库处理模块
#                 使用jdbc的方式连接
# History       :
#                 20181029  zhaogx     Create
#
# Remarks       :
################################################################################
import os          # 系统模块
import traceback   # 异常跟踪模块
import jaydebeapi  # jdbc连接模块

from didp_logger import Logger

# 日志类实例
LOG = Logger()


class DbOperator2(object):
    """ 数据库操作

    Attributes:
       __conn: 数据库连接变量
       __curs: 数据库cursor变量
       __db_user: 数据库连接用户
       __db_pwd: 数据库连接用户密码
       __jdbc_class: JDBC类
       __jdbc_url: JDBC连接URL
       __jdbc_driver_path: JDBC驱动地址
    """
    def __init__(self, db_user, db_pwd, jdbc_class, jdbc_url, jdbc_driver=""):
        self.__curs = ""
        self.__conn = ""
        self.__db_user = db_user
        self.__db_pwd = db_pwd
        self.__jdbc_class = jdbc_class
        self.__jdbc_url = jdbc_url
        self.__jdbc_driver = jdbc_driver

        self.__print_conn_info()

    def __print_conn_info(self):
        """ 打印数据库连接信息

        Args:

        Returns:

        Raise:

        """
        LOG.debug("----------------数据库连接信息----------------")
        LOG.debug("数据库用户     : {0}".format(self.__db_user))
        LOG.debug("数据库用户密码 : {0}".format(self.__db_pwd))
        LOG.debug("JDBC类名       : {0}".format(self.__jdbc_class))
        LOG.debug("JDBC URL地址   : {0}".format(self.__jdbc_url))
        if self.__jdbc_driver:
            LOG.debug("JDBC驱动地址   : {0}".format(self.__jdbc_driver))
        LOG.debug("----------------------------------------------")

    def connect(self):
        """ 数据库连接
        
        Args:

        Returns:
            
        Raise:
            使用jaydebeapi的异常
        """
        LOG.debug("连接数据库")
        try:
            if self.__jdbc_driver:
                self.__conn = jaydebeapi.connect(self.__jdbc_class, 
                                                 self.__jdbc_url,
                                                 {"user":"{0}".format(
                                                     self.__db_user),
                                                 "password":"{0}".format(
                                                     self.__db_pwd)},
                                                 self.__jdbc_driver)
            else:
                self.__conn = jaydebeapi.connect(self.__jdbc_class, 
                                                 self.__jdbc_url,
                                                 {"user":"{0}".format(
                                                     self.__db_user),
                                                 "password":"{0}".format(
                                                     self.__db_pwd)})
        except:
            LOG.error("连接数据库失败")
            traceback.print_exc() 
            raise
        else:
            LOG.debug("数据库连接成功")

    def close(self):
        if self.__curs != "":
            self.__curs.close()
            self.__curs = ""

        if self.__conn != "":
            self.__conn.close()
            self.__conn = ""

    def execute(self, sql):
        """ 执行无返回结果的sql
        
        Args:
            sql : 待执行的SQL
        Returns:

        Raise:
            使用jaydebeapi的异常
        """
        try:
            self.connect()
        except:
            raise
        else:
            try:
                self.__curs = self.__conn.cursor()
                self.__curs.execute(sql)
            except:        
                LOG.error("执行SQL失败")
                traceback.print_exc() 
                self.close()
                raise
            else:
                self.close()

    def do(self, sql):
        """ 执行并返回受影响的记录数
        
        Args:
            sql : 待执行的SQL
        Returns:

        Raise:
            使用jaydebeapi的异常
        """
        num = 0
        try:
            self.__curs = self.__conn.cursor()
            self.__curs.execute(sql)
            num = self.__curs.rowcount
        except:        
            LOG.error("执行SQL失败")
            traceback.print_exc() 
            return -1

        return num

    def fetchone(self, sql):
        """ 执行返回一条结果的sql
            外部自行处理连接,本函数只负责查询
        Args:
            sql : 待执行的SQL
        Returns:
            result_info : 查询结果(list)
        Raise:
            使用jaydebeapi的异常
        """
        result_info = ""

        try:
            if self.__curs == "":
                self.__curs = self.__conn.cursor()
                self.__curs.execute(sql)
            result_info = self.__curs.fetchone()
        except:        
            LOG.error("执行SQL失败")
            traceback.print_exc() 
            raise
        else:
            return result_info

    def fetchall(self, sql):
        """ 执行返回所有结果的sql(短链接)
        
        Args:
            sql : 待执行的SQL
        Returns:
            result_info : 查询结果(list)
        Raise:
            使用jaydebeapi的异常
        """
        result_info = ""

        try:
            self.connect()
        except:
            raise
        else:
            try:
                self.__curs = self.__conn.cursor()
                self.__curs.execute(sql)
                result_info = self.__curs.fetchall()
            except:        
                LOG.error("执行SQL失败")
                traceback.print_exc() 
                self.close()
                raise
            else:
                self.close()
                return result_info

    def ofetchall(self, sql):
        """ 执行返回所有结果的sql(短链接)
        
        Args:
            sql : 待执行的SQL
        Returns:
            result_info : 查询结果(list)
        Raise:
            使用jaydebeapi的异常
        """
        result_info = ""

        try:
            self.__curs = self.__conn.cursor()
            self.__curs.execute(sql)
            result_info = self.__curs.fetchall()
        except:        
            LOG.error("执行SQL失败")
            traceback.print_exc() 
            self.close()
            raise
        else:
            return result_info
