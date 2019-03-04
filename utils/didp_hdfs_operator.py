# -*- coding: UTF-8 -*-
################################################################################
# Date Time     : 2018-12-24
# Write By      : adtec(zhaogx)
# Function Desc : HDFS操作模块
#
# History       :
#                 20181224  zhaogx     Create
#
# Remarks       :
################################################################################
import datetime
import traceback

from hdfs.ext.kerberos import KerberosClient

from didp_logger import Logger

# 全局变量
LOG = Logger()

class HdfsOperator(KerberosClient):
    """ HDFS Kerberos客户端操作类(继承自hdfs.ext.kerberos.KerberosClient)
    
    Attributes:

    """

if __name__ == '__main__':
    h = HdfsOperator('http://kbapc001:50070;http://kbapc002:50070', principal='hive')

    a = h.list("/")
    print a

