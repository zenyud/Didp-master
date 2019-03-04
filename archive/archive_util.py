# -*- coding: UTF-8 -*-

# Date Time     : 2019/1/9
# Write By      : adtec(ZENGYU)
# Function Desc :  归档工具模块
# History       : 2019/1/9  ZENGYU     Create
# Remarks       :

import calendar
import os
import sys

reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append("{0}".format(os.environ["DIDP_HOME"]))
from uuid import uuid1

import datetime
from archive.archive_enum import PartitionKey, AddColumn
from archive.hive_field_info import HiveFieldInfo
from utils.didp_db_operator import DbOperator
from utils.didp_logger import Logger
from utils.didp_tools import get_db_login_info

LOG = Logger()

HIVE_CLASS = "org.apache.hive.jdbc.HiveDriver"
DRIVER_PATH = "drivers\\jdbc\\inceptor-driver-6.0.0.jar"


def get_uuid():
    return str(uuid1()).replace("-", "")


class BizException(Exception):
    """
     自定义异常
    """

    def __init__(self, *args):
        self.args = args


class DateUtil(object):
    @classmethod
    def get_day_of_day(cls, dayof, n=0):
        """
            获取给顶日期的 前N 天或后N 天数据
            if n>=0,date is larger than today
            if n<0,date is less than today
            date format = "YYYYMMDD"
        :param dayof: 给定日期
        :param n: 增减日期天数
        :return:
        """

        a = datetime.datetime.strptime(dayof, "%Y%m%d")
        if n < 0:
            n = abs(n)
            result = a - datetime.timedelta(n)
        else:
            result = a + datetime.timedelta(n)
        return result.strftime("%Y%m%d")

    @staticmethod
    def get_now_date():
        """
        :return:
        """
        dt = datetime.datetime.now()
        return dt.strftime("%Y%m%d %H:%M:%S")

    @staticmethod
    def get_now_date_format(format):
        dt = datetime.datetime.now()
        return dt.strftime(format)

    @staticmethod
    def get_now_date_standy():
        dt = datetime.datetime.now()
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def get_month_start(now_day):
        """
            获取月初日期
        :param now_day: 当日日期String
        :return: 月初日期String
        """
        d = datetime.datetime.strptime(now_day, "%Y%m%d")
        year = d.year
        month = d.month
        day_begin = '%d%02d01' % (year, month)

        return str(day_begin)

    @staticmethod
    def get_month_end(now_day):
        """
            获取指定日期的月末日期
        :param now_day:
        :return:
        """
        d = datetime.datetime.strptime(now_day, "%Y%m%d")
        year = d.year
        month = d.month
        month_range = calendar.monthrange(year, month)[1]
        day_end = '%d%02d%02d' % (year, month, month_range)
        return str(day_end)

    @staticmethod
    def get_year_start(now_day):

        d = datetime.datetime.strptime(now_day, "%Y%m%d")
        year = d.year
        return str(year) + "0101"

    @staticmethod
    def get_year_end(now_day):
        d = datetime.datetime.strptime(now_day, "%Y%m%d")
        year = d.year
        return str(year) + "1231"
        pass

    @staticmethod
    def get_quarter_start(now_day):
        d = datetime.datetime.strptime(now_day, "%Y%m%d")
        year = d.year
        month = d.month
        if 0 < month < 4:
            jd_begin = str(year) + '0101'

        elif 3 < month < 7:
            jd_begin = str(year) + '0401'

        elif 6 < month < 10:
            jd_begin = str(year) + '0701'

        else:
            jd_begin = str(year) + '1001'
        return jd_begin

    @staticmethod
    def get_quarter_end(now_day):
        """
            获取指定日期的季末时间
        :param now_day:
        :return:
        """
        d = datetime.datetime.strptime(now_day, "%Y%m%d")
        year = d.year
        month = d.month

        if 0 < month < 4:
            jd_end = str(year) + '0331'
        elif 3 < month < 7:
            jd_end = str(year) + '0731'
        elif 6 < month < 10:
            jd_end = str(year) + '0931'
        else:
            jd_end = str(year) + '1231'
        return jd_end

    @staticmethod
    def get_quarter(now_day):
        d = datetime.datetime.strptime(now_day, "%Y%m%d")
        year = d.year
        month = d.month
        if 1 <= month <= 3:
            quarter = str(year) + "Q1"
        elif 4 <= month <= 6:
            quarter = str(year) + "Q2"
        elif 7 <= month <= 9:
            quarter = str(year) + "Q3"
        else:
            quarter = str(year) + "Q4"
        return quarter


class StringUtil(object):
    """
        字符操作工具类
    """

    @staticmethod
    def is_blank(in_str):
        """
        判断字符串是否为空
        :param in_str: 输入的字符串
        :return: true or false
        """
        if in_str is None:
            return True
        elif in_str.strip().__len__() == 0:
            return True
        else:
            return False

    @staticmethod
    def eq_ignore(obj1, obj2):
        """
            忽视大小写的比较
        :param obj1:
        :param obj2:
        :return:
        """
        if obj1 is None and obj2 is None:
            return True
        if obj1 is None or obj2 is None:
            return False
        obj1 = str(obj1)
        obj2 = str(obj2)
        return obj1.upper().strip().__eq__(obj2.upper().strip())


class HiveUtil(object):
    """
        Hive 工具类
    """

    def __init__(self, schema_id):
        # 通过SCHEMA_ID 获取Hive的 连接信息
        self.login_info = get_db_login_info(schema_id)[1]
        self.db_oper = DbOperator(self.login_info["db_user"],
                                  self.login_info["db_pwd"], HIVE_CLASS,
                                  self.login_info["jdbc_url"], DRIVER_PATH)
        self.db_oper.connect()  # 建立连接

    def close(self):
        self.db_oper.close()

    def exist_table(self, db_name, table_name):
        """
        :param db_name: 数据库名
        :param table_name:  数据表名
        :return:
        """

        sql1 = "use {dbName}".format(dbName=db_name)
        sql = "show tables '{tableName}'".format(tableName=table_name)
        self.db_oper.do(sql1)
        result = self.db_oper.fetchone(sql)
        if result:
            return True
        else:
            return False

    def has_partition(self, common_dict, db_name, table_name):
        """
        :param db_name: 数据库名
        :param table_name:  表名
        :return: 是否存在分区
        """
        result = self.get_table_desc(db_name, table_name)
        # 在数据库中获取时间分区键的名称

        pri_key = common_dict.get(PartitionKey.DATE_SCOPE.value)
        flag = False
        for col in result:
            if StringUtil.eq_ignore(col[0], pri_key):
                flag = True
                break
        return flag

    def get_org_pos(self, common_dict, db_name, table_name):
        """
            获取机构分区字段
        :param db_name:
        :param table_name:
        :return:
        """
        result = self.get_table_desc(db_name, table_name)
        # 在数据库中获取机构分区键的位置
        p_key = common_dict.get(PartitionKey.ORG.value)
        a_key = common_dict.get(AddColumn.COL_ORG.value)
        # 机构字段位置（1-没有机构字段 2-字段在列中 3-字段在分区中）
        key = 1
        for col in result:
            if col[0].strip().upper().__eq__(p_key.upper()):
                key = 3
                break

            elif col[0].strip().upper().__eq__(a_key.upper()):
                key = 2
                break

        return key

    def get_table_descformatted(self, db_name, table_name):
        """
            获取表的详细描述
        :param db_name:
        :param table_name:
        :return:
        """
        sql1 = "use {database}".format(database=db_name)
        sql = "desc formatted {table}".format(table=table_name)
        self.db_oper.do(sql1)
        result = self.db_oper.fetchall(sql)
        return result

    def get_table_desc(self, db_name, table_name):
        """
                获取表的简单描述
        :param db_name:
        :param table_name:
        :return:
        """
        sql1 = "use {database}".format(database=db_name)
        sql = "desc {table}".format(table=table_name)

        self.db_oper.do(sql1)
        result = self.db_oper.fetchall(sql)
        # self.db_oper.close()
        return result

    def execute_with_dynamic(self, sql):
        """
            开启动态分区
        :param sql:
        :return:
        """
        sql1 = "set hive.exec.dynamic.partition=true"

        self.db_oper.do(sql1)
        self.db_oper.do(sql)

    def execute(self, sql):
        """
            执行无返回结果的SQL
        :param sql:
        :return:
        """
        self.db_oper.do(sql)

    def execute_sql(self, sql):
        """
        有返回结果
        :param sql:
        :return:
        """
        return self.db_oper.fetchall(sql)

    def get_hive_meta_field(self, common_dict, db_name, table_name, filter):
        # type: (dict, str, str, bool) -> list(HiveFieldInfo)
        """
            获取Hive的元数据信息
        :param common_dict:
        :param db_name:
        :param table_name:
        :param filter: 是否过滤添加字段(part_org,part_date等)
        :return:  字段信息列表
        """
        result = self.get_table_desc(db_name, table_name)
        add_cols = set()
        partition_cols = set()
        if filter:
            for add_col in AddColumn:
                v = common_dict.get(add_col.value)
                if v:
                    add_cols.add(v.upper().strip())
            for part_col in PartitionKey:
                x = common_dict.get(part_col.value)
                if x:
                    partition_cols.add(x.upper().strip())
        i = 0
        hive_meta_info_list = list()  # 字段信息列表
        # 迭代字段
        for x in result:
            if x[0].upper().strip() in add_cols:
                continue
            if x[0].upper().strip() in partition_cols:
                continue
            if x[0].__contains__("#") or StringUtil.is_blank(x[0]):
                continue
            hive_mate_info = HiveFieldInfo(x[0].upper(),
                                           x[1], x[2], x[3], x[4], x[5].strip(),
                                           i)

            hive_meta_info_list.append(hive_mate_info)
            i = i + 1

        return hive_meta_info_list

    def get_table_comment(self, db_name, table_name):
        """
            获取表的 备注
        :param db_name:
        :param table_name:
        :return:
        """
        desc_formmatted = self.get_table_descformatted(db_name,
                                                       table_name)
        result = ""
        for attr in desc_formmatted:
            if attr[0].strip().upper().__eq__("COMMENT"):
                result = attr[1].strip()

        return result

    def compare(self, common_dict, db_name1, table1, db_name2, table2,
                is_compare_comments):
        """
            表对比
        :param common_dict:
        :param db_name1:
        :param table1:
        :param db_name2:
        :param table2:
        :param is_compare_comments:
        :return:
        """

        meta1 = self.get_hive_meta_field(common_dict, db_name1, table1, False)
        meta2 = self.get_hive_meta_field(common_dict, db_name2, table2, False)
        if not meta1 and not meta2:
            return True
        elif meta1 is None or meta2 is None:
            return False
        elif len(meta1) != len(meta2):
            return False
        for i in range(0, len(meta1)):
            if not HiveUtil.compare_field(meta1[i], meta2[i],
                                          is_compare_comments):
                return False

        return True

    @classmethod
    def compare_field(cls, meta_info1, meta_info2, is_compare_comments):
        if meta_info1.col_seq != meta_info2.col_seq:
            return False
        if not StringUtil.eq_ignore(meta_info1.col_name, meta_info2.col_name):
            return False
        if not StringUtil.eq_ignore(meta_info1.data_type, meta_info2.data_type):
            return False
        if not StringUtil.eq_ignore(meta_info1.col_length,
                                    meta_info2.col_length):
            return False
        if not StringUtil.eq_ignore(meta_info1.col_scale, meta_info2.col_scale):
            return False
        if is_compare_comments:
            if not StringUtil.eq_ignore(meta_info1.comment, meta_info2.comment):
                return False
        return True


if __name__ == '__main__':
    hive_util = HiveUtil("d5852c01c3fd44c6b8ad0bcab9ea0de5")

    print hive_util.exist_table("orc_test", "mysql_test_table006_init_init")
