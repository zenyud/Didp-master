# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/24
# Write By      : adtec(ZENGYU)
# Function Desc :  归档枚举模块
# History       : 2018/12/24  ZENGYU     Create
# Remarks       :
import os

import enum
import sys


class DatePartitionRange(enum.Enum):
    """
        日期分区范围枚举
    """
    ALL_IN_ONE = "N"
    MONTH = "M"
    QUARTER_YEAR = "Q"
    YEAR = "Y"


class SaveMode(enum.Enum):
    """归档模式"""
    ALL = "1"
    ADD = "2"
    CHAIN = "4"
    LAST_ALL = "5"
    LAST_ADD = "6"


class OrgPos(enum.Enum):
    NONE = 1
    COLUMN = 2
    PARTITION = 3


class PartitionKey(enum.Enum):
    DATE_SCOPE = "partition.key.date.scope"
    ORG = "partition.key.org"
    GROUP = "back_code"


class DataArchiveConstance(enum.Enum):
    # 归档数据对象
    OBJ_NAME = "obj"

    # 归档机构
    ORG = "org"
    # 源数据存储模式
    SOURCE_DATA_MODE = "sourceDataMode"
    # 源系统名称
    SOURCE_DB_NAME = "sourceDbName"

    # 源表名称
    SOURCE_TBALE_NAME = "sourceTableName"

    FILTER_SQL = "filterSql"  # 采集过滤SQL条件（WHERE 后面部分）
    FILTER_COL = "filterCol"  # 过滤字段

    SCHEMA_ID = "schemaID"  # 获取用户名，密码，以及数据库名

    # 归档目标库
    DB_NAME = "dbName"

    # 归档表
    TABLE_NAME = "tableName"

    SAVE_MODE = "savaMode"  # 归档方式（1-历史全量、2-历史增量、
    # 4-拉链、5-最近一天增量、6-最近一天全量）

    DATA_DATE = "dataDate"  # 数据日期

    DATE_RANGE = "dateRange"  # 日期范围

    ORG_POS = "orgPos"  # 机构字段位置（1-没有机构字段 2-字段在列中 3-字段在分区中）

    CLUSTER_COL = "clusterCol"  # 分桶键
    BUCKETSNUM = "bucketsNum"  # 分桶数
    ALL_TABLE_NAME = "allTableName"  # 全量历史表名（当归档方式为历史增量，
    # 且源数据为全量时传入），格式 dbname.tablename
    ADD_TABLE_NAME = "addTableName"  # 增量历史表名（当归档方式为历史全量，
    # 且源数据为增量时传入），格式 dbname.tablename


class AddColumn(enum.Enum):
    """
        增加字段
    """
    #  日期
    COL_DATE = "add.column.date"
    # 机构
    COL_ORG = "add.column.org"
    # 拉链开始日期
    CHAIN_SDATE = "add.column.sdate"
    # 拉链结束日期
    CHAIN_EDATE = "add.column.edate"
    GROUP = "back_code"
    # TEMP_TAIL = "hds___tail"


class CommentChange(enum.Enum):
    TABLE_COMMENT_CHANGE_DDL = "table.comment.change.ddl"
    # ddl字段备注改变是否需要增加新版本
    FIELD_COMMENT_CHANGE_DDL = "field.comment.change.ddl"
    GROUP = "back_code"


class SourceDataMode(enum.Enum):
    ALL = 1
    ADD = 2
