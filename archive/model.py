# -*- coding: UTF-8 -*-  

# Date Time     : 2019/1/9
# Write By      : adtec(ZENGYU)
# Function Desc :
# History       : 2019/1/9  ZENGYU     Create
# Remarks       :
import time
from sqlalchemy import Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class HdsStructArchiveCtrl(Base):
    """
        映射HDS_STRUCT_ARCHIVE_CTRLS 表
    """
    __tablename__ = 'HDS_STRUCT_ARCHIVE_CTRL'
    OBJECT_NAME = Column(primary_key=True)
    STATUS = Column()
    ORG_CODE = Column(primary_key=True)
    STORAGE_MODE = Column(primary_key=True)


class DidpHdsStructArchiveCtrl(Base):
    """
        DIDP_HDS_STRUCT_ARCHIVE_CTRL 表
    """
    __tablename__ = 'DIDP_HDS_STRUCT_ARCHIVE_CTRL'
    OBJECT_NAME = Column(String(10), primary_key=True)
    ORG_CODE = Column(String(20), primary_key=True)


# class DidpAccountCtrl(Base):
#     """
#
#     """
#     __tablename__= 'DIDP_ACCT_PTY_COL_CONFIG'
#     SCHEMA_KEY = Column(primary_key=True, nullable=False)
#     TABLE_NAME = Column(primary_key=True)
#     COLUMN_NAME = Column()
#     ACCT_TYPE = Column()

class DidpAccount():
    def __init__(self, col_name, col_type):
        self.col_name = col_name
        self.col_type = col_type


class DidpHdsStructMetaCtrl(Base):
    """
        DIDP_HDS_STRUCT_META_CTRL
    """
    __tablename__ = 'DIDP_HDS_STRUCT_META_CTRL'
    OBJECT_NAME = Column(primary_key=True)
    ORG_CODE = Column(primary_key=True)


class DidpCommonParams(Base):
    __tablename__ = "DIDP_COMMON_PARAMS"
    PARAM_ID = Column(primary_key=True, nullable=False)
    LAST_UPDATE_TIME = Column(nullable=False)
    LAST_UPDATE_USER = Column(nullable=False)
    GROUP_NAME = Column(nullable=False)
    PARAM_NAME = Column(nullable=True)
    PARAM_VALUE = Column(nullable=True)
    DESCRIPTION = Column(nullable=True)


class DidpMetaColumnInfo(Base):
    __tablename__ = "DIDP_META_COLUMN_INFO"
    COLUMN_ID = Column(primary_key=True, nullable=False)
    TABLE_ID = Column(nullable=False)
    PROJECT_VERSION_ID = Column(nullable=False)
    LAST_UPDATE_TIME = Column(nullable=False)
    LAST_UPDATE_USER = Column(nullable=False)
    COL_SEQ = Column()
    COL_NAME = Column()
    COL_DESC = Column()
    COL_TYPE = Column()
    COL_LENGTH = Column(Integer)
    COL_SCALE = Column()
    COL_DEFAULT = Column()
    NULL_FLAG = Column()  # 0 否 1 是
    PK_FLAG = Column()  # 0 否 1 是
    PARTITION_FLAG = Column()  # 0 否 1 是
    BUCKET_FLAG = Column()  # 0 否 1 是
    DESCRIPTION = Column()


class DidpMetaColumnInfoHis(Base):
    __tablename__ = "DIDP_META_COLUMN_INFO_HIS"
    TABLE_HIS_ID = Column(nullable=False)
    COLUMN_ID = Column(nullable=False, primary_key=True)
    TABLE_ID = Column(nullable=False)
    PROJECT_VERSION_ID = Column(nullable=False)
    LAST_UPDATE_TIME = Column(nullable=False)
    LAST_UPDATE_USER = Column(nullable=False)
    COL_SEQ = Column()
    COL_NAME = Column()
    COL_DESC = Column()
    COL_TYPE = Column()
    COL_LENGTH = Column(Integer)
    COL_SCALE = Column()
    COL_DEFAULT = Column()
    NULL_FLAG = Column()  # 0 否 1 是
    PK_FLAG = Column()  # 0 否 1 是
    PARTITION_FLAG = Column()  # 0 否 1 是
    BUCKET_FLAG = Column()  # 0 否 1 是
    DESCRIPTION = Column()


class DidpMetaTableInfo(Base):
    __tablename__ = "DIDP_META_TABLE_INFO"  # 表元数据信息表
    TABLE_ID = Column(primary_key=True,
                      nullable=False)
    SCHEMA_ID = Column(nullable=False)
    PROJECT_VERSION_ID = Column()
    LAST_UPDATE_TIME = Column(nullable=False)
    LAST_UPDATE_USER = Column(nullable=False)
    TABLE_NAME = Column(nullable=False)
    TABLE_NAME_CN = Column()
    BUCKET_NUM = Column()
    DESCRIPTION = Column()
    RELEASE_DATE = Column(nullable=False)
    TABLE_STATUS = Column()  # 1 暂存 2 发布


class DidpMetaTableInfoHis(Base):
    __tablename__ = "DIDP_META_TABLE_INFO_HIS"  # 表元数据信息表
    TABLE_HIS_ID = Column(primary_key=True,
                          nullable=False)
    TABLE_ID = Column(nullable=False)
    SCHEMA_ID = Column(nullable=False)
    PROJECT_VERSION_ID = Column()
    LAST_UPDATE_TIME = Column(nullable=False)
    LAST_UPDATE_USER = Column(nullable=False)
    TABLE_NAME = Column(nullable=False)
    TABLE_NAME_CN = Column()
    BUCKET_NUM = Column()
    DESCRIPTION = Column()
    RELEASE_DATE = Column(nullable=False)
    TABLE_STATUS = Column()  # 1 暂存 2 发布


class DidpMonRunLog(Base):
    __tablename__ = "DIDP_MON_RUN_LOG"  # 执行日志
    PROCESS_ID = Column(primary_key=True,
                        nullable=False)  # 流程ID
    SYSTEM_KEY = Column()
    BRANCH_NO = Column(primary_key=True,
                       nullable=False)  # 机构号
    BIZ_DATE = Column(primary_key=True,
                      nullable=False)  # 业务日期
    BATCH_NO = Column(primary_key=True,
                      nullable=False)  # 批次号
    TABLE_NAME = Column(nullable=False)
    # TABLE_ID = Column(nullable=True)
    DATA_OBJECT_NAME = Column()
    PROCESS_TYPE = Column()  # 流程类型(1:数据库采集作业,2:文件采集,3:预处理作业,4:装载作业,5:归档作业)
    PROCESS_STARTTIME = Column(nullable=False)  # 加工开始时间
    PROCESS_ENDTIME = Column(nullable=False)  # 加工结束时间
    PROCESS_STATUS = Column(nullable=False)  # 加工状态 0:成功,1:失败
    INPUT_LINES = Column(nullable=False, default=0)  # 输入的记录数
    OUTPUT_LINES = Column(nullable=False, default=0)  # 输出的记录数
    REJECT_LINES = Column(nullable=False, default=0)  # 拒绝的记录数
    ERR_MESSAGE = Column()  # 错误信息
    EXTENDED1 = Column()  # 扩展字段1 记录save_mode
    EXTENDED2 = Column()  # 扩展字段2 记录source_table_name
    RECORD_TIME = Column(nullable=False, default=time.localtime())  # 创建时间


class DidpMonRunLogHis(Base):
    __tablename__ = "DIDP_MON_RUN_LOG_HIS"  # 执行日志
    PROCESS_ID = Column(primary_key=True,
                        nullable=False)  # 流程ID
    SYSTEM_KEY = Column()
    BRANCH_NO = Column(primary_key=True,
                       nullable=False)  # 机构号
    BIZ_DATE = Column(primary_key=True,
                      nullable=False)  # 业务日期
    BATCH_NO = Column(primary_key=True,
                      nullable=False)  # 批次号
    TABLE_NAME = Column(nullable=False)
    # TABLE_ID = Column(nullable=True)
    DATA_OBJECT_NAME = Column()
    PROCESS_TYPE = Column()  # 流程类型(1:数据库采集作业,2:文件采集,3:预处理作业,4:装载作业,5:归档作业)
    PROCESS_STARTTIME = Column(nullable=False)  # 加工开始时间
    PROCESS_ENDTIME = Column(nullable=False)  # 加工结束时间
    PROCESS_STATUS = Column(nullable=False)  # 加工状态 0:成功,1:失败
    INPUT_LINES = Column(nullable=False, default=0)  # 输入的记录数
    OUTPUT_LINES = Column(nullable=False, default=0)  # 输出的记录数
    REJECT_LINES = Column(nullable=False, default=0)  # 拒绝的记录数
    ERR_MESSAGE = Column()  # 错误信息
    EXTENDED1 = Column()  # 扩展字段1 记录save_mode
    EXTENDED2 = Column()
    RECORD_TIME = Column(nullable=False, default=time.localtime())  # 创建时间


class DidpProcessInfo(Base):
    __tablename__ = "DIDP_PROCESS_INFO"  # 流程信息表
    PROCESS_ID = Column(primary_key=True, nullable=False)
    PROJECT_VERSION_ID = Column(nullable=False)
    LAST_UPDATE_TIME = Column()
    LAST_UPDATE_USER = Column()
    PROCESS_CATE = Column()
    PROCESS_NAME = Column()
    PROCESS_STATUS = Column()
    CREATE_USER = Column()
    DEVELOPER = Column()
    DESCRIPTION = Column()
    PLAN_TYPE = Column()
