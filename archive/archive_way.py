# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/21
# Write By      : adtec(ZENGYU)
# Function Desc : 归档方式
# History       : 2018/12/21  ZENGYU     Create
# Remarks       :
import abc
import argparse
import os
import sys
import traceback

reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append("{0}".format(os.environ["DIDP_HOME"]))
from abc import ABCMeta
from utils.didp_accpty_warn_logger import AccPtyWarnLogger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from archive.archive_enum import *
from archive.hive_field_info import *
from archive.service import *
from utils.didp_logger import Logger

LOG = Logger()

USER = os.environ["DIDP_CFG_DB_USER"]
PASSWORD = os.environ["DIDP_CFG_DB_PWD"]
DB_URL = os.environ["DIDP_CFG_DB_JDBC_URL"]
PROCESS_TYPE = "5"  # 流程类型
# 账号映射库
ACCOUNT_MAP_TABLE = "HDSCBUSDB.ARC_ACC_PTY"  # 账号映射表
# ACCOUNT_MAP_TABLE = "test.ARC_ACC_PTY"  # 账号映射表
ACCOUNT_PRE_STR = "account.pre.str"  # 主键类账号前缀


class ArchiveData(object):
    """
        归档基类
    """
    __metaclass__ = ABCMeta

    """数据归档类
        args:参数
    """

    __lock_archive = False  # 归档锁
    __lock_meta = False  # 元数据锁
    date_scope = ""  # 数据日期范围
    start_date = ""  # 开始日期
    end_date = ""  # 结束日期
    field_change_list = None  # 变更字段列表
    field_type_change_list = None  # 变更字段类型列表
    source_count = 0  # 原始表数据量
    archive_count = 0  # 归档数据条数
    pro_start_date = None  # 流程开始时间
    pro_end_date = None  # 流程结束时间
    __PRO_STATUS = "0"  # 加工状态 0 成功，1失败
    error_msg = ""  # 错误信息
    is_already_load = False  # 是否已经归档完成
    _DELETE_FLG = "DELETE_FLG"  # 删除标记字段名 0 ：未删除 1 :已删除
    _DELETE_DT = "DELETE_DT"  # 删除日期字段名
    need_reload = False  # 是否需要重新建表

    def __init__(self):

        self.__args = self.archive_init()
        self.session = self.get_session()  # 数据库连接对象
        self.hive_util = HiveUtil(self.__args.schID)
        self.common_dict = self.init_common_dict()  # 初始化公共参数
        self.account_list = []  # 需要做账号转移的账号列表
        if self.__args.priAcc:
            for acc in self.__args.priAcc.split(","):
                self.account_list.append(DidpAccount(acc, 1))
        if self.__args.npriAcc:
            for acc in self.__args.npriAcc.split(","):
                self.account_list.append(DidpAccount(acc, 2))
        self.filter_sql = self.__args.filSql  # 过滤Sql
        self.filter_cols = self.__args.filCol  # 过滤字段
        self.meta_data_service = MetaDataService(self.session)
        self.mon_run_log_service = MonRunLogService(self.session)
        self.hds_struct_control = HdsStructControl(self.session)
        self.source_ddl = (self.meta_data_service.
                           parse_input_table(self.hive_util,
                                             self.__args.sDb,
                                             self.__args.sTable,
                                             self.filter_cols,
                                             True
                                             ))

        self.db_name = self.__args.db  # 目标库
        self.table_name = self.__args.table  # 目标表
        self.account_ctl = AccountCtrlDao(self.session)  # 操作日志类
        # 日期字段名
        self.col_date = self.common_dict.get(AddColumn.COL_DATE.value)
        # 机构字段名
        self.col_org = self.common_dict.get(AddColumn.COL_ORG.value)
        # 日期分区键
        self.partition_data_scope = self.common_dict.get(
            PartitionKey.DATE_SCOPE.value)
        # 机构分区键
        self.partition_org = self.common_dict.get(PartitionKey.ORG.value)
        # 机构字段位置
        self.org_pos = int(self.__args.orgPos)

        # 日期分区范围
        self.data_range = self.__args.dtRange

        self.org = self.__args.org  # 机构
        self.data_date = self.__args.dtDate  # 数据日期
        self.last_date = DateUtil.get_day_of_day(self.data_date, -1)  # 昨日日期
        self.buckets_num = self.__args.buckNum  # 分桶数
        self.cluster_col = self.__args.cluCol  # 分桶键
        self.source_data_mode = int(self.__args.sMode)  # 数据源模式
        self.is_first_archive = False  # 判断是否是第一次归档
        if self.source_data_mode == 3:
            self.source_data_mode = 1  # 初始化改全量
            self.is_first_archive = True  # 是第一次归档
        self.system = self.__args.system  # 系统
        self.batch = self.__args.batch  # 批次号
        self.obj = self.__args.obj  # 对象名
        self.source_db = self.__args.sDb  # 源库
        self.source_table = self.__args.sTable  # 源表
        self.pro_id = self.__args.proID  # 流程号
        self.pro_info = ProcessDao(self.session).get_process_info(self.pro_id)
        # 获取project_id
        self.project_id = self.pro_info.PROJECT_VERSION_ID if self.pro_info else None
        self.save_mode = self.__args.saveMd  # 存储模式
        self.schema_id = self.__args.schID
        self.pk_list = self.__args.pkList  # 主键
        self.all_table = self.__args.allTab  # 全量表表名
        self.add_table = self.__args.addTab  # 增量表表名
        self.all_range = self.__args.allRg  # 全量表日期分区范围
        self.add_range = self.__args.addRg  # 增量表日期分区范围
        self.account_table_name = None  # 账号转移临时表表名
        self.__print_arguments()  # 打印参数信息

    @abc.abstractmethod
    def print_save_mode(self):
        pass

    @staticmethod
    def get_session():
        """
         获取 sqlalchemy 的SESSION 会话
        :return:
        """
        x = DB_URL.index("//")
        y = DB_URL.index("?")
        db_url = DB_URL[x + 2:y]

        # db_name = db_login_info['db_name']

        engine_str = ("mysql+mysqlconnector://{db_user}:{password}@{db_url}".
                      format(db_user=USER, password=PASSWORD,
                             db_url=db_url,
                             ))
        engine = create_engine(engine_str)
        Session = sessionmaker(bind=engine)
        session = Session()
        return session

    @property
    def release_date(self):
        """
            格式化数据日期
        :return:
        """
        __data_date = self.data_date
        __data_date = (__data_date[0:4] + "-" + __data_date[4:6] + "-" +
                       __data_date[6:] + " 00:00:00")

        return __data_date

    # 拉链临时表名
    @property
    def app_table_name1(self):
        __app_name = self.common_dict.get("archive.proc.temporary.table")
        if StringUtil.is_blank(__app_name):
            __app_name = self.table_name + "_hds_tmp"
        if self.org_pos != OrgPos.NONE.value:
            __app_name = __app_name + "_" + self.org

        __app_name = (__app_name.replace("OBJ", self.obj).
                      replace("TABLE", self.table_name) + '_1')

        return __app_name

    # 是否删除临时表

    @property
    def is_drop_tmp_table(self):
        is_drop_tmp = self.common_dict.get("drop.archive.temp.table")
        if is_drop_tmp:
            return is_drop_tmp
        else:
            return False

    @property
    def input_table_name(self):
        input_table_name = self.common_dict.get(
            "archive.input.temporary.table") if self.common_dict.get(
            "archive.input.temporary.table") else self.table_name + "_hds_input"
        if self.org_pos != OrgPos.NONE.value:
            input_table_name = input_table_name + "_" + self.org
        return input_table_name

    @property
    def temp_db(self):
        temp_db = self.common_dict.get("archive.temporary.db")
        if not temp_db:
            return self.db_name
        return temp_db

    def create_temp_table_for_account(self):
        """
            如果有存在账号转移,则创建临时表
        :return:
        """
        if len(self.account_list) > 0:
            # 存在账号转移
            self.account_table_name = self.source_table + "_account_tmp"
            account_name_list = [acc.col_name.upper() for acc in self.account_list]
            is_exist_table = self.hive_util.exist_table(self.source_db, self.account_table_name)
            if is_exist_table:
                self.hive_util.execute("drop table {0}.{1}".format(self.source_db, self.account_table_name))

            hql = "CREATE TABLE IF NOT EXISTS {db_name}.{table_name} ( \n". \
                format(db_name=self.source_db,
                       table_name=self.account_table_name
                       )
            # 构建Body 语句
            sql = ""
            cols_str = ""
            for field in self.source_ddl:
                cols_str = cols_str + field.col_name + ","
                col_name = field.col_name_quote
                if field.col_name.upper() in account_name_list:
                    col_name = field.col_name + "_ORI"
                sql = sql + "{col_name} {field_type} ".format(
                    col_name=col_name,
                    field_type=field.get_full_type())
                if not StringUtil.is_blank(field.comment):
                    # 看是否有字段备注
                    sql = sql + "comment '{comment_content}'".format(
                        comment_content=field.comment)
                sql = sql + ","
            # 开始添加账号转移字段
            for acc in account_name_list:
                col_type = self.hive_util.get_column_desc(self.source_db, self.source_table, acc)[0][1]
                if col_type.__contains__("ORACLE"):
                    col_type = col_type.replace(",ORACLE", "")
                sql = sql + "{col_name} {col_type} ,".format(col_name=acc, col_type=col_type)
            hql = hql + sql[:-1] + " )"
            LOG.info("执行SQL：%s" % hql)
            self.hive_util.execute(hql)
            # 导入数据
            LOG.info("源数据导入到临时表")
            hql = " INSERT INTO TABLE {DB}.{TABLE} SELECT {COLS} ".format(DB=self.source_db,
                                                                          TABLE=self.account_table_name,
                                                                          COLS=cols_str[:-1]
                                                                          )
            hql = hql + self.case_when_acct_no()[:-1] + " FROM {source_db}.{source_table} S ".format(
                source_db=self.source_db,
                source_table=self.source_table)
            hql = hql + self.left_join_acct_no()
            LOG.info("执行SQL %s" % hql)
            self.hive_util.execute(hql)

    def init_common_dict(self):
        """
            初始化公共参数字典
        :return: 参数字典
        """
        common_dict = CommonParamsDao(self.session).get_all_common_code()
        if len(common_dict) == 0:
            raise BizException("初始化公共代码失败！请检查数据库")
        else:
            return common_dict

    def __print_arguments(self):
        """ 参数格式化输出

        Args:

        Returns:

        Raise:

        """

        LOG.debug("-------------------参数清单-------------------")
        LOG.debug("数据对象名     : {0}".format(self.__args.obj))
        LOG.debug("schema_id    : {0}".format(self.__args.schID))
        LOG.debug("流程ID       : {0}".format(self.__args.proID))
        LOG.debug("系统标识      : {0}".format(self.__args.system))
        LOG.debug("批次号        : {0}".format(self.__args.batch))
        LOG.debug("机构号        : {0}".format(self.__args.org))
        LOG.debug("源数据增全量   : {0}".format(self.__args.sMode))
        LOG.debug("源库名        : {0}".format(self.__args.sDb))
        LOG.debug("源表名        : {0}".format(self.__args.sTable))
        LOG.debug("主键列表       : {0}".format(self.__args.pkList))
        LOG.debug("过滤条件       : {0}".format(self.__args.filSql))
        LOG.debug("过滤字段       : {0}".format(self.__args.filCol))
        LOG.debug("归档库名       : {0}".format(self.__args.db))
        LOG.debug("归档表名       : {0}".format(self.__args.table))
        LOG.debug("归档方式       : {0}".format(self.__args.saveMd))
        LOG.debug("数据日期       : {0}".format(self.__args.dtDate))
        LOG.debug("日期分区范围    : {0}".format(self.__args.dtRange))
        LOG.debug("机构字段位置    : {0}".format(self.__args.orgPos))
        LOG.debug("分桶键         : {0}".format(self.__args.cluCol))
        LOG.debug("分桶数         : {0}".format(self.__args.buckNum))
        LOG.debug("全量历史表名    : {0}".format(self.__args.allTab))
        LOG.debug("增量历史表名    : {0}".format(self.__args.addTab))
        LOG.debug("全量历史表分区范围  : {0}".format(self.__args.allRg))
        LOG.debug("增量历史表分区范围  : {0}".format(self.__args.addRg))
        LOG.debug("----------------------------------------------")

    @staticmethod
    def archive_init():
        """
            参数初始化
        :return:
        """
        # 参数解析
        parser = argparse.ArgumentParser(description="数据归档组件")

        parser.add_argument("-obj", required=True, help="数据对象名")
        parser.add_argument("-org", required=True, help="机构")
        parser.add_argument("-sMode", required=True,
                            help="源数据增全量(1-全量 2-增量)")
        parser.add_argument("-sDb", required=True, help="源库名")
        parser.add_argument("-sTable", required=True, help="源表名")
        parser.add_argument("-pkList", required=False, help="主键列表(`|`线分割)")
        parser.add_argument("-filSql", required=False,
                            help="采集过滤SQL条件(WHERE 后面部分)")
        parser.add_argument("-filCol", required=False, help="过滤字段")
        parser.add_argument("-schID", required=True, help="取连接信息")
        parser.add_argument("-proID", required=True, help="流程ID")
        parser.add_argument("-system", required=True, help="系统标识")
        parser.add_argument("-batch", required=True, help="批次号")
        parser.add_argument("-db", required=True, help="归档库名")
        parser.add_argument("-table", required=True, help="归档表名")
        parser.add_argument("-saveMd", required=True,
                            help="归档方式(1-历史全量、2-历史增量、"
                                 "4-拉链、5-最近一天增量、6-最近一天全量)")
        parser.add_argument("-dtDate", required=True,
                            help="数据日期(yyyymmdd)")
        parser.add_argument("-dtRange", required=True,
                            help="日期分区范围（N-不分区、M-月、Q-季、Y-年）")
        parser.add_argument("-orgPos", required=True,
                            help="机构字段位置（1-没有机构字段 "
                                 "2-字段在列中 3-字段在分区中）")
        parser.add_argument("-cluCol", required=True, help="分桶键")
        parser.add_argument("-buckNum", required=True, help="分桶数")
        parser.add_argument("-allTab", required=False,
                            help="全量历史表名（当归档方式为历史增量,"
                                 "且源数据为全量时传入），格式 db.table")
        parser.add_argument("-addTab", required=False,
                            help="增量历史表名（当归档方式为历史全量,"
                                 "且源数据为增量时传入），格式 db.table")
        parser.add_argument("-allRg", required=False,
                            help="全量历史表日期分区范围（当归档方式为历史增量，"
                                 "且源数据为全量时传入）,格式同dtRange")
        parser.add_argument("-addRg", required=False,
                            help="增量历史表日期分区范围（当归档方式为历史全量，"
                                 "且源数据为增量时传入）,格式同dtRange")
        parser.add_argument("-priAcc", required=False, help="主键账号字段")
        parser.add_argument("-npriAcc", required=False, help="非主键账号字段")
        # parser.add_argument("-accList", required=False,
        #                     help="账号代理键字段列表"
        #                     )
        args = parser.parse_args()
        return args

    def lock(self):
        """
            归档任务锁
        :return:
        """
        if self.hds_struct_control.find_archive(self.__args.obj,
                                                self.__args.org) is None:
            try:
                self.hds_struct_control.archive_lock(self.__args.obj,
                                                     self.__args.org)
                self.__lock_archive = True
            except Exception as e:
                raise BizException(
                    "待归档表有另外正在归档的任务或后台数据库更新错，"
                    "请稍后再试。[{0}]".format(e.message))
        else:
            raise BizException("待归档表有另外正在归档的任务，请稍后再试 ")

    def data_partition_check(self):
        """
            日期分区字段检查
        :return:
        """
        if self.hive_util.exist_table(self.db_name,
                                      self.table_name):
            if (StringUtil.eq_ignore(self.data_range,
                                     DatePartitionRange.ALL_IN_ONE.value)
                    and self.hive_util.has_partition(self.common_dict,
                                                     self.db_name,
                                                     self.table_name)):
                raise BizException("归档日期分区与Hive表不一致 ！！！")

        def set_value(x, y, z):
            """ 赋值 """
            return x, y, z

        switch = {
            DatePartitionRange.MONTH.value: set_value(
                self.data_date[0:6],
                DateUtil().get_month_start(
                    self.data_date),
                DateUtil().get_month_end(
                    self.data_date)),

            DatePartitionRange.QUARTER_YEAR.value: set_value(
                DateUtil().get_quarter(self.data_date),
                DateUtil().get_month_start(
                    self.data_date),
                DateUtil().get_month_end(
                    self.data_date)),

            DatePartitionRange.YEAR.value: set_value(self.data_date[0:4],
                                                     DateUtil().get_year_start(
                                                         self.data_date),
                                                     DateUtil().get_year_end(
                                                         self.data_date)),
            DatePartitionRange.ALL_IN_ONE.value: ("", "", "")
        }
        # 获取分区范围，开始日期，结束日期
        x, y, z = switch[self.data_range]
        self.date_scope = x
        self.start_date = y
        self.end_date = z

    def case_when_acct_no(self):
        # 关联HISCBUSDB表 获取账号代理键
        i = 0
        hql = ""
        for col in self.account_list:
            if i == 0:
                hql = hql + ", "
            account_value = ""
            if col.col_type == 1:
                # 主键
                account_value = "concat('{0}', S.{1})".format(self.common_dict.get(ACCOUNT_PRE_STR), col.col_name)
            elif col.col_type == 2:
                account_value = "S." + col.col_name
            table_alias = "T" + str(i)
            hql = hql + " CASE WHEN  {T}.ACC_PTY is not null then {T}.ACC_PTY " \
                        " ELSE {account_value} END {col_name} ,".format(
                T=table_alias,
                account_value=account_value,
                col_name=col.col_name
            )
            i = i + 1
        return hql

    def left_join_acct_no(self):
        hql = ""
        i = 0
        for col in self.account_list:
            if i == 0:
                hql = hql[:-1]  # 删除末尾的逗号
            table_alias = "T" + str(i)
            hql = hql + " LEFT JOIN {TABLE_NAME} AS {T} " \
                        "ON  {T}.ACC_NO = S.{col_name} ".format(T=table_alias,
                                                                TABLE_NAME=ACCOUNT_MAP_TABLE,
                                                                col_name=col.col_name)
            i = i + 1
        return hql

    def register_acct_log(self):
        """
            记录账号转移的错误日志
        :return:
        """

    @staticmethod
    def get_data_scope(data_range, data_date):
        if StringUtil.eq_ignore(data_range, DatePartitionRange.MONTH.value):
            return data_date[:6]
        elif StringUtil.eq_ignore(data_range, DatePartitionRange.YEAR.value):
            return data_date[:4]
        elif StringUtil.eq_ignore(data_range,
                                  DatePartitionRange.QUARTER_YEAR.value):
            return DateUtil.get_quarter(data_date)

    def org_check(self):
        """
            检查机构字段是否和源表一致
        :return:
        """
        if self.hive_util.exist_table(self.db_name,
                                      self.table_name):
            if self.org_pos != self.hive_util.get_org_pos(
                    self.common_dict,
                    self.db_name,
                    self.table_name):
                raise BizException("归档机构分区与hive表中不一致 !!!")

    def meta_lock(self):
        """
            元数据处理加锁
        :return:
        """
        start_time = time.time()
        self.__lock_meta = False
        self.meta_lock_do()
        while not self.__lock_meta:
            if time.time() - start_time > 60000:
                raise BizException("元数据更新等待超时,请稍后再试！")
            try:
                time.sleep(1)
            except Exception as e:
                LOG.debug(e)
            self.meta_lock_do()

    def meta_lock_do(self):

        if not self.hds_struct_control.meta_lock_find(self.__args.obj,
                                                      self.__args.org):
            try:
                self.hds_struct_control.meta_lock(self.__args.obj,
                                                  self.__args.org)

                self.__lock_meta = True
            except Exception as e:
                LOG.debug("元数据更新队列等待中 。。。 ")

    def meta_unlock(self):
        """
                   元数据锁解除
               :return:
               """
        if self.__lock_meta:
            self.hds_struct_control.meta_unlock(self.__args.obj,
                                                self.__args.org)
            self.__lock_meta = False

    def upload_meta_data(self):
        """
            登记元数据
        :return:
        """
        # 获取表的评论
        table_comment = self.hive_util.get_table_comment(self.source_db,
                                                         self.source_table)
        # 先格式化dataDate

        self.meta_data_service.upload_meta_data(self.schema_id,
                                                self.db_name,
                                                self.source_ddl,
                                                self.table_name,
                                                self.release_date,  # RELEASE_DATE 的日期是加了时分秒的日期
                                                self.buckets_num,
                                                self.common_dict,
                                                table_comment,
                                                self.project_id,
                                                self.hive_util)

    @abc.abstractmethod
    def create_table(self, db_name, table_name):
        """
            创建表
        :return:
        """

    def change_table_columns(self):
        """
                   根据表结构变化增加新的字段
               :return:
               """
        change_detail_buffer = ""

        self.get_fields_rank_list(self.db_name, self.table_name,
                                  self.data_date)
        add_column_list = []
        if self.field_change_list:
            # 有字段的变化

            for field in self.field_change_list:
                # field type : class FieldState
                if field.hive_no == -2:
                    # hive 需要新增字段
                    add_column_list.append(field)
                    change_detail_buffer = (change_detail_buffer +
                                            " `{col_name}` {field_type},".
                                            format(col_name=field.col_name,
                                                   field_type=field.ddl_type.
                                                   get_whole_type))
                    LOG.debug("field length %s " % field.ddl_type.field_length)
                    LOG.debug("whole_type %s " % field.ddl_type.get_whole_type)
        if len(change_detail_buffer) > 0:
            # change_detail_buffer = change_detail_buffer[:-1]  # 去掉末尾的逗号
            # alter_sql = ("ALTER TABLE {db_name}.{table_name} "
            #              "ADD COLUMNS ({buffer}) ".
            #              format(db_name=self.__args.db,
            #                     table_name=self.__args.table,
            #                     buffer=change_detail_buffer))
            # LOG.info("新增字段，执行SQL %s" % alter_sql)
            # self.hive_util.execute(alter_sql)

            # #  将原来的Hive表重命名 ORC 事务表不支持重命名
            # rename = "ALTER TABLE {DB}.{TABLE} RENAME TO {DB}.{TABLE}_OLD".format(DB=self.db_name,
            #                                                                       TABLE=self.table_name)
            # LOG.info("执行SQL: %s" % rename)
            # self.hive_util.execute(rename)
            # 建一张新表
            LOG.info("创建临时表")
            self.need_reload = True
            self.create_table(self.db_name, self.table_name + "_NEW")

        alter_sql2 = ""
        if self.field_type_change_list:
            # 有字段类型改变
            for field in self.field_type_change_list:
                alter_sql2 = alter_sql2 + ("alter table {db_name}.{table_name} "
                                           "change column `{column}` `{column}` {type} ".
                                           format(db_name=self.__args.db,
                                                  table_name=self.__args.table,
                                                  column=field.col_name,
                                                  type=field.ddl_type.get_whole_type))
                LOG.debug("field length %s " % field.ddl_type.field_length)
                if not StringUtil.is_blank(field.comment_ddl):
                    # 若备注不为空 则添加备注
                    alter_sql2 = alter_sql2 + (" comment '{comment}' ".
                        format(
                        comment=field.comment_ddl))
                LOG.debug("修改表sql为：%s" % alter_sql2)
                self.hive_util.execute(alter_sql2)

    def get_fields_rank_list(self, db_name, table_name, data_date):
        """
            获取归档数据字段排列结果，将历史元数据信息与现有的HIVE元数据对照比较返回比较结果
        :param db_name:
        :param table_name:
        :param data_date:
        :return:
        """
        # meta_field_infos = self.meta_data_service.get_meta_field_info_list(
        #     table_name, data_date)  # 元数据表中的字段信息
        meta_field_infos = self.source_ddl
        # Hive 中的字段信息
        hive_field_infos = self.hive_util.get_hive_meta_field(self.common_dict,
                                                              db_name,
                                                              table_name,
                                                              True)

        # 字段名更改列表
        self.field_change_list = self.get_change_list(meta_field_infos,
                                                      hive_field_infos)

        LOG.debug("------字段变更列表------ {0}".format(self.field_change_list))
        # 检查 字段类型是否改变
        self.field_type_change_list = self.check_column_modify(
            self.field_change_list)

    @staticmethod
    def get_change_list(meta_field_infos, hive_field_infos):
        """
            获取所有字段
        :param meta_field_infos: 接入表元数据
        :param hive_field_infos: 现有的hive表
        :return:
        """
        hive_field_name_list = [field.col_name.upper() for field in
                                hive_field_infos]

        meta_field_name_list = [x.col_name.upper() for x in
                                meta_field_infos]
        LOG.debug("Hive字段个数为：%s" % len(hive_field_name_list))
        LOG.debug("接入数据字段个数为：%s" % len(meta_field_name_list))
        # 进行对比0
        field_change_list = list()
        for hive_field in hive_field_infos:
            hive_no = hive_field.col_seq  # 字段序号

            if hive_field.col_name.upper() in meta_field_name_list:
                meta_index = meta_field_name_list.index(
                    hive_field.col_name.upper())
                meta_current_no = meta_field_infos[meta_index].col_seq

                ddl_type = MetaTypeInfo(
                    meta_field_infos[meta_index].data_type,
                    meta_field_infos[meta_index].col_length,
                    meta_field_infos[meta_index].col_scale)
                meta_comment = meta_field_infos[meta_index].comment
            else:
                # 源数据中没有Hive的信息
                meta_current_no = -1
                hive_no = -1  # Hive 中有,元数据没有的字段
                ddl_type = None
                meta_comment = None

            hive_type = MetaTypeInfo(hive_field.data_type,
                                     hive_field.col_length,
                                     hive_field.col_scale)
            # 字段状态
            field_state = FieldState(hive_field.col_name.upper(),
                                     hive_field.col_seq,
                                     meta_current_no,
                                     ddl_type,
                                     hive_type,
                                     hive_field.comment,
                                     meta_comment,
                                     hive_no)

            field_change_list.append(field_state)

        for meta_field in meta_field_infos:
            if meta_field.col_name.upper() not in hive_field_name_list:
                # Hive 里不包含 元数据中的字段
                LOG.debug("meta_field.col_length : %s " % meta_field.col_length)
                ddl_data_type = MetaTypeInfo(meta_field.data_type,
                                             meta_field.col_length,
                                             meta_field.col_scale)

                field_state = FieldState(meta_field.col_name.upper(),
                                         -1,
                                         meta_field.col_seq,
                                         ddl_data_type,
                                         None,
                                         None,
                                         meta_field.comment,
                                         -2  # hive中需要新增
                                         )

                field_change_list.append(field_state)

        change = False  # 判断是否有字段改变 False 无变化 True 有变化
        for field in field_change_list:
            if field.hive_no < 0 or not StringUtil.eq_ignore(field.full_seq,
                                                             field.current_seq):
                change = True
        # 如果没有改变 则将field_change_list 置空

        if not change:
            field_change_list = None
        return field_change_list

    @staticmethod
    def check_column_modify(field_change_list):
        # type: (list(FieldState)) -> list(FieldState)
        """
            检查字段类型是否发生改变
        :param field_change_list: 字段更改列表
        :return: 更改字段的集合
        """
        is_error = False
        change_fields = set()  # 存放变更的字段
        if field_change_list is not None:
            for field in field_change_list:

                meta_type_hive = field.hive_type
                meta_type_ddl = field.ddl_type

                # 忽略Varchar和Char不一致的错误

                if not meta_type_hive or not meta_type_ddl:
                    # 新增字段 跳过
                    continue
                if not meta_type_ddl.__eq__(meta_type_hive):
                    # 字段类型不同,判断有哪些不同
                    LOG.debug("meta_type_ddl %s " % meta_type_ddl)
                    LOG.debug("meta_type_hive %s " % meta_type_hive)
                    if StringUtil.eq_ignore(meta_type_ddl.field_type,
                                            meta_type_hive.field_type):
                        # 类型相同判断精度,允许decimal字段精度扩大

                        if (meta_type_hive.field_length <
                                meta_type_ddl.field_length and
                                meta_type_hive.field_scale <
                                meta_type_ddl.field_scale):
                            LOG.debug(
                                "字段{col_name}精度扩大 {hive_type} -->> {ddl_type} \n"
                                "修改表字段精度为 {ddl_type} ".format(
                                    col_name=field.col_name,
                                    hive_type=meta_type_hive.get_whole_type,
                                    ddl_type=meta_type_ddl.get_whole_type))
                            change_fields.add(field)
                            continue

                        elif (
                                meta_type_hive.field_length >= meta_type_ddl.field_length
                                and meta_type_hive.field_scale < meta_type_ddl.field_scale):

                            old_type = meta_type_ddl.get_whole_type
                            meta_type_ddl.field_length = meta_type_hive.field_length
                            LOG.debug(
                                "字段{col_name}精度扩大 {hive_type} -->> {ddl_type1} \n"
                                "修改表字段精度为 {ddl_type2}".format(
                                    col_name=field.col_name,
                                    hive_type=meta_type_hive.get_whole_type,
                                    ddl_type1=old_type,
                                    ddl_type2=meta_type_ddl.get_whole_type
                                ))
                            continue
                        elif (
                                meta_type_hive.field_length < meta_type_ddl.field_length
                                and meta_type_hive.field_scale > meta_type_ddl.field_scale):
                            old_type = meta_type_ddl.get_whole_type
                            meta_type_ddl.field_scale = meta_type_hive.field_scale
                            LOG.debug("字段{col_name}精度扩大 "
                                      "{hive_type} -->> {ddl_type1} \n"
                                      "修改表字段精度为 {ddl_type2}".
                                      format(col_name=field.col_name,
                                             hive_type=meta_type_hive.get_whole_type,
                                             ddl_type1=old_type,
                                             ddl_type2=meta_type_ddl.get_whole_type
                                             ))
                            continue
                        else:
                            LOG.debug("字段{col_name} 精度缩小 "
                                      "{hive_type}-->> {ddl_type}\n"
                                      "不修改归档表字段精度 ！".
                                format(
                                col_name=field.col_name,
                                hive_type=meta_type_hive.get_whole_type,
                                ddl_type=meta_type_ddl.get_whole_type))
                    else:
                        # 不允许字段类型发生改变
                        is_error = True
                        LOG.error("字段{col_name} "
                                  "类型发送变化{hive_type} -->> {ddl_type} !".
                                  format(col_name=field.col_name,
                                         hive_type=meta_type_hive.get_whole_type,
                                         ddl_type=meta_type_ddl.get_whole_type))
                if is_error:
                    raise BizException("源数据字段类型变化过大，请处理后重新归档")

                # 判断字段备注的改变
                if not StringUtil.is_blank(field.comment_ddl):
                    # ddl 中存在备注
                    if StringUtil.is_blank(field.comment_hive):
                        # hive 没有备注
                        LOG.debug("字段{field} 备注发送变化 -->> {comment}".
                                  format(field=field.col_name,
                                         comment=field.comment_ddl))
                        change_fields.add(field)
                    elif not StringUtil.eq_ignore(field.comment_ddl,
                                                  field.comment_hive):
                        LOG.debug(
                            "字段{field} 备注发生变化: {comment1} -->> {comment2} ".
                                format(field=field.col_name,
                                       comment1=field.comment_hive,
                                       comment2=field.comment_ddl))
                        change_fields.add(field)

            ret_fields = list()
            for field in change_fields:
                ret_fields.append(field)
            return ret_fields

    @abc.abstractmethod
    def load_data(self):
        """
            装载数据
        :return:
        """

    def create_partition_sql(self, partition_range, date_scope, org):
        """
        构建表partition分区
        :param partition_range: 日期分区范围
        :param date_scope: 日期区间
        :param org: 机构分区
        :return:
        """
        s = ""
        result = ""
        if not StringUtil.eq_ignore(partition_range,
                                    DatePartitionRange.ALL_IN_ONE.value):
            s = s + "{data_scope_name} = '{date_scope}' ,". \
                format(data_scope_name=self.partition_data_scope,
                       date_scope=date_scope)
        if self.org_pos == OrgPos.PARTITION.value:
            s = s + "{partition_col} = '{partition_value}' ,". \
                format(partition_col=self.partition_org, partition_value=org
                       )
        if len(s) > 0:
            result = "PARTITION ({str})".format(str=s[:-1])

        return result

    def create_table_body(self, is_temp_table):
        """
                   构建表的body
               :param is_temp_table: 是否是临时表
               :return: sql str
               """

        sql = ""
        if is_temp_table:
            for field in self.source_ddl:
                sql = sql + "{col_name} string,".format(
                    col_name=field.col_name)
        else:
            for field in self.source_ddl:

                sql = sql + "{col_name} {field_type} ".format(
                    col_name=field.col_name_quote,
                    field_type=field.get_full_type())
                if not StringUtil.is_blank(field.comment):
                    # 看是否有字段备注
                    sql = sql + "comment '{comment_content}'".format(
                        comment_content=field.comment)
                sql = sql + ","
        # 添加删除标志和删除日期字段
        sql = sql + "{DELETE_FLG} VARCHAR(1),{DELETE_DT} VARCHAR(8) ,".format(DELETE_FLG=self._DELETE_FLG,
                                                                              DELETE_DT=self._DELETE_DT)
        return sql[:-1]

    def alter_table(self, db_name, table_name):
        """
            修改表结构
        :return:
        """

        if len(self.account_list) > 0:
            # 先获取全部字段的字段类型
            LOG.info("存在账号转移字段，更新表结构")
            # 获取 hive 字段信息
            # hive_field_infos = hive_util.get_hive_meta_field(self.common_dict, self.db_name, self.table_name, False)
            # hive_field_names = [ field.col_name.upper() for field in hive_field_infos]
            for account_field in self.account_list:
                col_name = account_field.col_name

                r = self.hive_util.get_column_desc(db_name, table_name, col_name + "_ORI")

                # 防止多次Alter出错
                if r is None:
                    # 遍历账号字段
                    #  将源字段加上ORI标识
                    col_name = account_field.col_name
                    LOG.debug(col_name)
                    field_type = self.hive_util.get_column_desc(db_name, table_name, col_name)[0][1]
                    # 加上"oracle的判断"
                    if field_type.__contains__("ORACLE"):
                        field_type = field_type.replace(",ORACLE", "")
                    hql = "ALTER TABLE {db_name}.{table_name} CHANGE {col_name} {col_name2} {col_type}".format(
                        db_name=db_name,
                        table_name=table_name,
                        col_name=account_field.col_name,
                        col_name2=account_field.col_name + '_ORI',
                        col_type=field_type
                    )
                    LOG.info("执行SQL:{0}".format(hql))
                    self.hive_util.execute(hql)
                    # 增加新字段
                    hql = "ALTER TABLE {db_name}.{table_name} ADD COLUMNS ({col_name} {col_type})".format(
                        db_name=db_name,
                        table_name=table_name,
                        col_name=account_field.col_name,
                        col_type=field_type
                    )
                    LOG.info("执行SQL：{0}".format(hql))
                    self.hive_util.execute(hql)

    def create_where_sql(self, table_alias, data_date, data_partition_range,
                         date_scope, org_pos, org, where_from_arg):
        """
            构建表(日期分区+机构)条件语句
        :param table_alias: 表别名
        :param data_date: 数据日期
        :param data_partition_range: 分区范围
        :param date_scope: 日期分区
        :param org_pos: 机构字段位置
        :param org: 机构
        :param where_from_arg : 传入的过滤条件
        :return:  sql str
        """
        tmp_str = ""
        if StringUtil.is_blank(table_alias):
            table_alias = ""
        else:
            table_alias = table_alias + "."
        if not StringUtil.is_blank(data_date):
            tmp_str = tmp_str + " {col_date} = '{value}' ".format(
                col_date=self.col_date,
                value=data_date)

        if not StringUtil.eq_ignore(DatePartitionRange.ALL_IN_ONE.value,
                                    data_partition_range):
            if len(tmp_str) > 0:
                tmp_str = tmp_str + "and "
            tmp_str = tmp_str + " {date_scope} = '{value}' ".format(
                date_scope=self.partition_data_scope,
                value=date_scope
            )

        if len(tmp_str) > 0:
            tmp_str = tmp_str + "and "
        if not StringUtil.eq_ignore(OrgPos.NONE.value, org_pos):
            col_name = ""
            if StringUtil.eq_ignore(OrgPos.PARTITION.value, org_pos):
                col_name = self.partition_org
            elif StringUtil.eq_ignore(OrgPos.COLUMN.value, org_pos):
                col_name = self.col_org
            tmp_str = tmp_str + table_alias + (" {col_name} = '{col_value}' ".
                                               format(col_name=col_name,
                                                      col_value=org))
        if where_from_arg:
            if len(tmp_str) > 0:
                tmp_str = tmp_str + " and "
            tmp_str = tmp_str + where_from_arg

        if len(tmp_str) == 0:
            return "1==1"
        else:
            return tmp_str

    def build_load_column_sql(self, table_alias, need_trim):
        """
                  构建column字段sql

              :param table_alias: 表别名
              :param need_trim:

              :return:
              """
        sql = ""
        account_name_list = []
        if len(self.account_list) > 0:
            hive_field_infos = self.hive_util.get_hive_meta_field(self.common_dict, self.db_name, self.table_name, True)
            self.field_change_list = self.get_change_list(self.source_ddl,
                                                          hive_field_infos)
            account_name_list = [acc.col_name.upper() for acc in self.account_list]
        LOG.debug("ACCOUNT_NAME_LIST的长度是 {0}".format(len(account_name_list)))
        if self.field_change_list:
            # 如果字段有变化
            LOG.debug("有字段的变化~ ")
            for field in self.field_change_list:
                if field.col_name.upper() in account_name_list:
                    continue
                LOG.debug("当前字段是：{0}".format(field.col_name))
                is_exists = False  # hive中的字段DDL里是否存在
                for ddl_field in self.source_ddl:
                    source_col_name = ddl_field.col_name

                    if StringUtil.eq_ignore(source_col_name,
                                            field.col_name):
                        is_exists = True
                        break
                if is_exists:
                    col_name = field.col_name
                    if self.field_change_list.index(field) == 0:
                        type = field.ddl_type.field_type if field.ddl_type else field.hive_type.field_type
                        sql = sql + self.build_column(table_alias,
                                                      col_name,
                                                      type,
                                                      need_trim)

                    else:
                        type = field.ddl_type.field_type if field.ddl_type else field.hive_type.field_type
                        sql = sql + "," + self.build_column(table_alias,
                                                            col_name,
                                                            type,
                                                            need_trim)
                else:
                    LOG.debug("Hive字段名：%s" % field.col_name)
                    if self.field_change_list.index(field) == 0:
                        sql = sql + " ''"
                    else:
                        sql = sql + " ,'' "
        else:
            # 无字段变化的情况
            LOG.debug("无字段的变化 ~ ")
            for field in self.source_ddl:
                if field.col_name.upper() in account_name_list:
                    continue
                if self.source_ddl.index(field) == 0:
                    sql = sql + self.build_column(table_alias, field.col_name,
                                                  field.data_type,
                                                  need_trim)
                else:
                    sql = sql + "," + self.build_column(table_alias,
                                                        field.col_name,
                                                        field.data_type,
                                                        need_trim)

        # 添加删除标识, 删除日期
        sql = sql + " ,'0' {DELETE_FLG} ,null {DELETE_DT}".format(
            DELETE_FLG=self._DELETE_FLG,
            DELETE_DT=self._DELETE_DT
        )
        if len(self.account_list) > 0:
            sql = sql + ","

            for acc in account_name_list:
                if not StringUtil.is_blank(table_alias):
                    acc = table_alias + "." + acc
                sql = sql + acc + ","
            sql = sql[:-1]
        LOG.debug("build_column %s" % sql)
        return sql

    def build_load_column_with_compare(self, compare_meta, base_meta,
                                       table_alias,
                                       need_trim):
        """
             比较2个表的结构 构造Sql字段
        :param compare_meta: 接入表元数据 如[id,name]
        :param base_meta: 基准表元数据 根据其结构构造SQL
                          [id,name,sex]
        :param table_alias: 重命名
        :param need_trim:
        :return:  `id`,`name`,'' as sex
        """
        # 元数据DDL信息 source_ddl
        account_name_list = []
        if len(self.account_list) > 0:
            account_name_list = [acc.col_name.upper() for acc in self.account_list]

        sql = ""
        field_change_list = self.get_change_list(compare_meta,
                                                 base_meta)
        base_col_names = [col.col_name.upper() for col in base_meta]
        compare_col_names = [col.col_name.upper() for col in compare_meta]
        if field_change_list:
            # 如果有字段变化
            for field in field_change_list:
                if field.col_name.upper() in account_name_list:
                    # 不处理
                    continue
                if (field.col_name.upper() in base_col_names and
                        field.col_name.upper() in compare_col_names):
                    sql = sql + self.build_column(None, field.col_name,
                                                  field.hive_type.field_type,
                                                  False) + ","

                elif (field.col_name.upper() in base_col_names and
                      field.col_name.upper() not in compare_col_names):
                    sql = sql + " '' as {0} ,".format(
                        field.col_name.upper())  # 加上空串

        else:
            # 无变化的情况
            for field in base_meta:
                if field.col_name.upper() in account_name_list:
                    # 账号转移的字段，都拼接在字段最后
                    continue
                sql = sql + self.build_column(table_alias,
                                              field.col_name,
                                              field.data_type,
                                              need_trim) + ","

        # 加上删除标识和删除时间
        sql = sql + " '0' delete_flg, null delete_dt ,"
        if len(account_name_list) > 0:
            # 在SQL的末端加上账号字段
            for acc in account_name_list:
                if not StringUtil.is_blank(table_alias):
                    acc = table_alias + "." + acc
                sql = sql + acc + ","
        return sql[:-1]

    @staticmethod
    def build_column(table_alias, col_name, col_type, need_trim):
        """
        :param table_alias:
        :param col_name:
        :param col_type:
        :param need_trim:
        :return:
        """
        result = ""
        if not col_name[0].__eq__("`"):
            col_name = "`" + col_name + "`"
        if StringUtil.is_blank(table_alias):
            result = col_name
        else:
            result = table_alias + "." + col_name
        if need_trim:
            #  如果类型string 做trim操作
            if col_type.upper() in ["STRING", "VARCHAR", "CHAR"]:
                result = "trim({value})".format(value=result)

        return result

    @abc.abstractmethod
    def count_archive_data(self):
        """
            统计归档条数
        :return:
        """

    # def delete_exists_archive(self):
    #     pass

    def register_run_log(self):
        """
            登记执行日志
        :return:
        """
        reject_count = 0
        if self.source_count != self.archive_count:
            reject_count = self.source_count - self.archive_count

        old_log = self.mon_run_log_service.get_log(self.pro_id, self.data_date,
                                                   self.org, self.batch)
        # LOG.info("old_log :{0}".format(old_log))
        if old_log:
            # 需要删除 旧的日志
            didp_mon_run_log_his = DidpMonRunLogHis(
                PROCESS_ID=old_log.PROCESS_ID,
                SYSTEM_KEY=old_log.SYSTEM_KEY,
                BRANCH_NO=old_log.BRANCH_NO,
                BIZ_DATE=old_log.BIZ_DATE,
                BATCH_NO=old_log.BATCH_NO,
                TABLE_NAME=old_log.TABLE_NAME,
                DATA_OBJECT_NAME=old_log.DATA_OBJECT_NAME,
                PROCESS_TYPE=old_log.PROCESS_TYPE,  # 加工类型
                PROCESS_STARTTIME=old_log.PROCESS_STARTTIME,
                PROCESS_ENDTIME=old_log.PROCESS_ENDTIME,
                PROCESS_STATUS=old_log.PROCESS_STATUS,
                INPUT_LINES=old_log.INPUT_LINES,
                OUTPUT_LINES=old_log.OUTPUT_LINES,
                REJECT_LINES=old_log.REJECT_LINES,
                EXTENDED1=old_log.EXTENDED1,  # 记录归档类型
                ERR_MESSAGE=old_log.ERR_MESSAGE)
            self.mon_run_log_service.delete_log(self.pro_id, self.data_date,
                                                self.org, self.batch)
            # 写入历史表
            self.mon_run_log_service.insert_log_his(didp_mon_run_log_his)

        didp_mon_run_log = DidpMonRunLog(PROCESS_ID=self.pro_id,
                                         SYSTEM_KEY=self.system,
                                         BRANCH_NO=self.org,
                                         BIZ_DATE=self.data_date,
                                         BATCH_NO=self.batch,
                                         TABLE_NAME=self.table_name,
                                         DATA_OBJECT_NAME=self.obj,
                                         PROCESS_TYPE=PROCESS_TYPE,  # 加工类型
                                         PROCESS_STARTTIME=self.pro_start_date,
                                         PROCESS_ENDTIME=self.pro_end_date,
                                         PROCESS_STATUS=self.__PRO_STATUS,
                                         INPUT_LINES=self.source_count,
                                         OUTPUT_LINES=self.archive_count,
                                         REJECT_LINES=reject_count,
                                         EXTENDED1=self.save_mode,  # 记录归档类型
                                         ERR_MESSAGE=self.error_msg
                                         )
        self.mon_run_log_service.create_run_log(didp_mon_run_log)

    def unlock(self):
        self.hds_struct_control.archive_unlock(self.__args.obj, self.__args.org)

    def check_run_log(self, start_date, end_date):
        """
            根据时间 判断是否有归档任务
        :return:
        """
        result = self.mon_run_log_service.find_run_logs(self.system,
                                                        self.obj,
                                                        self.org,
                                                        start_date, end_date)
        if result:
            return True
        else:
            return False

    @staticmethod
    def build_key_sql_on(table_alias1, table_alias2, pk_list):
        """
            更加别名关联主键 例如a.key=b.key
        :param table_alias1:
        :param table_alias2:
        :param pk_list:
        :return:
        """
        hql = ""
        table_alias1 = table_alias1 + "." if not StringUtil.is_blank(
            table_alias1) else ""
        table_alias2 = table_alias2 + "." if not StringUtil.is_blank(
            table_alias2) else ""
        for pk in pk_list:
            if pk_list.index(pk) > 0:
                hql = hql + " and "
            hql = hql + table_alias1 + pk + "=" + table_alias2 + pk
        return hql

    def drop_table(self, db_name, table_name):
        if self.hive_util.exist_table(db_name, table_name):
            self.hive_util.execute(
                "DROP TABLE {0}.{1} ".format(db_name, table_name))

    def get_ext_key(self):
        """
            获取代理表名，字段名
        :return:
        """

    def run(self):
        """
        归档程序运行入口
        :return:
         0 - 成功 1 - 失败
        """
        try:
            LOG.info("------归档作业开始执行------")
            self.pro_start_date = DateUtil.get_now_date_standy()  # 获取流程执行时间
            LOG.info(" 判断是否有在进行的任务,并加锁 ")
            self.lock()

            LOG.info("日期分区字段检查 ")
            self.data_partition_check()

            LOG.info("机构字段字段检查")
            self.org_check()

            LOG.info("参数初始化2 ")
            self.init_ext()

            LOG.info("元数据处理、表并发处理")
            self.meta_lock()

            if not self.hive_util.exist_table(self.db_name,
                                              self.table_name):
                LOG.info("表不存在，需要重建")
                self.create_table(self.db_name, self.table_name)
            else:
                LOG.info("表已存在")

            LOG.debug("根据表定义变化信息更新表结构 ")
            self.change_table_columns()
            # 更新表结构
            if self.need_reload:
                self.alter_table(self.db_name, self.table_name + "_NEW")
            else:
                self.alter_table(self.db_name, self.table_name)
            self.reload_data()  # 重新导入数据
            LOG.info("元数据登记与更新")
            # 根据目标表结构进行元数据的登记与更新
            self.upload_meta_data()
            LOG.info("元数据并发解锁")
            self.meta_unlock()
            LOG.info("源数据的数据量统计")
            sql = ("SELECT COUNT(1) FROM {db_name}.{table_name}   ".
                   format(db_name=self.__args.sDb,
                          table_name=self.__args.sTable,
                          ))
            if self.filter_sql:
                sql = sql + " where {0}".format(self.filter_sql)
            LOG.debug("执行SQL:{0}".format(sql))
            self.source_count = int(self.hive_util.execute_sql(sql)[0][0])

            LOG.info("接入元数据的数据条数为：{0}".format(int(self.source_count)))
            if self.source_count > 0:
                #     # 原始数据不为空
                LOG.info("数据载入")
                self.load_data()
                LOG.info("统计入库条数")
                self.archive_count = self.count_archive_data()
                # LOG.info("入库的条数为{0}".format(self.archive_count))

            else:
                if self.is_first_archive:
                    # 只有铺底的时候才报错，日常归档不报错
                    LOG.error("待归档数据源为空！请检查装载是否正常")
                    raise BizException("待归档数据源为空！")

        except Exception as e:
            traceback.print_exc()
            self.error_msg = str(e.message)
            self.__PRO_STATUS = "1"
            LOG.error("归档失败")
        finally:

            if self.__lock_archive:
                LOG.info("解除并发锁")
                self.unlock()
            if self.__lock_meta:
                self.meta_unlock()
            LOG.info("登记执行日志")
            self.pro_end_date = DateUtil.get_now_date_standy()
            self.register_run_log()

            # 如果有账号关联 则记录
            self.check_account_err()
            self.is_already_load = True
            LOG.info("删除临时表")
            self.clean()
            if self.session:
                self.session.close()  # 关闭连接
            self.hive_util.close()
            if self.__PRO_STATUS == "0":
                LOG.info("归档成功")
                return 0
            else:
                LOG.error("归档失败")
                return -1

    def check_account_err(self):
        if len(self.account_list) > 0:

            for acc in self.account_list:
                if acc.col_type == 1:
                    cols = acc.col_name + " like '{0}%' ".format(self.common_dict.get(ACCOUNT_PRE_STR))
                    # 查看是否存在未关联上的记录
                    hql = "SELECT COUNT(1) FROM {db_name}.{table_name} WHERE \n" \
                          " {cols} and {where_sql} and {date_col} = '{date}' ".format(
                        db_name=self.db_name,
                        table_name=self.table_name,
                        cols=cols[:-1],
                        where_sql=self.create_where_sql(None, None, self.data_range,
                                                        self.date_scope,
                                                        self.org_pos,
                                                        self.org,
                                                        None),
                        date_col=self.col_date,
                        date=self.data_date
                    )
                    LOG.info("执行SQL %s" % hql)
                    count = self.hive_util.execute_sql(hql)[0][0]
                    if count > 0:
                        # 登记错误日志
                        AccPtyWarnLogger(self.org, self.data_date, self.db_name, self.table_name, acc.col_name,
                                         count).insert_current_record()

    @abc.abstractmethod
    def init_ext(self):
        pass

    @abc.abstractmethod
    def clean(self):
        if self.is_drop_tmp_table:
            self.drop_table(self.temp_db, self.app_table_name1)

    def reload_data(self):
        # 需要动态分区
        if self.need_reload:
            partition_cols = ""
            cols = ""
            if not StringUtil.eq_ignore(self.data_range,
                                        DatePartitionRange.ALL_IN_ONE.value):
                partition_cols = self.partition_data_scope
            if self.org_pos == OrgPos.PARTITION.value:
                partition_cols = partition_cols + "," + self.partition_org
            old_fields = self.hive_util.get_hive_meta_field(self.common_dict, self.db_name, self.table_name,
                                                            False)
            tmp_fields = self.hive_util.get_hive_meta_field(self.common_dict, self.db_name, self.table_name + "_new",
                                                            False)
            tmp_col_names = [field.col_name for field in tmp_fields]
            old_col_names = [field.col_name for field in old_fields]

            cols = ""
            field_change_list = self.get_change_list(old_fields,
                                                     tmp_fields)
            for field in field_change_list:
                if (field.col_name.upper() in old_col_names and
                        field.col_name.upper() in tmp_col_names):
                    cols = cols + self.build_column(None, field.col_name,
                                                    field.hive_type.field_type,
                                                    False) + ","

                elif (field.col_name.upper() in tmp_col_names and
                      field.col_name.upper() not in old_col_names):
                    cols = cols + " '' as {0} ,".format(
                        field.col_name.upper())  # 加上空串

            hql = "INSERT INTO TABLE {DB_NAME}.{TABLE_NAME}_NEW PARTITION ({PARTITION_COLS})  " \
                  " SELECT {COLS} FROM {DB_NAME}.{TABLE_NAME} ".format(
                DB_NAME=self.db_name,
                TABLE_NAME=self.table_name,
                PARTITION_COLS=partition_cols,
                COLS=cols[:-1]
            )
            LOG.info("向临时表插入数据，执行SQL %s " % hql)
            self.hive_util.execute_with_dynamic(hql)
            table1 = self.db_name + '.' + self.table_name
            table2 = self.db_name + '.' + self.table_name + "_new"
            if self.check_num(table1, table2):
                # 删除原表
                sql = "DROP TABLE {DB}.{TABLE}".format(DB=self.db_name,
                                                       TABLE=self.table_name)
                LOG.info("删除原表，执行SQL %s" % sql)
                self.hive_util.execute(sql)

            # 创建新表
            sql = "CREATE TABLE {db}.{table_name} like {db}.{table_name}_new ".format(db=self.db_name,
                                                                                      table_name=self.table_name)
            LOG.info("创建新表，执行SQL %s" % sql)
            self.hive_util.execute(sql)
            # 导入数据
            sql = "INSERT INTO TABLE {db}.{table_name} partition( {p_cols} ) select * from {db}.{table_name}_new ".format(
                db=self.db_name,
                table_name=self.table_name,
                p_cols=partition_cols
            )
            LOG.info("向新表插入数据，执行SQL %s" % sql)
            self.hive_util.execute_with_dynamic(sql)

            # 检查是否执行成功
            if self.check_num(table1, table2):
                # 删除临时表
                sql = "DROP TABLE {DB}.{TABLE}_new".format(DB=self.db_name,
                                                           TABLE=self.table_name)
                LOG.info("删除临时表，执行SQL %s" % sql)
                self.hive_util.execute(sql)

    def check_num(self, table1, table2):
        count1 = self.hive_util.execute_sql("select count(1) from %s" % table1)[0][0]
        count2 = self.hive_util.execute_sql("select count(1) from %s" % table2)[0][0]
        return count1 == count2


class LastAddArchive(ArchiveData):
    """
        最近增量归档
    """

    def recreate_table(self, change_detail_buffer):
        pass

    def __init__(self):
        super(LastAddArchive, self).__init__()
        self.print_save_mode()

    def print_save_mode(self):
        LOG.info("》》》》》》执行最近一天增量归档》》》》》")

    def clean(self):
        super(LastAddArchive, self).clean()

    def init_ext(self):
        if int(self.source_data_mode) != SourceDataMode.ADD.value:
            raise BizException("当日增量不提供非增量数据模式转换存储!")

        if not StringUtil.eq_ignore(self.data_range,
                                    DatePartitionRange.ALL_IN_ONE.value):
            raise BizException("当日增量归档模式，不允许时间分区 ")

    def count_archive_data(self):

        hql = "select count(1) from {db_name}.{table_name} ".format(
            db_name=self.source_db,
            table_name=self.source_table)
        r = self.hive_util.execute_sql(hql)
        count = int(r[0][0])
        LOG.info("最近一天增量的入库数据为：{0}".format(count))
        return count

    def create_table(self, db_name, table_name):
        """
            创建Hive表
        :return:  None
        """
        self.hive_util.execute(
            "DROP TABLE {DB_NAME}.{TABLE_NAME} ".format(
                DB_NAME=db_name,
                TABLE_NAME=table_name))
        # 获取增加字段
        col_date = self.col_date
        execute_sql = ("CREATE TABLE {DB_NAME}.{TABLE_NAME} "
                       "( {COL_DATE} varchar(10), ".
                       format(DB_NAME=self.db_name,
                              TABLE_NAME=self.table_name,
                              COL_DATE=col_date
                              ))  # 原始执行Sql

        # org_pos  1-没有机构字段 2-字段在列中 3-字段在分区中
        if self.org_pos == OrgPos.COLUMN.value:
            execute_sql = execute_sql + ("{ORG_COL} string ,".
                                         format(ORG_COL=self.col_org))
            # print common_dict.get(AddColumn.COL_ORG.value)
        # 组装字段

        body = self.create_table_body(False)
        execute_sql = execute_sql + body + ")"

        if self.org_pos == OrgPos.PARTITION.value:
            # 机构字段在分区中
            execute_sql = execute_sql + (" PARTITIONED BY ({org_col} string)".
                                         format(org_col=self.partition_org))

        # 默认全部为事务表
        execute_sql = execute_sql + (" CLUSTERED  BY ({CLUSTER_COL}) "
                                     "INTO {BUCKET_NUM} BUCKETS STORED AS ORC "
                                     "tblproperties('orc.compress'='SNAPPY' ,"
                                     "'transactional'='true')".
                                     format(CLUSTER_COL=self.cluster_col,
                                            BUCKET_NUM=self.buckets_num))
        LOG.info("建表语句为：%s " % execute_sql)
        self.hive_util.execute(execute_sql)

    def load_data(self):
        if len(self.account_list) > 0:
            self.create_temp_table_for_account()  # 建表，导数据
            # 更新source_ddl
            self.source_ddl = (self.meta_data_service.
                               parse_input_table(self.hive_util,
                                                 self.source_db,
                                                 self.account_table_name,
                                                 self.filter_cols,
                                                 True
                                                 ))
        # 清空表数据
        hql = "TRUNCATE TABLE {DB_NAME}.{TABLE_NAME}".format(
            DB_NAME=self.db_name,
            TABLE_NAME=self.table_name)
        LOG.info("执行SQL 清空表数据 ：{0}".format(hql))
        self.hive_util.execute(hql)
        # 插入数据

        hql = (
            "  INSERT INTO TABLE {db_name}.{table_name} {partition_sql} \n "
            "  SELECT  '{data_date}',".
                format(source_db_name=self.source_db,
                       source_table_name=self.source_table if self.account_table_name is None else self.account_table_name,
                       db_name=self.db_name,
                       table_name=self.table_name,
                       partition_sql=self.create_partition_sql(self.data_range,
                                                               self.date_scope,
                                                               self.org),
                       data_date=self.data_date
                       ))
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + " '{0}' ,".format(self.org)

        # 构造字段的sql
        hql = hql + self.build_load_column_sql("", True)

        hql = hql + " FROM {source_db_name}.{source_table_name} S \n ".format(
            source_db_name=self.source_db,
            source_table_name=self.source_table if self.account_table_name is None else self.account_table_name)

        if not StringUtil.is_blank(self.filter_sql):
            # 如果有过滤条件,加入过滤条件
            hql = hql + "\n  WHERE {filter_col}".format(
                filter_col=self.filter_sql)
        LOG.info("执行SQL ： {0}".format(hql))
        self.hive_util.execute(hql)
        # self.archive_count = self.count_archive_data()


class LastAllArchive(ArchiveData):
    """
        最近一天全量
    """

    def print_save_mode(self):
        LOG.info("》》》》》》执行最近一天全量归档》》》》》")

    def __init__(self):
        super(LastAllArchive, self).__init__()
        self.print_save_mode()

    def clean(self):
        super(LastAllArchive, self).clean()

    def init_ext(self):
        # 判断是否存在主键信息
        if not self.pk_list or StringUtil.is_blank(self.pk_list):
            raise BizException("归档表{db_name}.{table_name}的主键不存在 ！".
                               format(db_name=self.db_name,
                                      table_name=self.table_name))
        self.pk_list = self.pk_list.split("|")
        self.pk_list = [pk.upper() for pk in self.pk_list]  # 保证主键全部大写
        # 判断前一天是否做过归档
        if self.check_run_log("00000101", self.data_date):
            if not self.check_run_log(self.data_date, self.data_date):
                if not self.check_run_log(self.last_date, self.last_date):
                    raise BizException("前一天没有做归档,不能做最近全量归档 ")

        if not StringUtil.eq_ignore(self.data_range,
                                    DatePartitionRange.ALL_IN_ONE.value):
            raise BizException(
                "当日全量归档模式，不允许时间分区;当前时间分区为：{0}".
                    format(self.data_range))

    def count_archive_data(self):
        hql = "select count(1) from {db_name}.{table_name} ".format(
            db_name=self.source_db,
            table_name=self.source_table)
        r = self.hive_util.execute_sql(hql)
        count = int(r[0][0])
        if self.source_data_mode == SourceDataMode.ADD.value:
            LOG.info("----增量 -》 最近一天全量----- \n"
                     "增量数据源数据条数：{0} \n"
                     "增量求全量入库数据条数：{1}".format(self.source_count, count))

        else:
            LOG.info("全量 -》 全量 的入库数据为：{0}".format(count))
        return count

    def create_table(self, db_name, table_name):
        col_date = self.col_date
        execute_sql = ("CREATE TABLE IF NOT EXISTS \n"
                       "  {DB_NAME}.{TABLE_NAME} ( {COL_DATE} varchar(10), ".
                       format(DB_NAME=db_name,
                              TABLE_NAME=table_name,
                              COL_DATE=col_date
                              ))  # 原始执行Sql

        # org_pos  1-没有机构字段 2-字段在列中 3-字段在分区中
        if self.org_pos == OrgPos.COLUMN.value:
            execute_sql = execute_sql + "{ORG_COL} string ,".format(
                ORG_COL=self.col_org)
            # print common_dict.get(AddColumn.COL_ORG.value)
        # 组装字段
        body = self.create_table_body(False)
        execute_sql = execute_sql + body + ")"

        if self.org_pos == OrgPos.PARTITION.value:
            # 机构字段在分区中
            execute_sql = execute_sql + (
                "\n  PARTITIONED BY ({org_col} string) ".
                    format(org_col=self.partition_org))

        # 默认全部为事务表
        execute_sql = execute_sql + ("\n  CLUSTERED  BY ({CLUSTER_COL}) "
                                     "INTO {BUCKET_NUM} BUCKETS \n"
                                     "  STORED AS ORC \n"
                                     "  tblproperties('orc.compress'='SNAPPY' ,"
                                     "'transactional'='true')".
                                     format(CLUSTER_COL=self.cluster_col,
                                            BUCKET_NUM=self.buckets_num))
        LOG.info("建表语句为：%s " % execute_sql)
        self.hive_util.execute(execute_sql)

    def load_data(self):
        # self.pre_table = self.temp_db + "." + self.input_table_name

        if len(self.account_list) > 0:
            self.create_temp_table_for_account()  # 建表，导数据
            # 更新source_ddl
            self.source_ddl = (self.meta_data_service.
                               parse_input_table(self.hive_util,
                                                 self.source_db,
                                                 self.account_table_name,
                                                 self.filter_cols,
                                                 True
                                                 ))

        if StringUtil.eq_ignore(self.source_data_mode,
                                SourceDataMode.ADD.value):
            # 增求全
            self.load_data_add()
        else:
            # 全量直接入库
            self.load_data_all()

    def load_data_add(self):
        LOG.info("删除日期为当天的数据")
        hql = ("DELETE FROM {db_name}.{table_name} {partition_sql} \n"
               "  WHERE {col_date} = '{data_date}' ".
               format(db_name=self.db_name,
                      table_name=self.table_name,
                      partition_sql=self.create_partition_sql(self.data_range,
                                                              self.date_scope,
                                                              self.org),
                      col_date=self.col_date,
                      data_date=self.data_date
                      ))
        if OrgPos.COLUMN.value == self.org_pos:
            hql = hql + " AND {col_org} = '{org}' ".format(
                col_org=self.col_org,
                org=self.org)
        LOG.info("删除数据的SQL为 ：{0}".format(hql))
        self.hive_util.execute(hql)
        LOG.info("根据input表主键删除源表数据")
        hql = ("DELETE FROM {db_name}.{table_name} {partition_sql} as A  \n"
               "  WHERE {where_sql} AND \n"
               "  EXISTS (SELECT 1 FROM {source_db}.{source_table} as B \n"
               "  WHERE {build_key_sql_on} ) "
               .format(db_name=self.db_name,
                       table_name=self.table_name,
                       partition_sql=self.create_partition_sql(
                           self.data_range,
                           self.date_scope,
                           self.org),
                       where_sql=self.create_where_sql(
                           "A",
                           None,
                           self.data_range,
                           self.date_scope,
                           self.org_pos,
                           self.org,
                           None),
                       source_db=self.source_db,
                       source_table=self.source_table if self.account_table_name is None else self.account_table_name,
                       build_key_sql_on=self.build_key_sql_on("A", "B",
                                                              self.pk_list)
                       ))
        LOG.info("删除源表数据的SQL为:{0}".format(hql))
        self.hive_util.execute(hql)
        # 插入数据
        LOG.debug("将数据插入归档表")
        hql = (
            "  INSERT INTO {db_name}.{table_name}  \n"
            "  {partition_sql} SELECT '{data_date}', ".
                format(source_db=self.source_db,
                       source_table=self.source_table if self.account_table_name is None else self.account_table_name,
                       db_name=self.db_name,
                       table_name=self.table_name,
                       partition_sql=self.create_partition_sql(
                           self.data_range,
                           self.date_scope,
                           self.org
                       ),
                       data_date=self.data_date))
        if OrgPos.COLUMN.value == self.org_pos:
            hql = hql + " '{0}', ".format(self.org)
        hql = hql + self.build_load_column_sql(None, True)
        hql = hql + " FROM {source_db_name}.{source_table_name} S \n ".format(
            source_db_name=self.source_db,
            source_table_name=self.source_table if self.account_table_name is None else self.account_table_name)

        if not StringUtil.is_blank(self.filter_sql):
            # 如果有过滤条件,加入过滤条件
            hql = hql + "\n  WHERE {filter_col}".format(
                filter_col=self.filter_sql)
        LOG.info("执行SQL :{0}".format(hql))
        self.hive_util.execute(hql)

    def load_data_all(self):
        # 先清空数据表
        self.hive_util.execute("TRUNCATE TABLE {db_name}.{table_name}".format(
            db_name=self.db_name,
            table_name=self.table_name))

        LOG.debug("将数据插入归档表")
        hql = ("FROM {source_db}.{source_table} \n"
               "  INSERT INTO {db_name}.{table_name} \n"
               "  {partition_sql} SELECT '{data_date}', ".
               format(source_db=self.source_db,
                      source_table=self.source_table if self.account_table_name is None else self.account_table_name,
                      db_name=self.db_name,
                      table_name=self.table_name,
                      partition_sql=self.create_partition_sql(
                          self.data_range,
                          self.date_scope,
                          self.org
                      ),
                      data_date=self.data_date))
        if OrgPos.COLUMN.value == self.org_pos:
            hql = hql + " '{0}', ".format(self.org)
        hql = hql + self.build_load_column_sql(None, True)

        if not StringUtil.is_blank(self.filter_sql):
            # 如果有过滤条件,加入过滤条件
            hql = hql + "\n  WHERE {filter_col}".format(
                filter_col=self.filter_sql)
        LOG.info("执行SQL: {0}".format(hql))
        self.hive_util.execute(hql)


class AddArchive(ArchiveData):
    """
        历史增量归档
    """

    def print_save_mode(self):
        LOG.info("》》》》》》执行历史增量归档》》》》》")

    all_org_pos = None  # 全量历史表的机构位置
    has_table_all = False  # 是否存在全量历史表
    is_drop_app_table = False  # 是否删除临时表

    def __init__(self):
        super(AddArchive, self).__init__()
        self.db_name_all = None
        self.table_name_all = None
        if self.all_table:
            self.db_name_all, self.table_name_all = self.all_table.split(
                ".")
        self.print_save_mode()

    def clean(self):
        super(AddArchive, self).clean()

    def init_ext(self):
        # 判断SourceDataMode
        if self.source_data_mode == SourceDataMode.ALL.value:
            # 需要获取All_table_name
            if self.all_table:

                if not self.all_range:
                    raise BizException("全量供数保存增量，全量存储策略日期分区范围参数不合法")
                if self.all_table:
                    self.has_table_all = self.hive_util.exist_table(
                        self.db_name_all,
                        self.table_name_all)
                if self.has_table_all:
                    self.all_org_pos = self.hive_util.get_org_pos(
                        self.common_dict,
                        self.db_name_all,
                        self.table_name_all)

    def count_archive_data(self):
        hql = ("SELECT COUNT(1) FROM {DB_NAME}.{TABLE_NAME} WHERE {WHERE_SQL}".
               format(DB_NAME=self.db_name,
                      TABLE_NAME=self.table_name,
                      WHERE_SQL=self.create_where_sql("", self.data_date,
                                                      self.data_range,
                                                      self.date_scope,
                                                      self.org_pos,
                                                      self.org,
                                                      None
                                                      )))
        x = self.hive_util.execute_sql(hql)
        count = int(x[0][0])
        if self.source_data_mode == SourceDataMode.ALL.value:
            LOG.info("-----全量数据源增量入库：-----\n"
                     "全量数据条数：{0}"
                     "全量求增量数据条数为：{1}".format(self.source_count, count))
        else:
            LOG.info("入库条数为：{0}".format(count))
        return count

    def create_table(self, db_name, table_name):

        hql = ("CREATE TABLE IF NOT EXISTS {db_name}.{table_name} ( \n"
               "  {col_date} VARCHAR(10) ,".
               format(db_name=db_name,
                      table_name=table_name,
                      col_date=self.col_date
                      ))
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + " {col_org} VARCHAR(10),".format(
                col_org=self.col_org
            )

        # 构建Body 语句
        body = self.create_table_body(False)
        hql = hql + body + " )"

        part_sql = ""
        # 构建partitioned by
        if not StringUtil.eq_ignore(self.data_range,
                                    DatePartitionRange.ALL_IN_ONE.value):
            part_sql = "{date_scope} string ,".format(
                date_scope=self.partition_data_scope)
        if self.org_pos == OrgPos.PARTITION.value:
            part_sql = part_sql + "{col_org} string,".format(
                col_org=self.partition_org)
        # 若存在分区字段
        if len(part_sql) > 0:
            hql = hql + " PARTITIONED BY ( " + part_sql[:-1] + ") \n"
        # 分桶
        hql = hql + ("  clustered by ({CLUSTER_COL}) into {BUCKET_NUM} \n"
                     "  BUCKETS  STORED AS orc \n"
                     "  tblproperties('orc.compress'='SNAPPY' ,"
                     "'transactional'='true')".
                     format(CLUSTER_COL=self.cluster_col,
                            BUCKET_NUM=self.buckets_num))
        LOG.info("执行SQL: {0}".format(hql))
        self.hive_util.execute(hql)

    def load_data(self):
        if len(self.account_list) > 0:
            self.create_temp_table_for_account()  # 建表，导数据
            # 更新source_ddl # 让临时表的DDL作为新的DDL
            self.source_ddl = (self.meta_data_service.
                               parse_input_table(self.hive_util,
                                                 self.source_db,
                                                 self.account_table_name,
                                                 self.filter_cols,
                                                 True
                                                 ))

        LOG.debug("先根据数据日期删除表中数据")
        hql = ("DELETE FROM  {DB_NAME}.{TABLE_NAME} {PARTITION_SQL} \n"
               "  WHERE {WHERE_SQL} ".
               format(DB_NAME=self.db_name,
                      TABLE_NAME=self.table_name,
                      PARTITION_SQL=self.create_partition_sql(self.data_range,
                                                              self.date_scope,
                                                              self.org),
                      WHERE_SQL=self.create_where_sql("", self.data_date,
                                                      self.data_range,
                                                      self.date_scope,
                                                      self.org_pos,
                                                      self.org,
                                                      None)
                      ))
        LOG.info("执行SQL: {0}".format(hql))
        self.hive_util.execute(hql)
        if self.source_data_mode == SourceDataMode.ADD.value:
            # 直接入库
            self.load_data1()
        elif self.source_data_mode == SourceDataMode.ALL.value:
            # 全求增转换入库
            self.load_data2()

    def load_data1(self):
        LOG.debug("------------直接入库-------")
        hql = (
            "  INSERT INTO TABLE {db_name}.{table_name} \n"
            "  {partition_sql} SELECT '{data_date}', ".
                format(db_name=self.db_name,
                       table_name=self.table_name,
                       partition_sql=self.create_partition_sql(
                           self.data_range,
                           self.date_scope,
                           self.org
                       ),
                       data_date=self.data_date))
        if OrgPos.COLUMN.value == self.org_pos:
            hql = hql + " '{0}', ".format(self.org)
        hql = hql + self.build_load_column_sql(None, True)
        # 如果有账号转移 则加入账号转移

        hql = hql + " FROM {source_db_name}.{source_table_name} S \n ".format(
            source_db_name=self.source_db,
            source_table_name=self.source_table if self.account_table_name is None else self.account_table_name)

        if not StringUtil.is_blank(self.filter_sql):
            # 如果有过滤条件,加入过滤条件
            hql = hql + "\n  WHERE {filter_col} ".format(
                filter_col=self.filter_sql)
        LOG.info("执行SQL : {0}".format(hql))
        self.hive_util.execute(hql)

    def load_data2(self):
        """
            全求增 转换入库
        :return:
        """
        LOG.debug("------------执行全求增转换入库----------")
        # 增量表字段信息
        meta_info = self.hive_util.get_hive_meta_field(
            self.common_dict,
            self.db_name,
            self.table_name,
            True)

        all_table_fields_infos = None

        if self.all_table:
            all_table_fields_infos = self.hive_util.get_hive_meta_field(
                self.common_dict,
                self.db_name_all,
                self.table_name_all,
                True)  # 全量表字段
        hive_field_infos = meta_info
        pk_list = self.pk_list
        if pk_list:
            pk_list = pk_list.split("|")
            if len(pk_list) < 0:
                raise BizException(
                    "数据对象{obj} 主键不存在 ！".format(obj=self.obj))
            else:
                pk_list = [pk.upper() for pk in pk_list]  # 大写
        key_dict = {}
        # 获取主键字典
        for pk in pk_list:
            for field in meta_info:
                if StringUtil.eq_ignore(pk, field.col_name):
                    key_dict[pk] = field.data_type  # 只需获取字段类型 无需精度，长度
                    break

        yes_day = DateUtil.get_day_of_day(self.data_date, -1)  # 前一天 str
        # 取得最近一次全量归档信息
        lastest_all_archive = None  # 最近一次历史全量归档记录
        if self.table_name_all:
            # 查找最近一次全量归档记录
            lastest_all_archive = self.mon_run_log_service. \
                find_latest_all_archive(self.system, self.table_name_all,
                                        self.org, yes_day)

        LOG.debug("是否存在全量记录 {0}".format(lastest_all_archive))
        has_yes_all_data = False  # 判断最近一次全量归档是否在昨日

        if lastest_all_archive:
            biz_date = lastest_all_archive.BIZ_DATE
            LOG.debug("最近全量的日期是：{0}".format(biz_date))
            if StringUtil.eq_ignore(yes_day, biz_date):
                has_yes_all_data = True

        # 当日全量数据入临时表
        if (not self.hive_util.exist_table(self.temp_db,
                                           self.app_table_name1) or
                not self.hive_util.compare(
                    self.common_dict,
                    self.db_name, self.table_name, self.temp_db,
                    self.app_table_name1, False)):
            self.drop_table(self.temp_db, self.app_table_name1)

            hql = ("CREATE TABLE {temp_db}.{app_name} "
                   "  like {db_name}.{table_name}".
                   format(temp_db=self.temp_db,
                          db_name=self.db_name,
                          app_name=self.app_table_name1,
                          table_name=self.table_name))

        else:
            hql = ("DELETE FROM  {temp_db}.{app_name} {partition_sql} \n"
                   "  WHERE 1=1 ".format(temp_db=self.temp_db,
                                         app_name=self.app_table_name1,
                                         partition_sql=self.create_partition_sql(
                                             self.data_range,
                                             "0",
                                             "0"
                                         )))
        LOG.info("执行SQL:{0}".format(hql))
        self.hive_util.execute(hql)
        if self.is_drop_tmp_table:
            self.is_drop_app_table = True
        # 入临时表语句构建
        hql = (" INSERT INTO TABLE \n"
               "  {TEMP_DB}.{APP_TABLE} {PARTITION} \n"
               "  SELECT '{DATA_DATE}', ".
               format(SOURCE_DB=self.source_db,
                      SOURCE_TABLE=self.source_table if self.account_table_name is None else self.account_table_name,
                      TEMP_DB=self.temp_db,
                      APP_TABLE=self.app_table_name1,
                      PARTITION=self.create_partition_sql(self.data_range, "0",
                                                          "0"),
                      DATA_DATE=self.data_date
                      ))
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + "'{org}',".format(org=self.org)
        hql = hql + self.build_load_column_sql(None, True)

        hql = hql + " FROM {source_db_name}.{source_table_name} S \n ".format(
            source_db_name=self.source_db,
            source_table_name=self.source_table if self.account_table_name is None else self.account_table_name)

        if not StringUtil.is_blank(self.filter_sql):
            # 过滤sql
            hql = hql + " WHERE {0}".format(self.filter_sql)
        LOG.info("执行SQL :{0}".format(hql))
        self.hive_util.execute(hql)

        sql = ("FROM {TEMP_DB}.{APP_TABLE} hds_temp_table \n"
               "  FULL OUTER JOIN  ( \n".format(TEMP_DB=self.temp_db,
                                                APP_TABLE=self.app_table_name1))
        if has_yes_all_data:
            # 对比2分全量数据 得出增量数据
            sql = sql + "  SELECT {col_date},".format(col_date=self.col_date)
            # if self.all_org_pos == OrgPos.COLUMN.value:
            #     sql = sql + "{col_org}, ".format(col_org=self.col_org)
            # LOG.debug("全量表的 机构字段位置是： {0}".format(self.all_org_pos))
            sql = sql + (" {cols}  FROM {ALL_TABLE_NAME} WHERE {WHERE_SQL} \n".
                format(
                cols=self.build_load_column_with_compare(all_table_fields_infos,
                                                         hive_field_infos,
                                                         None,
                                                         False
                                                         ),
                ALL_TABLE_NAME=self.all_table,
                WHERE_SQL=self.create_where_sql(
                    "", yes_day,
                    self.all_range,
                    self.get_data_scope(
                        self.all_range,
                        yes_day),
                    self.all_org_pos,
                    self.org, None)
            ))

        else:
            # 手动生成全量数据
            sql = sql + ("  SELECT * FROM (SELECT *,row_number() \n"
                         "  OVER(DISTRIBUTE BY \n")
            # 按主键分组
            for key in key_dict:
                if str(key_dict.get(key)).lower() in ["string", "varchar",
                                                      "char"]:
                    sql = sql + " trim({key}),".format(key=key)
                else:
                    sql = sql + key + ","
            sql = sql[:-1]

            #  按日期倒序、自定义分区顺序（全量分区-增量/减量分区-初始化分区）排序
            sql = sql + ("  SORT BY  {COL_DATE} DESC ) \n"
                         "  AS hds_section_rn FROM ( \n".format
                         (COL_DATE=self.col_date))
            # 定位需扫描的分区及自定义分区顺序

            # 最近一次全量
            if lastest_all_archive and self.has_table_all:
                build_cols = self.build_load_column_with_compare(
                    all_table_fields_infos,
                    hive_field_infos,
                    None,
                    False
                )
                sql = sql + "SELECT {COL_DATE},".format(COL_DATE=self.col_date)
                # if self.all_org_pos == OrgPos.COLUMN.value:
                #     sql = sql + " {col_org},".format(col_org=self.col_org)
                sql = sql + ("{COLS} FROM {all_table_name} "
                             "WHERE {WHERE_SQL} UNION ALL  ".
                             format(COLS=build_cols,
                                    all_table_name=self.all_table,
                                    WHERE_SQL=self.
                                    create_where_sql(
                                        "",
                                        lastest_all_archive.BIZ_DATE,
                                        self.all_range,
                                        self.get_data_scope(
                                            self.all_range,
                                            yes_day),
                                        self.all_org_pos,
                                        self.org,
                                        None
                                    )
                                    ))
                # LOG.debug("------ 构建的语句是 :{0}".format(build_cols))

            # 往日增量
            sql = sql + " SELECT {COL_DATE},".format(COL_DATE=self.col_date)
            # if self.org_pos == OrgPos.COLUMN.value:
            #     sql = sql + " {col_org},".format(col_org=self.col_org)
            sql = sql + ("{COLS}  FROM {DB_NAME}.{TABLE_NAME}"
                         " WHERE {COL_DATE}<  '{DATA_DATE}'   "
                         .format(COL_DATE=self.col_date,
                                 COLS=self.build_load_column_sql(None, False),
                                 DB_NAME=self.db_name,
                                 TABLE_NAME=self.table_name,
                                 DATA_DATE=self.data_date
                                 ))

            if lastest_all_archive and self.has_table_all:
                # 添加 Where 过滤条件
                sql = sql + (" AND {COL_DATE} > '{ALL_DATE}' "
                             .format(COL_DATE=self.col_date,
                                     ALL_DATE=lastest_all_archive.BIZ_DATE))
                date_range = self.data_range
                # 确定 Date_scope值
                date_scope = None
                if StringUtil.eq_ignore(date_range,
                                        DatePartitionRange.MONTH.value):
                    date_scope = str(lastest_all_archive.BIZ_DATE)[0:6]
                elif StringUtil.eq_ignore(date_range,
                                          DatePartitionRange.QUARTER_YEAR.value):
                    date_scope = DateUtil.get_quarter(
                        lastest_all_archive.BIZ_DATE)
                elif StringUtil.eq_ignore(date_range,
                                          DatePartitionRange.YEAR.value):
                    date_scope = str(lastest_all_archive.BIZ_DATE)[0:4]

                if date_scope:
                    sql = sql + (" AND  {DATE_SCOPE} <= '{VAL1}' AND "
                                 " {DATE_SCOPE} >= '{VAL2}' "
                                 .format(DATE_SCOPE=self.partition_data_scope,
                                         VAL1=self.date_scope,
                                         VAL2=date_scope
                                         ))

                # org_pos
                org_pos = self.org_pos
                # 确定 ORG_COL
                org_col = None
                if org_pos == OrgPos.PARTITION.value:
                    org_col = self.partition_org
                elif org_pos == OrgPos.COLUMN.value:
                    org_col = self.col_org
                if org_col:
                    sql = sql + (" AND {ORG_COL}= '{ORG}' "
                                 .format(ORG_COL=org_col,
                                         ORG=self.org))

            sql = sql + (" ) hds_section_t1 )  hds_section_t2 "
                         " WHERE hds_section_t2.hds_section_rn=1 ")

        sql = sql + (")  hds_section_yes ON  {build_key} "
                     " INSERT INTO TABLE {DB_NAME}.{TABLE_NAME} "
                     " {PARTITION} SELECT '{DATA_DATE}',"
                     .format(build_key=self.build_key_sql_on("hds_temp_table",
                                                             "hds_section_yes",
                                                             pk_list),
                             DB_NAME=self.db_name,
                             TABLE_NAME=self.table_name,
                             PARTITION=self.create_partition_sql(
                                 self.data_range,
                                 self.date_scope,
                                 self.org),
                             DATA_DATE=self.data_date
                             ))
        if self.org_pos == OrgPos.COLUMN.value:
            sql = sql + " '{org}', ".format(org=self.org)
        sql = sql + self.build_load_column_sql("hds_temp_table",
                                               False) + "\n WHERE "
        where_sql = ""
        for field in self.source_ddl:
            if field.col_name.upper() not in pk_list:
                where_sql = where_sql + (" hds_section_yes.{col_name} != "
                                         " hds_temp_table.{col_name} OR ".
                                         format(col_name=field.col_name_quote))
        if len(where_sql) > 0:
            sql = sql + " {WHERE_SQL} ".format(WHERE_SQL=where_sql)
        sql = sql + (" hds_section_yes.{COL_DATE} IS NULL "
                     .format(COL_DATE=self.col_date))

        LOG.info("执行SQL 语句：{0}".format(sql))
        self.hive_util.execute(sql)


class AllArchive(ArchiveData):
    """
        历史全量数据入库
    """

    def print_save_mode(self):
        LOG.info("》》》》》》执行历史全量归档》》》》》")

    table_add = None  # 增量历史表表名
    data_range_add = None  # 增量历史表分区范围
    org_pos_add = None  # 增量表机构字段位置
    is_drop_app_table = None

    def __init__(self):
        super(AllArchive, self).__init__()
        if self.add_table:
            self.db_name_add, self.table_name_add = self.add_table.split(
                ".")
        self.print_save_mode()

    def clean(self):
        super(AllArchive, self).clean()
        if self.is_drop_tmp_table:
            self.drop_table(self.temp_db, self.app_table_name1)

    def init_ext(self):
        source_data_mode = int(self.source_data_mode)
        if source_data_mode == SourceDataMode.ADD.value:
            #  需要增求全 取增量表表名
            self.table_add = self.add_table
            self.data_range_add = self.add_range
            if self.table_add:
                if self.data_range_add is None:
                    raise BizException("增量供数保存全量，增量存储策略日期分区范围参数不合法")
                db_name, table_name = self.table_add.split(".")
                self.org_pos_add = (self.hive_util.
                                    get_org_pos(self.common_dict, db_name,
                                                table_name))

    def count_archive_data(self):
        hql = ("SELECT COUNT(1) FROM {DB_NAME}.{TABLE_NAME} WHERE {WHERE_SQL}".
               format(DB_NAME=self.db_name,
                      TABLE_NAME=self.table_name,
                      WHERE_SQL=self.create_where_sql("", self.data_date,
                                                      self.data_range,
                                                      self.date_scope,
                                                      self.org_pos,
                                                      self.org,
                                                      None)))
        LOG.info("执行SQL：{0}".format(hql))
        x = self.hive_util.execute_sql(hql)
        count = int(x[0][0])
        if self.source_data_mode == SourceDataMode.ADD.value:
            LOG.info("----- 增量数据源全量入库：----- \n"
                     "增量数据源条数：{0} \n"
                     "增量求全量数据条数：{1}".format(self.source_count, count))
        else:
            LOG.info("全量数据入库条数：{0}".format(count))
        return count

    def create_table(self, db_name, table_name):
        hql = (" CREATE TABLE IF NOT EXISTS {DB_NAME}.{TABLE_NAME} ( \n"
               "  {COL_DATE} VARCHAR(10) ".format(DB_NAME=db_name,
                                                  TABLE_NAME=table_name,
                                                  COL_DATE=self.col_date
                                                  ))
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + ",{COL_ORG} VARCHAR(10) ".format(COL_ORG=self.col_org)
        # 如果有账号转移，需要在建表的时候加入账号转移字段。
        for field in self.source_ddl:
            hql = hql + (",{field_name} {field_type} ".
                         format(field_name=field.col_name_quote,
                                field_type=field.get_full_type()))
            if not StringUtil.is_blank(field.comment):
                hql = hql + " comment '{comment}' ".format(
                    comment=field.comment)
        # 添加删除标记和删除日期
        hql = hql + ", {DELETE_FLG} VARCHAR(1),{DELETE_DT} VARCHAR(8) ".format(DELETE_FLG=self._DELETE_FLG,
                                                                               DELETE_DT=self._DELETE_DT)
        hql = hql + " )"
        tmp_sql = ""
        if not StringUtil.eq_ignore(self.data_range,
                                    DatePartitionRange.ALL_IN_ONE.value):
            tmp_sql = "{col_data_scope} string,".format(
                col_data_scope=self.partition_data_scope)
        if self.org_pos == OrgPos.PARTITION.value:
            tmp_sql = tmp_sql + "{partion_org} string,".format(
                partion_org=self.partition_org)
        if tmp_sql.__len__() > 0:
            hql = hql + (" PARTITIONED BY ( {partition} ) \n".
                         format(partition=tmp_sql[:-1]))
        hql = hql + ("  CLUSTERED BY ({CLUSTER_COL}) INTO {BUCKET_NUM} \n"
                     "  BUCKETS  STORED AS orc \n"
                     "  tblproperties('orc.compress'='SNAPPY',"
                     "'transactional'='true')".
                     format(CLUSTER_COL=self.cluster_col,
                            BUCKET_NUM=self.buckets_num))

        LOG.info("执行SQL:{0}".format(hql))
        self.hive_util.execute(hql)

    def load_data(self):
        if len(self.account_list) > 0:
            self.create_temp_table_for_account()  # 建表，导数据
            # 更新source_ddl
            self.source_ddl = (self.meta_data_service.
                               parse_input_table(self.hive_util,
                                                 self.source_db,
                                                 self.account_table_name,
                                                 self.filter_cols,
                                                 True
                                                 ))
        #  先根据数据日期删除表中数据
        hql = ("DELETE FROM {DB_NAME}.{TABLE_NAME} {PARTITION} \n"
               "  WHERE {WHERE_SQL} \n".
               format(DB_NAME=self.db_name,
                      TABLE_NAME=self.table_name,
                      PARTITION=self.create_partition_sql(self.data_range,
                                                          self.date_scope,
                                                          self.org
                                                          ),
                      WHERE_SQL=self.create_where_sql("", self.data_date,
                                                      self.data_range,
                                                      self.date_scope,
                                                      self.org_pos, self.org,
                                                      None)))
        LOG.info("执行SQL:{0}".format(hql))
        self.hive_util.execute(hql)
        # 根据数据源模式执行

        if StringUtil.eq_ignore(self.source_data_mode,
                                SourceDataMode.ALL.value):
            # 直接入库
            self.load_data1()
        elif StringUtil.eq_ignore(self.source_data_mode,
                                  SourceDataMode.ADD.value):
            # 增求全转换入库
            self.load_data2()
        # self.update_delete_flg() # 更新删除状态

    def load_data1(self):
        """
         直接入库
        :return:
        """
        hql = ("  INSERT INTO TABLE \n"
               "  {DB_NAME}.{TABLE_NAME} {PARTITION} \n"
               "  SELECT '{DATA_DATE}',".
               format(SOURCE_DB=self.source_db,
                      SOURCE_TABEL_NAME=self.source_table if self.account_table_name is None else self.account_table_name,
                      DB_NAME=self.db_name,
                      TABLE_NAME=self.table_name,
                      PARTITION=self.create_partition_sql(self.data_range,
                                                          self.date_scope,
                                                          self.org),
                      DATA_DATE=self.data_date
                      ))
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + " '{org}',".format(org=self.org)
        hql = hql + self.build_load_column_sql(None, True)

        hql = hql + " FROM {source_db_name}.{source_table_name} S \n ".format(
            source_db_name=self.source_db,
            source_table_name=self.source_table if self.account_table_name is None else self.account_table_name)

        if not StringUtil.is_blank(self.filter_sql):
            # 如果有过滤条件,加入过滤条件
            hql = hql + "\n  WHERE {filter_col}".format(
                filter_col=self.filter_sql)
        LOG.info("执行SQL: {0}".format(hql))
        self.hive_util.execute(hql)

    def load_data2(self):
        """
            增求全转换入库
        :return:
        """
        LOG.info("开始增量求全量转换入库》》》")

        # 全量表字段信息
        meta_info = self.hive_util.get_hive_meta_field(self.common_dict,
                                                       self.db_name,
                                                       self.table_name,
                                                       True)

        add_table_field_infos = None
        if self.table_add:
            add_table_field_infos = self.hive_util.get_hive_meta_field(
                self.common_dict,
                self.db_name_add,
                self.table_name_add,
                True)  # 增量表字段信息

        # 全量表字段信息
        hive_field_infos = meta_info
        pk_list = self.pk_list
        if pk_list:
            pk_list = pk_list.split("|")
            if len(pk_list) < 0:
                raise BizException(
                    "数据对象{obj} 主键不存在 ！".format(obj=self.obj))
            else:
                pk_list = [pk.upper() for pk in pk_list]  # 大写
        key_dict = {}  # 主键名/主键类型 字典
        # 获取主键字典
        for pk in pk_list:
            for field in meta_info:
                if StringUtil.eq_ignore(pk, field.col_name):
                    key_dict[pk] = field.data_type
                    break
        yes_day = DateUtil.get_day_of_day(self.data_date, -1)  # 前一天

        # 取最近一次全量归档的信息
        lastest_all_archive = (self.mon_run_log_service.
                               find_latest_all_archive(self.system,
                                                       self.table_name,
                                                       self.org, yes_day))

        # 判断最近一次归档是否在昨日
        has_yes_full_data = False
        if lastest_all_archive:
            biz_date = lastest_all_archive.BIZ_DATE
            LOG.debug("最近一次全量归档的日期是：{0}".format(biz_date))
            if StringUtil.eq_ignore(yes_day, biz_date):
                has_yes_full_data = True

        # 写入临时表
        if (not self.hive_util.exist_table(self.temp_db,
                                           self.app_table_name1) or
                not self.hive_util.compare(self.common_dict,
                                           self.db_name, self.table_name,
                                           self.temp_db,
                                           self.app_table_name1, False)):
            self.drop_table(self.temp_db, self.app_table_name1)

            hql = ("CREATE TABLE {temp_db}.{app_name} \n"
                   "  LIKE {db_name}.{table_name}".
                   format(temp_db=self.temp_db,
                          db_name=self.db_name,
                          app_name=self.app_table_name1,
                          table_name=self.table_name))

        else:
            hql = ("DELETE FROM  {temp_db}.{app_name} {partition_sql} \n"
                   "  WHERE 1=1 ".format(temp_db=self.temp_db,
                                         app_name=self.app_table_name1,
                                         partition_sql=self.create_partition_sql(
                                             self.data_range,
                                             "0",
                                             "0"
                                         )))

        LOG.info("执行SQL:{0}".format(hql))
        self.hive_util.execute(hql)
        if self.is_drop_tmp_table:
            self.is_drop_app_table = True
        # 入临时表SQL
        hql = (" INSERT INTO TABLE \n"
               "  {TEMP_DB}.{APP_TABLE} {PARTITION} \n"
               "  SELECT '{DATA_DATE}', ".
               format(SOURCE_DB=self.source_db,
                      SOURCE_TABLE=self.source_table if self.account_table_name is None else self.account_table_name,
                      TEMP_DB=self.temp_db,
                      APP_TABLE=self.app_table_name1,
                      PARTITION=self.create_partition_sql(self.data_range, "0",
                                                          "0"),
                      DATA_DATE=self.data_date
                      ))
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + "'{org}',".format(org=self.org)
        hql = hql + self.build_load_column_sql(None, True)

        hql = hql + " FROM {source_db_name}.{source_table_name} S \n ".format(
            source_db_name=self.source_db,
            source_table_name=self.source_table if self.account_table_name is None else self.account_table_name)

        if not StringUtil.is_blank(self.filter_sql):
            # 如果有过滤条件,加入过滤条件
            hql = hql + "\n  WHERE {filter_col}".format(
                filter_col=self.filter_sql)

        LOG.info("执行SQL :{0}".format(hql))
        self.hive_util.execute(hql)

        hql = "  FROM ( SELECT *, row_number() over (DISTRIBUTE BY  \n"
        # 按主键分组
        for key in key_dict:
            if str(key_dict.get(key)).lower() in ["string", "varchar",
                                                  "char"]:
                hql = hql + "  trim({key}),".format(key=key)
            else:
                hql = hql + key + ","

        hql = hql[:-1]
        # 按日期倒序、自定义分区顺序（全量分区-增量/减量分区-初始化分区）排序
        hql = hql + ("  SORT BY {COL_DATE} DESC ) as hds_section_rn FROM (  \n".
                     format(COL_DATE=self.col_date))

        if lastest_all_archive:
            hql = hql + "  SELECT {COL_DATE},".format(COL_DATE=self.col_date)
            # if self.org_pos == OrgPos.COLUMN.value:
            #     hql = hql + "{0},".format(self.col_org)
            hql = hql + ("{COLS} FROM {DB_NAME}.{TABLE_NAME} \n"
                         "  WHERE {WHERE_SQL} \n"
                         "  UNION ALL \n".
                         format(COLS=self.build_load_column_sql(None, False),
                                DB_NAME=self.db_name,
                                TABLE_NAME=self.table_name,
                                WHERE_SQL=self.create_where_sql("",
                                                                lastest_all_archive.
                                                                BIZ_DATE,
                                                                self.data_range,
                                                                self.date_scope,
                                                                self.org_pos,
                                                                self.org, None
                                                                )
                                ))
        # 当日增量
        hql = hql + "  SELECT {COL_DATE},".format(COL_DATE=self.col_date)
        # if self.org_pos == OrgPos.COLUMN.value:
        #     hql = hql + "{0},".format(self.col_org)
        hql = hql + ("  {COLS} FROM {TEMP_DB}.{APP_TABLE} \n".
                     format(COLS=self.build_load_column_sql(None, False),
                            TEMP_DB=self.temp_db,
                            APP_TABLE=self.app_table_name1))
        # 往日增量
        if self.table_add and not has_yes_full_data:

            hql = hql + ("  UNION ALL \n"
                         "  SELECT {COL_DATE},".format(COL_DATE=self.col_date))
            # if self.org_pos_add == OrgPos.COLUMN.value:
            #     hql = hql + " '{0}',".format(self.col_org)
            hql = hql + ("{COLS} FROM {TABLE_ADD} \n"
                         "  WHERE {COL_DATE} < '{DATA_DATE}' \n".
                         format(COL_DATE=self.col_date,
                                COLS=self.build_load_column_with_compare(
                                    add_table_field_infos,
                                    hive_field_infos,
                                    None, False),
                                TABLE_ADD=self.table_add,
                                DATA_DATE=self.data_date))
            if lastest_all_archive:
                # 有全量数据
                hql = hql + ("  AND {COL_DATE} > '{ALL_DATE}' \n".
                             format(COL_DATE=self.col_date,
                                    ALL_DATE=lastest_all_archive.BIZ_DATE))
                # 拼接分区范围
                date_scope = None
                col_org = None

                if StringUtil.eq_ignore(self.data_range_add,
                                        DatePartitionRange.MONTH.value):
                    date_scope = str(lastest_all_archive.BIZ_DATE)[0:6]

                elif StringUtil.eq_ignore(self.data_range_add,
                                          DatePartitionRange.QUARTER_YEAR.value):
                    date_scope = DateUtil.get_quarter(
                        lastest_all_archive.BIZ_DATE)

                elif StringUtil.eq_ignore(self.data_range_add,
                                          DatePartitionRange.YEAR.value):
                    date_scope = str(lastest_all_archive.BIZ_DATE)[0:4]

                if date_scope:
                    hql = (hql + "  AND {partition_date_scope} "
                                 "<= '{date_scope1}' and "
                                 "{partition_date_scope} "
                                 " >= '{date_scope2}' ".
                           format(
                        partition_date_scope=self.partition_data_scope,
                        date_scope1=self.date_scope,
                        date_scope2=date_scope
                    ))
                # 确定 ADD表的机构字段 位置
                if self.org_pos_add == OrgPos.COLUMN.value:
                    col_org = self.col_org

                elif self.org_pos_add == OrgPos.PARTITION.value:
                    col_org = self.partition_org

                if col_org:
                    hql = hql + "  AND {0} = '{1}' ".format(col_org,
                                                            self.org)

        hql = hql + ("  ) hds_section_t1 \n"
                     "  ) hds_section_t2 \n"
                     "  INSERT INTO TABLE {db_name}.{table_name} \n"
                     "  {partition} SELECT '{data_date}',".
                     format(db_name=self.db_name,
                            table_name=self.table_name,
                            partition=self.create_partition_sql(self.data_range,
                                                                self.date_scope,
                                                                self.org),
                            data_date=self.data_date
                            ))

        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + " '{org}', ".format(org=self.org)

        hql = hql + self.build_load_column_sql(None, False) \
              + "\n  WHERE hds_section_rn=1"
        LOG.info("执行SQL:{0} ".format(hql))
        self.hive_util.execute(hql)

    # def update_delete_flg(self):
    #     """
    #         更新减量数据的标记
    #     :return:
    #     """
    #     # 更新减量数据,将减量数据的删除标志改为1
    #     yes_day = DateUtil.get_day_of_day(self.data_date, -1)  # 前一天
    #     lastest_all_archive = (self.mon_run_log_service.
    #                            find_latest_all_archive(self.system,
    #                                                    self.table_name,
    #                                                    self.org, yes_day))
    #
    #     pk_list = None
    #     if isinstance(self.pk_list, str):
    #         pk_list = self.pk_list.split("|")
    #     elif isinstance(self.pk_list, list):
    #         pk_list = pk_list
    #     i = 0
    #     sql = ""
    #     for pk in pk_list:
    #         if i == 0:
    #             sql = " {pk} not in (select {pk} from {db_name}.{table_name} where {date_col}='{data_date}') ". \
    #                 format(pk=pk,
    #                        db_name=self.db_name,
    #                        table_name=self.table_name,
    #                        date_col=self.col_date,
    #                        data_date=self.data_date
    #                        )
    #         else:
    #             sql = sql + " and {pk} not in (select {pk} from {db_name}.{table_name} where {date_col}='{data_date}') "
    #         i = i + 1
    #     if lastest_all_archive:
    #         #  如果之前有全量归档则更新最近一起全量数据的删除标记，删除日期
    #         last_archive_date = lastest_all_archive.BIZ_DATE
    #         hql = "UPDATE  {db_name}.{table_name} {partition_sql} set ({DELETE_FLG}= '1',{DELETE_DT}='{DATA_DATE}') " \
    #               "where  {date_col} = '{yes_date}' and  {not_in_sql} ".format(db_name=self.db_name,
    #                                                                            table_name=self.table_name,
    #                                                                            partition_sql=self.create_partition_sql(
    #                                                                                self.data_range, "0", "0"),
    #                                                                            DELETE_FLG=self._DELETE_FLG,
    #                                                                            DELETE_DT=self._DELETE_DT,
    #                                                                            DATA_DATE=self.data_date,
    #                                                                            date_col=self.col_date,
    #                                                                            yes_date=last_archive_date,
    #                                                                            not_in_sql=sql)
    #         LOG.info("执行SQL{0}".format(hql))
    #         self.hive_util.execute(hql)


class ChainTransArchive(ArchiveData):
    """
        拉链表归档按月、季度、年分区、不分区 允许出错重跑
    """

    def print_save_mode(self):
        LOG.info("》》》》》》执行拉链归档》》》》》")

    # 是否封链
    __is_close_chain = False
    # 闭链后下个分区值
    __next_date_scope = None
    # 归档后一天的日期
    __next_date = None
    log_head = ""
    columns = None  # 不需要做拉链的字段集合
    is_drop_table1 = False  # 是否删除app_table
    CHAIN_OPEN_DATE = "99991231"

    def __init__(self):
        super(ChainTransArchive, self).__init__()

        self.__chain_sdate = self.common_dict.get(AddColumn.CHAIN_SDATE.value)
        self.__chain_edate = self.common_dict.get(AddColumn.CHAIN_EDATE.value)

        self.print_save_mode()
        # 拉链表不做比较的字段

    @property
    def not_compare_column(self):
        not_compare_column = self.common_dict.get("chain.notcompare.column")
        return not_compare_column

    def clean(self):
        super(ChainTransArchive, self).clean()
        if self.is_already_load:
            if self.is_drop_table1:
                self.drop_table(self.temp_db, self.app_table_name1)

    def init_ext(self):
        pk_list = self.pk_list
        if not pk_list or StringUtil.is_blank(pk_list):
            raise BizException("归档表{db_name}.{table_name}的主键不存在 ！".format(
                db_name=self.db_name,
                table_name=self.table_name))
        self.pk_list = [pk.upper() for pk in pk_list.split("|")]

        if self.check_run_log("00000101", self.data_date):
            LOG.debug(" 存在历史归档数据 ")
            if not self.check_run_log(self.data_date, self.data_date):
                if not self.check_run_log(self.last_date, self.last_date):
                    raise BizException("前一天没有做归档,不能做全量拉链归档 ")
        else:
            self.source_data_mode = SourceDataMode.ADD.value
            LOG.info(
                "归档表{db_name}.{table_name} 日期：{data_date} "
                "以前未归档，归档模式按照 :{data_mode}  "
                    .format(db_name=self.db_name,
                            table_name=self.table_name,
                            data_date=self.data_date,
                            data_mode=self.source_data_mode))
        self.log_head = ("结构化数据归档[表{db_name}.{table_name} "
                         "机构：{org} 日期：{data_date} "
                         "数据源模式:{data_mode} 归档方式：{save_mode} "
                         .format(db_name=self.db_name,
                                 table_name=self.table_name,
                                 org=self.org,
                                 data_date=self.data_date,
                                 data_mode=self.source_data_mode,
                                 save_mode=self.save_mode
                                 ))
        #  分区值和封链判断
        if StringUtil.eq_ignore(DatePartitionRange.MONTH.value,
                                self.data_range):
            if self.end_date.__eq__(self.data_date):
                self.__is_close_chain = True
                self.__next_date = DateUtil.get_day_of_day(self.data_date, 1)
                self.__next_date_scope = self.__next_date[0:6]
        if StringUtil.eq_ignore(DatePartitionRange.QUARTER_YEAR.value,
                                self.data_range):
            if self.end_date.__eq__(self.data_date):
                self.__is_close_chain = True
                self.__next_date = DateUtil.get_day_of_day(self.data_date, 1)
                self.__next_date_scope = DateUtil.get_quarter(self.__next_date)

        if StringUtil.eq_ignore(DatePartitionRange.YEAR.value,
                                self.data_range):
            if self.end_date.__eq__(self.data_date):
                self.__is_close_chain = True
                self.__next_date = DateUtil.get_day_of_day(self.data_date, 1)
                self.__next_date_scope = self.__next_date[0:4]

        # 不需要做拉链的字段
        if not StringUtil.is_blank(self.not_compare_column):
            self.columns = self.not_compare_column.split(",")

    def count_archive_data(self):
        hql = "SELECT COUNT(1) FROM {source_db}.{table_name} ".format(
            source_db=self.source_db,
            table_name=self.source_table,
        )
        LOG.info("执行SQL：{0}".format(hql))
        if self.filter_sql:
            hql = hql + " where " + self.filter_sql
        r = self.hive_util.execute_sql(hql)
        count = int(r[0][0])
        LOG.info("入库条数为：{0}".format(count))
        return count

    def create_table(self, db_name, table_name):
        hql = ("CREATE TABLE IF NOT EXISTS {db_name}.{table_name} "
               "  ({chain_sdate} varchar(10),{chain_edate} varchar(10), ".
               format(db_name=db_name,
                      table_name=table_name,
                      chain_sdate=self.__chain_sdate,
                      chain_edate=self.__chain_edate
                      ))
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + "{col_org} string ,".format(
                col_org=self.col_org)
        hql = hql + self.create_table_body(False) + ")"
        tmp_buf = ""
        if not StringUtil.eq_ignore(self.data_range,
                                    DatePartitionRange.ALL_IN_ONE.value):
            tmp_buf = self.partition_data_scope + " string,"
        if self.org_pos == OrgPos.PARTITION.value:
            tmp_buf = tmp_buf + ("{partion_org} string,".
                                 format(partion_org=self.partition_org))
        if tmp_buf.__len__() > 0:
            hql = hql + "partitioned by (" + tmp_buf[:-1] + ")"

        hql = hql + ("clustered by ({clusterCol}) into {bucketsNum} "
                     "BUCKETS  STORED AS orc "
                     "tblproperties('orc.compress'='SNAPPY' ,"
                     "'transactional'='true')".
                     format(clusterCol=self.cluster_col,
                            bucketsNum=self.buckets_num))

        LOG.info("执行建表语句 {0}".format(hql))
        self.hive_util.execute(hql)

    def load_data(self):
        LOG.debug("source_data_mode:{0}".format(self.source_data_mode))

        # 判断是否需要创建临时表
        if len(self.account_list) > 0:
            self.create_temp_table_for_account()  # 建表，导数据
            # 更新source_ddl
            self.source_ddl = (self.meta_data_service.
                               parse_input_table(self.hive_util,
                                                 self.source_db,
                                                 self.account_table_name,
                                                 self.filter_cols,
                                                 True
                                                 ))
        if self.source_data_mode == SourceDataMode.ALL.value:
            LOG.info("---全量拉链---")
            self.load_data_trans_all()
        else:
            LOG.info("---增量拉链---")
            self.load_data_trans_add()

        # 是否封链
        if self.__is_close_chain:
            self.close_trans_chain()

    def load_data_trans_all(self):
        LOG.debug("----------------拉链表归档--------------")

        # 更新闭链区的数据
        # 避免重复
        hql = ("UPDATE  {db_name}.{table_name} {partition_sql} \n"
               "  SET {chain_edate} = '{chain_open_date}'  \n"
               "  WHERE \n"
               .format(db_name=self.db_name,
                       table_name=self.table_name,
                       partition_sql=self.create_partition_sql(
                           self.data_range,
                           self.date_scope,
                           self.org
                       ),
                       chain_edate=self.__chain_edate,
                       chain_open_date=self.CHAIN_OPEN_DATE

                       ))
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + "{col_org} = '{org}' AND \n".format(
                col_org=self.col_org,
                org=self.org
            )
        hql = hql + self.__chain_edate + "= '{0}'".format(self.data_date)
        LOG.info("执行SQL: {0}".format(hql))
        self.hive_util.execute(hql)

        LOG.debug("先删除开区间当天的数据开链日期 ")
        # 避免与当日数据重复
        hql = ("DELETE FROM {db_name}.{table_name} {partition_sql} \n"
               "  WHERE ".format(db_name=self.db_name,
                                 table_name=self.table_name,
                                 partition_sql=self.create_partition_sql(
                                     self.data_range,
                                     self.date_scope,
                                     self.org
                                 )))
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + " {col_org} = '{org}' AND ".format(
                col_org=self.col_org,
                org=self.org
            )
        hql = hql + self.__chain_sdate + "= '{0}'".format(self.data_date)
        LOG.info("执行SQL {0}".format(hql))
        self.hive_util.execute(hql)

        # 存入临时表
        if (not self.hive_util.exist_table(self.temp_db,
                                           self.app_table_name1) or
                not self.hive_util.compare(self.common_dict,
                                           self.db_name, self.table_name,
                                           self.temp_db,
                                           self.app_table_name1, False)):
            # drop 表
            self.drop_table(self.temp_db, self.app_table_name1)
            hql = ("CREATE TABLE {temp_db}.{app_name} \n"
                   "  LIKE {db_name}.{table_name}".
                   format(temp_db=self.temp_db,
                          db_name=self.db_name,
                          app_name=self.app_table_name1,
                          table_name=self.table_name))

        else:
            hql = ("DELETE FROM  {temp_db}.{app_name} {partition_sql} \n"
                   "  WHERE 1=1 ".format(temp_db=self.temp_db,
                                         app_name=self.app_table_name1,
                                         partition_sql=self.create_partition_sql(
                                             self.data_range,
                                             "0",
                                             "0"
                                         )))
        LOG.info("执行SQL {0}".format(hql))
        self.hive_util.execute(hql)
        if self.is_drop_tmp_table:
            self.is_drop_table1 = True

        LOG.debug("1、通过主键关联将变化量插入临时分区")
        hql = (
            "INSERT INTO TABLE  {temp_db}.{app_table_name} {partition_sql} \n"
            "  SELECT A.{chain_sdate},'{data_date}',"
                .format(temp_db=self.temp_db,
                        app_table_name=self.app_table_name1,
                        partition_sql=self.create_partition_sql(self.data_range,
                                                                self.date_scope,
                                                                self.org),
                        chain_sdate=self.__chain_sdate,
                        data_date=self.data_date
                        ))
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + "'{org}' ,".format(org=self.org)
        # 往日改变量
        hql1 = (" (SELECT * FROM {db_name}.{table_name} WHERE \n"
                "  {where_sql}  and {chain_edate} = '{chain_open_date}' ) A  \n"
                "  LEFT JOIN  {source_db}.{source_table}  B ON ( \n"
                .format(db_name=self.db_name,
                        table_name=self.table_name,
                        where_sql=self.create_where_sql(None, None,
                                                        self.data_range,
                                                        self.date_scope,
                                                        self.org_pos,
                                                        self.org,
                                                        None
                                                        ),
                        chain_edate=self.__chain_edate,
                        chain_open_date=self.CHAIN_OPEN_DATE,
                        source_db=self.source_db,
                        source_table=self.source_table if self.account_table_name is None else self.account_table_name
                        ))
        hql = hql + self.build_load_column_sql("A", False) + " \n FROM " + hql1

        # 主键关联
        hql = hql + self.build_key_sql_on("A", "B", self.pk_list) + (
            ") \n"
            "  WHERE CONCAT_WS('|',{col1}) != CONCAT_WS('|',{col2}) \n"
            "  UNION ALL  \n"
            "  SELECT '{data_date}','{CHAIN_OPENDATE}',"
                .format(col1=self.build_sql_column_with_not_compare("A"),
                        col2=self.build_sql_column_with_not_compare("B"),
                        data_date=self.data_date,
                        CHAIN_OPENDATE=self.CHAIN_OPEN_DATE))
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + "'{0}',".format(self.org)
        # 当日新增量
        hql = (hql + self.build_load_column_sql("A", False)
               + "\n FROM "
                 "  {source_db}.{source_table_name} A \n"
                 "  LEFT JOIN (SELECT * FROM {db_name}.{table_name} \n"
                 "  WHERE {where_sql} \n"
                 "  AND {chain_edate} ='{chain_open_date}' ) B \n"
                 "  ON  ({on_key}) \n"
                 "  WHERE  CONCAT_WS('|',{col1}) != CONCAT_WS('|',{col2}) "
               .format(
                    source_db=self.source_db,
                    source_table_name=self.source_table if self.account_table_name is None else self.account_table_name,
                    db_name=self.db_name,
                    table_name=self.table_name,
                    where_sql=self.create_where_sql(None,
                                                    None,
                                                    self.data_range,
                                                    self.date_scope,
                                                    self.org_pos,
                                                    self.org,
                                                    None),
                    chain_edate=self.__chain_edate,
                    chain_open_date=self.CHAIN_OPEN_DATE,
                    on_key=self.build_key_sql_on("A", "B", self.pk_list),
                    col1=self.build_sql_column_with_not_compare("A"),
                    col2=self.build_sql_column_with_not_compare("B")
                ))

        LOG.info("执行SQL :{0}".format(hql))
        self.hive_util.execute(hql)

        LOG.debug("2、通过临时表主键（闭链为当天）删除正式表闭链时间99991231中数据")
        #  因为临时表中已经存在了这部分数据 所以需要将旧数据删除
        #  删除旧数据
        hql = ("DELETE FROM {db_name}.{table_name} {partition_sql} AS A \n"
               "  WHERE {where_sql} \n"
               "  AND EXISTS \n"
               "  (SELECT * FROM {temp_db}.{app_table} AS B \n"
               "   WHERE {key_list} AND {chain_edate} ='{data_date}' ) \n"
               "   AND {chain_edate} = '{chain_open_date}' "
               .format(db_name=self.db_name,
                       table_name=self.table_name,
                       partition_sql=self.create_partition_sql(
                           self.data_range,
                           self.date_scope,
                           self.org
                       ),
                       where_sql=self.create_where_sql("A", None,
                                                       self.data_range,
                                                       self.date_scope,
                                                       self.org_pos,
                                                       self.org,
                                                       None
                                                       ),
                       temp_db=self.temp_db,
                       app_table=self.app_table_name1,
                       key_list=self.build_key_sql_on("B", "A", self.pk_list),
                       chain_edate=self.__chain_edate,
                       data_date=self.data_date,
                       chain_open_date=self.CHAIN_OPEN_DATE
                       ))

        LOG.info("执行SQL: {0}".format(hql))
        self.hive_util.execute(hql)

        LOG.debug("3、临时分区数据插入插入正式分区")
        hql = ("INSERT INTO TABLE  {db_name}.{table_name} {partition_sql} \n"
               "  SELECT {chain_sdate},{chain_edate}, ".
               format(db_name=self.db_name,
                      table_name=self.table_name,
                      partition_sql=self.create_partition_sql(self.data_range,
                                                              self.date_scope,
                                                              self.org),
                      chain_sdate=self.__chain_sdate,
                      chain_edate=self.__chain_edate
                      ))
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + "'{0}', ".format(self.org)
        hql = (hql + self.build_load_column_sql(None, False) + "\n" +
               "  FROM {temp_db}.{app_table}".format(temp_db=self.temp_db,
                                                     app_table=self.app_table_name1))
        if not StringUtil.is_blank(self.filter_sql):
            # 如果有过滤条件,加入过滤条件
            hql = hql + "\n" \
                        "  WHERE {filter_col}".format(
                filter_col=self.filter_sql)
        LOG.info("执行SQL: {0}".format(hql))
        self.hive_util.execute(hql)
        LOG.debug("----------------拉链表归档完成--------------")

    def load_data_trans_add(self):
        LOG.debug("----------------拉链表归档--------------")

        # 更新当天的闭链区和删除当天的开链区的数据都是为了避免数据重复
        LOG.debug("更新当天闭链区的数据为99991231")
        #  把业务日期当日的数据设置为有效数据
        hql = ("UPDATE {db_name}.{table_name} {partition_sql} \n"
               "  SET {chain_edate} = '{chain_open_date}' \n"
               "  WHERE ".
               format(db_name=self.db_name,
                      table_name=self.table_name,
                      partition_sql=self.create_partition_sql(self.data_range,
                                                              self.date_scope,
                                                              self.org),
                      chain_edate=self.__chain_edate,
                      chain_open_date=self.CHAIN_OPEN_DATE
                      ))
        # 判断机构字段位置
        if self.org_pos == OrgPos.COLUMN.value:
            hql = (hql + "{col_org} = '{org}' AND ".
                   format(col_org=self.col_org,
                          org=self.org))
        hql = (hql + "{chain_edate} = '{data_date}' ".
               format(chain_edate=self.__chain_edate,
                      data_date=self.data_date))
        LOG.info("执行SQL :{0}".format(hql))
        self.hive_util.execute(hql)

        LOG.debug("先删除开区间当天的数据开链日期")
        # 避免数据重复
        hql = ("DELETE FROM {db_name}.{table_name} {partition_sql} \n"
               "  WHERE ".format(db_name=self.db_name,
                                 table_name=self.table_name,
                                 partition_sql=self.create_partition_sql(
                                     self.data_range,
                                     self.date_scope,
                                     self.org)
                                 ))
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + "  {col_org} = '{org}' \n" \
                        "  AND ".format(col_org=self.col_org,
                                        org=self.org)

        hql = (hql + "  {chain_sdate} = '{data_date}'".
               format(chain_sdate=self.__chain_sdate,
                      data_date=self.data_date))
        LOG.info("执行SQL :{0}".format(hql))
        # 当日重做 SQL
        self.hive_util.execute(hql)

        LOG.debug("写入闭区间")
        # 发生更新的数据 则将之前的数据写入闭区间
        hql = ("UPDATE {db_name}.{table_name} {partition_sql} AS A \n"
               "  SET {chain_edate} = '{data_date}'  \n"
               "  WHERE  {where_sql} AND EXISTS \n"
               "  (SELECT 1 FROM {source_db}.{source_table} AS B \n"
               "  WHERE {pk_sql}) AND \n"
               "  {chain_edate} = '{chain_open_date}' ".
               format(db_name=self.db_name,
                      table_name=self.table_name,
                      partition_sql=self.create_partition_sql(self.data_range,
                                                              self.date_scope,
                                                              self.org),
                      chain_edate=self.__chain_edate,
                      data_date=self.data_date,
                      where_sql=self.create_where_sql("A", None,
                                                      self.data_range,
                                                      self.date_scope,
                                                      self.org_pos,
                                                      self.org, None),
                      source_db=self.source_db,
                      source_table=self.source_table if self.account_table_name is None else self.account_table_name,
                      pk_sql=self.build_key_sql_on("A", "B", self.pk_list),
                      chain_open_date=self.CHAIN_OPEN_DATE
                      ))
        LOG.info("执行SQL {0}".format(hql))
        self.hive_util.execute(hql)
        LOG.debug("写入开区间")
        # 直接将新增数据写入
        hql = ("INSERT INTO TABLE {db_name}.{table_name} {partition_sql} \n"
               "  SELECT '{data_date}','{chain_open_date}', ".
               format(db_name=self.db_name,
                      table_name=self.table_name,
                      partition_sql=self.create_partition_sql(self.data_range,
                                                              self.date_scope,
                                                              self.org),
                      data_date=self.data_date,
                      chain_open_date=self.CHAIN_OPEN_DATE
                      ))
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + "'{0}', ".format(self.org)
        hql = hql + self.build_load_column_sql(None, False)

        # if len(self.account_list) > 0:
        #     hql = str(hql + self.case_when_acct_no())[:-1]

        hql = hql + " FROM {source_db_name}.{source_table_name} S \n ".format(
            source_db_name=self.source_db,
            source_table_name=self.source_table if self.account_table_name is None else self.account_table_name)

        # if len(self.account_list) > 0:
        #     hql = hql + self.left_join_acct_no()

        if not StringUtil.is_blank(self.filter_sql):
            # 如果有过滤条件,加入过滤条件
            hql = hql + "\n  WHERE {filter_col}".format(
                filter_col=self.filter_sql)
        LOG.info("执行SQL :{0}".format(hql))
        self.hive_util.execute(hql)
        LOG.debug("----------------拉链表归档完成--------------")

    def close_trans_chain(self):
        LOG.info("------------------------封链开始----------------------------")
        LOG.info("先删除新分区数据")
        hql = ("DELETE FROM {db_name}.{table_name} {partition_sql} \n"
               "  WHERE ".format(db_name=self.db_name,
                                 table_name=self.table_name,
                                 partition_sql=self.create_partition_sql(
                                     self.data_range,
                                     self.__next_date_scope,
                                     self.org)
                                 ))
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + (" {col_org} = '{org}' AND \n".
                         format(col_org=self.col_org,
                                org=self.org))
        hql = hql + ("  {chain_sdate} >= '{data_date}' ".
                     format(chain_sdate=self.__chain_sdate,
                            data_date=self.data_date))

        LOG.info("执行SQL:{0}".format(hql))
        self.hive_util.execute(hql)

        LOG.info("并将99991231数据写入到下一分区中")
        hql = ("INSERT INTO TABLE {db_name}.{table_name} {partiton} \n"
               "  SELECT '{data_date}','{chain_open_date}', ".
               format(db_name=self.db_name,
                      table_name=self.table_name,
                      partiton=self.create_partition_sql(self.data_range,
                                                         self.__next_date_scope,
                                                         self.org),
                      data_date=self.data_date,
                      chain_open_date=self.CHAIN_OPEN_DATE))

        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + "'{0}',".format(self.org)
        hql = hql + (self.build_load_column_sql(None, False) +
                     "\n  FROM {db_name}.{table_name} "
                     "\n  WHERE {where_sql} "
                     "\n  AND "
                     "  {chain_edate} = '{chain_open_date}'".
                     format(db_name=self.db_name,
                            table_name=self.table_name,
                            where_sql=self.create_where_sql(None, None,
                                                            self.data_range,
                                                            self.date_scope,
                                                            self.org_pos,
                                                            self.org,
                                                            None),
                            chain_edate=self.__chain_edate,
                            chain_open_date=self.CHAIN_OPEN_DATE
                            ))

        LOG.info("执行SQL:{0}".format(hql))
        self.hive_util.execute(hql)
        LOG.info("----------------------------封链完成------------------------")

    def build_sql_column_with_not_compare(self, table_alias):
        """
        构建加载SQL中列字段部分不包含字段字段
        :return:
        """
        sql = ""
        # 有DDL 变化
        if self.field_change_list:
            for field in self.field_change_list:
                is_exists = False
                for source_field in self.source_ddl:
                    if StringUtil.eq_ignore(field.col_name,
                                            source_field.col_name):
                        is_exists = True
                        break
                if is_exists:
                    if self.contain_column(field.col_name):
                        continue
                    if self.field_change_list.index(field) == 0:
                        sql = sql + self.build_column1(table_alias,
                                                       field.col_name,
                                                       field.ddl_type)
                    else:
                        sql = sql + "," + self.build_column1(table_alias,
                                                             field.col_name,
                                                             field.ddl_type)
                else:
                    sql = sql + ",'' "

        else:
            for field in self.source_ddl:
                if self.contain_column(field.col_name):
                    continue
                field_type = MetaTypeInfo(field.data_type,
                                          field.col_length,
                                          field.col_scale)
                if self.source_ddl.index(field) == 0:
                    sql = sql + self.build_column1(table_alias, field.col_name,
                                                   field_type)
                else:
                    sql = sql + "," + self.build_column1(table_alias,
                                                         field.col_name,
                                                         field_type)

        return sql

    @staticmethod
    def build_column1(table_alias, col_name, col_type):
        if col_name[0].__eq__('`'):
            col_name = '`' + col_name + '`'
        ret = table_alias + '.' + col_name if not StringUtil.is_blank(
            table_alias) else col_name
        ret = ("nvl(CAST( {ret} as {col_type}  ),'')".
               format(ret=ret,
                      col_type=col_type.get_whole_type))
        return ret

    def contain_column(self, tmp_col):
        v = False
        if self.columns:
            for col in self.columns:
                if StringUtil.eq_ignore(tmp_col, col):
                    v = True
                    break
        return v


if __name__ == '__main__':
    a = ChainTransArchive()
    a.reload_data()
