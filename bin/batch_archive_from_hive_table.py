# -*- coding: UTF-8 -*-  

# Date Time     : 2019/1/17
# Write By      : adtec(ZENGYU)
# Function Desc : 归档批量初始化
# History       : 2019/1/17  ZENGYU     Create
# Remarks       :
import argparse
import os
import sys
import time

import traceback

reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append("{0}".format(os.environ["DIDP_HOME"]))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from archive.db_operator import CommonParamsDao, ProcessDao
from archive.archive_enum import AddColumn, DatePartitionRange, OrgPos, \
    PartitionKey
from archive.archive_util import HiveUtil, BizException, DateUtil, StringUtil
from archive.model import DidpMonRunLog, DidpMonRunLogHis
from archive.service import MetaDataService, MonRunLogService
from utils.didp_logger import Logger

LOG = Logger()


class BatchArchiveInit(object):
    """
        批量初始化
    """
    _DELETE_FLG = "DELETE_FLG"
    _DELETE_DT = "DELETE_DT"

    def __init__(self):

        self.__args = self.archive_init()  # 参数初始化
        self.session = self.get_session()
        self.__print_argument()
        self.pro_start_date = DateUtil.get_now_date_standy()
        self.schema_id = self.__args.schID
        self.hive_util = HiveUtil(self.schema_id)
        self.meta_data_service = MetaDataService(self.session)
        self.mon_run_log_service = MonRunLogService(self.session)
        self.source_db = self.__args.sDb  # 源库名
        self.source_table_name = self.__args.sTable  # 源表名
        self.filter_cols = self.__args.filCol  # 过滤字段
        self.db_name = self.__args.db  # 入库数据库名
        self.table_name = self.__args.table  # 入库表表名

        self.bucket_num = self.__args.buckNum  # 分桶数
        # 公共参数字典
        self.common_dict = self.init_common_dict()

        # 数据源字段信息
        self.source_ddl = self.meta_data_service. \
            parse_input_table(self.hive_util,
                              self.source_db,
                              self.source_table_name,
                              self.filter_cols,
                              True
                              )

        # 日期字段名
        self.col_date = self.common_dict.get(AddColumn.COL_DATE.value)

        # 机构字段名
        self.col_org = self.common_dict.get(AddColumn.COL_ORG.value)
        self.org_pos = int(self.__args.orgPos)  # 机构字段位置
        self.data_range = self.__args.dtRange  # 日期分区范围
        self.cluster_col = self.__args.cluCol  # 分桶键
        # 机构分区字段名
        self.partition_org = self.common_dict.get(PartitionKey.ORG.value)
        self.partition_date_scope = self.common_dict.get(
            PartitionKey.DATE_SCOPE.value)  # 分区字段名
        self.date_col = self.__args.dateCol  # 日期字段
        self.date_format = self.__args.dateFm  # 日期字段格式
        if self.__args.igErr is None:
            self.__args.igErr = 0
        self.ignore_err_line = True if int(self.__args.igErr) == 1 else False  # 是否忽略错误行

        # 当日日期
        # %Y-%m-%d %H:%M:%S
        self.now_date = DateUtil.get_now_date_standy()

        self.obj = self.__args.obj  # 数据对象名
        self.org = self.__args.org  # 机构字段名
        self.pro_status = 0  # 处理状态默认为成功
        self.error_msg = None  # 错误信息
        self.system = self.__args.system
        self.source_count = 0
        self.archive_count = 0
        self.reject_lines = 0
        self.pro_info = ProcessDao(self.session).get_process_info(
            self.__args.proID)
        # 获取project_id
        self.project_id = self.pro_info.PROJECT_VERSION_ID if self.pro_info else None

    @staticmethod
    def get_session():
        """
         获取 sqlalchemy 的SESSION 会话
        :return:
        """

        user = os.environ["DIDP_CFG_DB_USER"]
        password = os.environ["DIDP_CFG_DB_PWD"]
        db_url = os.environ["DIDP_CFG_DB_JDBC_URL"]
        x = db_url.index("//")
        y = db_url.index("?")
        db_url = db_url[x + 2:y]

        # db_name = db_login_info['db_name']

        engine_str = "mysql+mysqlconnector://{db_user}:{password}@{db_url}".format(
            db_user=user, password=password,
            db_url=db_url,
        )
        engine = create_engine(engine_str)
        Session = sessionmaker(bind=engine)
        session = Session()
        return session

    def init_common_dict(self):
        common_dict = CommonParamsDao(self.session).get_all_common_code()
        if len(common_dict) == 0:
            raise BizException("初始化公共代码失败！请检查数据库")
        else:
            return common_dict

    def __print_argument(self):
        LOG.debug("批量初始化归档")
        LOG.debug("-------------------参数清单-------------------")
        LOG.debug("数据对象名       : {0}".format(self.__args.obj))
        LOG.debug("SCHEMA ID       : {0}".format(self.__args.schID))
        LOG.debug("流程ID       : {0}".format(self.__args.proID))
        LOG.debug("系统标识       : {0}".format(self.__args.system))
        LOG.debug("批次号       : {0}".format(self.__args.batch))
        LOG.debug("机构号           : {0}".format(self.__args.org))
        LOG.debug("日期字段           : {0}".format(self.__args.dateCol))
        LOG.debug("日期格式           : {0}".format(self.__args.dateFm))
        LOG.debug("源库名           : {0}".format(self.__args.sDb))
        LOG.debug("源表名           : {0}".format(self.__args.sTable))
        LOG.debug("过滤条件         : {0}".format(self.__args.filSql))
        LOG.debug("过滤字段         : {0}".format(self.__args.filCol))
        LOG.debug("归档库名          : {0}".format(self.__args.db))
        LOG.debug("归档表名          : {0}".format(self.__args.table))
        LOG.debug("日期分区范围        : {0}".format(self.__args.dtRange))
        LOG.debug("机构字段位置        : {0}".format(self.__args.orgPos))
        LOG.debug("分桶键             : {0}".format(self.__args.cluCol))
        LOG.debug("分桶数             : {0}".format(self.__args.buckNum))
        LOG.debug("是否忽略错误行           : {0}".format(self.__args.igErr))
        LOG.debug("是否补记数据资产           : {0}".format(self.__args.asset))

        LOG.debug("----------------------------------------------")

    @staticmethod
    def archive_init():
        """
            参数初始化
        :return:
        """
        # 参数解析
        parser = argparse.ArgumentParser(description="归档批量初始化")

        parser.add_argument("-obj", required=True, help="数据对象名")
        parser.add_argument("-org", required=True, help="机构")

        parser.add_argument("-sDb", required=True, help="源库名")
        parser.add_argument("-sTable", required=True, help="源表名")
        parser.add_argument("-filSql", required=False,
                            help="采集过滤SQL条件（WHERE 后面部分）")
        parser.add_argument("-filCol", required=False, help="过滤字段")
        parser.add_argument("-dateCol", required=True, help="日期字段")
        parser.add_argument("-dateFm", required=True, help="日期字段格式")
        parser.add_argument("-schID", required=True, help="取连接信息")
        parser.add_argument("-proID", required=True, help="流程ID")
        parser.add_argument("-system", required=True, help="系统标识")
        parser.add_argument("-batch", required=True, help="批次号")
        parser.add_argument("-db", required=True, help="归档库名")
        parser.add_argument("-table", required=True, help="归档表名")

        parser.add_argument("-dtRange", required=True,
                            help="日期分区范围（N-不分区、M-月、Q-季、Y-年）")
        parser.add_argument("-orgPos", required=True, type=int,
                            help="机构字段位置（1-没有机构字段 "
                                 "2-字段在列中 3-字段在分区中）")
        parser.add_argument("-cluCol", required=True, help="分桶键")
        parser.add_argument("-buckNum", required=True, help="分桶数")
        parser.add_argument("-igErr", required=False, type=int,
                            help="是否忽略错误行（0-否 1-是）")
        parser.add_argument("-asset", required=False, help="是否补记数据资产（0-否 1-是）")
        args = parser.parse_args()
        return args

    def run(self):
        try:
            LOG.info("接入表结构解析，元数据登记 ")
            self.process_ddl()

            LOG.info("统计日期，并对日期做合法性判断")
            self.count_date()
            LOG.info("查看是否已经做过批量初始化")
            self.check_log()
            LOG.info("开始归档 ")
            self.load()

            self.source_count = self.hive_util. \
                execute_sql("select count(1) from {source_db}.{source_table}".
                            format(source_db=self.source_db,
                                   source_table=self.source_table_name))[0][0]
            self.archive_count = self.hive_util. \
                execute_sql("select count(1) from {db_name}.{table_name} ".
                            format(db_name=self.db_name,
                                   table_name=self.table_name))[0][0]
            self.reject_lines = int(self.source_count) - int(self.archive_count)

            if self.reject_lines != 0 and not self.ignore_err_line:
                # 如果不忽视错误行 则抛出异常
                raise BizException("归档条数 ：{archive} 与入库条数 {source}:不一致 如需忽视错误行，"
                                   "请在参数中加入 -igErr 1 ".
                                   format(archive=int(self.archive_count),
                                          source=int(self.source_count)))
        except Exception as e:
            traceback.print_exc()
            self.error_msg = str(e.message)
            self.pro_status = 1
        finally:
            LOG.info("登记执行日志")
            data_date = DateUtil.get_now_date_format("%Y%m%d")
            old_log = self.mon_run_log_service.get_log(self.__args.proID,
                                                       data_date,
                                                       self.org,
                                                       self.__args.batch)
            LOG.debug("old_log :{0}".format(old_log))
            if old_log and self.pro_status == 0:
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
                    EXTENDED2=self.source_table_name,
                    ERR_MESSAGE=old_log.ERR_MESSAGE)
                self.mon_run_log_service.delete_log(self.__args.proID,
                                                    data_date,
                                                    self.org, self.__args.batch)
                # 写入历史表
                self.mon_run_log_service.insert_log_his(didp_mon_run_log_his)

            pro_end_date = DateUtil.get_now_date_standy()
            LOG.info("归档条数：{0}".format(int(self.archive_count)))
            run_log = DidpMonRunLog(PROCESS_ID=self.__args.proID,
                                    SYSTEM_KEY=self.__args.system,
                                    BRANCH_NO=self.org,
                                    BIZ_DATE=data_date,
                                    BATCH_NO=self.__args.batch,
                                    TABLE_NAME=self.table_name,
                                    DATA_OBJECT_NAME=self.obj,
                                    PROCESS_TYPE="5",  # 加工类型
                                    PROCESS_STARTTIME=self.pro_start_date,
                                    PROCESS_ENDTIME=pro_end_date,
                                    PROCESS_STATUS=self.pro_status,
                                    INPUT_LINES=int(self.source_count),
                                    OUTPUT_LINES=int(self.archive_count),
                                    REJECT_LINES=self.reject_lines,
                                    EXTENDED1="init",  # 记录归档类型
                                    EXTENDED2=self.source_table_name,
                                    ERR_MESSAGE=self.error_msg
                                    )
            self.mon_run_log_service.create_run_log(run_log)
            # 关闭连接
            if self.session:
                self.session.close()
            self.hive_util.close()
            if self.pro_status == 0:
                LOG.info("归档成功")
                exit(0)
            else:
                LOG.error("归档失败")
                exit(-1)

    def process_ddl(self):
        """
            处理ddl
        :return:
        """
        if len(self.source_ddl) == 0:
            raise BizException("接入数据不存在！ ")
        now_date = DateUtil().get_now_date_standy()
        source_table_comment = self.hive_util. \
            get_table_comment(self.source_db, self.source_table_name)
        if not self.hive_util.exist_table(self.db_name, self.table_name):
            LOG.info("创建归档表 ")
            self.create_table()
        # 登记元数据
        self.meta_data_service.upload_meta_data(self.schema_id,
                                                self.db_name,
                                                self.source_ddl,
                                                self.table_name,
                                                now_date,
                                                self.bucket_num,
                                                self.common_dict,
                                                source_table_comment,
                                                self.project_id,
                                                self.hive_util
                                                )
    def create_table(self):
        """
            创建归档表
        :return:
        """
        hql = "CREATE TABLE IF NOT EXISTS {db_name}.{table_name} ( \n" \
              " {col_date} VARCHAR(10) ,". \
            format(db_name=self.db_name,
                   table_name=self.table_name,
                   col_date=self.col_date
                   )
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
                date_scope=self.partition_date_scope)
        else:
            raise BizException("日期不分区的表，不需要初始化加载")
        if self.org_pos == OrgPos.PARTITION.value:
            part_sql = part_sql + "{col_org} string,".format(
                col_org=self.partition_org)
        # 若存在分区字段
        if len(part_sql) > 0:
            hql = hql + "  PARTITIONED BY ( " + part_sql[:-1] + ") \n"
        # 分桶
        hql = hql + ("  CLUSTERED BY ({CLUSTER_COL}) INTO {BUCKET_NUM} \n"
                     "  BUCKETS  STORED AS orc \n"
                     "tblproperties('orc.compress'='SNAPPY' ,"
                     "'transactional'='true')".
                     format(CLUSTER_COL=self.cluster_col,
                            BUCKET_NUM=self.bucket_num))
        LOG.info("执行SQL: {0}".format(hql))
        self.hive_util.execute(hql)

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
        # 增加删除标记和删除时间
        sql = sql + " DELETE_FLG VARCHAR(1),DELETE_DT VARCHAR(8) ,"
        return sql[:-1]

    def count_date(self):
        """
            检查日期字段的合法性
        :return:
        """
        date_dict = {}
        is_contain_error = False
        date = ""
        hql = (
            "  SELECT  from_unixtime(unix_timestamp(`{date_col}`,'{date_format}'),"
            "'yyyyMMdd') AS col_date,count(1) \n"
            "  FROM\n"
            "    {source_db}.{source_table_name} \n"
            "  GROUP BY\n"
            "    from_unixtime(unix_timestamp(`{date_col}`,'{date_format}'),'yyyyMMdd') \n "
            "  ORDER BY \n"
            "    col_date ".format(date_col=self.date_col,
                                   date_format=self.date_format,
                                   source_db=self.source_db,
                                   source_table_name=self.source_table_name))
        LOG.debug("执行SQL {0}".format(hql))
        result = self.hive_util.execute_sql(hql)
        for x in result:
            date_str = x[0]
            count = x[1]
            LOG.debug("数据日期：{0}, 数据条数： {1}".format(date, count))
            try:
                date = time.strptime(date_str, "%Y%m%d")
            except Exception as e:
                LOG.debug("非法日期 ：{0}".format(date))
                is_contain_error = True
                continue
            now_date = DateUtil.get_now_date_format("%Y%m%d")

            if date_str > now_date:
                LOG.debug("日期大于等于今天不合法: {0}".format(date_str))
                is_contain_error = True
                continue
            date_dict[date_str] = count

        if not self.ignore_err_line and is_contain_error:
            raise BizException("数据不合法。如需忽略错误行请调用时采用参数 -igErr 1 ")

    def check_log(self):
        """
            查看是否需要继续再做批量初始化
        :return:
        """
        date = DateUtil.get_now_date_format("%Y%m%d")
        run_log = self.mon_run_log_service.find_log_with_table(self.system,
                                                               self.obj,
                                                               self.source_table_name,
                                                               self.org,
                                                               "00000101",
                                                               date
                                                               )
        if run_log:
            raise BizException("{0} 已有归档，不能做批量初始化".format(run_log.BIZ_DATE))

    def load(self):
        """
            加载数据
        :return:
        """
        hql = "  FROM {source_db}.{source_table} " \
              "  INSERT INTO  TABLE \n" \
              "    {db_name}.{table_name} \n" \
              "  {partition} \n" \
              "  SELECT \n" \
              "    from_unixtime(unix_timestamp(`{date_col}`,'{date_col_format}')," \
              "'yyyyMMdd') AS {col_date}, " \
            .format(source_db=self.source_db,
                    source_table=self.source_table_name,
                    db_name=self.db_name,
                    table_name=self.table_name,
                    partition=self.create_partiton_sql(),
                    date_col=self.date_col,
                    date_col_format=self.date_format,
                    col_date=self.col_date
                    )
        if self.org_pos == OrgPos.COLUMN.value:
            hql = hql + " '{org}',".format(org=self.org)
        hql = hql + self.build_load_column_sql(None, False) + ","

        def switch_data_range(data_range):
            """
                通过日期分区范围 获取分区值
            :param data_range:
            :return:
            """
            time_str = "from_unixtime(unix_timestamp(`{date_col}`," \
                       "'{date_format}'),'yyyyMMdd')".format(
                date_col=self.date_col,
                date_format=self.date_format)
            return {
                DatePartitionRange.MONTH.value:
                    " substr({time_str},1,6) as {partition_date_scope},".
                        format(time_str=time_str,
                               partition_date_scope=self.partition_date_scope),
                DatePartitionRange.YEAR.value:
                    " substr({time_str},1,4) as {partition_date_scope},".
                        format(time_str=time_str,
                               partition_date_scope=self.partition_date_scope),
                DatePartitionRange.QUARTER_YEAR.value:
                    "(CASE  WHEN \n"
                    "  substr({time_str},5,2)>=01 AND \n"
                    "  substr({time_str},5,2) <=03 \n"
                    " THEN  \n"
                    "   concat(substr({time_str},1,4),'Q1') \n"
                    " WHEN \n"
                    "  substr({time_str},5,2)>=04 AND \n"
                    "  substr({time_str},5,2) <=06 \n"
                    " THEN \n"
                    "  concat(substr({time_str},1,4),'Q2') \n"
                    " WHEN \n "
                    "  substr({time_str},5,2)>=07 AND \n"
                    "  substr({time_str},5,2) <=09 \n"
                    " THEN \n"
                    "  concat(substr({time_str},1,4),'Q3') \n"
                    " WHEN \n"
                    "  substr({time_str},5,2)>=10 AND \n"
                    "  substr({time_str},5,2) <=12 \n"
                    " THEN \n"
                    "  concat(substr({time_str},1,4),'Q4') \n"
                    " END ) AS {partition_date_scope}  ,".
                        format(time_str=time_str,
                               partition_date_scope=self.partition_date_scope
                               )

            }.get(data_range)

        hql = hql + switch_data_range(self.data_range)
        if self.org_pos == OrgPos.PARTITION.value:
            hql = hql + " '{org}' AS {partition_org},". \
                format(org=self.org,
                       partition_org=self.partition_org)
        LOG.info("执行SQL:{0}".format(hql[:-1]))
        self.hive_util.execute_with_dynamic(hql[:-1])

    def build_load_column_sql(self, table_alias, need_trim):
        """
            构建column字段sql
        :param table_alias: 表别名
        :param need_trim:
        :return:
        """
        sql = ""
        for field in self.source_ddl:
            if self.source_ddl.index(field) == 0:
                sql = sql + self.build_column(table_alias, field.col_name,
                                              field.data_type,
                                              need_trim)
            else:
                sql = sql + "," + self.build_column(table_alias,
                                                    field.col_name,
                                                    field.data_type,
                                                    need_trim)
        sql = sql + " ,'0' as {DELETE_FLG} ,null as {DELETE_DT}".format(
            DELETE_FLG=table_alias + '.' + self._DELETE_FLG if not StringUtil.is_blank(
                table_alias) else self._DELETE_FLG,
            DELETE_DT=table_alias + '.' + self._DELETE_DT if not StringUtil.is_blank(table_alias) else self._DELETE_DT)
        return sql

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

    def create_partiton_sql(self):
        hql = ""
        if not StringUtil.eq_ignore(DatePartitionRange.ALL_IN_ONE,
                                    self.data_range):
            hql = hql + " {date_scope},".format(
                date_scope=self.partition_date_scope)
        if self.org_pos == OrgPos.PARTITION.value:
            hql = hql + "{partiton_org} ,".format(
                partiton_org=self.partition_org)

        return " PARTITION (" + hql[:-1] + ")"


if __name__ == '__main__':
    batch_init = BatchArchiveInit()
    batch_init.run()
