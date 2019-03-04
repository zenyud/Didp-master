# -*- coding: UTF-8 -*-  

# Date Time     : 2019/1/9
# Write By      : adtec(ZENGYU)
# Function Desc :  操作服务
# History       : 2019/1/9  ZENGYU     Create
# Remarks       :
import sys
import os

reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append("{0}".format(os.environ["DIDP_HOME"]))
from archive.archive_enum import CommentChange
from archive.archive_util import *
from archive.db_operator import *
from archive.hive_field_info import HiveFieldInfo, MetaTypeInfo
from utils.didp_logger import Logger

LOG = Logger()

last_update_time = DateUtil.get_now_date_standy()
LAST_UPDATE_USER = "SYSTEM"


class HdsStructControl(object):
    """
        归档控制
    """

    def __init__(self, session):
        self.archive_lock_dao = ArchiveLockDao(session)
        self.meta_lock_dao = MetaLockDao(session)

    def find_archive(self, obj, org):
        """
            查看是否有正在执行的归档任务
        :param obj: 归档对象
        :param org: 归档机构
        :return:
        """
        try:
            result = self.archive_lock_dao.find_by_pk(obj, org)
            if len(result) == 0:
                return None
            else:
                return result
        except Exception as e:
            LOG.exception(e.message)

    def archive_lock(self, obj, org):
        """
            对归档任务进行加锁
        :return:
        """
        self.archive_lock_dao.add(obj, org)

    def archive_unlock(self, obj, org):
        """
            归档任务解锁
        :param obj:
        :param org:
        :return:
        """
        self.archive_lock_dao.delete_by_pk(obj, org)

    def meta_lock_find(self, obj, org):
        """
            元数据锁查找
        :return:
        """
        r = self.meta_lock_dao.find_by_pk(obj, org)
        if len(r) == 0:
            return None
        else:
            return r

    def meta_lock(self, obj, org):
        """
            元数据加锁
        :return:
        """
        self.meta_lock_dao.add(obj, org)
        pass

    def meta_unlock(self, obj, org):
        """
                   解除元数据控制锁
               :return:
               """

        self.meta_lock_dao.delete_by_pk(obj, org)


class MetaDataService(object):
    """
        元数据操作类
    """

    just_delete_col = False
    type_change = False
    field_comment_change = False

    def __init__(self, session):
        self.session = session
        self.meta_table_info_his_dao = MetaTableInfoHisDao(self.session)
        self.meta_column_info_his_dao = MetaColumnInfoHisDao(self.session)
        self.meta_table_info_dao = MetaTableInfoDao(self.session)
        self.meta_column_info_dao = MetaColumnInfoDao(self.session)

    def get_meta_field_info_list(self, schema_id, table_name):
        # type: (str, str) -> list(HiveFieldInfo)
        """
            获取元数据字段信息 封装成Hive字段类型
        :param schema_id:
        :param table_name:
        :return: list(HiveFieldInfo)
        """
        # 表元数据信息
        meta_table_info = self.meta_table_info_dao.get_meta_table_info(
            schema_id,
            table_name)

        if meta_table_info:
            # 字段元数据信息
            meta_column_info_his = self.meta_column_info_dao. \
                get_meta_data_by_table(meta_table_info.TABLE_ID)

            # 转换成Hive_field_info 类型
            hive_field_infos = list()
            for field in meta_column_info_his:
                # 拼接完整字段类型
                full_type = field.COL_TYPE
                if field.COL_LENGTH and field.COL_SCALE:
                    full_type = full_type + "({col_len},{col_scale})".format(
                        col_len=field.COL_LENGTH,
                        col_scale=field.COL_SCALE)
                elif field.COL_LENGTH and not field.COL_SCALE:
                    full_type = full_type + "({col_len})".format(
                        col_len=field.COL_LENGTH)

                # 封装成HiveFieldInfo
                hive_field_info = HiveFieldInfo(field.COL_NAME,
                                                full_type,
                                                field.COL_DEFAULT,
                                                field.NULL_FLAG,
                                                "No",
                                                field.COL_DESC,
                                                field.COL_SEQ
                                                )
                hive_field_infos.append(hive_field_info)
            return hive_field_infos
        else:
            return None

    @staticmethod
    def parse_input_table(hive_util, db_name, table_name, filter_cols):
        # type: (HiveUtil, str, str, str) -> list(HiveFieldInfo)
        """
            解析接入表表结构
        :param db_name:
        :param table_name:
        :param filter_cols: 过滤字段 逗号分割
        :return:  list(HiveFieldInfo)
        """
        source_field_infos = []  # 获取接入数据字段列表

        # 字段信息
        cols = hive_util.get_table_desc(db_name, table_name)

        filter_col_list = None
        #  过滤不需要的字段
        if filter_cols:
            filter_col_list = [col.upper() for col in filter_cols.split(",")]

        i = 0  # 字段序号从0开始
        for col in cols:
            col_name = col[0]
            if StringUtil.eq_ignore("# Partition Information", col_name):
                break
            if filter_col_list:
            if filter_col_list:
                # 过滤过滤字段
                if col_name.upper() in filter_col_list:
                    continue

            filed_info = HiveFieldInfo(col[0], col[1], col[2], col[3], col[4],
                                       col[5], i)
            source_field_infos.append(filed_info)
            i = i + 1
        return source_field_infos

    def upload_meta_data(self, schema_id, db_name, source_ddl, table_name,
                         data_date, bucket_num,
                         common_dict, source_table_comment, project_id):
        # type: (str, str, str, str, str, str, dict,str, str) -> None

        """
            登记元数据

        :param schema_id:
        :param source_ddl: 源数据 ddl
        :param db_name: 目标库
        :param table_name: 目标表
        :param data_date: 执行日期
        :param bucket_num: 分桶数
        :param common_dict: 公共代码参数
        :param source_table_comment: 表备注
        :param project_id
        :return: void
        """
        # 检查当日是否已经登记元数据

        LOG.info("------------元数据登记检查------------")
        # 接入数据字段信息
        LOG.info("接入表信息解析")
        LOG.debug("---------------data_date is : ".format(data_date))
        source_field_info = source_ddl
        length = len(source_field_info)

        if length == 0:
            raise BizException("接入表信息解析失败！请检查接入表是否存在 ")
        LOG.info("接入表字段数为：{0}".format(length))

        # 取元数据信息
        meta_table_info = self.get_meta_table(schema_id, table_name)

        if meta_table_info:
            # 判断表结构是否发生变化，如果未发生变化 则进行元数据登记
            # 获取源DDL信息
            # 获取元数据DDL 信息
            table_id = meta_table_info.TABLE_ID
            meta_field_info = self.meta_column_info_dao.get_meta_data_by_table(
                table_id)  # 获取表字段元数据

            # 判断是否发生 表结构变更
            is_change = self.get_change_result(source_field_info,
                                               meta_field_info, common_dict)
            LOG.info("表结构是否发生变化：{0}".format(is_change))

            # 判断表备注是否发生变化
            table_comment_change = self.get_table_comment_change_result(
                source_table_comment, meta_table_info.TABLE_NAME_CN,
                common_dict)
            if not is_change and not table_comment_change:
                LOG.debug("当日表元数据已登记,无需再登记 ！")
                return
            else:
                # 需要变更元数据信息
                self.update_meta_info(table_id, schema_id, table_name,
                                      bucket_num,
                                      source_table_comment, data_date,
                                      source_field_info, meta_field_info,
                                      project_id)
        # 直接登记元数据
        else:
            self.register_meta_data(schema_id, source_field_info, table_name,
                                    bucket_num, source_table_comment, data_date,
                                    project_id)

    def register_meta_data(self, schema_id, source_field_info, table_name,
                           bucket_num, source_table_comment, data_date,
                           project_id):
        # type: (str, list(HiveFieldInfo), str, str, str, str, str) -> None
        """
            登记元数据信息
        :param schema_id:
        :param source_field_info: 接入字段信息
        :param table_name: 归档表名
        :param bucket_num: 分桶数
        :param source_table_comment: 接入表备注
        :param data_date: 数据日期
        :param project_id:  项目ID
        """

        # 当日未登记元数据 直接增加新 的元数据
        LOG.debug("---- 不存在元数据,登记新的元数据  ------ ")
        table_id = get_uuid()
        # 登记表元数据
        new_meta_table_info = DidpMetaTableInfo(
            TABLE_ID=table_id,
            SCHEMA_ID=schema_id,
            PROJECT_VERSION_ID=project_id,
            LAST_UPDATE_TIME=last_update_time,
            LAST_UPDATE_USER=LAST_UPDATE_USER,
            TABLE_NAME=table_name,
            BUCKET_NUM=bucket_num,
            TABLE_NAME_CN=source_table_comment,
            DESCRIPTION="表生成",
            RELEASE_DATE=data_date,
            TABLE_STATUS="2"
        )

        table_his_id = get_uuid()  # 表历史id

        new_meta_table_info_his = DidpMetaTableInfoHis(
            TABLE_HIS_ID=table_his_id,
            TABLE_ID=table_id,
            PROJECT_VERSION_ID=project_id,
            SCHEMA_ID=schema_id,
            LAST_UPDATE_TIME=last_update_time,
            LAST_UPDATE_USER=LAST_UPDATE_USER,
            TABLE_NAME=table_name,
            BUCKET_NUM=bucket_num,
            TABLE_NAME_CN=source_table_comment,
            DESCRIPTION="表生成",
            RELEASE_DATE=data_date,
            TABLE_STATUS="2"
        )
        # 写入表元数据表
        LOG.info("表元数据登记")
        self.meta_table_info_dao.add_meta_table_info(
            new_meta_table_info)
        LOG.info("表元数据登记成功！")
        # 写入表元数据历史表
        LOG.info("表历史元数据登记")
        self.meta_table_info_his_dao.add_meta_table_info_his(
            new_meta_table_info_his)
        LOG.info("表历史元数据登记成功 ！ ")
        # 登记字段元数据
        LOG.info("登记字段元数据 ")
        for filed in source_field_info:
            column_id = get_uuid()
            meta_field_info = DidpMetaColumnInfo(
                COLUMN_ID=column_id,
                TABLE_ID=table_id,
                PROJECT_VERSION_ID=project_id,
                LAST_UPDATE_TIME=last_update_time,
                LAST_UPDATE_USER=LAST_UPDATE_USER,
                COL_SEQ=filed.col_seq,
                COL_NAME=filed.col_name,
                COL_DESC=filed.comment,
                COL_TYPE=filed.data_type,
                COL_LENGTH=filed.col_length,
                COL_SCALE=filed.col_scale,
                COL_DEFAULT=filed.default_value,
                NULL_FLAG=filed.not_null)

            self.meta_column_info_dao.add_meta_column(meta_field_info)

            meta_field_info_his = DidpMetaColumnInfoHis(
                TABLE_HIS_ID=table_his_id,
                COLUMN_ID=column_id,
                PROJECT_VERSION_ID=project_id,
                TABLE_ID=table_id,
                LAST_UPDATE_TIME=last_update_time,
                LAST_UPDATE_USER=LAST_UPDATE_USER,
                COL_SEQ=filed.col_seq,
                COL_NAME=filed.col_name,
                COL_DESC=filed.comment,
                COL_TYPE=filed.data_type,
                COL_LENGTH=filed.col_length,
                COL_SCALE=filed.col_scale,
                COL_DEFAULT=filed.default_value,
                NULL_FLAG=filed.not_null
            )

            self.meta_column_info_his_dao.add_meta_column_his(
                meta_field_info_his)
        LOG.info("登记字段元数据成功 ！  ")

    def get_meta_table(self, schema_id, table_name):
        """
            获取表元数据信息
        :param schema_id:
        :param table_name: 表名
        :return:
        """
        return self.meta_table_info_dao.get_meta_table_info(schema_id,
                                                            table_name)

    def get_change_result(self, source_field_info, meta_field_info,
                          common_dict):
        """
            比较接入字段与元数据是否一致
        :param source_field_info: 接入字段对象集合
        :param meta_field_info: 字段元数据对象集合

        :return: True 有不一致字段
                False 无不一致字段
        """
        get_new_col = False
        len_lower = False  # 字段数是否变少
        if len(source_field_info) != len(meta_field_info):
            LOG.debug("-----字段数发生变化------")
            return True
        meta_field_names = [field.COL_NAME.strip().upper() for field in
                            meta_field_info]
        for i in range(0, len(source_field_info)):
            source_field = source_field_info[i]

            if source_field.col_name.upper() not in meta_field_names:
                # 判断接入字段是否存在于元数据表中
                LOG.debug("-------出现新增字段-------")
                return True
            else:
                for j in range(0, len(meta_field_info)):
                    if StringUtil.eq_ignore(meta_field_info[j].COL_NAME,
                                            source_field.col_name):
                        if not StringUtil.eq_ignore(meta_field_info[j].COL_TYPE,
                                                    source_field.data_type) or \
                                not StringUtil.eq_ignore(
                                    meta_field_info[j].COL_LENGTH,
                                    source_field.col_length) or \
                                not StringUtil.eq_ignore(
                                    meta_field_info[j].COL_SCALE,
                                    source_field.col_scale) or \
                                not StringUtil.eq_ignore(
                                    meta_field_info[j].COL_SEQ,
                                    source_field.col_seq):
                            LOG.debug("-----字段的精度发生了变化-------")
                            self.type_change = True
                            return True

                    # 判断字段备注改变是否增加新版本
                    comment_change = common_dict.get(
                        CommentChange.FIELD_COMMENT_CHANGE_DDL.value)

                    if comment_change.upper().__eq__("TRUE"):
                        comment1 = source_field.comment if source_field.comment else ""
                        comment2 = meta_field_info[j].COL_DESC if \
                            meta_field_info[j].COL_DESC else ""
                        if not comment1.__eq__(comment2):
                            return True

        return False

    @staticmethod
    def get_table_comment_change_result(source_table_comment,
                                        meta_table_comment, common_dict):
        """
            判断表描述是否相同
        :param source_table_comment 接入数据描述
        :param meta_table_comment: 元数据表描述

        :return: True： 不一致 False 一致
        """
        comment_change = common_dict.get(
            CommentChange.TABLE_COMMENT_CHANGE_DDL.value)
        if comment_change.upper().strip().__eq__("TRUE"):
            comment1 = source_table_comment
            comment2 = meta_table_comment if meta_table_comment else ""
            if not StringUtil.eq_ignore(comment1, comment2):
                LOG.debug("表的备注发生了变化 {0} -> {1}".format(comment2, comment1))
                return True
            else:
                return False
        else:
            return False

    def update_field_comment(self, entity_list, bean_list, comment_change):
        """
            更新字段备注
        :param entity_list: 接入字段数据对象集合
        :param bean_list: 字段元数据对象集合
        :param comment_change
        :return:
        """
        LOG.debug(
            "comment_change %s" % StringUtil.eq_ignore(comment_change, "true"))

        if StringUtil.eq_ignore(comment_change, "true"):
            return

        for bean in bean_list:
            for entity in entity_list:
                if StringUtil.eq_ignore(bean.COL_NAME, entity.col_name):

                    if bean.COL_DESC is None:
                        bean.COL_DESC = ""
                    if entity.comment is None:
                        entity.comment = ""

                    if not StringUtil.is_blank(
                            entity.comment) and not StringUtil.eq_ignore(
                        bean.COL_DESC, entity.comment):
                        LOG.debug("更新DDL备注，field = {0},comment = {1}".format(
                            bean.COL_NAME, entity.comment))
                        # 更新
                        self.meta_column_info_dao.update_meta_column(
                            bean.TABLE_ID,
                            bean.COL_NAME, {"COL_DESC": entity.comment})
                        self.meta_column_info_his_dao.update_meta_column_his(
                            bean.TABLE_ID,
                            bean.COL_NAME, {"COL_DESC": entity.comment}
                        )

    def update_meta_info(self, table_id, schema_id, table_name, bucket_num,
                         source_table_comment,
                         data_date, source_field_info, meta_field_infos,
                         project_id):
        """
            更新元数据信息  如果只是减少了字段 则不改变当前表结构，当前元数据不进行变更

        :param table_id:  表Id
        :param schema_id: schema_id
        :param table_name:  表名
        :param bucket_num:  分桶数
        :param source_table_comment:
        :param data_date: 业务日期
        :param source_field_info:  接入数据字段信息
        :param meta_field_infos:  元数据字段信息
        :param project_id: 项目ID
        :return:
        """
        if not self.just_delete_col:
            # 如果不是只是减少了字段 就直接更新
            self.meta_table_info_dao.delete_meta_table_info(table_id)
            new_meta_table_info = DidpMetaTableInfo(
                TABLE_ID=table_id,
                SCHEMA_ID=schema_id,
                PROJECT_VERSION_ID=project_id,
                LAST_UPDATE_TIME=last_update_time,
                LAST_UPDATE_USER=LAST_UPDATE_USER,
                TABLE_NAME=table_name,
                BUCKET_NUM=bucket_num,
                TABLE_NAME_CN=source_table_comment,
                DESCRIPTION="字段更新",
                RELEASE_DATE=data_date,
                TABLE_STATUS="2"
            )
            LOG.debug("登记表元数据 ")
            self.meta_table_info_dao.add_meta_table_info(
                new_meta_table_info)
            table_his_id = get_uuid()

            new_meta_table_info_his = DidpMetaTableInfoHis(
                TABLE_HIS_ID=table_his_id,
                TABLE_ID=table_id,
                PROJECT_VERSION_ID=project_id,
                SCHEMA_ID=schema_id,
                LAST_UPDATE_TIME=last_update_time,
                LAST_UPDATE_USER=LAST_UPDATE_USER,
                TABLE_NAME=table_name,
                BUCKET_NUM=bucket_num,
                TABLE_NAME_CN=source_table_comment,
                DESCRIPTION="字段更新",
                RELEASE_DATE=data_date,
                TABLE_STATUS="2"  # 发布状态
            )
            self.meta_table_info_his_dao.add_meta_table_info_his(
                new_meta_table_info_his)
            # 登记元数据字段
            LOG.debug("登记字段元数据")
            # 先删除存在的字段元数据
            meta_col_names = [x.COL_NAME.upper() for x in meta_field_infos]
            # self.meta_column_info_dao.delete_all_column(table_id)
            for field in source_field_info:
                if field.col_name.upper() not in meta_col_names:
                    # 新增字段
                    column_id = get_uuid()
                    meta_field_info = DidpMetaColumnInfo(
                        COLUMN_ID=column_id,
                        TABLE_ID=table_id,
                        PROJECT_VERSION_ID=project_id,
                        LAST_UPDATE_TIME=last_update_time,
                        LAST_UPDATE_USER=LAST_UPDATE_USER,
                        COL_SEQ=field.col_seq,
                        COL_NAME=field.col_name,
                        COL_DESC=field.comment,
                        COL_TYPE=field.data_type,
                        COL_LENGTH=field.col_length,
                        COL_SCALE=field.col_scale,
                        COL_DEFAULT=field.default_value,
                        NULL_FLAG=field.not_null)
                    self.meta_column_info_dao.add_meta_column(meta_field_info)

                    meta_field_info_his = DidpMetaColumnInfoHis(
                        TABLE_HIS_ID=table_his_id,
                        COLUMN_ID=column_id,
                        TABLE_ID=table_id,
                        PROJECT_VERSION_ID=project_id,
                        LAST_UPDATE_TIME=last_update_time,
                        LAST_UPDATE_USER=LAST_UPDATE_USER,
                        COL_SEQ=field.col_seq,
                        COL_NAME=field.col_name,
                        COL_DESC=field.comment,
                        COL_TYPE=field.data_type,
                        COL_LENGTH=field.col_length,
                        COL_SCALE=field.col_scale,
                        COL_DEFAULT=field.default_value,
                        NULL_FLAG=field.not_null
                    )
                    self.meta_column_info_his_dao.add_meta_column_his(
                        meta_field_info_his)
                else:
                    # 更新
                    column_id = self.meta_column_info_dao. \
                        get_column(table_id,
                                   field.col_name)[0].COLUMN_ID
                    self.meta_column_info_dao. \
                        update_meta_column(table_id,
                                           field.col_name,
                                           {
                                               "LAST_UPDATE_TIME": last_update_time,
                                               "LAST_UPDATE_USER": LAST_UPDATE_USER,
                                               "COL_DESC": field.comment,
                                               "COL_TYPE": field.data_type,
                                               "COL_LENGTH": field.col_length,
                                               "COL_SCALE": field.col_scale,
                                               "COL_DEFAULT": field.default_value,
                                               "NULL_FLAG": field.not_null}
                                           )

                    meta_field_info_his = DidpMetaColumnInfoHis(
                        TABLE_HIS_ID=table_his_id,
                        COLUMN_ID=column_id,
                        TABLE_ID=table_id,
                        PROJECT_VERSION_ID=project_id,
                        LAST_UPDATE_TIME=last_update_time,
                        LAST_UPDATE_USER=LAST_UPDATE_USER,
                        COL_SEQ=field.col_seq,
                        COL_NAME=field.col_name,
                        COL_DESC=field.comment,
                        COL_TYPE=field.data_type,
                        COL_LENGTH=field.col_length,
                        COL_SCALE=field.col_scale,
                        COL_DEFAULT=field.default_value,
                        NULL_FLAG=field.not_null
                    )
                    self.meta_column_info_his_dao.add_meta_column_his(
                        meta_field_info_his)

        if self.just_delete_col:
            # 表元数据不用更新
            if not self.type_change:
                # 无需更新
                LOG.debug("无新增字段或字段精度变化 ")
            else:
                for field in source_field_info:
                    ddl_type = MetaTypeInfo(field.data_type, field.col_length,
                                            field.col_scale)
                    index = meta_field_info.index(field.col_name)
                    meta_field = meta_field_info[index]
                    meta_type = MetaTypeInfo(meta_field.COL_TYPE,
                                             meta_field.COL_LENGTH,
                                             meta_field.COL_SCALE)

                    # 检查是否有字段精度的 更新
                    if not ddl_type.__eq__(meta_type):
                        # 对字段类型进行更新
                        LOG.debug("字段精度更新")
                        LOG.debug("{0} >> {1}".format(meta_type.get_whole_type,
                                                      ddl_type.get_whole_type
                                                      ))
                        self.meta_column_info_dao. \
                            update_meta_column(table_id,
                                               meta_field.COL_NAME,
                                               {
                                                   "COL_TYPE": ddl_type.field_type,
                                                   "COL_LENGTH": ddl_type.field_length,
                                                   "COL_SCALE": ddl_type.filed_scale
                                               }
                                               )


class MonRunLogService(object):
    def __init__(self, session):
        self.mon_run_log_dao = MonRunLogDao(session)
        self.mon_run_log_his_dao = MonRunLogHisDao(session)

    def create_run_log(self, didp_mon_run_log):
        """
            新增运行执行日志
        :param didp_mon_run_log:  执行日志对象
        :return:
        """
        self.mon_run_log_dao.add_mon_run_log(didp_mon_run_log)

    def find_run_logs(self, system, obj, org, start_date, end_date):
        """
            查看某个数据对象的执行日志
        :param system:
        :param obj:
        :param org:
        :param start_date:
        :param end_date:
        :return:
        """
        return self.mon_run_log_dao.get_mon_run_log_list(system, obj,
                                                         "5",
                                                         org,
                                                         start_date,
                                                         end_date)

    def find_log_with_table(self, system, obj, table_name, org, start_date,
                            end_date):
        # return self.mon_run_log_dao.get_mon_run_log_with_table(system,
        #                                                        obj,
        #                                                        table_name,
        #                                                        "5",
        #                                                        org,
        #                                                        start_date,
        #                                                        end_date
        #                                                        )
        return self.mon_run_log_dao.get_mon_log_with_r_table(system,
                                                             obj,
                                                             table_name,
                                                             "5",
                                                             org,
                                                             start_date,
                                                             end_date)

    def find_latest_all_archive(self, system, table_name, org, biz_date):
        """
            查询最近的全量数据归档记录
        :param system: 系统
        :param table_name: 全量历史表表名
        :param org: 机构
        :param biz_date: 业务日期
        :return:
        """

        return self.mon_run_log_dao.find_latest_all_archive(system, table_name,
                                                            org,
                                                            biz_date)

    def delete_log(self, pro_id, biz_date, org, batch_no):
        """
            删除日志
        :param pro_id:
        :param biz_date:
        :param org:
        :param batch_no:
        :return:
        """
        self.mon_run_log_dao.delete_mon_run_log(pro_id, biz_date, org, batch_no)

    def get_log(self, pro_id, biz_date, org, batch_no):
        """
            获取日志
        :param pro_id:
        :param biz_date:
        :param org:
        :param batch_no:
        :return:
        """
        return self.mon_run_log_dao.get_mon_run_log(pro_id, biz_date, org,
                                                    batch_no)

    def insert_log_his(self, mon_run_log_his):
        """
            插入log his
        :param mon_run_log_his:
        :return:
        """
        self.mon_run_log_his_dao.add_mon_run_log_his(mon_run_log_his)


if __name__ == '__main__':
    pass
