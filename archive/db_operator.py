# -*- coding: UTF-8 -*-  

# Date Time     : 2019/1/9
# Write By      : adtec(ZENGYU)
# Function Desc :
# History       : 2019/1/9  ZENGYU     Create
# Remarks       :
import os
import sys

reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append("{0}".format(os.environ["DIDP_HOME"]))

from archive.archive_enum import SaveMode
from archive.model import *


class AccountCtrlDao(object):
    def __init__(self, session):
        self.SESSION = session

    # def get_account_cols(self, sch_key, table_name):
    #     """
    #         获取账号字段列表
    #     :return:
    #     """
    #     result = self.SESSION.query(DidpAccountCtrl).filter(
    #         DidpAccountCtrl.SCHEMA_KEY == sch_key,
    #         DidpAccountCtrl.TABLE_NAME == table_name
    #     ).all()
    #
    #     return result


# class DidpSchemaDao(object):
#
#     def __init__(self, session):
#         self.SESSION = session
#
#     def get_schema_key(self, sch_id):
#         result = self.SESSION.query(DidpSchemaInfo).\
#             filter(DidpSchemaInfo.SCHEMA_ID == sch_id).one()
#         return result


class CommonParamsDao(object):
    """
        操作公共代码类
    """

    def __init__(self, session):
        self.SESSION = session

    def get_common_param(self, group_name, param_name):
        """
        获取公共参数
        :param group_name: 组名
        :param param_name: 参数名
        :return:
        """
        result = self.SESSION.query(DidpCommonParams).filter(
            DidpCommonParams.GROUP_NAME == group_name,
            DidpCommonParams.PARAM_NAME == param_name).one()
        return result

    def get_all_common_code(self):
        """
            获取所有的公共参数放于dict中
        :return:
        """
        result = self.SESSION.query(DidpCommonParams).all()
        common_dict = {}
        for r in result:
            common_dict[r.PARAM_NAME] = r.PARAM_VALUE

        return common_dict


class ProcessDao(object):
    def __init__(self, session):
        self.SESSION = session

    def get_process_info(self, pro_id):
        """
            获取流程信息
        :return:
        """
        result = self.SESSION.query(DidpProcessInfo). \
            filter(DidpProcessInfo.PROCESS_ID == pro_id).all()
        if len(result) > 0:
            return result[0]
        else:
            return None


class MetaColumnInfoDao(object):
    """
        元数据字段信息访问
    """

    def __init__(self, session):
        self.SESSION = session

    def delete_all_column(self, table_id):
        self.SESSION.query(DidpMetaColumnInfo).filter(
            DidpMetaColumnInfo.TABLE_ID == table_id).delete()
        self.SESSION.commit()

    def delete_column(self, table_id, col_name):
        self.SESSION.query(DidpMetaColumnInfo).filter(
            DidpMetaColumnInfo.TABLE_ID == table_id,
            DidpMetaColumnInfo.COL_NAME == col_name,
        ).delete()
        self.SESSION.commit()

    def get_meta_data_by_table(self, table_id):
        result = self.SESSION.query(DidpMetaColumnInfo).filter(
            DidpMetaColumnInfo.TABLE_ID == table_id).all()

        return result

    def get_column(self, table_id, col_name):
        result = self.SESSION.query(DidpMetaColumnInfo).filter(
            DidpMetaColumnInfo.TABLE_ID == table_id,
            DidpMetaColumnInfo.COL_NAME == col_name,
        ).all()

        return result

    def add_meta_column(self, meta_field_info):
        """
            添加字段元数据
        :param meta_field_info: 字段元数据对象
        :return:
        """
        self.SESSION.add(meta_field_info)
        self.SESSION.commit()

    def update_meta_column(self, table_id, col_name, update_dict):
        """
            更新字段
        :param table_id: 表id
        :param col_name: 字段名
        :param update_dict: 更新字典
        :return:
        """
        self.SESSION.query(DidpMetaColumnInfo).filter(
            DidpMetaColumnInfo.TABLE_ID == table_id,
            DidpMetaColumnInfo.COL_NAME == col_name).update(update_dict)

        self.SESSION.commit()


class MetaColumnInfoHisDao(object):
    def __init__(self, session):
        self.SESSION = session

    def update_meta_column_his(self, table_id, column_name, update_dict):
        """
            更新表元数据字段
        :param table_id: 表ID
        :param column_name: 字段名
        :param update_dict: 更新内容
        :return:
        """
        self.SESSION.query(DidpMetaColumnInfoHis).filter(
            DidpMetaColumnInfoHis.TABLE_ID == table_id,
            DidpMetaColumnInfoHis.COL_NAME == column_name).update(update_dict)
        self.SESSION.commit()

    def add_meta_column_his(self, meta_field_info_his):
        """
            添加字段元数据
        :param meta_field_info_his: 字段元数据对象
        :return:
        """
        self.SESSION.add(meta_field_info_his)
        self.SESSION.commit()

    def get_meta_column_info(self, table_his_id):
        """
                获取字段的信息
        :param table_his_id:
        :return:
        """
        result = self.SESSION.query(DidpMetaColumnInfoHis).filter(
            DidpMetaColumnInfoHis.TABLE_HIS_ID == table_his_id).all()

        return result


class MetaTableInfoDao(object):
    """
     表元数据信息表
    """

    def __init__(self, session):
        self.SESSION = session

    def add_meta_table_info(self, meta_table_info):
        """

        :param meta_table_info: 表元数据对象
        :return:
        """

        self.SESSION.add(meta_table_info)
        self.SESSION.commit()

    def get_meta_table_info(self, schema_id, table_name):
        """
            获取Meta_table_info
        :param schema_id: SCHEMA_ID
        :param table_name: 表名
        :return: 返回唯一的 表
        """
        meta_table_info = self.SESSION.query(DidpMetaTableInfo).filter(
            DidpMetaTableInfo.SCHEMA_ID == schema_id,
            DidpMetaTableInfo.TABLE_NAME == table_name).all()

        if len(meta_table_info) == 0:
            return None
        return meta_table_info[0]

    def get_meta_table_info_by_time(self, table_name, release_time):
        """
            通过表名，日期获取Meta_table_info
        :param table_name:
        :param release_time:
        :return:
        """
        meta_table_info = self.SESSION.query(DidpMetaTableInfo).filter(
            DidpMetaTableInfo.TABLE_NAME == table_name,
            DidpMetaTableInfo.RELEASE_DATE == release_time).all()

        return meta_table_info

    def delete_meta_table_info(self, table_id):
        """
            删除表元数据
        :param table_id:
        :return:
        """

        self.SESSION.query(DidpMetaTableInfo).filter(
            DidpMetaTableInfo.TABLE_ID == table_id, ).delete()
        self.SESSION.commit()

    def update_meta_table_info(self, schema_id, table_name, update_dict):
        """
            更新表元数据信息
        :param schema_id:
        :param table_name:
        :param update_dict:
        :return:
        """
        self.SESSION.query(DidpMetaTableInfo) \
            .filter_by(TABLE_NAME=table_name,
                       SCHEMA_ID=schema_id).update(update_dict)
        self.SESSION.commit()


class MetaTableInfoHisDao(object):
    def __init__(self, session):
        self.SESSION = session

    def get_recent_table_info_his(self, table_name, release_date):
        """
                    获取最近的表元数据信息
                :param table_name:
                :param release_date:
                :return: 最近一天的元数据信息
                """
        result = self.SESSION.query(DidpMetaTableInfoHis).filter(
            DidpMetaTableInfoHis.TABLE_NAME == table_name,
            DidpMetaTableInfoHis.RELEASE_DATE <= release_date).order_by(
            DidpMetaTableInfoHis.RELEASE_DATE.desc()).all()

        if len(result) == 0:
            result = self.SESSION.query(DidpMetaTableInfo).filter(
                DidpMetaTableInfo.TABLE_NAME == table_name,
                DidpMetaTableInfo.RELEASE_DATE >= release_date).order_by(
                DidpMetaTableInfo.RELEASE_DATE.asc()).all()
        if len(result) > 0:
            return result[0]
        else:
            return None

    def get_all(self):
        result = self.SESSION.query(DidpMetaTableInfoHis).all()
        return result

    def update_meta_table_info_his(self, table_his_id, update_dict):
        """
                更新历史表元数据信息
        :param table_his_id:
        :param update_dict:
        :return:
        """
        self.SESSION.query(DidpMetaTableInfoHis).filter(
            DidpMetaTableInfoHis.TABLE_HIS_ID == table_his_id).update(
            update_dict)
        self.SESSION.commit()

    def add_meta_table_info_his(self, meta_table_info_his):
        self.SESSION.add(meta_table_info_his)
        self.SESSION.commit()

    def get_meta_table_info_his_list(self, table_id, schema_id, data_date):
        result = self.SESSION.query(DidpMetaTableInfoHis).filter(
            DidpMetaTableInfoHis.TABLE_ID == table_id,
            DidpMetaTableInfoHis.SCHEMA_ID == schema_id,
            DidpMetaTableInfoHis.RELEASE_DATE == data_date).all()

        return result

    def get_meta_table_info_his(self, table_his_id):
        result = self.SESSION.query(DidpMetaTableInfoHis).filter(
            DidpMetaTableInfoHis.TABLE_HIS_ID == table_his_id
        ).one()
        return result

    def get_before_meta_table_infos(self, schema_id, table_name, data_date):
        """
            获取data_date 之前的表元数据版本
        :param schema_id:
        :param table_name: 表名
        :param data_date: 归档日期
        :return: List<DidpMetaTableInfo>
        """
        result = self.SESSION.query(DidpMetaTableInfoHis). \
            filter(DidpMetaTableInfoHis.SCHEMA_ID == schema_id,
                   DidpMetaTableInfoHis.TABLE_NAME == table_name,
                   DidpMetaTableInfoHis.RELEASE_DATE < data_date). \
            order_by(DidpMetaTableInfoHis.RELEASE_DATE.desc()).all()

        return result

    def get_after_meta_table_infos(self, schema_id, table_name, data_date):
        """
                获取data_date 之后的表元数据版本
            :param schema_id:
            :param table_name: 表名
            :param data_date: 归档日期
            :return: List<DidpMetaTableInfo>
            """
        result = self.SESSION.query(DidpMetaTableInfoHis). \
            filter(
            DidpMetaTableInfoHis.SCHEMA_ID == schema_id,
            DidpMetaTableInfoHis.TABLE_NAME == table_name,
            DidpMetaTableInfoHis.RELEASE_DATE > data_date). \
            order_by(DidpMetaTableInfoHis.RELEASE_DATE.asc()).all()

        return result

    def get_meta_table_info_by_time(self, schema_id, table_name, data_date):
        """
            可能会返回多个结果
        :param schema_id:
        :param table_name:
        :param data_date:
        :return:
        """
        result = self.SESSION.query(DidpMetaTableInfoHis). \
            filter(
            DidpMetaTableInfoHis.SCHEMA_ID == schema_id,
            DidpMetaTableInfoHis.TABLE_NAME == table_name,
            DidpMetaTableInfoHis.RELEASE_DATE == data_date).all()

        return result

    def get_meta_table_info_by_detail(self, schema_id, table_name, data_date,
                                      comment,
                                      table_comment_change_ddl):
        """
            根据详细的字段信息来获取表的元数据
        :param schema_id:
        :param table_name:
        :param data_date:
        :param bucket_num:
        :param comment:
        :param table_comment_change_ddl : 表备注改变是否新增表版本 "true" "false"
        :return:
        """
        if table_comment_change_ddl.lower().__eq__("true"):
            result = self.SESSION.query(DidpMetaTableInfoHis). \
                filter(
                DidpMetaTableInfoHis.SCHEMA_ID == schema_id,
                DidpMetaTableInfoHis.TABLE_NAME == table_name,
                DidpMetaTableInfoHis.RELEASE_DATE == data_date,
                DidpMetaTableInfoHis.TABLE_NAME_CN == comment
            ).all()
        else:
            result = self.SESSION.query(DidpMetaTableInfoHis). \
                filter(
                DidpMetaTableInfoHis.SCHEMA_ID == schema_id,
                DidpMetaTableInfoHis.TABLE_NAME == table_name,
                DidpMetaTableInfoHis.RELEASE_DATE == data_date
            ).all()

        return result

        pass


class MonRunLogDao(object):
    def __init__(self, session):
        self.SESSION = session

    def add_mon_run_log(self, didp_mon_run_log):
        """
            新增执行日记记录
        :param didp_mon_run_log: 执行日志对象
        :return:
        """
        self.SESSION.add(didp_mon_run_log)
        self.SESSION.commit()

    def get_mon_run_log(self, pro_id, biz_date, org, batch_no):
        """
            获取执行日志
        :param pro_id: 执行号
        :param biz_date: 业务日期
        :param org: 机构
        :param batch_no: 批次号
        :return:
        """
        result = self.SESSION.query(DidpMonRunLog).filter(
            DidpMonRunLog.PROCESS_ID == pro_id,
            DidpMonRunLog.BIZ_DATE == biz_date,
            DidpMonRunLog.BRANCH_NO == org,
            DidpMonRunLog.BATCH_NO == batch_no).all()
        if len(result) > 0:
            return result[0]
        else:
            return None

    def delete_mon_run_log(self, pro_id, biz_date, org, batch_no):
        """
            删除 执行日志
        :param pro_id:
        :param biz_date:
        :param org:
        :param batch_no:
        :return:
        """
        self.SESSION.query(DidpMonRunLog).filter(
            DidpMonRunLog.PROCESS_ID == pro_id,
            DidpMonRunLog.BIZ_DATE == biz_date,
            DidpMonRunLog.BRANCH_NO == org,
            DidpMonRunLog.BATCH_NO == batch_no).delete()
        self.SESSION.commit()

    def get_mon_run_log_list(self, system, obj, pros_type, org, start_date,
                             end_date):
        """
            获取执行日志集合
        :param system:  系统名
        :param obj: 数据对象
        :param pros_type: 加工类型
        :param start_date: 执行日期
        :param end_date: 结束
        :return:
        """
        result = self.SESSION.query(DidpMonRunLog).filter(
            DidpMonRunLog.SYSTEM_KEY == system,
            DidpMonRunLog.DATA_OBJECT_NAME == obj,
            DidpMonRunLog.BRANCH_NO == org,
            DidpMonRunLog.PROCESS_TYPE == pros_type,
            DidpMonRunLog.BIZ_DATE >= start_date,
            DidpMonRunLog.BIZ_DATE <= end_date,
            DidpMonRunLog.PROCESS_STATUS == "0"  # 执行状态为成功

        ).all()
        if len(result) < 1:
            return None
        else:
            return result

    def get_mon_run_log_with_table(self, system, obj, table, pros_type, org,
                                   start_date, end_date):
        """

        :param system:
        :param obj:
        :param table:
        :param pros_type:
        :param org:
        :param start_date:
        :param end_date:
        :return:
        """
        result = self.SESSION.query(DidpMonRunLog).filter(
            DidpMonRunLog.SYSTEM_KEY == system,
            DidpMonRunLog.DATA_OBJECT_NAME == obj,
            DidpMonRunLog.TABLE_NAME == table,
            DidpMonRunLog.BRANCH_NO == org,
            DidpMonRunLog.PROCESS_TYPE == pros_type,
            DidpMonRunLog.BIZ_DATE >= start_date,
            DidpMonRunLog.BIZ_DATE <= end_date,
            DidpMonRunLog.PROCESS_STATUS == "0"  # 执行状态为成功
        ).all()
        if len(result) < 1:
            return None
        else:
            return result[0]

    def get_mon_log_with_r_table(self, system, obj, r_table, pros_type, org, start_date, end_date):
        """
            根据R层的表判断表是否需要做初始化归档
        :param system:
        :param obj:
        :param r_table:
        :param pros_type:
        :param org:
        :param start_date:
        :param end_date:
        :return:
        """
        result = self.SESSION.query(DidpMonRunLog).filter(
            DidpMonRunLog.SYSTEM_KEY == system,
            DidpMonRunLog.DATA_OBJECT_NAME == obj,
            DidpMonRunLog.EXTENDED2 == r_table,
            DidpMonRunLog.BRANCH_NO == org,
            DidpMonRunLog.PROCESS_TYPE == pros_type,
            DidpMonRunLog.BIZ_DATE >= start_date,
            DidpMonRunLog.BIZ_DATE <= end_date,
            DidpMonRunLog.PROCESS_STATUS == "0"  # 执行状态为成功
        ).all()
        if len(result) < 1:
            return None
        else:
            return result[0]

    def find_latest_all_archive(self, system, table_name, org, biz_date):
        """
            获取最近的全量归档
        :param system: 系统
        :param table_name: 全量历史表表名
        :param org:
        :param biz_date:
        :return:
        """
        result = self.SESSION.query(DidpMonRunLog).filter(
            DidpMonRunLog.SYSTEM_KEY == system,
            DidpMonRunLog.TABLE_NAME == table_name,
            DidpMonRunLog.BRANCH_NO == org,
            DidpMonRunLog.PROCESS_STATUS == '0',
            DidpMonRunLog.EXTENDED1 == str(SaveMode.ALL.value),
            DidpMonRunLog.BIZ_DATE <= biz_date
        ).order_by(DidpMonRunLog.BIZ_DATE.desc()).all()

        if len(result) > 0:
            return result[0]
        else:
            return None


class MonRunLogHisDao(object):
    def __init__(self, session):
        self.SESSION = session

    def add_mon_run_log_his(self, mon_run_log_his):
        self.SESSION.add(mon_run_log_his)
        self.SESSION.commit()


class ArchiveLockDao(object):
    """
        归档控制Dao
    """

    def __init__(self, session):
        self.SESSION = session

    def add(self, obj, org):
        didp_archive_ctrl = DidpHdsStructArchiveCtrl(OBJECT_NAME=obj,
                                                     ORG_CODE=org)
        self.SESSION.add(didp_archive_ctrl)
        self.SESSION.commit()

    def delete_by_pk(self, obj, org):
        self.SESSION.query(DidpHdsStructArchiveCtrl). \
            filter(DidpHdsStructArchiveCtrl.OBJECT_NAME == obj,
                   DidpHdsStructArchiveCtrl.ORG_CODE == org).delete()
        self.SESSION.commit()

    def find_by_pk(self, obj, org):
        """
            通过主键查找
        :param obj: 数据对象名
        :param org: 机构名
        :return:  查询结果
        """
        result = self.SESSION.query(DidpHdsStructArchiveCtrl). \
            filter(DidpHdsStructArchiveCtrl.OBJECT_NAME == obj,
                   DidpHdsStructArchiveCtrl.ORG_CODE == org,
                   ).all()
        return result


class MetaLockDao(object):
    """
        元数据控制Dao
    """

    def __init__(self, session):
        self.SESSION = session

    def add(self, obj, org):
        mate_ctrl = DidpHdsStructMetaCtrl(OBJECT_NAME=obj,
                                          ORG_CODE=org)
        self.SESSION.add(mate_ctrl)
        self.SESSION.commit()

    def delete_by_pk(self, obj, org):
        self.SESSION.query(DidpHdsStructMetaCtrl). \
            filter(DidpHdsStructMetaCtrl.OBJECT_NAME == obj,
                   DidpHdsStructMetaCtrl.ORG_CODE == org).delete()
        self.SESSION.commit()

    def find_by_pk(self, obj, org):
        """
            通过主键查找
        :param obj: 数据对象名
        :param org: 机构名
        :return:  查询结果
        """
        result = self.SESSION.query(DidpHdsStructMetaCtrl). \
            filter(DidpHdsStructMetaCtrl.OBJECT_NAME == obj,
                   DidpHdsStructMetaCtrl.ORG_CODE == org,
                   ).all()
        return result


if __name__ == '__main__':
    pass
