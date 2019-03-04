# -*- coding: UTF-8 -*-
################################################################################
# Date Time     : 2018-11-20
# Write By      : adtec(xiazhy,zhaogx)
# Function Desc : Inceptor卸数,加载插件
#
# History       :
#                 20181120  xiazhy     Create
# Remarks       :
################################################################################
import os
import re
import sys
import traceback
import jaydebeapi

from hdfs.ext.kerberos import KerberosClient
from utils.didp_logger import Logger
from utils.didp_db_operator import DbOperator

sys.path.append("{0}".format(os.environ["DIDP_HOME"]))
LOG = Logger()

# 运行的临时目录
DIDP_HOME = os.environ["DIDP_HOME"]
TMP_DIR = "{0}/conf/load_config_file".format(os.environ["DIDP_HOME"])

class BeforeHandler:
    """ 加载前处理
    ATTributes:
       __args           : 参数
    """
    #input
    __args = ""
    __table_name = ""
    __col_info = ""
    __props = ""
    __db_info = {}
    #inner
    __drop_sql = ""
    __create_sql = ""
    __truncate_sql = ""
    __col_defs = []

    __conn = ""

    def __init__(self, args, col_info, db_info, props):
        self.__args = args
        self.__table_name = self.__args.table
        self.__col_info = col_info
        self.__db_info = db_info
        self.__props = props

        LOG.debug("BeforeHandler init.")

    def __del__(self):
        self.__table_name = ""
        self.__col_info = []
        self.__props = ""

    def std_to_ddl(self):
        for field_info in self.__col_info:

            field_name = field_info['fieldname']
            field_ty = field_info['fieldtype']
            field_isnull = field_info['fieldisnull']

            ddl_type = ""
            LOG.debug("field_type:{0}".format(field_ty))

            if field_ty == 'YYYY-MM-DD' or field_ty == 'YYYYMMDD':
                ddl_type = 'DATE'
            elif field_ty == 'HH:MM:SS:NNN':
                ddl_type = 'VARCHAR(12)'
            elif field_ty == 'HHMMSSNNN':
                ddl_type = 'VARCHAR(9)'
            elif field_ty == 'HH:MM:SS':
                ddl_type = 'VARCHAR(8)'
            elif field_ty == 'HHMMSS':
                ddl_type = 'VARCHAR(6)'
            elif field_ty == 'YYYY-MM-DDTHH:MM:SS.NNNNNN' or field_ty == 'YYYYMMDDHHMMSSNNNNNN':
                ddl_type = 'TIMESTAMP(6)'
            elif field_ty == 'YYYY-MM-DDTHH:MM:SS.NNN' or field_ty == 'YYYYMMDDHHMMSSNNN':
                ddl_type = 'TIMESTAMP(3)'
            elif field_ty == 'YYYY-MM-DDTHH:MM:SS' or field_ty == 'YYYYMMDDHHMMSS':
                ddl_type = 'TIMESTAMP(0)'
            elif field_ty == 'YYYY-MM':
                ddl_type = 'VARCHAR(7)'
            elif field_ty == 'YYYYMM':
                ddl_type = 'VARCHAR(6)'
            elif field_ty == 'MM-DD':
                ddl_type = 'VARCHAR(5)'
            elif field_ty == 'MMDD':
                ddl_type = 'VARCHAR(4)'
            elif field_ty == 'YYYY':
                ddl_type = 'VARCHAR(4)'
            elif field_ty == 'MM' or field_ty == 'DD':
                ddl_type = 'VARCHAR(2)'
            elif field_ty == 'CLOB' or field_ty == 'BLOB':
                ddl_type = 'VARCHAR(4000)'
            elif re.match(r"^(\d+)n\((\d+)\)$", field_ty):
                # 匹配pn(s)
                m = re.match(r"^(\d+)n\((\d+)\)$", field_ty)
                ddl_type = 'DECIMAL({0},{1})'.format(m.group(1), m.group(2))
            elif re.match(r"^(\d+)n$", field_ty):
                # 匹配pn
                m = re.match(r"^(\d+)n$", field_ty)
                ddl_type = 'DECIMAL({0})'.format(m.group(1))
            elif re.match(r"^(\d+)\![an]+$", field_ty):
                # 匹配!an
                m = re.match(r"^(\d+)\![an]+$", field_ty)
                ddl_type = 'CHAR({0})'.format(m.group(1))
            elif re.match(r"^(\d+)\![anc]+$", field_ty):
                # 匹配!anc
                m = re.match(r"^(\d+)\![anc]+$", field_ty)
                ddl_type = 'CHAR({0})'.format(int(m.group(1)))
            elif re.match(r"^[an]+\.\.(\d+)$", field_ty):
                # 匹配an..
                m = re.match(r"^[an]+\.\.(\d+)$", field_ty)
                ddl_type = 'VARCHAR({0})'.format(m.group(1))
            elif re.match(r"^[anc]+\.\.(\d+)$", field_ty):
                # 匹配anc..
                m = re.match(r"^[anc]+\.\.(\d+)$", field_ty)
                ddl_type = 'VARCHAR({0})'.format(int(m.group(1)))
            else:
                ddl_type = 'VARCHAR(4000)'

            # self.__col_defs.append("{0} {1}".format(field_name, ddl_type))
            self.__col_defs.append("{0} {1} {2}".format(field_name, ddl_type, "NOT NULL" if field_isnull == "0" else ""))

        return 0

    def generate_create_sql(self):
        self.__create_sql = "CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1}" \
                          "\n(" \
                          "\n{2}" \
                          "\n)" \
                          "\nROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'" \
                          "\nWITH SERDEPROPERTIES ('input.delimited'='{3}')" \
                          "\nSTORED AS TEXTFILE" \
                          "\nLOCATION '{4}/{5}/';".format(self.__db_info['db_schema'], self.__table_name, "\n,".join(self.__col_defs), self.__args.delim,
                    self.__args.loaddir, self.__table_name)

        return 0

    def generate_drop_sql(self):

        self.__drop_sql = "DROP TABLE IF EXISTS {0}.{1}".format(self.__db_info['db_schema'], self.__table_name)

        return 0

    def generate_truncate_sql(self):

        self.__truncate_sql = "TRUNCATE TABLE {0}.{1}".format(self.__db_info['db_schema'], self.__table_name)

        return 0

    def export_meta_info(self):
        """ 导出元数据信息（用于比对）
        Args:
            None
        Returns:
            0, 结构信息 - 成功 | -1, [] - 失败
        Raise:
            None
        """
        pass

    def generate_cols_ddl(self):

        for col in self.__col_info:

            c = ""

            col_name  = col['column_name']
            data_type = col['data_type']
            data_len  = col['column_define_length']
            data_scale= col['column_scale']
            is_null   = col['is_null']
            is_pk     = col['is_pk']

            if col['data_type'] == 'CHAR':
                c = "{0} CHAR({1})".format(col_name, data_len)

            elif col['data_type'] == 'VARCHAR':
                c = "{0} VARCHAR({1})".format(col_name, data_len)

            elif col['data_type'] == 'DECIMAL' and data_scale == 0:
                c = "{0} DECIMAL({1})".format(col_name, data_len)

            elif col['data_type'] == 'DECIMAL' and data_scale != 0:
                c = "{0} DECIMAL({1},{2})".format(col_name, data_len, data_scale)

            elif col['data_type'] == 'NUMERIC' and data_scale == 0:
                c = "{0} NUMERIC({1})".format(col_name, data_len)

            elif col['data_type'] == 'NUMERIC' and data_scale != 0:
                c = "{0} NUMERIC({1},{2})".format(col_name, data_len, data_scale)

            elif col['data_type'] == 'NUMBER' and data_scale == 0:
                c = "{0} NUMERIC({1})".format(col_name, data_len)

            elif col['data_type'] == 'NUMBER' and data_scale != 0:
                c = "{0} NUMERIC({1},{2})".format(col_name, data_len, data_scale)

            elif col['data_type'] == 'DATE':
                c = "{0} DATE".format(col_name)

            elif col['data_type'] == 'TIME':
                #c = "{0} TIME".format(col_name)
                c = "{0} VARCHAR(8)".format(col_name)

            elif col['data_type'] == 'TIMESTAMP':
                c = "{0} TIMESTAMP({1})".format(col_name, data_len)

            elif data_scale != 0:
                c = "{0} {1}({2},{3})".format(col_name, data_type, data_len, data_scale)

            elif data_scale == 0:
                c = "{0} {1}({2})".format(col_name, data_type, data_len)

            else:
                LOG.error("不支持的数据类型[{0}][{1}][{2}]".format(col_name, data_type, data_len))
                return -1
                
            if is_null == "N":
                c = "{0} NOT NULL".format(c)

            self.__col_defs.append(c)

        return 0


    def run(self):
        # 生成建表语句的字段ddl
        ret = self.generate_cols_ddl()
        if ret != 0:
            LOG.error("转目标DDL类型失败")
            return ret

        # 生成DROP语句
        self.generate_drop_sql()
        LOG.info("DROP语句：{0}".format(self.__drop_sql))

        # 生成CREATE语句
        self.generate_create_sql()
        LOG.info("CREATE语句：{0}".format(self.__create_sql))

        # 生成TRUNCATE语句
        self.generate_truncate_sql()
        LOG.info("TRUNCATE语句：{0}".format(self.__truncate_sql))

        # 根据模式进行前处理
        LOG.debug("数据加载模式[{0}]".format(self.__args.mode))
        if self.__args.mode == 'CREATE':
            dboper = DbOperator(self.__db_info['db_user'], self.__db_info['db_pwd'],
                                "org.apache.hive.jdbc.HiveDriver", self.__db_info['jdbc_url'],
                                "{0}/drivers/jdbc/inceptor-driver-6.0.0.jar".format(DIDP_HOME))

            #dboper.execute("!set plsqlClientDialect db2;")
            #dboper.execute("set plsql.server.dialect=db2;")
            dboper.execute(self.__drop_sql)
            dboper.execute(self.__create_sql)

            LOG.debug("Before Handler[DROP&&CREATE]")

        elif self.__args.mode == 'TRUNCATE':
            dboper = DbOperator(self.__db_info['db_user'], self.__db_info['db_pwd'],
                                "org.apache.hive.jdbc.HiveDriver", self.__db_info['jdbc_url'],
                                "{0}/drivers/jdbc/inceptor-driver-6.0.0.jar".format(DIDP_HOME))

            #dboper.execute("!set plsqlClientDialect db2;")
            #dboper.execute("set plsql.server.dialect=db2;")
            dboper.execute(self.__truncate_sql)

            LOG.debug("Before Handler[TRUNCATE]")

        else:
            LOG.debug("Before Handler[None].")

        return 0


class Exporter:
    pass

class Loader:

    sql_file = "{0}/inceptor_{1}.sql".format(TMP_DIR, os.getpid())

    def write_file(self, file_name, text):
        fh = open(file_name, 'w')
        fh.write(text)
        fh.close()

    def __init__(self, args, col_info, db_info, props):
        self.__args = args
        self.__col_info = col_info
        self.__props = props
        self.__db_info = db_info

    def run(self):

        # 前处理
        tc = BeforeHandler( self.__args, self.__col_info, self.__db_info, self.__props )
        ret = tc.run()
        if ret != 0:
            LOG.error("加载前处理失败")
            return ret

        # 加载处理

        # 上传文件到指定hdfs目录
        HDFS_WORK_DIR = "{0}/{1}".format(self.__args.loaddir, self.__args.table)
        #put_cmd = "KRB5_CONFIG={0}" \
        #          " && kinit -kt {1} {2}" \
        #          " && hadoop fs -rm -r -f {3}" \
        #          " && hadoop fs -mkdir -p {3}" \
        #          " && hadoop fs -put {4} {5}"\
        #    .format(self.__args.krbfile, self.__args.ktfile, self.__args.ktuser, HDFS_WORK_DIR, self.__args.srcfile, HDFS_WORK_DIR)

        #LOG.info("HDFS PUT CMD[{0}]".format(put_cmd))
        #ret = os.system(put_cmd)
        #if ret != 0:
        #    LOG.error("上传文件到hdfs失败")
        #    return -1

        print "AAA:{0}".format(HDFS_WORK_DIR)
        try:
            # 建立连接
            hdfs_client = KerberosClient(self.__args.nnurl, principal="{0}".format(self.__args.ktuser)) 

            # 删除历史目录
            hdfs_client.delete(HDFS_WORK_DIR, recursive=True)

            # 创建新的目录
            hdfs_client.makedirs(HDFS_WORK_DIR)

            # 上传文件到HDFS
            hdfs_client.upload(HDFS_WORK_DIR, self.__args.srcfile)
        except:
            traceback.print_exc()
            LOG.error("数据加载失败")
            return -1

        LOG.info("数据加载成功")

        # 后处理
        tc = AfterHandler(self.__args, self.__db_info, self.__props)
        ret = tc.run()
        if ret != 0:
            LOG.error("加载后处理失败")
            return ret
        return 0

class AfterHandler:

    def __init__(self, args, __db_info, __props):
        self.__args = args
        self.__table_name = self.__args.table
        self.__db_info = __db_info
        self.__props = __props

    def run(self):
        return 0

