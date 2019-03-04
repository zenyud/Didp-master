# -*- coding: UTF-8 -*- 
################################################################################
# Date Time     : 2018-10-26
# Write By      : adtec(zhaogx)
# Function Desc : 工具模块
#
# History       :
#                 20181026  zhaogx     Create
#
# Remarks       :
################################################################################
import re
import os
import uuid
import codecs
import datetime
import traceback

from didp_logger import Logger
from didp_db_operator import DbOperator

LOG = Logger()

# 配置库用户
DIDP_CFG_DB_USER = os.environ["DIDP_CFG_DB_USER"]
# 配置库用户密码
DIDP_CFG_DB_PWD = os.environ["DIDP_CFG_DB_PWD"]
# JDBC信息
DIDP_CFG_DB_JDBC_CLASS = os.environ["DIDP_CFG_DB_JDBC_CLASS"]
DIDP_CFG_DB_JDBC_URL = os.environ["DIDP_CFG_DB_JDBC_URL"]


def get_db_login_info(schema_id):
    """ 从配置库中获取目标库的信息
    
    Args:
        schema_id : SCHAME ID
    Returns:
        [0, 目标库信息] : 成功 | [-1, ''] : 失败
    Raise:

    """
    LOG.info("获取数据库连接信息")
    result_info = []    # 结果信息
    target_db_info = {} # 目标库连接信息

    sql = ("SELECT"
           "\n  CASE T2.DB_TYPE"
           "\n  WHEN '1' THEN 'ORACLE'"
           "\n  WHEN '2' THEN 'DB2'"
           "\n  WHEN '3' THEN 'MYSQL'"
           "\n  WHEN '4' THEN 'SQLSERVER'"
           "\n  WHEN '5' THEN 'INCEPTOR'"
           "\n  END,"
           "\n  T1.USER_NAME,"
           "\n  T1.USER_PWD,"
           "\n  T1.SCHEMA_NAME,"
           "\n  CASE WHEN T2.DB_PARAMS IS NULL"
           "\n  THEN T2.DB_URL"
           "\n  ELSE CONCAT(T2.DB_URL, T2.DB_PARAMS)"
           "\n  END,"
           "\n  T2.DB_NAME,"
           "\n  T1.SCHEMA_SHORT_KEY"
           "\nFROM DIDP_META_SCHEMA_INFO T1, "
           "\n     DIDP_META_SOURCE_DB_CONFIG T2, "
           "\n     DIDP_META_DATA_SOURCE_INFO T3 "
           "\nWHERE T1.SOURCE_ID = T3.SOURCE_ID "
           "\nAND T3.CONFIG_ID = T2.CONFIG_ID "
           "\nAND T1.SCHEMA_ID = '{0}'").format(schema_id)

    LOG.info("SQL:\n{0}".format(sql))

    db_oper = DbOperator(DIDP_CFG_DB_USER, DIDP_CFG_DB_PWD, 
                         DIDP_CFG_DB_JDBC_CLASS, DIDP_CFG_DB_JDBC_URL)

    try:
        result_info = db_oper.fetchall_direct(sql)
    except Exception as e:
        LOG.error("获取数据库连接信息失败")
        return -1, ''

    if result_info:
        LOG.info("------------目标数据库连接信息------------")
        LOG.info("数据库类型     : {0}".format(result_info[0][0]))
        LOG.info("数据库用户     : {0}".format(result_info[0][1]))
        LOG.info("数据库用户密码 : {0}".format(result_info[0][2]))
        LOG.info("数据库SCHEME   : {0}".format(result_info[0][3]))
        LOG.info("JDBC URL       : {0}".format(result_info[0][4]))
        LOG.info("数据库名       : {0}".format(result_info[0][5]))
        LOG.info("schema_short_key : {0}".format(result_info[0][6]))
        LOG.info("----------------------------------------------")
       
        target_db_info['db_type'] = result_info[0][0]
        target_db_info['db_user'] = result_info[0][1]
        target_db_info['db_pwd'] = result_info[0][2]
        target_db_info['db_schema'] = result_info[0][3]
        target_db_info['jdbc_url'] = result_info[0][4]
        target_db_info['db_name'] = result_info[0][5]
        target_db_info['schema_short_key'] = result_info[0][6]

        return 0, target_db_info
    else:
        LOG.error("未找到SCHEMA ID为[{0}]的连接信息".format(schema_id))
        return -1, ''

def generate_common_ddl_type(src_type, src_length, src_scale):
    """ 生成通用DDL类型

    Args:
        src_type : 源字段类型 
        src_length : 源字段长度
        src_scale : 源字段精度
    Returns:
        通用DDL类型
    Raise:

    """
    src_type = src_type.upper() # 转大写处理
    common_type = "" # 通用类型

    if (src_type == "CHAR" or src_type == "NCHAR"
        or src_type == "CHARACTER" or src_type == "BINARY"):
        common_type = "{0}!anc".format(src_length)
    elif (src_type == "VARCHAR" or src_type == "NVARCHAR" 
          or src_type == "VARCHAR2" or src_type == "NVARCHAR2"
          or src_type == "ENUM" or src_type == "SET"
          or src_type == "VARBINARY"):
        common_type = "anc..{0}".format(src_length)
    elif src_type == "TINYINT":
        common_type = "3n"
    elif src_type == "SMALLINT":
        common_type = "5n"
    elif src_type == "MEDIUMINT":
        common_type = "7n"
    elif src_type == "INT" or src_type == "INTEGER":
        common_type = "10n"
    elif src_type == "BIGINT":
        common_type = "19n"
    elif (src_type == "FLOAT" or src_type == "DOUBLE"
          or src_type == "BINARY_FLOAT" or src_type == "BINARY_DOUBLE"):
        common_type = "39n(10)"
    elif src_type == "REAL" or src_type == "DECFLOAT":
        common_type = "31n(10)"
    elif src_type == "UROWID" or src_type == "ROWID":
        common_type = "anc..18"
    elif (src_type == "CLOB" or src_type == "TINYTEXT"
          or src_type == "TEXT" or src_type == "MEDIUMTEXT"
          or src_type == "LONGTEXT"):
        common_type = "CLOB"
    elif (src_type == "TINYBLOB" or src_type == "BLOB"
          or src_type == "MEDIUMBLOB" or src_type == "LONGBLOB"):
        common_type = "BLOB"
    elif src_type == "DATE":
        common_type = "YYYY-MM-DD"
    elif src_type == "YEAR":
        common_type = "YYYY"
    elif src_type == "TIME":
        common_type = "HH:MM:SS"
    elif (src_type == "TIMESTAMP" or src_type == "TIMESTAMP(6)"
          or src_type == "DATETIME"):
        common_type = "YYYY-MM-DDTHH:MM:SS.NNNNNN"
    elif src_type == "TIMESTAMP(0)" or src_type == "DATETIME(0)":
        common_type = "YYYY-MM-DDTHH:MM:SS"
    elif src_type == "TIMESTAMP(3)" or src_type == "DATETIME(3)":
        common_type = "YYYY-MM-DDTHH:MM:SS.NNN"
    elif (src_type == "LONG VARCHAR" or src_type == "LONG VARGRAPHIC"
          or src_type == "GRAPHIC"):
        common_type = "anc..4000"
    elif (src_type == "NUMBER" or src_type == "DECIMAL"
          or src_type == "NUMERIC"):
        if src_scale == 0:
            if src_length > 19 :
                common_type = "19n"
            else:
                common_type = "{0}n".format(src_length)
        else:
            common_type = "{0}n({1})".format(src_length, src_scale)
    else:
        match_obj = re.match(r"^LONG", src_type)
        if match_obj:
            common_type = "anc..10"
        else:
            match_obj = re.match(r"^RAW", src_type)
            if match_obj:
                common_type = "anc..10"
            else:
                common_type = src_type

    return common_type


def generate_uuid():
    """ 生成UUID

    Args:
    Returns:
        UUID
    Raise:
        None
    """
    uuid_str = str(uuid.uuid1()).replace("-", "")

    return uuid_str

def escape_str(str):
    """ 将参数字符串转义字串替换为相应字符

    Args:
        str - 待处理字符串
    Returns:
        处理后的字符串
    Raise:
        None
    """
    str = re.sub(r"\$", "\\\$", str)
    return str

def check_path(check_file):
    """ 检查文件对应的目录,如果目录不存在则自动创建

    Args:
        check_file - 待处理文件全路径
    Returns:
        0 - 成功 | -1 - 失败
    Raise:
        None
    """
    try:
        #check_path = check_file[:check_file.rindex("/")+1]
        check_path = os.path.dirname(check_file)
    except Exception as e:
        traceback.print_exc()
        return -1

    if not os.path.exists(check_path):
        LOG.info("目录[{0}]不存在,自动创建".format(check_path))
        try:
            os.makedirs(check_path) 
        except Exception as e:
            traceback.print_exc()
            LOG.error("目录创建失败")
            return -1
    else:
        return 1

    return 0

def write_file(write_file, write_content, write_charset):
    """ 根据将指定的内容根据指定的编码写入文件

    Args:
        write_file : 待处理文件全路径
        write_content : 待写入文件的内容
        write_charset : 待写入文件的字符集
    Returns:
        0 : 成功 | -1 : 失败
    Raise:

    """
    try:
        FILE = codecs.open(write_file, "w", write_charset)
    except Exception as e:
        traceback.print_exc()
        LOG.error("打开文件失败")
        return -1 

    FILE.write(write_content)

    FILE.close()
    return 0

def generate_ddl_file(ddl_file, version, table_name, is_fixed, columns_info,
                      charset, db_type, select_columns, record_delim):
    """ 生成数据集成平台传输用DDL文件

    Args:
        ddl_file : 待处理DDL文件全路径
        table_name : 表名
        is_fixed : 定长标识
        columns_info : 字段信息,结构如下:
                          [("column_name":"xxx", 
                            "column_std_type":"xxx",
                            "column_type":"xxx",
                            "is_pk": "x",
                            "is_null: "x",
                            "column_desc":"xxx"), (...), (...)]
        charset : 待写入DDL文件的字符集
        db_type : 数据库类型
    Returns:
        0 : 成功 | -1 : 失败
    Raise:

    """
    ddl_content = ""     # ddl文件内容
    key_content = ""     # 主键内容
    column_content = ""  # 字段内容
    tmp_key_content = "" # 临时存放主键内容

    column_records = len(columns_info) # 字段数

    record_delim = re.sub("\n", r"\\n", record_delim)
    record_delim = re.sub("\r", r"\\r", record_delim)

    ret = check_path(ddl_file)
    if ret == -1:
        return -1

    if charset == "0":
        charset = "GBK"
    else:
        charset = "UTF-8"

    # 处理字段信息
    order = 1
    for column_info in columns_info:
        if select_columns != "":
            if re.match("^{0}$|^{0},|.*?,{0},.*?|.*?,{0}$".format(column_info["column_name"]),
                        select_columns, re.I):
                column_desc = ""
                if column_info["column_desc"]:
                    column_desc = "<![CDATA[{0}]]>".format(column_info["column_desc"])

                column_content += ("<field name=\"{0}\" type=\"{1}\" "
                                   "seq=\"{2}\"/>\n").format(
                                     column_info["column_name"],
                                     column_info["column_type"],
                                     order)
                order += 1
 
                if column_info["is_pk"] == "1":
                    tmp_key_content += "\n<keyname>{0}</keyname>".format(
                                         column_info["column_name"])
        else:
            column_desc = ""
            if column_info["column_desc"]:
                column_desc = "<![CDATA[{0}]]>".format(column_info["column_desc"])

            column_content += ("<field name=\"{0}\" type=\"{1}\" "
                               "seq=\"{2}\"/>\n").format(
                                 column_info["column_name"],
                                 column_info["column_type"],
                                 order)
            order += 1
 
            if column_info["is_pk"] == "1":
                tmp_key_content += "\n<keyname>{0}</keyname>".format(
                                     column_info["column_name"])

    # 处理主键
    if tmp_key_content != "":
        key_content = ("\n<keydescription>"
                       "{0}"
                       "\n</keydescription>").format(tmp_key_content)
    
    delim = columns_info[0]["delim"]
    ddl_content = ("<?xml version=\"1.0\" encoding=\"{0}\" ?>"
                   "\n<file>"
                   "\n<filename>{1}</filename>"
                   "\n<fileversion>{2}</fileversion>"
                   "\n<fieldcount>{3}</fieldcount>"
                   "\n<isfixedlength>{4}</isfixedlength>"
                   "\n<fieldseperator>{5}</fieldseperator>"
                   "\n<lineseperator>{6}</lineseperator>"
                   "\n<dbtype>{7}</dbtype>"
                   "\n<fielddescription>\n"
                   "{8}"
                   "</fielddescription>"
                   "{9}"
                   "\n</file>").format(charset, table_name, version,
                                      column_records, is_fixed, delim,
                                      record_delim, db_type, column_content,
                                      key_content)
     
    LOG.debug("生成的DDL文件内容为:\n{0}".format(ddl_content))

    try:
        DDL_FILE = codecs.open(ddl_file, "w", charset)
    except Exception as e:
        traceback.print_exc()
        LOG.error("打开DDL文件失败")
        return -1

    DDL_FILE.write(ddl_content)

    DDL_FILE.close

    return 0 

def generate_common_ddl_file(ddl_file, version, table_name, is_fixed, columns_info,
                      in_charset, out_charset):
    """ 生成数据集成平台通用DDL文件

    Args:
        ddl_file : 待处理DDL文件全路径
        table_name : 表名
        is_fixed : 定长标识
        columns_info : 字段信息,结构如下:
                          [("column_name":"xxx", 
                            "column_std_type":"xxx",
                            "column_type":"xxx",
                            "is_pk": "x",
                            "is_null: "x",
                            "column_desc":"xxx"), (...), (...)]
        charset      : 待写入DDL文件的字符集
    Returns:
        0 : 成功 | -1 : 失败
    Raise:

    """
    ddl_content = ""     # ddl文件内容
    key_content = ""     # 主键内容
    column_content = ""  # 字段内容
    tmp_key_content = "" # 临时存放主键内容
    charset = ""         # ddl文件字符集
    expand_flag = "0"    # ddl字符串类型长度扩展标识

    column_records = len(columns_info) # 字段数

    if out_charset == "0":
        charset = "GBK"
    else:
        charset = "UTF-8"

    if in_charset == "0" and out_charset == "1":
        expand_flag = "1"

    ret = check_path(ddl_file)
    if ret == -1:
        return -1

    # 处理字段信息
    for column_info in columns_info:
        column_desc = ""
        if column_info["column_desc"]:
            column_desc = "<![CDATA[{0}]]>".format(column_info["column_desc"])

        if expand_flag == "1":
            m = re.match("^((a|n|c|an|ac|anc|nc)\.\.)(.*?)$",
                         column_info["column_std_type"])
            if m:
                column_info["column_std_type"] = "{0}{1}".format(m.group(1),
                                                  int(m.group(3))*3/2)
            else:
                m = re.match("^(.*?)(!(a|n|c|an|ac|anc|nc))$",
                             column_info["column_std_type"])
                if m:
                    column_info["column_std_type"] = "{0}{1}".format(
                                                         int(m.group(1))*3/2,
                                                         m.group(2))

        column_content = ("{0}\n<fieldname>{1}</fieldname>"
                          "\n<fieldtype>{2}</fieldtype>"
                          "\n<fieldchiname>{3}</fieldchiname>"
                          "\n<fieldsrctype>{4}</fieldsrctype>"
                          "\n<fieldisnull>{5}</fieldisnull>").format(
                              column_content,
                              column_info["column_name"],
                              column_info["column_std_type"],
                              column_desc,
                              column_info["column_type"],
                              column_info["is_null"])
 
        if column_info["is_pk"] == "1":
            tmp_key_content += "\n<keyname>{0}</keyname>".format(
                                 column_info["column_name"])

    # 处理主键
    if tmp_key_content != "":
        key_content = ("\n<keydescription>"
                       "{0}"
                       "\n</keydescription>").format(tmp_key_content)
    
    ddl_content = ("<?xml version=\"1.0\" encoding=\"{0}\" ?>"
                   "\n<transmit-content>"
                   "\n<file>"
                   "\n<filename>{1}</filename>"
                   "\n<fileversion>{2}</fileversion>"
                   "\n<fieldcount>{3}</fieldcount>"
                   "\n<isfixedlength>{4}</isfixedlength>"
                   "\n<fielddescription>"
                   "{5}"
                   "\n</fielddescription>"
                   "{6}"
                   "\n</file>"
                   "\n</transmit-content>").format(charset, table_name, version,
                                                   column_records, is_fixed,
                                                   column_content, key_content)
     
    LOG.debug("生成的DDL文件内容为:\n{0}".format(ddl_content))

    try:
        DDL_FILE = codecs.open(ddl_file, "w", charset)
    except Exception as e:
        traceback.print_exc()
        LOG.error("打开DDL文件失败")
        return -1

    DDL_FILE.write(ddl_content)

    DDL_FILE.close

    return 0 

def generate_ctrl_file(ctrl_file, data_file, data_file_records, data_type,
                       charset):
    """ 生成控制文件

    Args:
        ctrl_file : 控制文件全路径
        data_file : 数据文件全路径
        data_file_records : 数据文件记录数
        data_type : 数据类型
        charset : 文件字符集
    Returns:
        0 : 成功 | -1 : 失败
    Raise:

    """
    ret = check_path(ctrl_file)
    if ret == -1:
        return -1

    ctrl_content = "" # 文件内容

    verify_code  = "" # 校验码
    batch_count = 0   # 累计批次数
    number_count = 0  # 累计顺序数

    # 转换字符集
    if charset == "1":
        charset = "UTF-8"
    else:
        charset = "GBK"

    data_file_size = os.path.getsize(data_file) # 数据文件大小

    # 获取文件名 
    data_file = os.path.basename(data_file)

    # 格式化当前时间
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 拼接内容
    ctrl_content = ("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>"
                    "\n<file>"
                    "\n<datatype>{0}</datatype>"
                    "\n<character-encoding>{1}</character-encoding>"
                    "\n<filename>{2}</filename>"
                    "\n<recordnum>{3}</recordnum>"
                    "\n<filesize>{4}</filesize>"
                    "\n<verifycode>{5}</verifycode >"
                    "\n<batchcount>{6}</batchcount>"
                    "\n<numbercount>{7}</numbercount>"
                    "\n<starttimestamp>{8}</starttimestamp>"
                    "\n<endtimestamp>{8}</endtimestamp>"
                    "\n</file>").format(data_type, charset, data_file, 
                                       data_file_records, data_file_size, 
                                       verify_code, batch_count, number_count,
                                       current_time)

    LOG.debug("生成的CTRL文件内容为:\n{0}".format(ctrl_content))

    try:
        DDL_FILE = codecs.open(ctrl_file, "w", "UTF8")
    except Exception as e:
        traceback.print_exc()
        LOG.error("打开CTRL文件失败")
        return -1

    DDL_FILE.write(ctrl_content)

    DDL_FILE.close

    return 0 

def generate_schema_file(schema_file, table_name, is_fixed,
                         delim, columns_info):
    """ 生成检核用的XML文件

    Args:
        schema_file : 待处理SCHEMA文件全路径
        table_name : 表名
        is_fixed : 定长标识
        columns_info : 字段信息,结构如下:
                          [("column_name":"xxx", 
                            "column_std_type":"xxx",
                            "is_pk": "x",
                            "is_null: "x"), (...), (...)]
        charset : 待写入XML文件的字符集
    Returns:
        0 : 成功 | -1 : 失败
    Raise:
 
    """
    schema_content = ""  # ddl文件内容
    key_content = ""     # 主键内容
    column_content = ""  # 字段内容
    tmp_key_content = "" # 临时存放主键内容

    column_records = len(columns_info) # 字段数

    ret = check_path(schema_file)
    if ret == -1:
        return -1

    # 处理字段信息
    for column_info in columns_info:
        column_content = ("{0}\n<fieldname>{1}</fieldname>"
                          "\n<fieldtype>{2}</fieldtype>"
                          "\n<nullok>{3}</nullok>"
                          "\n<format></format>").format(
                              column_content,
                              column_info["column_name"],
                              column_info["column_std_type"],
                              column_info["is_null"])
 
        if column_info["is_pk"] == "1":
            tmp_key_content += "\n<keyname>{0}</keyname>".format(
                                 column_info["column_name"])

    # 处理主键
    if tmp_key_content != "":
        key_content = ("\n<keydescription>"
                       "{0}"
                       "\n</keydescription>").format(tmp_key_content)
    
    schema_content = ("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>"
                      "\n<transmit-content>"
                      "\n<file>"
                      "\n<filename>{0}</filename>"
                      "\n<fileversion>V1.0</fileversion>"
                      "\n<fieldcount>{1}</fieldcount>"
                      "\n<isfixedlength>{2}</isfixedlength>"
                      "\n<delim>{5}</delim>"
                      "\n<fielddescription>"
                      "{3}"
                      "\n</fielddescription>"
                      "{4}"
                      "\n</file>"
                      "\n</transmit-content>").format(table_name,
                                                  column_records,
                                                  is_fixed, column_content,
                                                  key_content, delim)
     
    LOG.debug("生成的SCHEMA文件内容为:\n{0}".format(schema_content))

    try:
        DDL_FILE = codecs.open(schema_file, "w", "UTF8")
    except Exception as e:
        traceback.print_exc()
        LOG.error("打开SCHEMA文件失败")
        return -1

    DDL_FILE.write(schema_content)

    DDL_FILE.close

    return 0 

def stat_file_record(file_name):
    """ 获取文件的记录数

    Args:
        file_name : 文件名
    Returns:
        -1 : 失败 | 其它 : 文件记录数
    Raise:

    """
    
    count = 0 # 记录数

    try:
        for index, line in enumerate(open(file_name, 'r')):
            count += 1
    except Exception as e:
        traceback.print_exc()
        return -1

    return count

def stat_table_record(db_info, table_name):
    """ 获取表的记录数

    Args:
        db_info : 数据库连接信息
        tablename : 表名
    Returns:
        -1 : 失败 | 其它 : 表记录数
    Raise:

    """
    cnt = 0 
    dboper = DbOperator(db_info['db_user'], db_info['db_pwd'], get_driver_classname(db_info['db_type']), db_info['jdbc_url'], "")
    dboper.connect()
    
    sql_txt = "SELECT COUNT(1) FROM {0}.{1}".format(db_info['db_schema'], table_name)
    LOG.debug("QUERY SQL:[{0}]".format(sql_txt))

    try:
        cnt = dboper.fetchone(sql_txt)[0]
    except Exception as e:
        dboper.close()
        return -1

    dboper.close()
    return cnt

def get_driver_classname(db_type):
    """ 获取表的记录数

    Args:
        db_type : 数据库类型
    Returns:
        驱动类名称
    Raise:

    """

    __class_name = ""
    __db_type = db_type.upper()

    if __db_type == "ORACLE":
        __class_name = "oracle.jdbc.driver.OracleDriver"
    elif __db_type == "DB2":
        __class_name = "com.ibm.db2.jcc.DB2Driver"
    elif __db_type == "MYSQL":
        __class_name = "com.mysql.jdbc.Driver"
    elif __db_type == "SQLSERVER":
        __class_name = "net.sourceforge.jtds.jdbc.Driver"
    elif __db_type == "INCEPTOR":
        __class_name = "org.apache.hive.jdbc.HiveDriver"
    else:
        LOG.error("不支持的数据库类型[{0}]".format(__db_type))
        return -1

    LOG.debug("CLASS NAME:[{0}]".format(__class_name))
    return __class_name


def search_file(path_name, file_name):
    """ 使用正则搜索指定目录下的文件

    Args:
        path_name : 目录名
        file_name : 文件名,可使用正则
    Returns:
        matched_file : 搜索到的文件的列表
    Raise:

    """
    matched_file =[]
    for root, dirs, files in os.walk(path_name):
        for file in files:
            if re.match(file_name, file):
                tmp_file = os.path.abspath(os.path.join(root, file))
                matched_file.append(tmp_file)
    return matched_file

