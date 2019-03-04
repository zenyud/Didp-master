#-*- coding: UTF-8 -*-  
################################################################################
# Date Time     : 2018-11-27
# Write By      : adtec(zhaogx)
# Function Desc : ddl操作类
#
# History       :
#                 20181127  xiazhy     Create
#
# Remarks       :
################################################################################
import re
import os
import xml.dom.minidom

from didp_logger import Logger
from didp_tools import generate_common_ddl_type

LOG = Logger()

class DDLFileParser:
    """ DDL文件解析类
    Attributes:
    """

    # ddl文件类型: 1-SQL文件 2-XML文件 3-JSON文件 4-STD检核后生成
    __ddl_file_type = ""
    __file = ""
    # 目标数据库类型 Inceptor/db2
    __tar_db_type = "INCEPTOR"

    def __init__(self, fname, file_type="XML"):
        self.__file = fname
        self.__ddl_file_type = file_type

    def __del__(self):
        __ddl_file_type = ""

    def __get_ddl_info_from_sql(self):
        pass

    def __is_pk(self, key_list, col_name):

        is_pk = "0"
        for i in key_list:
            if i == col_name:
                is_pk = "1"
                break
        return is_pk

    def __col_type_parse(self, type):

        data_type = ""
        column_define_length = ""
        column_scale = ""

        re.purge()

        m = re.match(r"^CHAR\((\d+)\)$", type, flags=re.IGNORECASE)
        if m:
            return (0, "CHAR", m.group(1), 0)

        m = re.match(r"^VARCHAR\((\d+)\)$", type, flags=re.IGNORECASE)
        if m:
            return (0, "VARCHAR", m.group(1), 0)

        m = re.match(r"^NUMBER\((\d+)\)$", type, flags=re.IGNORECASE)
        if m:
            return (0, "NUMBER", m.group(1), 0)

        m = re.match(r"^NUMBER\((\d+),(\d+)\)$", type, flags=re.IGNORECASE)
        if m:
            return (0, "NUMBER", m.group(1), m.group(2))

        m = re.match(r"^NUMERIC\((\d+)\)$", type, flags=re.IGNORECASE)
        if m:
            return (0, "NUMERIC", m.group(1), 0)

        m = re.match(r"^NUMERIC\((\d+),(\d+)\)$", type, flags=re.IGNORECASE)
        if m:
            return (0, "NUMERIC", m.group(1), m.group(2))

        m = re.match(r"^DECIMAL\((\d+)\)$", type, flags=re.IGNORECASE)
        if m:
            return (0, "DECIMAL", m.group(1), 0)

        m = re.match(r"^DECIMAL\((\d+),(\d+)\)$", type, flags=re.IGNORECASE)
        if m:
            return (0, "DECIMAL", m.group(1), m.group(2) )

        m = re.match(r"^DATE$", type, flags=re.IGNORECASE)
        if m:
            return (0, "DATE", 10, 0)

        m = re.match(r"^TIME$", type, flags=re.IGNORECASE)
        if m:
            return (0, "TIME", 8, 0)

        m = re.match(r"^TIMESTAMP$", type, flags=re.IGNORECASE)
        if m:
            return (0, "TIMESTAMP", 26, 0)

        m = re.match(r"^TIMESTAMP\((\d+)\)$", type, flags=re.IGNORECASE)
        if m:
            return (0, "TIMESTAMP", m.group(1), 0)

        return -1, "", ""

    def __get_ddl_info_from_xml(self):

        LOG.info("解析XML类型的DDL文件[{0}]".format(self.__file))

        DOMTree = xml.dom.minidom.parse(self.__file)

        ref = DOMTree.documentElement

        filename = ref.getElementsByTagName("filename")[0].childNodes[0].data
        schemaname = ref.getElementsByTagName("schemaName")[0].childNodes[0].data
        tablename = ref.getElementsByTagName("tableName")[0].childNodes[0].data
        fileversion = ref.getElementsByTagName("fileversion")[0].childNodes[0].data
        fieldcount = int(ref.getElementsByTagName("fieldcount")[0].childNodes[0].data)
        isfixedlength = ref.getElementsByTagName("isfixedlength")[0].childNodes[0].data

        LOG.debug("-----------------------------------------")
        LOG.debug("filename:{0}".format(filename))
        LOG.debug("schemaname:{0}".format(schemaname))
        LOG.debug("tablename:{0}".format(tablename))
        LOG.debug("fileversion:{0}".format(fileversion))
        LOG.debug("filedcount:{0}".format(fieldcount))
        LOG.debug("isfixedlength:{0}".format(isfixedlength))
        LOG.debug("-----------------------------------------")

        # 主键信息节点
        keys_node = ref.getElementsByTagName("keydescription")[0]
        key = keys_node.getElementsByTagName("keyname")
        key_cnt = len(key)

        LOG.debug("KEY CNT: {0}".format(key_cnt))
        key_list = []
        for i in range(key_cnt):
            key_list.append(key[i].firstChild.data)

        # 字段信息节点
        fields_node = ref.getElementsByTagName("fielddescription")[0]

        fields = fields_node.getElementsByTagName("field")
        field_cnt = len(fields)
        LOG.debug("FIELD CNT: {0}".format(field_cnt))

        if fieldcount != field_cnt:
            LOG.error("预期字段数[{0}]与实际字段数[{1}]不一致".format(fieldcount, field_cnt))
            return -1

        ddl_info = []
        for i in range(field_cnt):
            # column_name/column_desc/data_type/column_define_length/column_precision/is_null/is_pk/partition_flag/bucket_flag
            fieldname = fields[i].getAttribute("fieldname")
            fielddesc = fields[i].getAttribute("description")
            ret, data_type, column_define_length, column_precision = self.__col_type_parse(fields[i].getAttribute("fieldtype"))
            if ret != 0:
                LOG.error("解析字段类型出错[{0}:{1}]".format(fields[i].getAttribute("fieldname"), fields[i].getAttribute("fieldtype")))
                return -1
            is_pk = self.__is_pk(key_list, fieldname)

            ddl_info.append({
                'column_name' : fieldname,
                'column_desc': fielddesc,
                'data_type': data_type,
                'column_define_length': column_define_length,
                'column_precision': column_precision,
                'is_pk': is_pk,
                'tablename': tablename ,
                'fixed': int(float(isfixedlength))
            })

        return 0, ddl_info

    def __get_ddl_info_from_xml_bak(self):

        LOG.info("解析XML类型的DDL文件[{0}]".format(self.__file))

        DOMTree = xml.dom.minidom.parse(self.__file)

        ref = DOMTree.documentElement

        filename = ref.getElementsByTagName("filename")[0].childNodes[0].data
        fileversion = ref.getElementsByTagName("fileversion")[0].childNodes[0].data
        fieldcount = int(ref.getElementsByTagName("fieldcount")[tablenamea].childjodes[0].data)
        isfixedlength = ref.getElementsByTagName("isfixedlength")[0].childNodes[0].data
        fieldseperator = ref.getElementsByTagName("fieldseperator")[0].childNodes[0].data
        lineseperator = ref.getElementsByTagName("lineseperator")[0].childNodes[0].data

        LOG.debug("-----------------------------------------")
        LOG.debug("filename:{0}".format(filename))
        LOG.debug("schemaname:{0}".format(schemaname))
        LOG.debug("tablename:{0}".format(tablename))
        LOG.debug("fileversion:{0}".format(fileversion))
        LOG.debug("filedcount:{0}".format(fieldcount))
        LOG.debug("isfixedlength:{0}".format(isfixedlength))
        LOG.debug("fieldseperator:{0}".format(fieldseperator))
        LOG.debug("lineseperator:{0}".format(lineseperator))
        LOG.debug("-----------------------------------------")

        # 主键信息节点
        keys_node = ref.getElementsByTagName("keydescription")[0]
        key = keys_node.getElementsByTagName("keyname")
        key_cnt = len(key)

        LOG.debug("KEY CNT: {0}".format(key_cnt))
        key_list = []
        for i in range(key_cnt):
            key_list.append(key[i].firstChild.data)

        # 字段信息节点
        fields_node = ref.getElementsByTagName("fielddescription")[0]

        fields = fields_node.getElementsByTagName("field")
        field_cnt = len(fields)
        LOG.debug("FIELD CNT: {0}".format(field_cnt))

        if fieldcount != field_cnt:
            LOG.error("预期字段数[{0}]与实际字段数[{1}]不一致".format(fieldcount, field_cnt))
            return -1

        ddl_info = []
        for i in range(field_cnt):
            # column_name/column_desc/data_type/column_define_length/column_scale/is_null/is_pk/partition_flag/bucket_flag
            fieldname = fields[i].getAttribute("name")
            fielddesc = fields[i].getAttribute("desc")
            ret, data_type, column_define_length, column_scale = self.__col_type_parse(fields[i].getAttribute("type"))
            if ret != 0:
                LOG.error("解析字段类型出错[{0}:{1}]".format(fields[i].getAttribute("name"), fields[i].getAttribute("type")))
                return -1

            # 默认主键非空,其他可空
            is_null = "1"
            is_pk = self.__is_pk(key_list, fieldname)
            if is_pk == "1":
                is_null = "0"

            column_std_type = generate_common_ddl_type(data_type,
                                  column_define_length, column_scale)

            ddl_info.append({
                'table_name' : filename,
                'delim' : fieldseperator,
                'rcdelim' : lineseperator,
                'fixed' : isfixedlength,
                'column_name' : fieldname,
                'column_desc': fielddesc,
                'data_type': data_type,
                'column_std_type': column_std_type,
                'column_define_length': column_define_length,
                'column_scale': column_scale,
                'is_pk': is_pk,
                'is_null': is_null,
                'partition_flag': '',
                'bucket_flag': '' 
            })

        return 0, ddl_info

    def __get_ddl_info_from_json(self):
        pass

    def set_target_db_type(self, db_type):

        self.__tar_db_type = db_type

    def get_target_db_type(self):

        return self.__tar_db_type

    def get_load_file(self):

        return self.__file

    def __stdcol_type_parse(self, field_ty):

        if self.__tar_db_type.upper() == "INCEPTOR":
            if field_ty == 'YYYY-MM-DD' or field_ty == 'YYYYMMDD':
                ddl_type = 'DATE'
                return (0, "DATE", 0, 0)

            elif field_ty == 'HH:MM:SS:NNN':
                ddl_type = 'VARCHAR(12)'
                return (0, "VARCHAR", 12, 0)

            elif field_ty == 'HHMMSSNNN':
                # ddl_type = 'VARCHAR(9)'
                return (0, "VARCHAR", 9, 0)

            elif field_ty == 'HH:MM:SS':
                # ddl_type = 'TIME'
                return (0, "TIME", 0, 0)

            elif field_ty == 'HHMMSS':
                # ddl_type = 'VARCHAR(6)'
                return (0, "VARCHAR", 6, 0)

            elif field_ty == 'YYYY-MM-DDTHH:MM:SS.NNNNNN' or field_ty == 'YYYYMMDDHHMMSSNNNNNN':
                # ddl_type = 'TIMESTAMP(3)'
                return (0, "TIMESTAMP", 6, 0)

            elif field_ty == 'YYYY-MM-DDTHH:MM:SS.NNN' or field_ty == 'YYYYMMDDHHMMSSNNN':
                # ddl_type = 'TIMESTAMP(3)'
                return (0, "TIMESTAMP", 3, 0)

            elif field_ty == 'YYYY-MM-DDTHH:MM:SS' or field_ty == 'YYYYMMDDHHMMSS':
                # ddl_type = 'TIMESTAMP(0)'
                return (0, "TIMESTAMP", 0, 0)

            elif field_ty == 'YYYY-MM':
                # ddl_type = 'VARCHAR(7)'
                return (0, "VARCHAR", 7, 0)

            elif field_ty == 'YYYYMM':
                # ddl_type = 'VARCHAR(6)'
                return (0, "VARCHAR", 6, 0)

            elif field_ty == 'MM-DD':
                # ddl_type = 'VARCHAR(5)'
                return (0, "VARCHAR", 5, 0)

            elif field_ty == 'MMDD':
                # ddl_type = 'VARCHAR(4)'
                return (0, "VARCHAR", 4, 0)

            elif field_ty == 'YYYY':
                # ddl_type = 'VARCHAR(4)'
                return (0, "VARCHAR", 4, 0)

            elif field_ty == 'MM' or field_ty == 'DD':
                # ddl_type = 'VARCHAR(2)'
                return (0, "VARCHAR", 2, 0)

            elif field_ty == 'CLOB' or field_ty == 'BLOB':
                # ddl_type = 'VARCHAR(4000)'
                return (0, "VARCHAR", 4000, 0)

            else :
                # 匹配pn(s)
                m = re.match(r"^(\d+)n\((\d+)\)$", field_ty)
                if m:
                    #ddl_type = 'DECIMAL({0},{1})'.format(m.group(1), m.group(2))
                    return (0, "DECIMAL", m.group(1), m.group(2))

                # 匹配pn
                m = re.match(r"^(\d+)n$", field_ty)
                if m:
                    #ddl_type = 'DECIMAL({0})'.format(m.group(1))
                    return (0, "DECIMAL", m.group(1),0)

                # 匹配!an
                m = re.match(r"^(\d+)\![an]+$", field_ty)
                if m:
                    #ddl_type = 'CHAR({0})'.format(m.group(1))
                    return (0, "CHAR", m.group(1), 0)

                # 匹配!anc
                m = re.match(r"^(\d+)\![anc]+$", field_ty)
                if m:
                    #ddl_type = 'CHAR({0})'.format(int(m.group(1)) * 3)
                    return (0, "CHAR", int(m.group(1)) * 3, 0)

                # 匹配an..
                m = re.match(r"^[an]+\.\.(\d+)$", field_ty)
                if m:
                    #ddl_type = 'VARCHAR({0})'.format(m.group(1))
                    return (0, "VARCHAR", int(m.group(1)), 0)

                # 匹配anc..
                m = re.match(r"^[anc]+\.\.(\d+)$", field_ty)
                if m:
                    # ddl_type = 'VARCHAR({0})'.format(int(m.group(1)) * 3)
                    return (0, "VARCHAR", int(m.group(1)) * 3, 0)

            return -1, "", "", ""

        elif self.__tar_db_type.upper() == "DB2":
            LOG.error("暂未支持DB2")
            return -1, "", "", ""

        else:
            LOG.error("不支持的数据库类型[{0}]".format(self.__tar_db_type.upper()))
            return -1, "", "", ""


    def __get_ddl_info_from_std(self):

        LOG.info("解析STD类型的DDL文件[{0}]".format(self.__file))

        DOMTree = xml.dom.minidom.parse(self.__file)

        ref = DOMTree.documentElement
        file = ref.getElementsByTagName("file")[0]

        filename = file.getElementsByTagName("filename")[0].childNodes[0].data
        fileversion = file.getElementsByTagName("fileversion")[0].childNodes[0].data
        fieldcount = int(file.getElementsByTagName("fieldcount")[0].childNodes[0].data)
        isfixedlength = file.getElementsByTagName("isfixedlength")[0].childNodes[0].data

        LOG.debug("-----------------------------------------")
        LOG.debug("filename:{0}".format(filename))
        LOG.debug("fileversion:{0}".format(fileversion))
        LOG.debug("filedcount:{0}".format(fieldcount))
        LOG.debug("isfixedlength:{0}".format(isfixedlength))
        LOG.debug("-----------------------------------------")

        # 字段信息节点
        fields_node = file.getElementsByTagName("fielddescription")[0]

        fieldnames = fields_node.getElementsByTagName("fieldname")
        if fieldcount != len(fieldnames):
            LOG.error("字段名：预期[{0}]与实际[{1}]不一致".format(fieldcount, len(fieldnames)))
            return -1

        fieldtypes = fields_node.getElementsByTagName("fieldtype")
        if fieldcount != len(fieldtypes):
            LOG.error("字段类型：预期[{0}]与实际[{1}]不一致".format(fieldcount, len(fieldtypes)))
            return -1

        fieldchinames = fields_node.getElementsByTagName("fieldchiname")
        if fieldcount != len(fieldchinames):
            LOG.error("字段中文描述：预期[{0}]与实际[{1}]不一致".format(fieldcount, len(fieldchinames)))
            return -1

        fieldsrctypes = fields_node.getElementsByTagName("fieldsrctype")
        if fieldcount != len(fieldsrctypes):
            LOG.error("字段源类型：预期[{0}]与实际[{1}]不一致".format(fieldcount, len(fieldsrctypes)))
            return -1

        fieldisnulls = fields_node.getElementsByTagName("fieldisnull")
        if fieldcount != len(fieldisnulls):
            LOG.error("可空：预期[{0}]与实际[{1}]不一致".format(fieldcount, len(fieldisnulls)))
            return -1

        # 主键信息节点
        key_list = []
        if len(file.getElementsByTagName("keydescription")) > 0:
            key_node = file.getElementsByTagName("keydescription")[0]
            keystr = key_node.getElementsByTagName("keyname")[0].firstChild.data
            key_list.append(keystr)

        ddl_info = []
        for i in range(fieldcount):
            # column_name/column_desc/data_type/column_define_length/column_scale/is_null/is_pk/partition_flag/bucket_flag
            column_name = fieldnames[i].firstChild.data
            if fieldchinames[i].firstChild == None:
                column_desc = ""
            else:
                column_desc = fieldchinames[i].firstChild.data
            ret, data_type, column_define_length, column_scale = self.__stdcol_type_parse(fieldtypes[i].firstChild.data)
            is_null = fieldisnulls[i].firstChild.data
            is_pk = self.__is_pk(key_list, column_name)

            ddl_info.append({
                'column_name': column_name,
                'column_desc': column_desc,
                'data_type': data_type,
                'column_define_length': column_define_length,
                'column_scale': column_scale,
                'is_null' : is_null,
                'is_pk': is_pk,
                'partition_flag': '',
                'bucket_flag': '' 
            })

        return 0, ddl_info


    def get_ddl_info(self):

        ret = 0
        ddl_info = []
        if self.__ddl_file_type == "SQL":
            ret, ddl_info = self.__get_ddl_info_from_sql()

        elif self.__ddl_file_type == "XML":
            ret, ddl_info = self.__get_ddl_info_from_xml()

        elif self.__ddl_file_type == "JSON":
            ret, ddl_info = self.__get_ddl_info_from_json()

        elif self.__ddl_file_type == "STD":
            ret, ddl_info = self.__get_ddl_info_from_std()

        else:
            LOG.error("不支持的DDL文件类型[{0}]".format(self.__ddl_file_type))
            return -1, ""

        if ret != 0:
            LOG.error("解析DDL文件[{0}]出错".format(self.__ddl_file_type))

        return 0, ddl_info


if __name__ == "__main__":
    a = DDLFileParser("F:\900-TMP\800_TCHANNEL_20180505.ddl", "XML")
    ret, ddl_info = a.get_ddl_info()
    LOG.debug(ddl_info)
