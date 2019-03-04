#-*- coding: UTF-8 -*-  
################################################################################
# Date Time     : 2018-11-27
# Write By      : adtec
# Function Desc : ctl文件操作类
#
# History       :
#                 20181127  xiazhy     Create
#
# Remarks       :
################################################################################
import re
import xml.dom.minidom

from didp_logger import Logger

LOG = Logger()

class CtlFileParser:
    """ DDL文件解析类
    Attributes:
    """

    # ctl文件类型: 1-TXT文件 2-XML文件 3-JSON文件
    __ctl_file_type = ""
    __file = ""
    # 目标数据库类型 Inceptor/db2
    __tar_db_type = "INCEPTOR"

    def __init__(self, fname, file_type="XML"):
        self.__file = fname
        self.__ctl_file_type = file_type

    def __del__(self):
        __ctl_file_type = ""

    def __get_ctl_info_from_ok(self):
        pass

    def __get_ctl_info_from_xml(self):

        LOG.info("解析XML类型的控制文件[{0}]".format(self.__file))

        DOMTree = xml.dom.minidom.parse(self.__file)

        ref = DOMTree.documentElement

        datatype  = ref.getElementsByTagName("datatype")[0].childNodes[0].data
        encoding  = ref.getElementsByTagName("character-encoding")[0].childNodes[0].data
        recordnum = ref.getElementsByTagName("recordnum")[0].childNodes[0].data
        filesize  = ref.getElementsByTagName("filesize")[0].childNodes[0].data
        verifycode = int(ref.getElementsByTagName("verifycode")[0].childNodes[0].data)
        lowergear  = ref.getElementsByTagName("lowergear")[0].childNodes[0].data
        separative = ref.getElementsByTagName("separative")[0].childNodes[0].data
        splitfilenum = ref.getElementsByTagName("splitfilenum")[0].childNodes[0].data

        LOG.debug("-----------------------------------------")
        LOG.debug("datatype    :{0}".format(datatype))
        LOG.debug("encoding    :{0}".format(encoding))
        LOG.debug("recordnum   :{0}".format(recordnum))
        LOG.debug("filesize    :{0}".format(filesize))
        LOG.debug("verifycode  :{0}".format(verifycode))
        LOG.debug("lowergear   :{0}".format(lowergear))
        LOG.debug("separative  :{0}".format(separative))
        LOG.debug("splitfilenum:{0}".format(splitfilenum))
        LOG.debug("-----------------------------------------")

        return 0, {'datatype': datatype,
                   'encoding': encoding,
                   'recordnum': recordnum,
                   'filesize': filesize,
                   'verifycode': verifycode,
                   'lowergear': lowergear,
                   'separative': separative,
                   'splitfilenum': splitfilenum}

    def __get_ctl_info_from_json(self):
        pass

    def get_ctl_info(self):

        ret = 0
        ctl_info = []
        if self.__ctl_file_type == "TXT":
            ret, ctl_info = self.__get_ctl_info_from_ok()

        elif self.__ctl_file_type == "XML":
            ret, ctl_info = self.__get_ctl_info_from_xml()

        elif self.__ctl_file_type == "JSON":
            ret, ctl_info = self.__get_ctl_info_from_json()

        else:
            LOG.error("不支持的控制文件类型[{0}]".format(self.__ctl_file_type))
            return -1, ""

        if ret != 0:
            LOG.error("解析控制文件[{0}]出错".format(self.__ctl_file_type))

        return 0, ctl_info

