# -*- coding: UTF-8 -*-
################################################################################
# Date Time     : 2018-10-29
# Write By      : adtec(xiazhy,zhaogx)
# Function Desc : 日志处理模块
#
# History       :
#                 20181029  zhaogx     Create
#
# Remarks       :
################################################################################
import os
import logging.handlers


class Logger(logging.Logger):
    """ 日志格式化输出,可设置日志打印级别用于调试
    """
    def __init__(self):
        super(Logger, self).__init__(self)

        # 创建一个handler，用于输出到控制台 
        console_handler = logging.StreamHandler() 

        # 根据环境变量DIDP_LOG_LEVEL判断是否输出DEBUG日志
        # DEBUG日志可用于调试,运行时不输出
        debug_level = os.getenv("DIDP_LOG_LEVEL")
        if debug_level == "DEBUG":
            console_handler.setLevel(logging.DEBUG) 
        else:
            console_handler.setLevel(logging.INFO) 

        # 定义handler的输出格式 
        formatter = logging.Formatter(fmt="[%(asctime)s][%(module)21s]"
                                      "[%(lineno)4d][%(levelname)5s] %(message)s",
                                      datefmt="%Y-%m-%d %H:%M:%S")
        console_handler.setFormatter(fmt=formatter) 

        # 给logger添加handler 
        self.addHandler(console_handler) 

