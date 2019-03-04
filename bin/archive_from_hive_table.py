# -*- coding: UTF-8 -*-
# Date Time     : 2019/1/2
# Write By      : adtec(ZENGYU)
# Function Desc :  归档执行主类
# History       : 2019/1/2  ZENGYU     Create
# Remarks       :
import os
import sys

reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append("{0}".format(os.environ["DIDP_HOME"]))
from archive.archive_way import *


def get_archive_way(save_mode):
    return {
        "1": AllArchive,
        "2": AddArchive,
        "4": ChainTransArchive,
        "5": LastAddArchive,
        "6": LastAllArchive
    }.get(save_mode)


def main():
    args = ArchiveData.archive_init()
    # 判断归档模式 执行不同的实现类

    save_mode = args.saveMd
    archive_class = get_archive_way(save_mode)
    archive = archive_class()

    if archive:
        ret = archive.run()
        if ret == 0:
            exit(0)
        else:
            exit(-1)


if __name__ == '__main__':
    main()
