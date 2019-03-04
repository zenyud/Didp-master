#-*- coding: UTF-8 -*-
################################################################################
# Date Time     : 2018-12-18
# Write By      : adtec(xiazhy)
# Function Desc : 流水档标准化组件
#
# History       :
#                 20181218  xiazhy     Create
#
# Remarks       :
################################################################################
import re
import os
import sys
import argparse

sys.path.append("{0}".format(os.environ["DIDP_HOME"]))

reload(sys)
sys.setdefaultencoding( "utf-8" )

from datetime import datetime
from utils.didp_process_tools import Logger, DateOper, ProcessDbOper

# 全局变量
LOG = Logger()

CODE_TRANS_TABLE="DWSFDSDB.F_CL_CD_MPNG_TBL"
AREA_NAME ="SDS"

RUNLOCKDIR="/tmp/didp_run_lock"

class StandardProcessor(object):
    """ 标准层加工类

    Attributes:
        __args     : 命令行参数
        __db_handle: 数据库连接句柄
        __tmp_table : 临时表
        __src_sys   : 源系统名
        __src_tbl   : 源表名
        __clean_flag: 清理标志
        __ins_num   : 加工到目标表的记录数
    """

    def __init__(self, args):

        self.__args = args

        self.__print_arguments()
  
        self.__tmp_table = self.__args.tartbl
            
        self.__db_handle = ProcessDbOper(AREA_NAME)

        self.__ins_num = 0

    def __del__(self):

        self.__db_handle.close()

    def __param_check(self):

        flag = True

        if not DateOper.isValidDate(self.__args.batchdt):
            LOG.error("跑批日期输入错误[" + self.__args.batchdt + "]")
            flag = False

        #stype_list = ['ADD', 'ALL']
        #if not self.__args.stype.upper() in stype_list:
        #    LOG.error("不能识别的源档类型[" + self.__args.stype + "]")
        #    flag = False

        return flag

    def __touch_file(self,file):

        dir = os.path.dirname(file)

        if not os.path.exists(dir):
            os.makedirs(dir)

        if not os.path.exists(dir):
            return -1

        f = open(file,'w')
        f.close()

        return 0

    def negative_deal(self):

        # 清理目标表
        LOG.info("支持重跑，清理目标表数据")
        sql_str = ("TRUNCATE TABLE {0}.{1} "
                   "PARTITION(BATCH_DT='{2}', BANK_ID='{3}')"
                  ).format(self.__args.tardb, self.__args.tartbl, 
                           self.__args.batchdt, self.__args.bankid)
        LOG.info(sql_str)

        self.__db_handle.do(sql_str)
        LOG.info("当日数据清理完成")

        return 0

    def before_deal(self):

        # 校验参数
        if not self.__param_check():
            LOG.error("参数校验失败")
            return -1

        # 连接目标库
        self.__db_handle.connect()

        # 初始化参数
        self.__clean_flag = False

        lck_file = "{0}/{1}/{2}.lck".format(RUNLOCKDIR, AREA_NAME, os.path.basename(sys.argv[0]))
        if not os.path.exists(lck_file):
            self.__touch_file(lck_file)
            LOG.info("作业当日首次运行,创建作业标志文件[" + lck_file + "]")
        else:
            LOG.info("作业运行过,需要进行清理")
            self.__clean_flag = True

        return 0

    def __after_deal(self):
        return 0

    def __print_arguments(self):
        """ 参数格式化输出

        Args:
            None
        Returns:
            None
        Raise:
            None
        """
        LOG.info("-------------------参数清单-------------------")
        LOG.info("目标表名            : {0}".format(self.__args.tartbl))
        LOG.info("源表名              : {0}".format(self.__args.srctbl))
        #LOG.info("源档类型            : {0}".format(self.__args.stype))
        LOG.info("主键字段列表        : {0}".format(self.__args.pklist))
        LOG.info("机构号              : {0}".format(self.__args.bankid))
        LOG.info("跑批日期            : {0}".format(self.__args.batchdt))

    def __table_desc(self, table):

        sql_str = "DESC " + self.__args.tardb + "." + table;

        LOG.debug(sql_str)

        try:
            result_info = self.__db_handle.fetchall(sql_str)
        except:
            traceback.print_exc()
            return -1, ''

        col_list = []
        part_col_list = []
        part_flag = 0;
        for i in result_info:
            if i[0] == '# Partition Information':
                part_flag = 1
                continue
            if part_flag == 0:
               col_list.append(i[0].upper())
            else:
               part_col_list.append(i[0].upper())
        
        out_col_list = []
        part_flag = 0
        for i in col_list:
            part_flag = 0
            for j in part_col_list:
                if i == j:
                    part_flag = 1
                    break;
            if part_flag == 0:
                out_col_list.append(i)

        return out_col_list

    def __tech_col_replace(self, col):
        tech_col_list = ['SCRIPT_NAME', 'ETL_DT', 'DEL_DT']
        if col == 'SCRIPT_NAME':
            return os.path.basename(sys.argv[0]).split(".")[0];
        else:
            return col

    def __get_code_conv_list(self, src_table):
        
        # 根据R层表名分离出源系统和源表名
        arr = src_table.split("_")
        self.__src_sys = arr[2]
        self.__src_tbl = arr[3]

        sql_str = (
            "SELECT DISTINCT SRC_SYS_CD_FLD_ENG_NM"
          "\n  FROM {2}"
          "\n  WHERE UPPER(SRC_SYS_ID)=UPPER('{0}') AND UPPER(SRC_SYS_TBL_ENG_NM)=UPPER('{1}')"
        ).format(self.__src_sys, self.__src_tbl, CODE_TRANS_TABLE)

        LOG.info(sql_str)

        try:
            result_info = self.__db_handle.fetchall(sql_str)
        except:
            traceback.print_exc()
            return -1, ''

        code_conv_list = []
        for i in result_info:
            code_conv_list.append(i[0].upper())

        return code_conv_list

    def __conv_select_list(self, col_list, conv_col_list):

        no = 2
        out_list = []
        join_list = []

        for i in col_list:
            flag = False
            for j in conv_col_list:
                if i.upper() == j.upper():
                    flag = True
                    out_list.append(("CASE WHEN T{0}.TRGT_CD IS NULL "
                                     "THEN '#'||T1.{1} ELSE T{0}.TRGT_CD END"
                                    ).format(no,i)
                                   )
                    join_list.append(("LEFT JOIN {0} T{1} "
                                     "\n  ON T{1}.SRC_SYS_ID = '{2}' AND T{1}.SRC_SYS_TBL_ENG_NM = '{3}' AND T{1}.SRC_SYS_CD_FLD_ENG_NM = T1.{4}"
                                     ).format(CODE_TRANS_TABLE, no, self.__src_sys, self.__src_tbl, j))
                    no += 1
                    break;
            if flag == False:
                #out_list.append("T1.{0}".format(self.__tech_col_replace(i)))
                out_val = self.__tech_col_replace(i)
                if out_val == i:
                    out_list.append("T1.{0}".format(i))
                else:
                    out_list.append("'" + out_val  +"'")

        return ( out_list, join_list )

    def __all_column_compare_str(self, col_list):
        ''' 获取所有字段比对的列表
        '''

        compare_list = []
        for i in col_list:
            compare_list.append("T1.{0} = T2.{0}".format(i))

        compare_str = "\n        AND ".join(compare_list)
        return compare_str

    def __pk_column_compare_str(self):
        ''' 获取所有字段比对的列表
        '''
        pk_list = self.__args.pklist.split(",")
        pk_compare_list = []
        for i in pk_list:
            pk_compare_list.append("T1.{0} = T2.{0}".format(i))

        pk_compare_str = "\n        AND ".join(pk_compare_list)
        return pk_compare_str

    def __standard_to_tmp_table(self, col_list, conv_col_list):
        
        # INSERT 字段列表
        insert_cols_str = "\n,".join(col_list)

        # SELECT 字段列表
        (select_list, join_list) = self.__conv_select_list(col_list, conv_col_list)
        select_cols_str = "\n,".join(select_list)
        join_str = "\n  ".join(join_list)
        
        # 拼接SQL语句
        sql_str = (
            "INSERT INTO TABLE {0}.{1} PARTITION(BANK_ID='{2}', BATCH_DT='{3}')"
          "\n("
          "\n {4}"
          "\n)"
          "\nSELECT"
          "\n {5}"
          "\nFROM {6}.{7} T1"
          "\n  {8}"
        ).format(self.__args.tardb, self.__tmp_table, self.__args.bankid, 
                 self.__args.batchdt, insert_cols_str, select_cols_str, 
                 self.__args.srcdb, self.__args.srctbl, join_str)

        LOG.info(sql_str)

        row = self.__db_handle.do(sql_str)
        LOG.info("影响行数:"+str(row))
        if row == -1:
            LOG.error("执行SQL失败")
            return -1

        return row
        

    def __clean_not_real_data(self, col_list):

        # 字段清单
        insert_cols_str = "\n,".join(col_list)

        # 全字段比较列表
        comp_str = self.__all_column_compare_str(col_list)

        # 主键比较列表
        pk_comp_str = self.__pk_column_compare_str()

        # 步骤2 清理非增量数据
        LOG.info("步骤2 清理目标表重复数据")
        sql_str = (
            "DELETE FROM {0}.{1} PARTITION(BANK_ID='{2}', BATCH_DT='{3}') T1"
          "\nWHERE EXISTS "
          "\n  ("
          "\n    SELECT 1 FROM {0}.{1} T2"
          "\n    WHERE T2.BANK_ID='{2}' AND T2.BATCH_DT<'{3}'"
          "\n        AND {6}"
          "\n        AND T1.{7} = T2.{7}"
          "\n  )"
        ).format(self.__args.tardb, self.__args.tartbl, self.__args.bankid, self.__args.batchdt, 
                 insert_cols_str, self.__tmp_table, pk_comp_str, self.__args.bzdtcol)

        LOG.info(sql_str)
        rownum = self.__db_handle.do(sql_str)
        LOG.info("影响行数:"+str(rownum))

        return 0

    def positive_deal(self):

        # 获取目标表结构信息
        column_list = self.__table_desc(self.__args.tartbl)
        LOG.debug(column_list)

        # 获取需要转码的字段
        LOG.info("获取需要代码转换的字段列表")
        conv_column_list = self.__get_code_conv_list(self.__args.srctbl)
        LOG.debug(conv_column_list)

        # 步骤一：标准化处理
        LOG.info("步骤1：数据标准化入目标表")
        row = self.__standard_to_tmp_table(column_list, conv_column_list)
        if row < 0: 
            LOG.error("数据标准化入目标表失败")
            return -1
        self.__ins_num = self.__ins_num + row

        # 步骤二：清理虚增数据
        row = self.__clean_not_real_data(column_list)
        if row < 0: 
            LOG.error("标准化处理失败")
            return row

        self.__ins_num = self.__ins_num - row

        return 0

    def run(self):


        start_time = datetime.now()  # 记录开始时间

        # 前处理
        ret = self.before_deal()
        if ret != 0:
            LOG.error("前处理执行失败")
            return -1

        # 需要清理，则运行反向处理
        if self.__clean_flag == True:
            ret = self.negative_deal()
            if ret != 0: 
                LOG.error("反向处理逻辑失败")
                return -1

        # 运行正向处理
        ret = self.positive_deal()
        if ret != 0: 
            LOG.error("正向处理逻辑失败")
            return -1

        # 后处理
        ret = self.__after_deal()
        if ret != 0:
            LOG.error("后处理执行失败")
            return -1
        
        end_time = datetime.now()  # 记录结束时间
        cost_time = (end_time - start_time).seconds  # 计算耗费时间
        LOG.info("作业运行总耗时:{0}s".format(cost_time))
        LOG.info("作业实际入库记录数:{0}".format(self.__ins_num))

        return 0

# main
if __name__ == "__main__":
    ret = 0  # 状态变量

    # 参数解析
    parser = argparse.ArgumentParser(description="标准层加工组件")
    parser.add_argument("-tardb",  required=True, help="目标库名")
    parser.add_argument("-tartbl", required=True, help="目标表名")
    parser.add_argument("-srcdb",  required=True, help="源库名")
    parser.add_argument("-srctbl", required=True, help="源表名")
    parser.add_argument("-pklist", required=True, help="主键字段列表")
    parser.add_argument("-bankid", required=True, help="机构号")
    parser.add_argument("-batchdt",required=True, help="加工日期")
    parser.add_argument("-bzdtcol",required=True, help="业务日期字段名")

    args = parser.parse_args()

    LOG.info("流水表标准层加工处理")
    sp = StandardProcessor(args)
    ret = sp.run()
    if ret == 0:
        LOG.info("流水表标准层加工处理完成")
        exit(0)
    else:
        LOG.error("流水表标准层加工处理失败")
        exit(ret)
