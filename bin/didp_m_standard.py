#-*- coding: UTF-8 -*-
################################################################################
# Date Time     : 2018-12-18
# Write By      : adtec(xiazhy)
# Function Desc : 主档标准化组件
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
       __args : 参数
       __db_handle : 数据库连接句柄
       __tmp_table  : 临时表
       __src_sys    : 源系统名
       __src_tbl    : 源表名
       __yesterday  : 昨日
       __clean_flag: 清理标志
       __ins_num   : 加工到目标表的记录数
    """

    def __init__(self, args):

        self.__args = args

        self.__print_arguments()
  
        self.__tmp_table = self.__args.tartbl
        if self.__args.etltype.upper() == "MAPPING":
            self.__tmp_table = self.__args.tartbl
        else:
            if self.__args.tartbl[-1].upper() == "W":
                self.__tmp_table = self.__tmp_table[0:len(self.__tmp_table)-1] + "A"
            else:
                self.__tmp_table = self.__tmp_table[0:len(self.__tmp_table)-1] + "W"
            
        self.__db_handle = ProcessDbOper(AREA_NAME)

        self.__clean_flag = False

        self.__ins_num = 0

    def __del__(self):

        self.__db_handle.close()

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
        LOG.info("目标表[{0}]清理完成".format(self.__args.tartbl))

        # 清理中间表
        if self.__args.etltype.upper() == "ALLTOADD" or self.__args.etltype.upper() == "ADDTOALL":
           
            LOG.info("支持重跑，清理中间表数据")
            sql_str = ("TRUNCATE TABLE {0}.{1} "
                       "PARTITION(BATCH_DT='{2}', BANK_ID='{3}')"
                      ).format(self.__args.tardb, self.__tmp_table, 
                               self.__args.batchdt, self.__args.bankid)
            LOG.info(sql_str)

            self.__db_handle.do(sql_str)
            LOG.info("中间表[{0}]清理完成".format(self.__tmp_table))

        return 0 

    def __after_deal(self):
        return 0

    def __param_check(self):

        flag = True

        if not DateOper.isValidDate(self.__args.batchdt):
            LOG.error("跑批日期输入错误[" + self.__args.batchdt + "]")
            flag = False

        stype_list = ['ADD', 'ALL']
        if not self.__args.stype.upper() in stype_list:
            LOG.error("不能识别的源档类型[" + self.__args.stype + "]")
            flag = False

        etltype_list = ['ALLTOADD', 'ADDTOALL', 'MAPPING']
        if not self.__args.etltype.upper() in etltype_list:
            LOG.error("不能识别的加工类型[" + self.__args.etltype + "]")
            flag = False

        if flag == False:
            return flag

        if self.__args.etltype.upper() == "ALLTOADD":

            if self.__args.stype.upper() != "ALL":
                LOG.error("源为增量，加工类型不能配置为全求增")
                flag = False

            if self.__args.tartbl[-2:].upper() != "_A":
                LOG.error("全求增的目标表表命名不合规范{0},后缀应为_W".format(self.__args.tartbl))
                flag = False

        elif self.__args.etltype.upper() == "ADDTOALL":

            if self.__args.stype.upper() != "ADD":
                LOG.error("源为全量，加工类型不能配置为增求全")
                flag = False

            if self.__args.tartbl[-2:].upper() != "_W":
                LOG.error("增求全的目标表表命名不合规范{0},后缀应为_W".format(self.__args.tartbl))
                flag = False

        elif self.__args.etltype.upper() == "MAPPING":

            if self.__args.stype.upper() == "ADD":
                if self.__args.tartbl[-2:].upper() != "_A":
                    LOG.error("增量映射的目标表表命名不合规范{0},后缀应为_W".format(self.__args.tartbl))
                    flag = False

            if self.__args.stype.upper() == "ALL":
                if self.__args.tartbl[-2:].upper() != "_W":
                    LOG.error("全量映射的目标表表命名不合规范{0},后缀应为_W".format(self.__args.tartbl))
                    flag = False

        return flag

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
        LOG.info("源档类型            : {0}".format(self.__args.stype))
        LOG.info("加工类型            : {0}".format(self.__args.etltype))
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
        if col == 'ETL_DT':
            return "{0}"
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

        return row
        

    def __addtoall_algo(self, col_list):

        # 字段清单
        insert_cols_str = "\n,".join(col_list)

        # 全字段比较列表
        comp_str = self.__all_column_compare_str(col_list)

        # 主键比较列表
        pk_comp_str = self.__pk_column_compare_str()

        # 求出增量切片表的真增量数据
        LOG.info("清理虚增数据")
        sql_str = (
            "DELETE FROM {0}.{1} PARTITION(BANK_ID='{2}', BATCH_DT='{3}') T1"
          "\nWHERE EXISTS "
          "\n  ("
          "\n    SELECT 1 FROM {0}.{4} T2 "
          "\n      WHERE T2.BANK_ID='{2}' AND T2.BATCH_DT='{5}'"
          "\n        AND {6}"
          "\n  )"
        ).format(self.__args.tardb, self.__tmp_table, self.__args.bankid, 
                 self.__args.batchdt, self.__args.tartbl, self.__yesterday, comp_str)
        LOG.info(sql_str)
        rownum = self.__db_handle.do(sql_str)
        LOG.info("影响行数:"+str(rownum))
        if rownum == -1:
            LOG.error("执行SQL失败")
            raise

        # 拼接SQL语句
        sql_str = (
            "INSERT INTO TABLE {0}.{1} PARTITION(BANK_ID='{2}', BATCH_DT='{3}')"
          "\n("
          "\n {4}"
          "\n)"
          "\nSELECT"
          "\n {4}"
          "\nFROM {0}.{1} T1"
          "\nWHERE T1.BANK_ID='{2}' AND T1.BATCH_DT='{3}' AND NOT EXISTS"
          "\n("
          "\n  SELECT 1 FROM {0}.{5} T2"
          "\n  WHERE T2.BANK_ID='{2}' AND T2.BATCH_DT='{3}'"
          "\n    AND {6}"
          "\n)"
          "\nUNION"
          "\n SELECT"
          "\n {4}"
          "\nFROM {0}.{5} WHERE BANK_ID='{2}' AND BATCH_DT='{3}'"
        ).format(self.__args.tardb, self.__args.tartbl, self.__args.bankid, 
                 self.__args.batchdt, insert_cols_str, self.__tmp_table, pk_comp_str )
        LOG.info(sql_str)
        rownum = self.__db_handle.do(sql_str)
        LOG.info("影响行数:"+str(rownum))

        return rownum

    def __alltoadd_algo(self, col_list):

        # 字段清单
        insert_cols_str = "\n,".join(col_list)

        # 全字段比较列表
        comp_str = self.__all_column_compare_str(col_list)

        # 主键比较列表
        pk_comp_str = self.__pk_column_compare_str()

        # 求出增量到目标表
        LOG.info("比对增量数据插入目标表")
        sql_str = (
            "INSERT INTO TABLE {0}.{1} PARTITION(BANK_ID='{2}', BATCH_DT='{3}') "
          "\n("
          "\n {4}"
          "\n)"
          "\nSELECT"
          "\n {4}"
          "\nFROM {0}.{5} T1"
          "\nWHERE BANK_ID='{2}' AND BATCH_DT='{3}' AND NOT EXISTS "
          "\n  ("
          "\n    SELECT 1 FROM {0}.{5} T2 "
          "\n      WHERE T2.BANK_ID='{2}' AND T2.BATCH_DT='{6}'"
          "\n        AND {7}"
          "\n  )"
        ).format(self.__args.tardb, self.__args.tartbl, self.__args.bankid, self.__args.batchdt, 
                 insert_cols_str, self.__tmp_table, self.__yesterday, comp_str)
        LOG.info(sql_str)
        rownum = self.__db_handle.do(sql_str)
        LOG.info("影响行数:"+str(rownum))

        return rownum

    def __mapping_algo_m(self, col_list):
        
        # 字段清单
        insert_cols_str = "\n,".join(col_list)

        # 全字段比较列表
        comp_str = self.__all_column_compare_str(col_list)

        # 主键比较列表
        pk_comp_str = self.__pk_column_compare_str()

        # 步骤2.1 直接映射到目标表
        LOG.info("步骤2.1 映射数据到目标表")
        sql_str = (
            "INSERT INTO TABLE {0}.{1} PARTITION(BANK_ID='{2}', BATCH_DT='{3}') "
          "\n("
          "\n {4}"
          "\n)"
          "\nSELECT"
          "\n {4}"
          "\nFROM {0}.{5} T1"
          "\nWHERE BANK_ID='{2}' AND BATCH_DT='{3}'"
        ).format(self.__args.tardb, self.__args.tartbl, self.__args.bankid, self.__args.batchdt, 
                 insert_cols_str, self.__tmp_table)

        LOG.info(sql_str)
        rownum1 = self.__db_handle.do(sql_str)
        LOG.info("影响行数:"+str(rownum1))

        '''
        # 步骤2.2 如果来源是全量数据库，目标表求出减量
        if self.__args.stype.upper() == 'ALL':
            LOG.info("步骤2.2 求出减量数据")
            sql_str = (
                "UPDATE {0}.{1} PARTITION(BANK_ID='{2}', BATCH_DT='{3}') T1"
              "\nSET DEL_FLAG = '1' DEL_DT = '{3}' "
              "\nWHERE NOT EXISTS ("
              "\n  SELECT 1 FROM {0}.{4} T2"
              "\n  WHERE BANK_ID='{2}' AND BATCH_DT='{3}'"
              "\n    AND {5}"
              "\n)"
            ).format(self.__args.tardb, self.__args.tartbl, self.__args.bankid, self.__args.batchdt, 
                     self.__tmp_table, pk_comp_str)

            LOG.info(sql_str)
            rownum2 = self.__db_handle.do(sql_str)
            LOG.info("影响行数:"+str(rownum2))
        '''

        return rownum1

    def __mapping_algo_f(self, col_list):

        # 字段清单
        insert_cols_str = "\n,".join(col_list)

        # 全字段比较列表
        comp_str = self.__all_column_compare_str(col_list)

        # 主键比较列表
        pk_comp_str = self.__pk_column_compare_str()

        # 步骤2.1 直接映射到目标表
        LOG.info("步骤2.1 映射数据到目标表")
        sql_str = (
            "INSERT INTO TABLE {0}.{1} PARTITION(BANK_ID='{2}', BATCH_DT='{3}') "
          "\n("
          "\n {4}"
          "\n)"
          "\nSELECT"
          "\n {4}"
          "\nFROM {0}.{5} T1"
          "\nWHERE NOT EXISTS "
          "\n  ("
          "\n    SELECT 1 FROM {0}.{1} T2"
          "\n    WHERE {6}"
          "\n      AND T1.{7} = T2.{7}"
          "\n  )"
          "\n  AND BANK_ID='{2}' AND BATCH_DT='{3}'"
        ).format(self.__args.tardb, self.__args.tartbl, self.__args.bankid, self.__args.batchdt, 
                 insert_cols_str, self.__tmp_table, pk_comp_str, self.__args.bzdtcol)

        LOG.info(sql_str)
        rownum = self.__db_handle.do(sql_str)
        LOG.info("影响行数:"+str(rownum))

        return 0

    def before_deal(self):

        # 校验参数
        if not self.__param_check():
            LOG.error("参数校验失败")
            return -1

        # 连接目标库
        self.__db_handle.connect()

        # 初始化参数
        self.__yesterday = DateOper.getYesterday(self.__args.batchdt)

        lck_file = "{0}/{1}/{2}.lck".format(RUNLOCKDIR, AREA_NAME, os.path.basename(sys.argv[0]))
        if not os.path.exists(lck_file):
            self.__touch_file(lck_file)
            LOG.info("作业当日首次运行,创建作业标志文件[" + lck_file + "]")
        else:
            LOG.info("作业运行过,需要进行清理")
            self.__clean_flag = True

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
        #self.__ins_num = self.__ins_num + row

        # 步骤二：加工处理
        row = 0
        if self.__args.etltype.upper() == "ADDTOALL":
            LOG.info("步骤2：增求全算法处理")
            row = self.__addtoall_algo(column_list)
        elif self.__args.etltype.upper() == "ALLTOADD":
            LOG.info("步骤2：全求增算法处理")
            row = self.__alltoadd_algo(column_list)
        elif self.__args.etltype.upper() == "MAPPING":
            #直接映射数据直接入目标表，不做任何处理
            LOG.info("步骤2：映射算法处理")
            row = self.__mapping_algo_m(column_list)
        else:
            LOG.error("标准层加工类型配置有误[{0}]".format(self.__args.etltype))
            return -1;

        if ret != 0: 
            LOG.error("标准化处理失败")
            return -1

        self.__ins_num = row

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
    parser.add_argument("-stype",  required=True, help=("源档类型,add:增量 all:全量 "))
    parser.add_argument("-etltype",required=True, help=("加工类型,addtoall:增求全 "
                                                        "alltoadd:全求增 "
                                                        "mapping:直接映射"))
    parser.add_argument("-pklist", required=True, help="主键字段列表")
    parser.add_argument("-bankid", required=True, help="机构号")
    parser.add_argument("-batchdt",required=True, help="加工日期")

    args = parser.parse_args()

    # 调用标准层加工类
    LOG.info("主档表标准层加工处理")
    sp = StandardProcessor(args)
    ret = sp.run()
    if ret == 0:
        LOG.info("主档表标准层加工处理完成")
        exit(0)
    else:
        LOG.error("主档表标准层加工处理失败")
        exit(ret)
