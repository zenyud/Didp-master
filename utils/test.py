import os
import sys

sys.path.append("{0}".format(os.environ["DIDP_HOME"]))

from utils.didp_db_operator import DbOperator


db = DbOperator("sharkstart", "000000", "oracle.jdbc.driver.OracleDriver",
                "jdbc:oracle:thin:@168.16.5.120:1521:orcl", "{0}/ojdbc6.jar".format(os.environ["DIDP_JDBC_DRIVER_PATH"]))


try:
    result_info = db.fetchall("select * from SKS_DEV_DB_ACC_INFO")
    
    print result_info
except:
    print "bala bala bala"

