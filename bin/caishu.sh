#!bin/bash
cat MIBS.txt | while read tb
do
tb=`echo ${tb}|sed -e 's/\\r//g'`
db2 connect to MIBS user db2inst1 using db2inst1
db2 "select count(*) from MIBS.${tb}" >>$tb.log
db2 "export to ${tb}.dat of del modified by nochardel select * from MIBS.${tb}" >>$tb.log
du -s ${tb}.dat>>$tb.log
db2 connect reset
done
