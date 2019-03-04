 #!bin/sh
 
DBSCHEMA=$1
DBUSER=$2
DBPASSWORD=$3
DBNAME=$4
EXPORTLIMIT=$5

cat MIBS.txt | while read tb
#while read tabnametmp
do
tabname=`echo ${tb}|sed -e 's/\\r//g'`
db2 connect to $4 user $2 using $3 >>$tabname.log
db2 "select count(*) from $1.$tabname where $5" >>$tabname.log
db2 "export to /home/moiaagent/test/$tabname.dat of del modified by nochardel select * from $1.$tabname where $5" >>$tabname.log

db2look -d $4 -i $2 -w $3 -a -e -t $tabname -o /home/moiaagent/test/$tabname.ddl >>$tabname.log

du -s /home/moiaagent/test/$tabname.dat  >>$tabname.log

#rm /home/moiaagent/test/$tabname.dat

db2 terminate

done 
