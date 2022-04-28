account="root"
password="987617162"
host="192.168.2.222"
post="3306"
database="fund"
update= 
echo "开始执行数据更新"
python ./fund_data_down.py -account=$account -password=$password -host=$host -post=$post -database=$database -update=$update
