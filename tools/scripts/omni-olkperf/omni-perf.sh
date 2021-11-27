#!/bin/bash
sqlpath=/opt/sql
server=localhost:8090
catalog=hive
schema=tpcds_100g_no_decimal
binpath=/opt/cicd/omniruntime/hetu-server-1.2.0-SNAPSHOT/bin
clientpath=$binpath/hetu-cli-1.2.0-SNAPSHOT-executable.jar
arthas_path=/root/tools/arthas/arthas-boot.jar
#nodes_ip="7.212.84.2 7.212.70.149 7.212.72.118"
nodes_ip="localhost"
sessionset="enable_dynamic_filtering=false"

#java -jar $clientpath --server $server --catalog $catalog --schema $schema -f /opt/nodecimal10g
time=`date +'%Y-%m-%d--%H:%M:%S'`
resultdir=/opt/omniruntime/result_omniruntime_$time

function restart()
{
    for ip in $nodes_ip
	do
		ssh $ip "source /etc/profile;$binpath/launcher restart"
	done
	sleep 8
	status=1
	while [[ $status != 0 ]]
	do
	    java -jar $clientpath --server $server --catalog $catalog --schema $schema --session $sessionset --execute "select * from system.runtime.nodes" &> /dev/null
	    status=$?
	done
	sleep 2
}

function getTiming(){ 
    start_time=$1
    end_time=$2
    
    start_s=${start_time%.*}
    start_nanos=${start_time#*.}
    end_s=${end_time%.*}
    end_nanos=${end_time#*.}
    
    if [ "$end_nanos" -lt "$start_nanos" ];then
        end_s=$(( 10#$end_s - 1 ))
        end_nanos=$(( 10#$end_nanos + 10**9 ))
    fi
    
# get timediff
    timems=$(( 10#$end_s - 10#$start_s )).`printf "%03d\n" $(( (10#$end_nanos - 10#$start_nanos)/10**6 ))`
    
    echo $timems
} 
function unittrans()
{
    timewithunit=$1
    time=`echo  "$timewithunit" | grep -oP '\d*\.\d+' `
    unit=`echo  "$timewithunit" | grep -oP '[a-z]*' `
    result=""
    case $unit in
        "ms")
            result=`echo "$time 1000" |awk '{printf("%.6f",$1/$2)}'`;;
        "s")
            result=`echo $time`;;
        "m")
            result=`echo "$time 60" |awk '{printf("%.3f",$1*$2)}'`;;
	"h")
            result=`echo "$time 3600" |awk '{printf("%.3f",$1*$2)}'`;;
    esac
    echo "$result"
}
function exe_py()
{
	querytime=$1
	if [[ -f _tmpfile.py ]]
	then
		rm -f _tmpfile.py
	fi
	touch _tmpfile.py
	chmod u+x _tmpfile.py
	echo "#!/usr/bin/python" >> _tmpfile.py
	echo "import sys" >> _tmpfile.py
	echo "import json" >> _tmpfile.py
	echo "from operator import itemgetter" >> _tmpfile.py
	echo "import requests" >> _tmpfile.py
	version=`python -c 'import sys; print(sys.version_info[:])'`
	pyversion=${version:1:1}
	if [[ $pyversion == 2 ]]
	then
		echo "sql=open('$file').read().replace(';','').strip()" >> _tmpfile.py
	else
		echo "sql=open('$file').read().replace(';','').replace('\r\n','\n').strip()" >> _tmpfile.py
	fi
	echo "headers={'X-Presto-User':'root'}" >> _tmpfile.py
	echo "response=requests.get(url='http://$server/v1/query',headers=headers)" >> _tmpfile.py
	echo "content=response.text" >> _tmpfile.py
	echo "print(sorted(filter(lambda  elem:elem['query'].replace('\r\n','\n').strip()==sql.replace(';','').replace('\r\n','\n').strip(),json.loads(content)), key=itemgetter('queryId'),reverse=True)[0]['queryStats']['$1'])" >> _tmpfile.py
	./_tmpfile.py 
	rm -f _tmpfile.py
}
 function run()
 {
 for i in $(seq 1 11) 
 do
    echo "-----------$filename--$i------------"
    java -jar $clientpath --server $server --catalog $catalog --schema $schema --session $sessionset --execute "$sqlcontent" > $resultdir/$filename/result/$i
    res=$?
	if [[ $res == 0 ]]
	then
		elapsedTimeUnit=`exe_py elapsedTime`
		elapsedTime=$(unittrans $elapsedTimeUnit)
		totalCpuTimeUnit=`exe_py totalCpuTime`
		totalCpuTime=$(unittrans $totalCpuTimeUnit)
	fi
        if [[ ! -f $cputime_result ]]
        then
            touch $cputime_result
            echo "omni--catalog:$catalog--schema:$schema--session:$sessionset" > $cputime_result
        fi
		if [[ ! -f $elapsedtime_result ]]
        then
            touch $elapsedtime_result
            echo "omni--catalog:$catalog--schema:$schema--session:$sessionset" > $elapsedtime_result
        fi
        if [[ $res == 0 ]]
        then
            echo -e -n "\t${elapsedTime}" >> $elapsedtime_result
			echo -e -n "\t${totalCpuTime}" >> $cputime_result
        else
            echo -e -n "\tError" >> $cputime_result
			echo -e -n "\tError" >> $elapsedtime_result
        fi
		sleep 2
done
 }

restart
if [[ ! -d $resultdir ]]
    then
        mkdir -p $resultdir
    fi
for file in $sqlpath/*.sql
do
	filename=${file##*/}
	if [[ ! -d $resultdir/$filename ]]
	then
	    mkdir -p $resultdir/$filename/result
	fi
    cputime_result=$resultdir/cputime_result
	elapsedtime_result=$resultdir/elapsedtime_result
	#restart

	echo -e -n "${filename%%.*}" >> $cputime_result
	echo -e -n "${filename%%.*}" >> $elapsedtime_result
	sqlcontent=`cat $file`
    run 
        echo -e -n "\n" >> $cputime_result
	echo -e -n "\n" >> $elapsedtime_result
	cat $elapsedtime_result
	cat $cputime_result
done


