#!/bin/bash
time=`date +'%Y-%m-%d--%H:%M:%S'`
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
    esac
    echo "$result"
}
 function run()
 {
 for i in $(seq 1 6) 
 do
    echo "-----------$filename--$i------------"
    java -jar $clientpath --server $server --catalog $catalog --schema $schema --session $sessionset --execute "$sqlcontent" &> /dev/null
    res=$?
    if [[ ! -d $resultdir/${filename%.*} ]]
    then
        mkdir -p $resultdir/${filename%.*}
    fi
    resultfile=$resultdir/${filename%.*}/result_$filename
    if [[ ! -f $resultfile ]]
    then
        touch $resultfile
	echo -e -n "sql\tloopindex\toperatorType\tstageId\tpipelineId\taddInputWall\taddInputCpu\tgetOutputWall\tgetOutputCpu\tfinishWall\tfinishCpu\tinputPositions\tinputDataSize\toutputPositions\toutputDataSize\n" > $resultfile
    fi
    if [[ $res == 0 ]]
    then
        ./omni-operator-analyze.py "queryInfo" http://$server $resultfile $i ${filename%.*} "$file"
    else
        echo -e -n "ERROR\tERROR\tERROR\tERROR\tERROR\tERROE\tERROE\tERROE\tERROE\tERROE\tERROE\tERROE\tERROE\tERROE\n" >> $resultfile
    fi
done
./omni-operator-analyze.py 'avgValue' $resultdir/${filename%.*}
 }
 
function runAndAvg()
{
	if [[ ! -d $resultdir ]]
	then
		mkdir -p $resultdir
	fi
	restart
	for file in $sqlpath/*.sql
	do
		filename=${file##*/}
	#	restart
		sqlcontent=`cat $file`
		run
	done
}
#common config
sqlpath=/opt/sql
catalog=hive
schema=tpcds_10g_no_decimal
arthas_path=/root/tools/arthas/arthas-boot.jar
nodes_ip="localhost"
sessionset="enable_dynamic_filtering=false"

#omniruntime config
server=localhost:8090
binpath=/root/code/lab/hetu-core/hetu-server/target/hetu-server-1.2.0-SNAPSHOT/bin
clientpath=$binpath/hetu-cli-1.2.0-SNAPSHOT-executable.jar
resultdir=/opt/omniruntime/result_omnioperator_$time

runAndAvg

#olk1.2 config
server=localhost:8080
binpath=/opt/olk/hetu-server-1.2.0/bin
clientpath=$binpath/hetu-cli-1.2.0-executable.jar
resultdir=/opt/omniruntime/result_olkoperator_$time

runAndAvg

./omni-operator-analyze.py 'comparefile' ${resultdir/olk/omni} $resultdir
