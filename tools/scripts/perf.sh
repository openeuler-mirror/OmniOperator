# please replace the path of the jar and async-profiler in your own machine

read -p "do you want run sql or benchmark: sql/bench " type

if [$type=="bench"]
then
  echo "please enter: use omni or not(omni or other words),concurrency,total page count,page distinct count,distinct value repeat count"
  read p1 p2 p3 p4 p5
  java -cp /opt/hong/async-profiler-2.0-linux-x64/OmniBenchmark-1.2.0-SNAPSHOT-jar-with-dependencies.jar operator.HashAggBench $p1 $p2 $p3 $p4 $p5 & /opt/hong/async-profiler-2.0-linux-x64/profiler.sh -e cpu start -f perf.html HashAggBench

else
  echo "run sql!!"
  /opt/hong/async-profiler-2.0-linux-x64/profiler.sh -e cpu start -f perf.html PrestoServer &
  java -jar /opt/hetu-server-1.2.0-SNAPSHOT/bin/hetu-cli-1.2.0-SNAPSHOT-executable.jar --server localhost:8080 --catalog hive --schema tpch100 --execute "explain analyze select  i_returnflag, suppkey, sum(i_quantity) as sum_qty, sum(i_discount) as sun_disc from longlineitem group by i_returnflag, suppkey ;" ;
  /opt/hong/async-profiler-2.0-linux-x64/profiler.sh -e cpu stop -f perf.html PrestoServer
fi
