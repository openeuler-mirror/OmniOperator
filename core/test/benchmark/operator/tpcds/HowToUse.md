1. follow this guideline to dump the tpcds data: https://codehub-y.huawei.com/lWX1158207/data-dump-java/files?ref=join-refactor&filePath=README.md&isFile=true
   you can dump top5 or top10 sqls of different operators,you can refer to this doc(page:"sort on operator") to find the top sqls: https://onebox.huawei.com/p/963a6f0f54893c57b0f7023c6d805a89
2. build this project ,and put the "ombenchmark" executable file to the data dump path
3. run the benchmark with a filter: ombenchmark --benchmark_filter=TpcDs* 