# Script for parsing output of run of query generator
#
# $1:  log file
# $2:  output dir
# $3:  starting thread of interest
# $4:  ending thread of interest
# $5:  num threads

mkdir -p $2

#for i in `seq $3 $4`;
#for i in {$3..$4};
for i in `jot $5 $3 $4`;
do echo Creating file for Thread-$i;
fgrep Thread-$i $1 > $2/Thread-$i.log;
done