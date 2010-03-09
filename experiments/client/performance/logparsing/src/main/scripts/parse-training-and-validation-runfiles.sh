# Script for parsing an experiment's training & validation data files.
#
# Arguments:
# $1:  experiment's directory
# $2:  # validation runs
# $3:  starting thread of interest
# $4:  ending thread of interest
# $5:  num threads -- DEPRECATED FOR R CLUSTER VERSION
#
# Arguments for runfile-parser.sh:
# $1:  log file
# $2:  output dir
# $3:  starting thread of interest
# $4:  ending thread of interest
# $5:  num threads -- DEPRECATED FOR R CLUSTER VERSION

mkdir -p $1/training-logs
mkdir -p $1/validation-logs
mv $1/validation*-out.txt $1/validation-logs

echo Deinterleaving training data
#./runfile-parser.sh $1/training-out.txt $1/training-logs $3 $4 $5
./runfile-parser.sh $1/training-out.txt $1/training-logs $3 $4

#for i in `jot $2 1 $2`;
for i in `seq 1 $2`
do echo Deinterleaving validation $i data;
#./runfile-parser.sh $1/validation-logs/validation$i-out.txt $1/validation-logs/validation$i-logs $3 $4 $5;
./runfile-parser.sh $1/validation-logs/validation$i-out.txt $1/validation-logs/validation$i-logs $3 $4;
done