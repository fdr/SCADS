# Script for parsing an experiment's training & validation data files.
#
# Arguments:
# $1:  experiment's directory
# $2:  # validation runs
# $3:  starting thread of interest
# $4:  ending thread of interest
# $5:  num threads
#
# Arguments for runfile-parser.sh:
# $1:  log file
# $2:  output dir
# $3:  starting thread of interest
# $4:  ending thread of interest
# $5:  num threads

echo Deinterleaving training data
./runfile-parser.sh $1/training-out.txt $1/training-logs $3 $4 $5

for i in `jot $2 1 $2`;
do echo Deinterleaving validation $i data;
./runfile-parser.sh $1/validation$i-out.txt $1/validation$i-logs $3 $4 $5;
done