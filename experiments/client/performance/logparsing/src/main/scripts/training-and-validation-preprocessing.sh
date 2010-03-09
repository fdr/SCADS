## Complete script to perform all preprocessing
#
# Meant to be run on R cluster.
#
# Args:
# $1:  experiment's directory
# $2:  # validation runs
# $3:  starting thread of interest
# $4:  ending thread of interest
# $5:  parser's directory

# DEINTERLEAVING => One file/run -> one file/thread
mkdir -p $1/training-logs
mkdir -p $1/validation-logs
mv $1/validation*-out.txt $1/validation-logs

echo Deinterleaving training data
./runfile-parser.sh $1/training-out.txt $1/training-logs $3 $4

#for i in `jot $2 1 $2`;
for i in `seq 1 $2`
do echo Deinterleaving validation $i data;
./runfile-parser.sh $1/validation-logs/validation$i-out.txt $1/validation-logs/validation$i-logs $3 $4;
done


# PARSING => CSV per thread
echo Parsing training per-thread logs...
java -DlogDir=$1/training-logs -cp $5/logparsing-1.0-SNAPSHOT-jar-with-dependencies.jar parser.ParsePerThreadLogs

#for i in `jot $2 1 $2`;
for i in `seq 1 $2`
do echo Parsing validation $i per-thread logs...;
java -DlogDir=$1/validation-logs/validation$i-logs -cp $5/logparsing-1.0-SNAPSHOT-jar-with-dependencies.jar parser.ParsePerThreadLogs;
done

