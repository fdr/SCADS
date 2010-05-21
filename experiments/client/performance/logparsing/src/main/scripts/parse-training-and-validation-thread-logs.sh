## NOTE:  this version is for my desktop

# Parse per-thread logs from training & validation runs using ParsePerThreadLogs
#
# $1:  experiment directory
# $2:  # validation runs

cd $1

echo Parsing training per-thread logs...
java -DlogDir=training-logs -cp ~/Desktop/scads/experiments/client/performance/logparsing/target/logparsing-1.0-SNAPSHOT-jar-with-dependencies.jar parser.ParsePerThreadLogs

for i in `jot $2 1 $2`;
do echo Parsing validation $i per-thread logs...;
java -DlogDir=validation$i-logs -cp ~/Desktop/scads/experiments/client/performance/logparsing/target/logparsing-1.0-SNAPSHOT-jar-with-dependencies.jar parser.ParsePerThreadLogs;
done
