## 2.9.10
## Attempting to model using the higher-level ops (rather than primitives)

data = as.data.frame(read.csv(file="~/Desktop/thread-logs2/Thread-11.csv"))

dim(data)
colnames(data)
data[1:10,]

data[data$queryNum==1,]
sum(data[data$queryNum==1 & data$opLevel==2,"latency_ms"])/data[data$queryNum==1 & data$opLevel==3,"latency_ms"] # portion of total query latency accounted for by ops (level 2)

# TODO:  get this percentage for all queries (& plot distr)

# Make models

# "prefixGet" => opLevel=2, opType=2
par(mar=c(5,5,4,2)+0.1)
h.prefixGet = hist(data[data$opLevel==2 & data$opType==2, "latency_ms"], breaks=15, xlab="Latency (ms)", main="prefixGet Latency Distribution")

h.sDI = hist(data[data$opLevel==2 & data$opType==3, "latency_ms"], breaks=15, xlab="Latency (ms)", main="sDI Latency Distribution")

h.materialize = hist(data[data$opLevel==2 & data$opType==6, "latency_ms"], breaks=15, xlab="Latency (ms)", main="materialize Latency Distribution")

# TODO:  make breaks choice fair (to differences in range) 

# NEXT:  Try sampling & cf distr(training data, sampled)

nsamples = 1000

samples = matrix(nrow=1,ncol=nsamples)

for (i in 1:nsamples) {
	pG = sample(h.prefixGet$mids, 1, replace=TRUE, prob=h.prefixGet$density)

	sDI = sample(h.sDI$mids, 1, replace=TRUE, prob=h.sDI$density)
	
	m = sample(h.materialize$mids, 1, replace=TRUE, prob=h.materialize$density)
	
	samples[i] = pG + sDI + m
}

par(mfrow=c(2,1))
hist(samples, xlim=c(0,400), breaks=25, main="Sampled Data", xlab="Query Latency (ms)")
hist(data$latency_ms[data$opLevel==3], xlim=c(0,400), breaks=25, main="Actual Data", xlab="Query Latency (ms)")





