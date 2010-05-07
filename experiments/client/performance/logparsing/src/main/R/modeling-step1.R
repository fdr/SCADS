# Try to match the userByEmail latency distribution using the op hists from my op benchmarking

rm(list=ls())
load("~/Desktop/op2-hists/i=10,j=10,k=10,l=10.RData")
ls()
h2 = h
op2Bin = bin
op2Bin[1:10,]

load("~/Desktop/op3-hists/i=10,j=10,k=10,l=10.RData")
h3 = h
op3Bin = bin
ls()

load("~/Desktop/op6-hists/i=10,j=10,k=10,l=10.RData")
h6 = h
op6Bin = bin
ls()


userByEmailSampler = function(h2, h3, h6, sampleID, numSamples) {
	samples=matrix(data=0, nrow=1, ncol=numSamples)

	for (i in 1:numSamples) {
		samples[i] = samples[i] + sample(h2$mids, 1, replace=TRUE, prob=h2$density)
		samples[i] = samples[i] + sample(h3$mids, 1, replace=TRUE, prob=h3$density)
		samples[i] = samples[i] + sample(h6$mids, 1, replace=TRUE, prob=h6$density)
	}

	save(samples, file=paste("~/Desktop/userByEmailSamples/sample", sampleID,".RData",sep=""))

	return(samples)
}


numSamples = 1000
for (i in 1:10) {
	samples = userByEmailSampler(h2, h3, h6, i, numSamples)
	print(quantile(samples, 0.99))
}


load("~/Desktop/vdata1.RData")
ls()
opData = vdata1[vdata1$opLevel==2,]
opData[1:10,]

# Op 2
par(mfrow=c(2,1))
xmax = max(opData$latency_ms[opData$opType==2], op2Bin$latency_ms)
validationOp2 = hist(opData$latency_ms[opData$opType==2], breaks=25, xlim=c(0,xmax))
hist(op2Bin$latency_ms, breaks=25, xlim=c(0,xmax))


# Op 3
par(mfrow=c(2,1))
xmax = max(opData$latency_ms[opData$opType==3], op3Bin$latency_ms)
validationOp3 = hist(opData$latency_ms[opData$opType==3], breaks=25, xlim=c(0,xmax))
hist(op3Bin$latency_ms, breaks=25, xlim=c(0,xmax))


# Op 6
par(mfrow=c(2,1))
#xmax = max(opData$latency_ms[opData$opType==6], op6Bin$latency_ms)
xmax=1000
validationOp6 = hist(opData$latency_ms[opData$opType==6], breaks=25, xlim=c(0,xmax))
hist(op6Bin$latency_ms, breaks=200, xlim=c(0,xmax))


# Query
par(mfrow=c(2,1))
xmax = max(vdata1$latency_ms[vdata1$opLevel==3], samples)
hist(vdata1$latency_ms[vdata1$opLevel==3], breaks=25, xlim=c(0,xmax))
load("~/Desktop/userByEmailSamples/sample1.RData")
hist(samples, breaks=25, xlim=c(0,xmax))



# Look at query on EC2
pdf(file="~/Desktop/sampled-actualR-actualEC2-userByEmail.pdf")

queryData = as.data.frame(read.csv(file="~/Desktop/userByEmail-actual/query/out.csv"))

par(mar=c(5,5,4,2)+0.1)
par(mfrow=c(3,1))
xmax = max(vdata1$latency_ms[vdata1$opLevel==3], samples, queryData$latency_ms[queryData$threadNum > 50])
hist(samples, breaks=25, xlim=c(0,xmax), main="Sampled latency", xlab="Latency (ms)")
abline(v=quantile(samples, 0.99), col="red", lw=2)
legend("topright", legend=(paste("99th Percentile =", round(quantile(samples, 0.99)), "ms")), lwd=2, col="red")

hist(vdata1$latency_ms[vdata1$opLevel==3], breaks=25, xlim=c(0,xmax), main="Actual (R Cluster)", xlab="Latency (ms)")
abline(v=quantile(vdata1$latency_ms[vdata1$opLevel==3], 0.99), col="red", lw=2)
legend("topright", legend=(paste("99th Percentile =", round(quantile(vdata1$latency_ms[vdata1$opLevel==3], 0.99)), "ms")), lwd=2, col="red")

error = abs(actual99th-sampled99th)/actual99th
hist(queryData$latency_ms[queryData$threadNum > 50], breaks=25, xlim=c(0,xmax), main="Actual (EC2)", xlab="Latency (ms)")
abline(v=quantile(queryData$latency_ms[queryData$threadNum > 50], 0.99), col="red", lw=2)
legend("topright", legend=(paste("99th Percentile =", round(quantile(queryData$latency_ms[queryData$threadNum > 50], 0.99)), "ms, error =", round(error, digits=2))), lwd=2, col="red")

actual99th=quantile(queryData$latency_ms[queryData$threadNum > 50], 0.99)
sampled99th=quantile(samples, 0.99)

dev.off()

## COMPARING OPERATOR HISTS -- actual & training
# Operator hists (training data)
rm(list=ls())
load("~/Desktop/op2-hists/i=10,j=10,k=10,l=10.RData")
ls()
h2 = h
op2Bin = bin
op2Bin[1:10,]

load("~/Desktop/op3-hists/i=10,j=10,k=10,l=10.RData")
h3 = h
op3Bin = bin
ls()

load("~/Desktop/op6-hists/i=10,j=10,k=10,l=10.RData")
h6 = h
op6Bin = bin
ls()

par(mfrow=c(3,1))
hist(op2Bin$latency_ms, breaks=25)
hist(op3Bin$latency_ms, breaks=25)  # gamma will still work well here (like k=2 example on wikipedia)
hist(op6Bin$latency_ms[op6Bin$latency_ms < 5], breaks=25)




# Validation data
vOps = as.data.frame(read.csv("~/Desktop/userByEmail/operator/ops.csv"))
vOps[1:10,]
dim(vOps)

vOpsRun = vOps[vOps$threadNum > 50,]
dim(vOpsRun)

pdf("~/Desktop/ops-training-and-actual.pdf")

par(mfrow=c(3,2))
par(mar=c(5,5,4,2)+0.1)
xmax = max(op2Bin$latency_ms, vOpsRun$latency_ms[vOpsRun$opType==2])
hist(op2Bin$latency_ms, breaks=25, xlim=c(0,xmax), main="Op2-Training", xlab="Latency (ms)")
hist(vOpsRun$latency_ms[vOpsRun$opType==2], breaks=50, xlim=c(0,xmax), main="Op2-Actual", xlab="Latency (ms)")

xmax = max(op3Bin$latency_ms, vOpsRun$latency_ms[vOpsRun$opType==3])
hist(op3Bin$latency_ms, breaks=25, xlim=c(0,xmax), main="Op3-Training", xlab="Latency (ms)")
hist(vOpsRun$latency_ms[vOpsRun$opType==3], breaks=50, xlim=c(0,xmax), main="Op3-Actual", xlab="Latency (ms)")

hist(op6Bin$latency_ms[op6Bin$latency_ms < 5], breaks=25, main="Op6-Training", xlab="Latency (ms)")
hist(vOpsRun$latency_ms[vOpsRun$opType==6 & vOpsRun$latency_ms < 5], breaks=25, main="Op6-Actual", xlab="Latency (ms)")

dev.off()


# Try bigger data sz for op3 b/c the scadr users' email field is more like 20 chars
load("~/Desktop/op3-hists/i=40,j=10,k=10,l=10.RData")
h3.40 = h
op3Bin.40 = bin
ls()

par(mfrow=c(2,1))
hist(op3Bin$latency_ms)
hist(op3Bin.40$latency_ms)
# almost same
