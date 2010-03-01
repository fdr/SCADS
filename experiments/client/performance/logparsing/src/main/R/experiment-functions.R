# logPath shouldn't end in "/"
getSingleDataset = function(startingThread, endingThread, logPath) {
	print(startingThread)
	data = as.data.frame(read.csv(file=paste(logPath,"/Thread-", startingThread, ".csv", sep="")))

	for (i in (startingThread+1):endingThread) {
		print(i)
		newdata = as.data.frame(read.csv(file=paste(logPath,"/Thread-", i, ".csv", sep="")))
		data = rbind(data, newdata)
	}
	
	return(data)
}


getTrainingData = function(startingThread, endingThread, basePath) {
	print("Reading in training data...")
	
	data = getSingleDataset(startingThread, endingThread, paste(basePath,"/training-logs", sep=""))
	
	print("Saving training data...")
	save(data, file=paste(basePath, "/trainingData.RData", sep=""))

	return(data)
}


getValidationStats = function(startingThread, endingThread, basePath, numValidationRuns, latencyQuantile) {
	validationStats = matrix(nrow=numValidationRuns, ncol=2)
	colnames(validationStats) = c("latencyQuantile", "numQueries")
	
	for (j in 1:numValidationRuns) {
		print(paste("Reading in validation data from run ", j, "...", sep=""))
		vdata = getSingleDataset(startingThread, endingThread, paste(basePath,"/validation",j,"-logs", sep=""))
		validationStats[j,"latencyQuantile"] = quantile(vdata$latency_ms[vdata$opLevel==3], latencyQuantile)
		validationStats[j,"numQueries"] = length(which(vdata$opLevel==3))
	}
	
	print("Saving validation stats...")
	save(validationStats, file=paste(basePath, "/validationStats.RData", sep=""))
	
	return(validationStats)
}


# basePath shouldn't end in "/"
createAndSaveThoughtstreamOpHistograms = function(basePath) {
	load(file=paste(basePath, "/trainingData.RData", sep=""))  # => "data"
	
	# #breaks may be an interesting thing to adjust.  What if I had 50 breaks per hist?  10?
	h1 = hist(data[data$opLevel == 2 & data$opType == 1,"latency_ms"], breaks=25)
	h3 = hist(data[data$opLevel == 2 & data$opType == 3,"latency_ms"], breaks=25)
	h4 = hist(data[data$opLevel == 2 & data$opType == 4,"latency_ms"], breaks=25)
	h5 = hist(data[data$opLevel == 2 & data$opType == 5,"latency_ms"], breaks=25)
	h6 = hist(data[data$opLevel == 2 & data$opType == 6,"latency_ms"], breaks=25)
	h7 = hist(data[data$opLevel == 2 & data$opType == 7,"latency_ms"], breaks=20)
	h8 = hist(data[data$opLevel == 2 & data$opType == 8,"latency_ms"], breaks=20)
	h9 = hist(data[data$opLevel == 2 & data$opType == 9,"latency_ms"], breaks=20)
	
	save(h1, h3, h4, h5, h6, h7, h8, h9, file=paste(basePath,"/histograms.RData", sep=""))
}


# Automatically sets 
getPredictedThoughtstreamQueryLatencyQuantiles = function(numSampleSets, basePath, latencyQuantile) {
	load(file=paste(basePath, "/histograms.RData", sep=""))  # => histograms
	
	predictedQueryLatencyQuantiles = matrix(nrow=1,ncol=numSampleSets)
	
	load(file=paste(basePath, "/validationStats.RData", sep="")) # => validationStats
	numSamplesPerSet = floor(mean(validationStats[,"numQueries"]))
	print(paste("Using", numSamplesPerSet, "samples per set."))

	for (j in 1:numSampleSets) {
		print(paste("Sample Set", j))
		samples=matrix(data=0, nrow=1, ncol=numSamplesPerSet)

		for (i in 1:numSamplesPerSet) {
			samples[i] = samples[i] + sum(sample(h1$mids, 2, replace=TRUE, prob=h1$density))
			samples[i] = samples[i] + sample(h3$mids, 1, replace=TRUE, prob=h3$density)
			samples[i] = samples[i] + sum(sample(h4$mids, 2, replace=TRUE, prob=h4$density))
			samples[i] = samples[i] + sample(h5$mids, 1, replace=TRUE, prob=h5$density)
			samples[i] = samples[i] + sum(sample(h6$mids, 5, replace=TRUE, prob=h6$density))
			samples[i] = samples[i] + sample(h7$mids, 1, replace=TRUE, prob=h7$density)
			samples[i] = samples[i] + sample(h8$mids, 1, replace=TRUE, prob=h8$density)
			samples[i] = samples[i] + sample(h9$mids, 1, replace=TRUE, prob=h9$density)
		}

		predictedQueryLatencyQuantiles[j]=quantile(samples, latencyQuantile)	}
		
	save(predictedQueryLatencyQuantiles, file=paste(basePath,"/predictedStats.RData", sep=""))
		
	return(predictedQueryLatencyQuantiles)
}







