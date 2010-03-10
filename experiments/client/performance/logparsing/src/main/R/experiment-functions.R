###  Note:  all path args should not end in "/"



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
		vdata = getSingleDataset(startingThread, endingThread, paste(basePath,"/validation-logs/validation",j,"-logs", sep=""))
		validationStats[j,"latencyQuantile"] = quantile(vdata$latency_ms[vdata$opLevel==3], latencyQuantile)
		validationStats[j,"numQueries"] = length(which(vdata$opLevel==3))
	}
	
	print("Saving validation stats...")
	save(validationStats, file=paste(basePath, "/validationStats.RData", sep=""))
	
	return(validationStats)
}


createAndSaveThoughtstreamOpHistograms = function(basePath) {
	print("Loading training data...")
	load(file=paste(basePath, "/trainingData.RData", sep=""))  # => "data"
	
	print("Creating histograms...")
	# #breaks may be an interesting thing to adjust.  What if I had 50 breaks per hist?  10?
	h1 = hist(data[data$opLevel == 2 & data$opType == 1,"latency_ms"], breaks=25)
	h3 = hist(data[data$opLevel == 2 & data$opType == 3,"latency_ms"], breaks=25)
	h4 = hist(data[data$opLevel == 2 & data$opType == 4,"latency_ms"], breaks=25)
	h5 = hist(data[data$opLevel == 2 & data$opType == 5,"latency_ms"], breaks=25)
	h6 = hist(data[data$opLevel == 2 & data$opType == 6,"latency_ms"], breaks=25)
	h7 = hist(data[data$opLevel == 2 & data$opType == 7,"latency_ms"], breaks=20)
	h8 = hist(data[data$opLevel == 2 & data$opType == 8,"latency_ms"], breaks=20)
	h9 = hist(data[data$opLevel == 2 & data$opType == 9,"latency_ms"], breaks=20)
	
	print("Saving histograms...")
	save(h1, h3, h4, h5, h6, h7, h8, h9, file=paste(basePath,"/histograms.RData", sep=""))
}


createAndSaveUserByEmailOpHistograms = function(basePath) {
	print("Loading training data...")
	load(file=paste(basePath, "/trainingData.RData", sep=""))  # => "data"
	
	print("Creating histograms...")
	h2 = hist(data[data$opLevel == 2 & data$opType == 2,"latency_ms"], breaks=25)
	h3 = hist(data[data$opLevel == 2 & data$opType == 3,"latency_ms"], breaks=25)
	h6 = hist(data[data$opLevel == 2 & data$opType == 6,"latency_ms"], breaks=25)

	print("Saving histograms...")
	save(h2, h3, h6, file=paste(basePath,"/histograms.RData", sep=""))
}


createAndSaveUserByNameOpHistograms = function(basePath) {
	print("Loading training data...")
	load(file=paste(basePath, "/trainingData.RData", sep=""))  # => "data"
	
	print("Creating histograms...")
	h1 = hist(data[data$opLevel == 2 & data$opType == 1,"latency_ms"], breaks=25)
	h6 = hist(data[data$opLevel == 2 & data$opType == 6,"latency_ms"], breaks=25)

	print("Saving histograms...")
	save(h1, h6, file=paste(basePath,"/histograms.RData", sep=""))
}


createAndSaveThoughtsByHashTagOpHistograms = function(basePath) {
	print("Loading training data...")
	load(file=paste(basePath, "/trainingData.RData", sep=""))  # => "data"
	
	print("Creating histograms...")
	h2 = hist(data[data$opLevel==2 & data$opType==2,"latency_ms"], breaks=25)
	h5 = hist(data[data$opLevel==2 & data$opType==5,"latency_ms"], breaks=25)
	h6 = hist(data[data$opLevel==2 & data$opType==6,"latency_ms"], breaks=25)
	h8 = hist(data[data$opLevel==2 & data$opType==8,"latency_ms"], breaks=25)

	print("Saving histograms...")
	save(h2, h5, h6, h8, file=paste(basePath,"/histograms.RData", sep=""))
}


createAndSaveUserByEmailOpHistogramsFromOtherQueries = function(basePathThoughtstream, basePathThoughtsByHashTag, outputPath1, outputPath2) {
	print("Loading histograms for thoughtstream...")
	load(file=paste(basePathThoughtstream, "/histograms.RData", sep=""))  
	# => h1, h3, h4, h5, h6, h7, h8, h9
	# want to keep h3 & h6
	
	h3.thoughtstream = h3
	h6.thoughtstream = h6
	
	print("Loading histograms for thoughtsByHashTag...")
	print(file=paste(basePathThoughtsByHashTag, "/histograms.RData", sep=""))
	# => h2, h5, h6, h8
	# want to keep h2 & h6
	
	h2.thoughtsByHashTag = h2
	h6.thoughtsByHashTag = h6
	
	print("Saving histogram files (2 options) for userByEmail...")
	
	# First option:  h2=h2.thoughtsByHashTag, h3=h3.thoughtstream, h6=h6.thoughtstream
	h2=h2.thoughtsByHashTag
	h3=h3.thoughtstream
	h6=h6.thoughtstream
	save(h2,h3,h6,file=paste(outputPath1,"/histograms.RData", sep=""))
	
	# Second option:  h2=h2.thoughtsByHashTag, h3=h3.thoughtstream, h6=h6.thoughtsByHashTag
	#h2=h2.thoughtsByHashTag
	#h3=h3.thoughtstream
	h6=h6.thoughtsByHashTag
	save(h2,h3,h6,file=paste(outputPath2,"/histograms.RData", sep=""))
}


## DEPRECATED
## Note more general version which follows.
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


getPredictedQueryLatencyQuantiles = function(queryType, numSampleSets, basePath, latencyQuantile) {
	predictedQueryLatencyQuantiles = matrix(nrow=1,ncol=numSampleSets)
	
	load(file=paste(basePath, "/validationStats.RData", sep="")) # => validationStats
	numSamplesPerSet = floor(mean(validationStats[,"numQueries"]))
	print(paste("Using", numSamplesPerSet, "samples per set."))

	for (j in 1:numSampleSets) {
		print(paste("Sample Set", j))
		
		if (queryType == "thoughtstream") {
			samples=thoughtstreamSampler(basePath, j, numSamplesPerSet)
		} else if (queryType == "userByEmail") {
			samples=userByEmailSampler(basePath, j, numSamplesPerSet)
		} else if (queryType == "userByName") {
			samples=userByNameSampler(basePath, j, numSamplesPerSet)
		} else if (queryType == "thoughtsByHashTag") {
			samples=thoughtsByHashTagSampler(basePath, j, numSamplesPerSet)
		} else {
			print("Incorrect queryType specified.")
		}
			

		predictedQueryLatencyQuantiles[j]=quantile(samples, latencyQuantile)	}
		
	save(predictedQueryLatencyQuantiles, file=paste(basePath,"/predictedStats.RData", sep=""))
		
	return(predictedQueryLatencyQuantiles)
}


# Allows validationStats.RData & histogram.RData to be stored in different directories.
# Output is saved in directory represented by basePathHistograms.
getPredictedQueryLatencyQuantiles2 = function(queryType, numSampleSets, basePathValidationStats, basePathHistograms, latencyQuantile) {
	predictedQueryLatencyQuantiles = matrix(nrow=1,ncol=numSampleSets)
	
	load(file=paste(basePathValidationStats, "/validationStats.RData", sep="")) # => validationStats
	numSamplesPerSet = floor(mean(validationStats[,"numQueries"]))
	print(paste("Using", numSamplesPerSet, "samples per set."))

	for (j in 1:numSampleSets) {
		print(paste("Sample Set", j))
		
		if (queryType == "thoughtstream") {
			samples=thoughtstreamSampler(basePathHistograms, j, numSamplesPerSet)
		} else if (queryType == "userByEmail") {
			samples=userByEmailSampler(basePathHistograms, j, numSamplesPerSet)
		} else if (queryType == "userByName") {
			samples=userByNameSampler(basePathHistograms, j, numSamplesPerSet)
		} else if (queryType == "thoughtsByHashTag") {
			samples=thoughtsByHashTagSampler(basePathHistograms, j, numSamplesPerSet)
		} else {
			print("Incorrect queryType specified.")
		}
			

		predictedQueryLatencyQuantiles[j]=quantile(samples, latencyQuantile)	}
		
	save(predictedQueryLatencyQuantiles, file=paste(basePathHistograms,"/predictedStats.RData", sep=""))
		
	return(predictedQueryLatencyQuantiles)
}




# Samplers produce & save a single sample set.
# Called from "getPredictedQueryLatencyQuantiles" function.
thoughtstreamSampler = function(basePath, sampleID, numSamples) {
	load(file=paste(basePath, "/histograms.RData", sep=""))  # => histograms
	
	samples=matrix(data=0, nrow=1, ncol=numSamples)

	for (i in 1:numSamples) {
		samples[i] = samples[i] + sum(sample(h1$mids, 2, replace=TRUE, prob=h1$density))
		samples[i] = samples[i] + sample(h3$mids, 1, replace=TRUE, prob=h3$density)
		samples[i] = samples[i] + sum(sample(h4$mids, 2, replace=TRUE, prob=h4$density))
		samples[i] = samples[i] + sample(h5$mids, 1, replace=TRUE, prob=h5$density)
		samples[i] = samples[i] + sum(sample(h6$mids, 5, replace=TRUE, prob=h6$density))
		samples[i] = samples[i] + sample(h7$mids, 1, replace=TRUE, prob=h7$density)
		samples[i] = samples[i] + sample(h8$mids, 1, replace=TRUE, prob=h8$density)
		samples[i] = samples[i] + sample(h9$mids, 1, replace=TRUE, prob=h9$density)
	}

	save(samples, file=paste(basePath,"/sample", sampleID,".RData",sep=""))

	return(samples)
}


userByEmailSampler = function(basePath, sampleID, numSamples) {
	load(file=paste(basePath, "/histograms.RData", sep=""))  # => histograms
	
	samples=matrix(data=0, nrow=1, ncol=numSamples)

	for (i in 1:numSamples) {
		samples[i] = samples[i] + sample(h2$mids, 1, replace=TRUE, prob=h2$density)
		samples[i] = samples[i] + sample(h3$mids, 1, replace=TRUE, prob=h3$density)
		samples[i] = samples[i] + sample(h6$mids, 1, replace=TRUE, prob=h6$density)
	}

	save(samples, file=paste(basePath,"/sample", sampleID,".RData",sep=""))

	return(samples)
}


userByNameSampler = function(basePath, sampleID, numSamples) {
	load(file=paste(basePath, "/histograms.RData", sep=""))  # => histograms
	
	samples=matrix(data=0, nrow=1, ncol=numSamples)

	for (i in 1:numSamples) {
		samples[i] = samples[i] + sample(h1$mids, 1, replace=TRUE, prob=h1$density)
		samples[i] = samples[i] + sample(h6$mids, 1, replace=TRUE, prob=h6$density)
	}

	save(samples, file=paste(basePath,"/sample", sampleID,".RData",sep=""))

	return(samples)
	
}


thoughtsByHashTagSampler = function(basePath, sampleID, numSamples) {
	load(file=paste(basePath, "/histograms.RData", sep=""))  # => histograms
	
	samples=matrix(data=0, nrow=1, ncol=numSamples)

	for (i in 1:numSamples) {
		samples[i] = samples[i] + sample(h2$mids, 1, replace=TRUE, prob=h2$density)
		samples[i] = samples[i] + sample(h5$mids, 1, replace=TRUE, prob=h5$density)
		samples[i] = samples[i] + sum(sample(h6$mids, 2, replace=TRUE, prob=h6$density))
		samples[i] = samples[i] + sample(h8$mids, 1, replace=TRUE, prob=h8$density)
	}

	save(samples, file=paste(basePath,"/sample", sampleID,".RData",sep=""))

	return(samples)
}


## Note: just for testing
testSwitch = function(queryType) {
	if(queryType == "thoughtstream") {
		print("thoughtstream")
	} else if (queryType == "userByEmail") {
		print("userByEmail")
	} else if (queryType == "userByName") {
		print("userByName")
	} else {
		print("no")
	}	
}


# Q:  is this a good way to measure error?  Could ask PB then MJ.
getPredictionError = function(basePath) {
	load(file=paste(basePath,"/validationStats.RData",sep=""))  # => validationStats
	load(file=paste(basePath,"/predictedStats.RData",sep=""))   # => predictedQueryLatencyQuantiles
	
	#error = abs(mean(validationStats[,"latencyQuantile"]) - mean(predictedQueryLatencyQuantiles))/mean(validationStats[,"latencyQuantile"])
	error = abs(median(validationStats[,"latencyQuantile"]) - median(predictedQueryLatencyQuantiles))/median(validationStats[,"latencyQuantile"])
	return(error)
}


getPredictionError2 = function(basePathValidationStats, basePathPredictedStats) {
	load(file=paste(basePathValidationStats,"/validationStats.RData",sep=""))  # => validationStats
	load(file=paste(basePathPredictedStats,"/predictedStats.RData",sep=""))   # => predictedQueryLatencyQuantiles
	
	error = abs(median(validationStats[,"latencyQuantile"]) - median(predictedQueryLatencyQuantiles))/median(validationStats[,"latencyQuantile"])
	return(error)
}






