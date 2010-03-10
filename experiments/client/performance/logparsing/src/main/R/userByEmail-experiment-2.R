# 3.9.10
# Goal: predict userByEmail's 99th percentile latency using op models from thoughtstream & thoughtsByHashTag

# Note:  userByEmail has ops 2, 3, & 6

queryType="userByEmail"
numSampleSets=10
latencyQuantile=0.99

## Version 1:  
# h2=h2.thoughtsByHashTag, h3=h3.thoughtstream, h6=h6.thoughtstream
source("/work/ksauer/scads/experiments/client/performance/logparsing/src/main/R/experiment-functions.R")

# Setting up paths
basePathThoughtstream = "/work/ksauer/2.12.10-thoughtstream-experiment"
basePathThoughtsByHashTag = "/work/ksauer/3.8.10-thoughtsByHashTag-experiment"
basePath = "/work/ksauer/3.9.10-thoughtsByHashTag-experiment-2"
outputPath1 = paste(basePath, "/option1", sep="")
outputPath2 = paste(basePath, "/option2", sep="")

# Training phase:  get histograms
createAndSaveUserByEmailOpHistogramsFromOtherQueries(basePathThoughtstream, basePathThoughtsByHashTag, outputPath1, outputPath2)

# Validation phase:
print("Getting predicted latency...")
getPredictedQueryLatencyQuantiles2(queryType, numSampleSets, basePathThoughtsByHashTag, outputPath1, latencyQuantile)
getPredictedError2(basePathThoughtsByHashTag, outputPath1)










