#!/bin/env Rscript
## create a "big" version of ercot data
library(data.table)
ercot <- fread("../data/ercot-load-2017.csv.bz2")
cat("Current dataset size is", object.size(ercot)/2^20, "MB\n")
for(i in 1:6) {
   ercot <- rbind(ercot, ercot)
}
cat("ERCOT big size", object.size(ercot)/2^20, "MB\n")
outFName <- "ercot-big.csv"
fwrite(ercot, outFName, sep="\t")
system(paste("pbzip2 -f", outFName))
for(i in 1:6) {
   ercot <- rbind(ercot, ercot)
}
cat("ERCOT huge size", object.size(ercot)/2^20, "MB\n")
outFName <- "ercot-huge.csv"
fwrite(ercot, outFName, sep="\t")
system(paste("pbzip2 -f", outFName))
