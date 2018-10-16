#!/usr/bin/env Rscript
### This is not really an exercise but a test: can you install spark and get it to run?
### Try to perform the following steps:

## Adjust the following command: set the "SPARK_HOME" to point to the actual place where
## you installed spark.
## For instance:
## SPARK_HOME <- `/c/users/jintao/info201/spark`
SPARK_HOME <- file.path(Sys.getenv("HOME"), "local", "spark")


## how many instances in parallel.  Don't do more than CPU-s on your computer!
## 1-4 should work for most people.
## start with 1 and see if you can do more!
parallel <- 1

## input file name (full path).  Should point to the ercot-load-2017.csv file in the exercise folder
iFName <- file.path(Sys.getenv("HOME"), "tyyq", "info201", "exercises", "exercise-spark", "data", "ercot-load-2017.csv.bz2")
                           # Data from Electric Reliability Council of Texas (ERCOT) home page:
                           # http://www.ercot.com/gridinfo/load/load_hist/
                           # I guess load is in MW

## -------------------- nothing to set below --------------------
library(magrittr)
library("SparkR", lib.loc=file.path(SPARK_HOME, "R", "lib"))
ss <- sparkR.session(master = paste0("local[", parallel, "]"),
                     sparkHome = SPARK_HOME,
                     appName = "testing spark",
                     sparkConfig=list(spark.sql.parquet.compression.codec="gzip",
                                      spark.driver.memory="2g")
                     )

## First read the data
df <- loadDF(iFName, source="csv", header="true", sep="\t")
cat("ERCOT 2017 data with", nrow(df), "observations\n")
cat("Raw data example:\n")
head(df) %>%
   print()

## Next, convert the columns into normal form:
df <- df %>% 
   mutate(date = to_timestamp(column("Hour Ending"), "MM/dd/yyyy kk:mm") %>%
                           # see java date formats https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
             to_utc_timestamp("America/Chicago"),
                           # convert time to UTC taking into account that Texas in
                           # in the Chicago time zone
          load = regexp_replace(column("ERCOT"), ",", "") %>%
                           # remove the thousand-separator ',' in numbers ...
             cast("double")
                           # ... and convert to double
          ) %>%
   select("date", "load")
                           # select only relevant columns
   
cat("Converted data in UTC time zone:\n")
head(df, 20) %>%
   print()
   
## Now compute the average daily schedule by hour
cat("\nFinal table: load by hour h\n")
dailySchedule <- df %>%
   mutate(h = hour(column("date"))) %>%
                           # extract hour from date
   groupBy("h") %>%
                           # group by hour
   agg(load = avg(column("load")),
                           # compute average load ...
       n = n(column("load"))) %>%
                           # ... and # of obs by hour
   arrange("h") %>%
                           # order by hour
   collect() %>%
                           # make it an R dataframe
   print()
                           # Note: there is one NA in hours.  This is related to how switching to DST is coded in data.

## Plot will be saved to Rplot.pdf if you are not using it interactively
plot(load ~ h, type="l", data=dailySchedule)
