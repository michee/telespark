# telespark

ahoy hoy, spark!

PreNote: 

I wouldnt choose to base the task on Dataframes, that are loosley typed
it requires overhead of checking if the data is in the correct format.
For a complicated Job one is required to make frequent sanity checks.  
 
I would prefer using Datasets or RDD's and take advantage of scalas typesystem. 
Its a huge advantage to guarantee that  ...... from finish to end. 
And already at compile time. 



*In scala Dataframes is just an alias for  


My preferred strategies for ETL jobs 
- selfhealing
- autodetection of delta
- idempotent

Task journey: 

For the Task i was torn between two strategies. 

A.  read in ALL source data and all result data, cancel out the source data that already have a result. 
    Perform calculation on the remaining source that does not have a result. (delta)
    In practice read in all..
    Advantage: more robust, doesnt matter if wrong data is in wrong day file. 
    disadvantage: 
   *   impossible strategy if you have huge datasets. alot of input/result data 
   * The spark job becomes more complex since the dataSet contains ALL technologies from ALL days, and 
     
B. only read in data day by day (outside of spark)
   advantages: 
   * Remove alot of input data early on.
   * less complex job, job only needs to handle one day at a time. 
   disatvatages: 
   * Vulnerable for the scenario if day file contains data from another day. 
   * depends heavily on file structure. 
   
   
For the task I choosed to go with the latter (B) because of the DATASIZE. 

It detects via the file structure what result is missing. 

 
   
In addition please provide a readme on 
* how your task is configured and run. 

## HowTo configure and run

see **run_locally.sh** for an example. 
The job is run by "sbt run" 

The job is configured with via environment variables. 

currently it has: 
- SPARK_HOME 
- SPARK_MASTER - url to spark master eg: "spark://localhost:7077" or "local[*]" 
- INPUT_PATH - path to Archive folder 
- OUTPUT_PATH - path 


Some datasources might need login credentials or other extra configuration, 
theese 


## Lifecycle Thoughts

* Include some thoughts about the lifecycle of the application. 
    That should include steps which are part of the process of putting code into production. 
    Examples of what that might include are: 
    * How would you schedule your code to run every day? 
    * How would you handle configuration? 
    * How do you make sure refactoring in the future will not break your logic? 
    * Did you see something strange in the data?


Something strange with the data: 
    Better file formats than CSV
    Avro, Parquett. 
    Shared Schema beteen this and parent job eg. avro-scheema. contract between producer and consumer.  

Notes:
* I would use TYPED datasets for the assignment instead, 
* Schemas for site and cell data should be shared between this job and the job that writes the files. 



