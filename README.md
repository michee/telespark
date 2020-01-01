# Telespark

ahoy hoy, spark!

Spark Job to count daily occurences of 
* cells per site.
* frequency bands per site. 


## HowTo configure and run
    *how your task is configured and run. 

The job is run by **"sbt run"**

The job is configured with via environment variables.

current variables: 

- **SPARK_HOME** 
- **SPARK_MASTER** - url to spark master eg: "local[*]" or "spark://localhost:7077" 
- **INPUT_PATH** - path to Archive folder can be 
- **OUTPUT_PATH** - path 

see **run_locally.sh** for an example.\
\
\
NOTES:\
The Archive might be located on a source that need login credentials\
or other extra configuration for access. Eg. S3 login username and password.\
One would need to add more environment variables or path to a **secrets** file. 


##Task Journey

I wouldnt choose to base the task on Dataframes, that are loosley typed\
it requires overhead of checking if the data is in the correct format.\
For a complicated Job one is required to make frequent sanity checks.  
 
I would prefer using Datasets or RDD's and take advantage of scalas typesystem.\
Strict types is of great help and huge advantage to guarantee that the data is in the right format\
from finish to end, 
And already at compile time. 

Although I can buy usage of Dataframes for performance reasons. 

My preferred strategies for ETL jobs 
- selfhealing
- autodetection of delta
- idempotent

#### the journey: 

I went for a job that detects automatically what the new input data is (the new delta).

For the Task i was torn between two strategies. 

**A**.  Letting spark calculate the new delta input by read in ALL source data and all result data, cancel out the source data that already have a result. 
    Perform calculation on the remaining source that does not have a result.\
    Advantages:
        more robust, doesnt matter if wrong data is in wrong day file. 
    disadvantage: 
   *   impossible strategy if you have huge datasets. alot of input/result data 
   * The spark job becomes more complex since the dataSet contains ALL technologies from ALL days, and 
     
**B**. Only read in data day by day (outside of spark)
   advantages: 
   * Remove alot of input data early on.
   * less complex job, job only needs to handle one day at a time. 
   disatvatages: 
   * Vulnerable for the scenario if day file contains data from another day. 
   * depends heavily on file structure. 
   
   
For the task I choosed to go with the latter **B** because of the risk of to big datasize
of reading in ALL input data and result data.
Currently I am It detects via the file structure what result is missing. 

The Two jobs of counting technology and counting bands are very similair and follow the same formula. 
1. perform some transformation on "technology column"
2. sum up the different techologies in the "technology" column (per site)




####Reafacotring for Task v2.0  
* If 
 
 
## Lifecycle Thoughts
   
   * Include some thoughts about the lifecycle of the application. 
       That should include steps which are part of the process of putting code into production. 
       Examples of what that might include are: 
       * How would you schedule your code to run every day? 
       * How would you handle configuration? 
       * How do you make sure refactoring in the future will not break your logic? 
       * Did you see something strange in the data?
   
   
   * scheduling -
   * Configuration - 
   * Input data -  
   
   Something strange with the data: 
       Better file formats than CSV
       Avro, Parquett. 
       Shared Schema beteen this and parent job eg. avro-scheema. contract between producer and consumer.  
   
   Notes:
   * I would use TYPED datasets for the assignment instead, 
   * Schemas for site and cell data should be shared between this job and the job that writes the files. 






