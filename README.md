# TeleSpark - ahoy hoy
Spark Job to count daily occurences of 
* cells per site.
* frequency bands per site. 


## HowTo configure and run

The job is run by 
```
sbt run
```
see **run_locally.sh** for an example.

The job is configured with via environment variables.

current variables: 

- **SPARK_HOME** 
- **SPARK_MASTER** - _url to spark master eg: "local[*]" or "spark://localhost:7077"_ 
- **INPUT_PATH***  - _path to Archive folder root, local or remote. eg: "./Archive" or "hdfs://someArchive"_
- **OUTPUT_PATH*** - _path to result folder root, local or remote. eg: "./result" or "hdfs://someplace/result"_ 

\* _variable should have more unique name_


## Lifecycle Thoughts   
    Include some thoughts about the lifecycle of the application. 
    That should include steps which are part of the process of putting code into production. 
        Examples of what that might include are: 
        * How would you schedule your code to run every day? 
        * How would you handle configuration? 
        * How do you make sure refactoring in the future will not break your logic? 
        * Did you see something strange in the data?
   
My view is:  
You should treat an applications builds as events in a immutable data pipeline,
every new build should be stored in a archive.\
At any point in time one should be able to deploy any version of the application from the archive to production.

Speculating about an applications lifecycle is a bit difficult in some areas like "handling configuration"\
since it depends on the backend stack, and the backend stack in its turn depends on requirements from stakeholders and (Telias) Business. 
Therefore "thoughts" can become a bit high level and more general recommendations, 
Ill paint out a rough description of a backend stack and navigate the application in it to production. 


The Backend is composed of many services and cron jobs (data-pipeline) which are most likely 
orchestrated by a resource handler, eg Apache Aurora/Mesos or Kubernetes. 
Its a good idea to have a configuration file listing all Currently active cron jobs (and services) and how much resources or instances they need.
per environment. 
Assuming there are at least two environments testing/integration and production. 
 
* The application would be added 

####configuration
The different types of configuration parameters into the application. 
* Infrastructural - eg. URL to archive, Url to spark master - should be provided by the host thats running the application (An AWS node or a Docker container). 
* Application logic specific - eg. directory on archive. Application logic specific parameters. 
* Spark configurations - configurations to tweak spark performance.
 
The general goal is avoiding a rebuild of the application when configu

####scheduling
For scheduling I would use a scheduler that supports running jobs in succession.\
Where you create chains of jobs in a hierarchy, DAG graph.
As soon as a parent job finishes successfully it trigger its children jobs to run. 
* the application would be scheduled to run directly after ALL jobs creating the input has finished successfully 

####refactoring
Every application containing some logic should have Unit-tests. 
* Unit-tests describe the logics intention and are in a way a "specification". 
* Unit-tests is a proof the logic is doing its intended job. 
* **Unit-tests enables refactoring without breaking the logic**. 

The application could break if the format of the input-data is changed. 
after refactoring in a parent job.\
I would create a schema For the input (site and cell) data, that is shared between the this job and the parent.\
Shared between producer and consumer, a contract between producer and consumer.

####error handling
Extensive logging should be used in the application, 


####the data 
The data is in CSV format, 
there are file formats than eg: 
* Avro, Parquett, Thrift
Shared Schema beteen this and parent job eg. avro-scheema. contract between producer and consumer. 
   
#### Left over thoughts
   Notes:
   * I would use TYPED datasets for the assignment instead, 
   * Schemas for site and cell data should be shared between this job and the job that writes the files. 
\
\
\
##Task Journey

I would use TYPED DataSets or TYPED RDD's for the assignment instead of loosely typed DataFrames. 
Loosely typed DataFrames can blow up at runtime,\
and requires overhead of checking if the data is in the correct format.\
For a complicated Job one is required to make frequent sanity checks/checkpoints.\  
 
I would strongly recommend using typed DataSets or RDD's and take advantage of scalas typesystem.\
Strict types is of great help and huge advantage to guarantee that:\
 * The data is in the right format all the way through a complicated calculation, from finish to end.
 * At compile time you can guarantee the data is in the correct format and wont blow up at runtime. 

I can buy the argument of using Dataframes for performance reasons. 

My preferred strategies for ETL jobs 
- selfhealing
- autodetection of delta
- idempotent

#### the journey: 

I went for a job that automatically detects what the new input data is (the new delta).

For the Task i was torn between two strategies. 

**A**. Letting spark calculate the new delta input, by reading in ALL source data and all result data, cancel out the source data that already have a result. 
    Perform calculation on the remaining source that does not have a result.\
    Advantages:
        more robust, doesnt matter if wrong data is in wrong day file. 
    disadvantage: 
   * Impossible if you have huge datasets. Alot of input/result data 
   * The spark job becomes more complex since the dataSet contains ALL technologies from ALL days, and one would need 
     
**B**. Use the filesystem to calculate the delta (outside of spark)
   advantages: 
   * Remove alot of input data early on.
   * Less complex job, job only needs to handle one day at a time. 
   disatvatages: 
   * Vulnerable for the scenario if day file contains data from another day. 
   * depends heavily on file structure. 
   
   
I prefer strategy **A** BUT For the task I choosed to go with the latter **B** because of the risk of to big datasize
of reading in ALL input data and result data.
Currently the job detects what result is missing via the file structure, missing csv files.
 
The two "calculations" of counting technology and counting bands are very similar and follow the same formula:
1. perform some transformation on "technology column"
2. sum up the different techologies in the "technology" column (per site)

A first instinct would be to create ONE general function for summing up the technology. 
And letting the two 

But the two "calculations" are quite small and not so complicated, therefore it was a borderline 
case whether to create general functions both calculations would use.
I choosed to completely separate the two calculations. 


#### Reafacotring for Task v2.0  

