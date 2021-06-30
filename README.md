#MARYL
Maryl (MapReduce Yarn Library) is a (naive) Scala implementation of the MapReduce pattern for Hadoop clusters. It allows for the programmatic creation and execution of simple, textfile based MR-Jobs, returning the future-wrapped results. This allows for the output of MapReduce to be directly accessible for further local calculations. The necessary functions are not supplied through inheritance, but are instead passed as a lambda parameter.
##Usage
###Writing the Driver-App
1. Create a YarnContext -> Information about the job's resource requirements, JAR-Locations, etc.
2. Create a MapReduceContext -> All necessary information that are typical for a MapReduce-Job like Mapper and Reducer counts, queue sizes, in/output files, etc.
3. Pass the context objects to the MapReduceBuilder's create-method to receive a MARYLApp object, which can be submitted to start the MapReduce-Job on the cluster,
4. Submitting the MARYLApp directly returns a Future-wrapped Iterator over the results
###Deploying to the cluster
1. Build a fatjar from your Project
2. Copy your fatjar to any node of the cluster
3. Submit your Application to YARN:        $yarn jar <appname>.jar </local/path/to/your/jar/on/the/node> <HDFS input file> <HDFS output file>
##License
[GPL](https://choosealicense.com/licenses/gpl-3.0/)
