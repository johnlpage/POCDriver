Introduction
------------
This is open source, immature, and undoubtably buggy code - if you find bugs fix them and send me a pull request or let me know (johnlpage@gmail.com)
 
This tool is to make it easy to answer many of the questions people have during a MongoDB 'Proof of Concept'

* How fast will it be on my hardware.
* How could it handle my workload.
* How does MongoDB scale.
* How does the High Availability Work ( *NB POCDriver does NOT YET have this part*)

POCDriver a single JAR file which allows you to specify and run a number of different workloads easily from the command line. It is intended to show how MongoDB should be used for various tasks and avoid's testing your own client code versus MongoDB's capailities. POCDriver is an alternative to using generic tools like YCSB. Unlike these tools POCDriver:
  * Only works with MongoDB - showing what MongoDB can do rather than comparing lowest common denominator between systems that aren't directly comaprable.
  * Includes much more sopisticated workloads - using the appropriate MongoDB feature.

  This is NOT in any way an official MongoDB product or project.

Build
-----

Execute

```
$ mvn clean package
```

and you will find POCDriver.jar in bin folder.



Basic usage
-----------

If run with no arguments POCDriver will insert records onto a monogdb running on localhost as quickly as possible. 
There will be only the _id index and records will have 10 fields.

Use --print to see what the records look like.

Client options
-------------
```
-h show help
-p show what the records look like in the test
-t how many threads to run on the client and thus how many connections.
-s what threshold to consider slow when repoting latency percentages in ms
-o output stats to a file rather then the screen
-n use a namespace 'schema.collection' of your choiuce
-d how long to run the loader for.
-c a mongodb connection string, you can include write concerns and thread pool size info in this
```


Basic operations.
-----------------
```
 -k Fetch a single record using it's primary key
 -r fetch a range of 10 records
 -u increment an integer field in a random record
 -i add a new record
```

Complex operations
------------------
```
 -g update a random value in the array (must have arrays enabled)
 -v perform sets of operations on a stack so -v iuu will insert then update that record twice -v kui will find a record then update it then insert a new one. the last record is placed on a stack and p pops it off so
     -v kiippu  Finds a record, adds two, then pops them off and updates the original one found.
 ```
 
Control options
---------------
```
 -m when updating a record use findAndModify to fetch a copy of the new incremented value
 -j when updating or querying limit the set to the last N% of records added
 -b what size to use for operation batches.
```
Collection options
-------------------
```
-x How many fields to index aside from _id
-w Do not shard this collection on a sharded system
-e empty this collection at the start of the run.
```
Record shape options
--------------------
```
-a add an X by Y array of integers to each record using -a X:Y
-f asisde from arrays and _id add f fields to the record, after the first 3 every third is an integer, every fifth a date, the rest are text.
-l how many characters to haev in the text fields
```

Requirements to Build
---------------------

  * commons-cli-1.2.jar
  * commons-codec-1.10.jar
  * gson-2.2.4.jar
  * loremipsum-1.0.jar (http://sourceforge.net/projects/loremipsum/files/)
  * mongo-java-driver-2.13.0.jar


