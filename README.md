***NOTE***
Recently upgraded to MongoDB 3.4.0 Driver.

Introduction
------------
This is open source, immature, and undoubtedly buggy code - if you find bugs fix them and send me a pull request or let me know (johnlpage@gmail.com)
 
This tool is to make it easy to answer many of the questions people have during a MongoDB 'Proof of Concept'

* How fast will it be on my hardware.
* How could it handle my workload.
* How does MongoDB scale.
* How does the High Availability Work / how do I handle a failover.


POCDriver a single JAR file which allows you to specify and run a number of different workloads easily from the command line. It is intended to show how MongoDB should be used for various tasks and avoid's testing your own client code versus MongoDB's capabilities. POCDriver is an alternative to using generic tools like YCSB. Unlike these tools POCDriver:
  * Only works with MongoDB - showing what MongoDB can do rather than comparing lowest common denominator between systems that aren't directly comparable.

  * Includes much more sophisticated workloads - using the appropriate MongoDB feature.

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

If run with no arguments POCDriver will insert records onto a mongoDB running on localhost as quickly as possible. 
There will be only the _id index and records will have 10 fields.

Use --print to see what the records look like.

Client options
-------------
```
-h show help
-p show what the records look like in the test
-t how many threads to run on the client and thus how many connections.
-s what threshold to consider slow when reporting latency percentages in ms
-o output stats to a file rather then the screen
-n use a namespace 'schema.collection' of your choice
-d how long to run the loader for.
-q *try* to limit rate to specified ops per second.
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
 
 Note: If you specify a workflow via the `-v` flag, the basic operations above will be ignored and the operations listed will be performed instead.
 
Control options
---------------
```
 -m when updating a record use findAndModify to fetch a copy of the new incremented value
 -j when updating or querying limit the set to the last N% of records added
 -b what size to use for operation batches.
 --rangedocs     number of documents to fetch for range queries (default 10)
 --updatefields  number of fields to update (default 1)
 --projectfields number of fields to project in finds (default 0 - return full document)
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
-f aside from arrays and _id add f fields to the record, after the first 3 every third is an integer, every fifth a date, the rest are text.
-l how many characters to have in the text fields
--depth The depth of the document to create.
```

Example
-------

```
MacPro:POCDriver jlp$ java -jar POCDriver.jar -p -a 3:4
MongoDB Proof Of Concept - Load Generator
{
  "_id": {
    "w": 1,
    "i": 12345678
  },
  "fld0": 195727,
  "fld1": {
    "$date": "1993-11-20T04:21:16.218Z"
  },
  "fld2": "Stet clita kasd gubergren, no ",
  "fld3": "rebum. Stet clita kasd gubergr",
  "fld4": "takimata sanctus est Lorem ips",
  "fld5": {
    "$date": "2007-12-26T07:28:49.386Z"
  },
  "fld6": 53068,
  "fld7": "et justo duo dolores et ea reb",
  "fld8": "kasd gubergren, no sea takimat",
  "fld9": 531837,
  "arr": [
    [0,0,0,0],
    [0,0,0,0],
    [0,0,0,0]
  ]
}


MacPro:POCDriver jlp$ java -jar POCDriver.jar -k 20 -i 10 -u 10 -b 20
MongoDB Proof Of Concept - Load Generator
------------------------
After 10 seconds, 20016 new records inserted - collection has 89733 in total 
1925 inserts per second since last report 99.75 % in under 50 milliseconds
3852 keyqueries per second since last report 99.99 % in under 50 milliseconds
1949 updates per second since last report 99.84 % in under 50 milliseconds
0 rangequeries per second since last report 100.00 % in under 50 milliseconds

------------------------
After 20 seconds, 53785 new records inserted - collection has 123502 in total 
3377 inserts per second since last report 99.91 % in under 50 milliseconds
6681 keyqueries per second since last report 99.99 % in under 50 milliseconds
3322 updates per second since last report 99.94 % in under 50 milliseconds
0 rangequeries per second since last report 100.00 % in under 50 milliseconds

------------------------
After 30 seconds, 69511 new records inserted - collection has 139228 in total 
1571 inserts per second since last report 99.92 % in under 50 milliseconds
3139 keyqueries per second since last report 99.99 % in under 50 milliseconds
1595 updates per second since last report 99.94 % in under 50 milliseconds
0 rangequeries per second since last report 100.00 % in under 50 milliseconds

```


Requirements to Build
---------------------

  * commons-cli-1.3.jar
  * commons-codec-1.10.jar
  * gson-2.2.4.jar
  * loremipsum-1.0.jar (http://sourceforge.net/projects/loremipsum/files/)
  * mongo-java-driver-3.4.0.jar


Troubleshooting
---------------

If you are running a mongod with `--auth` enabled, you must pass a user and password with read/write and replSetGetStatus privileges, e.g. `readWriteAnyDatabase` and `clusterMonitor` roles.  
