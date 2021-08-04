*** Latest Update December 2020 ***
Prior to the latest version - POCDriver allowed you to specify a ratio of operation types. For example 50:50 Inserts and Queries. However It stuck to this ratio regardless of the relative performace - if for example the server could do 20,000 queries per second but only 3,000 updates per second You would get:

100% Queries / 0% Updates - 20,000 queries/s

0% Queries / 100% Updates - 3,000 updates/s

50% Queries / 50% Updates -   2,000 updates/s, 2,000 queries/s

This isn't right but it was because it was launching at a 1:1 ratio of opperations/  as queries are quicker than updates you still get time for 2,000 not 1,500 however you dont get as many queries as they are throttled by the speed of updates (having to match 1:1)

This is now changed by default - when you specifcy -i, -u , -k etc you specify _how many milliseconds_ of each cycle to spend doing these operations, assuming these cycles are longer than a single operation takes then you get proper differentiation. You need to be aware though that -i 1 -k 1 , despite being a 1:1 ratio is not quite the same thing as -i 100 -k 100 . In the first case there is likely only time for one operation in the cycle, there is a rounding error if you like. In the latter you might get 10 of one thing done and 500 of another in that 100 milliseconds showing a far better ratio.

Also be wary of batches, and mixing finds (which cannot be batched) with writes (which can) - either use a batch size of one or underatand that you can write far faster than you can read simply because you can send meny writes to the server in one attempt.

Note there is an extra flag --opsratio which enables the previous behaviour. Also if using --zipfian this new behaviour does not apply.

***NOTE***
Recently upgraded to [MongoDB 3.8.x Java Driver](http://mongodb.github.io/mongo-java-driver/3.8/).

Introduction
------------
Disclaimer: POCDriver is NOT in any way an official MongoDB product or project.

This is open source, immature, and undoubtedly buggy code. If you find bugs please fix them and [send a pull request](https://github.com/johnlpage/POCDriver/pulls) or report in the [GitHub issue queue](https://github.com/johnlpage/POCDriver/issues).

This tool is designed to make it easy to answer many of the questions people have during a MongoDB 'Proof of Concept':

* How fast will MongoDB be on my hardware?
* How could MongoDB handle my workload?
* How does MongoDB scale?
* How does High Availability work (aka How do I handle a failover)?

POCDriver is a single JAR file which allows you to specify and run a number of different workloads easily from the command line. It is intended to show how MongoDB should be used for various tasks and avoids testing your own client code versus MongoDB's capabilities.

POCDriver is an alternative to using generic tools like YCSB. Unlike these tools, POCDriver:

  * Only works with MongoDB. This shows what MongoDB can do rather than comparing lowest common denominator between systems that aren't directly comparable.

  * Includes much more sophisticated workloads using the appropriate MongoDB feature.

Build
-----

Execute

```
$ mvn clean package
```

and you will find `POCDriver.jar` in `bin` folder.



Basic usage
-----------

If run with no arguments, POCDriver will try to insert documents into a MongoDB deployment running on localhost as quickly as possible.

There will be only the `_id` index and documents will have 10 fields.

Use `--print` to see what the documents look like.

Client options
-------------
```
-h show help
-p show what the documents look like in the test
-t how many threads to run on the client and thus how many connections.
-s what threshold to consider slow when reporting latency percentages in ms
-o output stats to a file rather then the screen
-n use a namespace 'schema.collection' of your choice
-d how long to run the loader for.
-q *try* to limit rate to specified ops per second.
-c a MongoDB connection string (note: you can include write concerns and thread pool size info in this)
```


Basic operations.
-----------------
```
 -k Fetch a single document using its primary key
 -r fetch a range of 10 documents
 -u increment an integer field in a random document
 -i add a new document
```

Complex operations
------------------
```
 -g update a random value in the array (must have arrays enabled)
 -v perform sets of operations on a stack:
    -v iuu will insert then update that document twice
    -v kui will find a document, update it, then insert a new document
 
 The last document is placed on a stack and p pops it off so:
    -v kiippu  Finds a document, adds two, then pops them off and updates the original document found.
```

 Note: If you specify a workflow via the `-v` flag, the basic operations above will be ignored and the operations listed will be performed instead.

Control options
---------------
```
 -m when updating a document use findAndModify to fetch a copy of the new incremented value
 -j when updating or querying limit the set to the last N% of documents added
 -b what size to use for operation batches.
 --rangedocs     number of documents to fetch for range queries (default 10)
 --updatefields  number of fields to update (default 1)
 --projectfields number of fields to project in finds (default 0 - return full document)
```
Collection options
-------------------
```
-x how many fields to index aside from _id
-w do not shard this collection on a sharded system
-e empty this collection at the start of the run.
```
Document shape options
--------------------
```
-a add an X by Y array of integers to each document using -a X:Y
-f aside from arrays and _id add f fields to the document, after the first 3 every third is an integer, every fifth a date, the rest are text.
-l how many characters to have in the text fields
--depth The depth of the document to create.
--location Adds a field by name location and provides ISO-3166-2 code. One can provide "random" to fill the field with random values. This field is required for zone sharding with Atlas.
```

Example
-------

```
$ java -jar POCDriver.jar -p -a 3:4
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


$ java -jar POCDriver.jar -k 20 -i 10 -u 10 -b 20
MongoDB Proof Of Concept - Load Generator
------------------------
After 10 seconds, 20016 new documents inserted - collection has 89733 in total
1925 inserts per second since last report 99.75 % in under 50 milliseconds
3852 keyqueries per second since last report 99.99 % in under 50 milliseconds
1949 updates per second since last report 99.84 % in under 50 milliseconds
0 rangequeries per second since last report 100.00 % in under 50 milliseconds

------------------------
After 20 seconds, 53785 new documents inserted - collection has 123502 in total
3377 inserts per second since last report 99.91 % in under 50 milliseconds
6681 keyqueries per second since last report 99.99 % in under 50 milliseconds
3322 updates per second since last report 99.94 % in under 50 milliseconds
0 rangequeries per second since last report 100.00 % in under 50 milliseconds

------------------------
After 30 seconds, 69511 new documents inserted - collection has 139228 in total
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
  * mongo-driver-sync-3.8.1.jar


Troubleshooting
---------------

If you are running a mongod with `--auth` enabled, you must pass a user and password with read/write and replSetGetStatus privileges (e.g. `readWriteAnyDatabase` and `clusterMonitor` roles).
