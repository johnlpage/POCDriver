Introduction
------------
This is open source, immature and undoubtably buggy code - if you find bugs fix them and send me a pull request or let me know (johnlpag@gmail.com)

This tool is to make it easy to answer many of the questions people have during a MongoDB 'Proof of Concept'

* How fast will it be on my hardware.
* How could it handle my workload.
* How does MongoDB scale.
* How does the High Availability Work ( *NB POCDriver does NOT YET have this part*)

POCDriver a single JAR file which allows you to specify anf run a number of different workloads easily from the command line. It is intended to show how MongoDB should be used for various tasks and avoid's testing your own client code versus MongoDB's capailities. POCDriver is an alternative to using generic tools like YCSB. Unlike these tools POCDriver:
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


Usage
-----
```
$ java -jar POCDriver.jar --help

  MongoDB Proof Of Concept - Load Generator
  usage: POCDriver
   -a,--arrays <arg>          Shape of any arrays in new sample records x:y
                              so -a 12:60 adds an array of 12 length 60
                              arrays of integers
   -b,--bulksize <arg>        Bulk op size (default 512)
   -c,--host <arg>            Mongodb connection details (default
                              'mongodb://localhost:27017' )
   -d,--duration <arg>        Test duration in seconds, default 18,000
   -e,--empty                 Remove data from collection on startup
   -f,--numfields <arg>       Number of top level fields in test records
                              (default 10)
   -g,--arrayupdates <arg>    Ratio of array increment ops requires option
                              'a' (default 0)
   -h,--help                  Show Help
   -i,--inserts <arg>         Ratio of insert operations (default 100)
   -j,--workingset <arg>      Percentage of database to be the working set
                              (default 100)
   -k,--keyqueries <arg>      Ratio of key query operations (default 0)
   -l,--textfieldsize <arg>   Length of text fields in bytes (default 30)
   -m,--findandmodify         Use findandmodify instead of update and
                              retireve record (with -u or -v only)
   -n,--namespace <arg>       Namespace to use , for example
                              myDatabase.myCollection
   -o,--logfile <arg>         Output stats to  <file>
   -p,--print                 Print out a sample record according to the
                              other parameters then quit
   -r,--rangequeries <arg>    Ratio of range query operations (default 0)
   -s,--slowthreshold <arg>   Slow operation threshold in ms(default 50)
   -t,--threads <arg>         Number of threads (default 4)
   -u,--updates <arg>         Ratio of update operations (default 0)
   -v,--workflow <arg>        Specify a set of ordered operations per thread
                              from [iukp]
   -w,--nosharding            Do not shard the collection
   -x,--indexes <arg>         Number of secondary indexes - does not remove
                              existing (default 0)
```

Requirements to Build
---------------------

  * commons-cli-1.2.jar
  * commons-codec-1.10.jar
  * gson-2.2.4.jar
  * loremipsum-1.0.jar (http://sourceforge.net/projects/loremipsum/files/)
  * mongo-java-driver-2.13.0.jar


