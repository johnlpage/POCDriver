This tool is to make it easy to answer many of the questions people have during a MongoDB 'Proof of Concept'

How fast will it be on my hardware.
How could it handle my workload.
Does it scale.
How does the High Availability Work ( *NB POCDriver does NOT YET have this part*)

It's a single JAR file which allows you to specify anf run a number of different workloads easily.

It is intended to show how MongoDB should be used for various tasks and avoid's testing your own client code versus MongoDB's capailities.

It is an alternative to YCSB which:
  o Only works with MongoDB - showing what MongoDB can do rather than comparing lowest common denominator.
  o Includes much more sopisticated workloads - using the appropriate MongoDB feature.
  
  This is NOT in any way an official MongoDB product or project.
  
  
  Usage:
  
  
  
  Requirements to Build.
  
  commons-cli-1.2.jar
  commons-codec-1.10.jar
  gson-2.2.4.jar
  loremipsum-1.0.jar (http://sourceforge.net/projects/loremipsum/files/)
  mongo-java-driver-2.13.0.jar
  
  
