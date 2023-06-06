*** Latest Update December 2020 ***
Prior to the latest version - POCDriver allowed you to specify a ratio of operation types. For example, 50:50 Inserts and Queries. However, it stuck to this ratio regardless of the relative performance - if for example the server could do 20,000 queries per second but only 3,000 updates per second you would get:

100% Queries / 0% Updates - 20,000 queries/s

0% Queries / 100% Updates - 3,000 updates/s

50% Queries / 50% Updates -   2,000 updates/s, 2,000 queries/s

This isn't right but it was because it was launching at a 1:1 ratio of operations/ as queries are quicker than updates you still get time for 2,000 not 1,500 however you don't get as many queries as they are throttled by the speed of updates (having to match 1:1)

This is now changed by default - when you specify `-i`, `-u`, `-k` etc you specify _how many milliseconds_ of each cycle to spend doing these operations, assuming these cycles are longer than a single operation takes then you get proper differentiation. You need to be aware though that `-i 1 -k 1`, despite being a 1:1 ratio is not quite the same thing as `-i 100 -k 100`. In the first case there is likely only time for one operation in the cycle, there is a rounding error if you like. In the latter you might get 10 of one thing done and 500 of another in that 100 milliseconds showing a far better ratio.

Also be wary of batches, and mixing finds (which cannot be batched) with writes (which can) - either use a batch size of one or understand that you can write far faster than you can read simply because you can send many writes to the server in one attempt.

Note there is an extra flag `--opsratio` which enables the previous behaviour. Also if using `--zipfian` this new behaviour does not apply.

***NOTE***
Recently upgraded to [MongoDB 4.1.x Java Driver](http://mongodb.github.io/mongo-java-driver/4.1/).

## Introduction

Disclaimer: POCDriver is NOT in any way an official MongoDB product or project.

This is open source, immature, and undoubtedly buggy code. If you find bugs please fix them and [send a pull request](https://github.com/johnlpage/POCDriver/pulls) or report in the [GitHub issue queue](https://github.com/johnlpage/POCDriver/issues).

This tool is designed to make it easy to answer many of the questions people have during a MongoDB 'Proof of Concept':

- How fast will MongoDB be on my hardware?
- How could MongoDB handle my workload?
- How does MongoDB scale?
- How does High Availability work (aka How do I handle a failover)?

POCDriver is a single JAR file which allows you to specify and run a number of different workloads easily from the command line. It is intended to show how MongoDB should be used for various tasks and avoids testing your own client code versus MongoDB's capabilities.

POCDriver is an alternative to using generic tools like YCSB. Unlike these tools, POCDriver:

- Only works with MongoDB. This shows what MongoDB can do rather than comparing lowest common denominator between systems that aren't directly comparable.

- Includes much more sophisticated workloads using the appropriate MongoDB feature.

## Build

Execute:

```bash
mvn clean package
```

and you will find `POCDriver.jar` in `bin` folder. You can execute this program by running,

```bash
java -jar ./bin/POCDriver.jar
```

Then append the flags and arguments you want to this command, which can be found specified below. 

### Requirements to Build

- commons-cli-1.3.jar
- commons-codec-1.10.jar
- gson-2.2.4.jar
- loremipsum-1.0.jar (<http://sourceforge.net/projects/loremipsum/files/>)
- mongo-driver-sync-4.1.1.jar

## Basic usage

If run with no arguments, POCDriver will try to insert documents into a MongoDB deployment running on localhost as quickly as possible.

There will be only the `_id` index and documents will have 10 fields.

Use `--print` to see what the documents look like.

### Client options

| Flag                                | Description |
| ----------------------------------- | ----------- |
| `-h`, `--help`                      | Show Help
| `-p`, `--print`                     | Print out a sample document according to the other parameters then quit
| `-t <arg>`, `--threads <arg>`       | Number of threads (default 4)
| `-s <arg>`, `--slowthreshold <arg>` | Slow operation threshold in ms, use comma to separate multiple thresholds (default 50)
| `-q <arg>`, `--opsPerSecond <arg>`  | Try to rate limit the total ops/s to the specified amount
| `-c <arg>`, `--host <arg>`          | MongoDB connection details (default `mongodb://localhost:27017`)

The `-c`/`--host` flag is the MongoDB connection string (aka connection URI) from the MongoDB Java driver. Documentation on its format and available options can be found here: <http://mongodb.github.io/mongo-java-driver/4.1/apidocs/mongodb-driver-core/com/mongodb/ConnectionString.html>

### Basic operations

| Flag                                | Description |
| ----------------------------------- | ----------- |
| `-k <arg>`, `--keyqueries <arg>`    | Ratio of key query operations (default 0)
| `-r <arg>`, `--rangequeries <arg>`  | Ratio of range query operations (default 0)
| `-u <arg>`, `--updates <arg>`       | Ratio of update operations (default 0)
| `-i <arg>`, `--inserts <arg>`       | Ratio of insert operations (default 100)

### Complex operations

| Flag                                | Description |
| ----------------------------------- | ----------- |
| `-g <arg>`, `--arrayupdates <arg>`  | Ratio of array increment ops (requires option `-a`/`--arrays`) (default 0)
| `-v <arg>`, `--workflow <arg>`      | Specify a set of ordered operations per thread from character set `IiuKkp`.

For the `-v`/`--workflow` flag, the valid options are:

- `i` (lowercase `i`): Insert a new record, push it's key onto our stack
- `I` (uppercase `i`): Increment single stack record
- `u` (lowercase `u`): Update single stack record
- `p` (lowercase `p`): Pop off a stack record
- `k` (lowercase `k`): Find a new record an put it on the stack
- `K` (uppercase `k`): Get a new `_id` but don't read the doc and put it on the stack

Examples:

- `-v iuu` will insert then update that document twice
- `-v kui` will find a document, update it, then insert a new document

The last document is placed on a stack and `p` pops it off so:
- `-v kiippu` Finds a document, adds two, then pops them off and updates the original document found.

Note: If you specify a workflow via the `-v` flag, the basic operations above will be ignored and the operations listed will be performed instead.

### Control options

| Flag                                | Description |
| ----------------------------------- | ----------- |
| `-m`, `--findandmodify`             | Use findAndModify instead of update and retrieve document (with `-u` or `-v` only)
| `-j <arg>`, `--workingset <arg>`    | Percentage of database to be the working set (default 100)
| `-b <arg>`, `--bulksize <arg>`      | Bulk op size (default 512)
| `--rangedocs <arg>`                 | Number of documents to fetch for range queries (default 10)
| `--updatefields <arg>`              | Number of fields to update (default 1)
| `--projectfields <arg>`             | Number of fields to project in finds (default 0, which is no projection)

### Collection options

| Flag                                | Description |
| ----------------------------------- | ----------- |
| `-x <arg>`, `--indexes <arg>`       | Number of secondary indexes - does not remove existing (default 0)
| `-w`, `--nosharding`                | Do not shard the collection
| `-e`, `--empty`                     | Remove data from collection on startup

### Document shape options

| Flag                                | Description |
| ----------------------------------- | ----------- |
| `-a <arg>`, `--arrays <arg>`        | Shape of any arrays in new sample documents `x:y` so `-a 12:60` adds an array of 12 length 60 arrays of integers
| `-f <arg>`, `--numfields <arg>`     | Number of top level fields in test documents. After the first 3 every third is an integer, every fifth a date, the rest are text. (default 10)
| `-l <arg>`, `--textfieldsize <arg>` | Length of text fields in bytes (default 30)
| `--depth <arg>`                     | The depth of the document created (default 0)
| `--location <arg>`                  | Adds a field by name location and provided ISO-3166-2 code (args: `comma,seperated,list,of,country,code`). One can provide `--location random` to fill the field with random values. This field is required for zone sharding with Atlas.

## Example

```console
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
```

```console
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

## Troubleshooting

### Connecting with auth

If you are running a mongod with `--auth` enabled, you must pass a user and password with read/write and replSetGetStatus privileges (e.g. `readWriteAnyDatabase` and `clusterMonitor` roles).

### Connecting with TLS/SSL

If you are using TLS/SSL, then make sure to have the certificates and keys added to the Java keystore.

Add the CA certificate to the certstore:

```bash
cd $JAVA_HOME/lib/security
keytool -import -trustcacerts -file /path/to/mongodb/ca.crt -keystore ./cacerts -storepass changeit
```

You need the client certificate and key in `pkcs12` format. If you have them in PEM format, you can convert them via openssl like so:

```bash
# The cert & key must be both in the same file. You can combine them like so:
cat /path/to/mongodb/tls.pem /path/to/mongodb/tls-key.pem > /tmp/tls-cert-and-key.pem

openssl pkcs12 -export -out /tmp/mongodb.pkcs12 -in /tmp/tls-cert-and-key.pem
# When prompted by the openssl command, enter "changeit" as the password (without quotes)
```

When running POCDriver, supply the `javax.net.ssl` properties, and set the `?ssl=true` field on the connection string:

```bash
java \
  -Djavax.net.ssl.trustStore="$JAVA_HOME/lib/security/cacerts" \
  -Djavax.net.ssl.trustStorePassword="changeit" \
  -Djavax.net.ssl.keyStore="/tmp/mongodb.pkcs12" \
  -Djavax.net.ssl.keyStorePassword="changeit" \
  -jar ./bin/POCDriver.jar \
  --host "mongodb://localhost:27017/?ssl=true"
```
