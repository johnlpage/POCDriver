
MongoDB POC Test Driver.
------------------------

This is code to allow you to simulate a variety of different workloads on MongoDB to remove the 
need to write your own code when testing.

It can performs.

Inserts
Updates
Deletes
Queries by Primary Key
Queries by Secondary Keys

It allows you to define the record size to use both the overall size and the number of fields.

It allows you to make use of bulk operators in MongoDB


Development Plan / Backlog

- fast sample Record Generator [Done]
- Inserts [Done]
- Multithreaded Inserts & Connection pools [Done]
- Command line options
	o Threads [Done]
	o Record Shape [Done]
	o Connection String	 [Done]
- Reporting of database state / timing [Done]
- Batch Inserts [Done]


(V1 MVP)

- Key Query Support [Done]
Log to file [Done]

- Secondary Index Support [Done]
- Updates [Done]

- Shard Support [Done]


- Secondary Query Support
- Document Depth
- Show Document


- Good Replica Support

- Reporting of primary, secondary etc
- Internal record counting (TO COMPARE AFTER FAILOVER)?

