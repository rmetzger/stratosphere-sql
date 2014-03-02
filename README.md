stratosphere-sql: Work in Progress
================

This repository contains work in progress for the new Stratosphere SQL interface. 

The code here will eventually end up in this repository: https://github.com/stratosphere/stratosphere-sql

I will move the code to the "stratosphere" repository once the work here stabilized and became a community effort.


Please open issues if you want to help with the development!

I'm happy about any help, but beware that this is work in progress! I'll quickly change stuff and this implementation does not reflect how we want to implement the SQL interface in the end!


You need to check out this branch of Stratosphere to use the SQL interface https://github.com/rmetzger/stratosphere/tree/sql_mainline_changes 


Development Roadmap
[x] understand optiq
[ ] implement a VERY simple way to create table definitions. I think json based.
[ ] implement all standard SQL operators, but only for ints and some operations.
[ ] implement support for more types
[ ] avro input table support with schema extraction
[ ] cstore support (paquet)
[ ] code gen for operators (filters)

Goals
[ ] performance better than Hive
[ ] support all queries from https://amplab.cs.berkeley.edu/benchmark/
[ ] support most TPC-H queries?

