stratosphere-sql: Work in Progress
================

This repository contains work in progress for the new Stratosphere SQL interface. 

The code here will eventually end up in this repository: https://github.com/stratosphere/stratosphere-sql

I will move the code to the "stratosphere" repository once the work here stabilized and became a community effort.


Please open issues if you want to help with the development!

I'm happy about any help, but beware that this is work in progress! I'll quickly change stuff and this implementation does not reflect how we want to implement the SQL interface in the end!

## Development Requirements

You need to check out this branch of Stratosphere to use the SQL interface https://github.com/rmetzger/stratosphere/tree/sql_mainline_changes_update1


You need to check out this branch of Optiq to use the SQL interface:
https://github.com/rmetzger/optiq (currently the new_rex_test branch)

## Development Roadmap

- [x] understand optiq
- [x] implement a VERY simple way to create table definitions. I think json based.
- [ ] implement all standard SQL operators, but only for ints and some operations.
- [ ] implement support for more types
- [ ] avro input table support with schema extraction
- [ ] cstore support (paquet)
- [x] code gen for operators (filters)

Initial Goals
- [ ] performance better than Hive
- [ ] support all queries from https://amplab.cs.berkeley.edu/benchmark/
- [ ] support most TPC-H queries?

