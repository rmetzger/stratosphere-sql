stratosphere-sql
================

Area 51: Secret project.

```
./sqlline
!connect jdbc:optiq:model=src/test/resources/model.json admin admin
```


## Questions 
 * TableFactory and Schema Factory are the only entry points?
 * It seems that TableFactory depends on linq4j (Queryable)
 * 

## Mail

**Using Optiq as an SQL parser/optimizer for Stratosphere**

Hello,

I'm new to optiq, evaluating it for Stratosphere (stratosphere.eu) which is a general purpose "big data" processing engine.
The engine supports operators such as map, reduce, join, cross, union etc. and it is able to represent plans as data flow graphs. We also have an optimizer (that decides on join strategy, data shipping etc.).

We would like to use Optiq to do the JDBC interface, SQL parsing and query rewriting.

The goal will be to have (at least) three ways to run SQL queries on Stratosphere:

1) JDBC Connector: This allows to connect the query processor to a wide variety of analytic tools.
2) Command-line: Run queries from the command line.
3) Stratosphere data flow integration: Use SQL queries as data source to dataflows, intermediate processing steps, and sink.

I think goal 1) and 2) are the same since the "sqlline" tool is just an interface for JDBC.
Goal 3) would require to convert a SQL query (as a string) into a set of Stratosphere operators. We do not want to achive goal 3) immediately .. we just keep it in mind during the planning

My questions:
1) How would you recommend to implement this using Optiq?
	a) Generate a logically optimized query plan using optiq and translate it into a Stratosphere plan?
	b) plug ourselves deeper into optiq so that it generates a Stratosphere plan for us?
	?
2) Can you point us to the classes we have to look into and where exactly to start?

We would like to reuse the current infrastructure within optiq that maintains the data catalogue, so that we can fully concentrate on the plan generation.

My first goal with optiq is to get a very simple example (SELECT * FROM tbl) running to see how everything works together. It seems that all examples (csv, mongodb) only implement the basic table access but the execution itself is done in-memory by linq4j.

Thank you very much in advance!


Thanks,
Robert


