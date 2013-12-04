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

Hello,

I'm new to optiq, evaluating it for Stratosphere (stratosphere.eu) which is a general purpose "big data" processing engine.
Our engine supports, unlike MapReduce more operators such as join, cross, union etc. and we are able to represent Jobs as data flow graphs. We also have an optimizer (that decides on join strategy, data shipping etc.).
So our system brings everything you need to pack a SQL interface on top and use Stratosphere as a runtime.
Optiq is very attractive for us because of its extensibility.

My first goal with optiq is to get a very simple example (SELECT * FROM tbl) running to see how everything works together.
It seems that all examples (csv, mongodb) only implement the basic table access but the execution itself is done in-memory by linq4j.
I also looked into the code of Cascading Lingual but this application is quite large so it's not very easy to see the integration points.

So how can I get something like a query execution plan (tree)?
Or would you suggest another approach for me?


Thanks,
Robert


