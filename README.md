#NodeJS CSV Module#
This module is designed to allow for database style queries to be carried out on a CSV file, such as SELECT, UPDATE, INSERT as well as some useful standalone operations such as DEDUPE, COMPARE, SPLIT and RANDOM.

Incomplete currently - SELECT, UPDATE, INSERT, REMOVE are implemented

##Example call##

```
var test = new CSV();

var testQuery = {
  queries  : [
    {'header' : 'email', 'condition' : 'contains', 'value' : 'gmail.com'},
    {'header' : 'age', 'condition' : 'higher than', 'value' : '30'},
    {'matchCondition' : 'ALL'}],
  select : "\*"
};
test.select('./CSV/demo.csv', testQuery);
```

##Known bugs##

- Data types not established for data so no handling of date operations in current module


**THIS IS VERY MUCH AN EXPERIMENTAL REPO **
