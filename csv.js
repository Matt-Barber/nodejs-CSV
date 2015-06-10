// Problem :: Users can't easily parse and query CSV files
'use strict';
var fs = require('fs');

// 1. Upload CSV file to a storage location
// 2. Get the query from the User
// 3. Process CSV and inform user of Process


/**
  RunQuery is used to perform set tests specified by a query on a line of data
  @param  query   data structure    contains header, value and condition to perform on the data

  @return  boolean    dependant on the condition
**/
function runQuery(query){
  switch(query.condition.toUpperCase()){
    //TODO: Add a handler for type?

    case 'EQUAL':
      return (query.value === this[query.header]) ? true : false;
      break;
    case 'LOWER THAN':
      return (this[query.header] < query.value) ? true : false;
      break;
    case 'HIGHER THAN':
      return (this[query.header] > query.value) ? true : false;
      break;
    case 'NOT':
      return (query.value != this[query.header]) ? true : false;
      break;
    case 'CONTAINS':
      return (this[query.header].indexOf(query.value) != -1) ? true : false;
      break;
    case 'DOES NOT CONTAIN':
      return (this.indexOf(query.value) === -1) ? true : false;
  }
}
/**
queryLines takes in the line as a data object to perform the query on,
the operation to perform, the set of queries, an AND / OR condition
and a writeStream

@param  line            data oject    A set of key value pairs {header : value, header : value}
@param  operation       string        what type of query to perform on the data
@param  operationParam         array({})     An array of queries to perform on the line
@param  matchCondition  string        ANY = OR, ALL = AND
@param  outStream       writeStream   An output for the data

@return boolean         lineMatch     success or fail?
**/
function queryLines(line, operation, operationParam, matchCondition, outStream){
  var queries = operationParam.queries;
  var select = operationParam.selectFields;

  if('undefined' !== typeof operationParam.update){
    var update = operationParam.update;
  }
  //If the line meets the query(s) then we'll set this to true
  var lineMatch = false;
  //Loop across the queries
  if(matchCondition === 'ANY'){
    //if some equate to true
    lineMatch = queries.some(runQuery, line);
  }
  else if(matchCondition === 'ALL'){
    //if they are all true
    lineMatch = queries.every(runQuery, line);
  }
  if(operation === 'REMOVE'){
    lineMatch = !lineMatch;
  }
  //Assuming the conditions are met, loop across the values and write them to the out file
  if(lineMatch){
    if(operation === 'UPDATE'){
      /**
        Too much looping here hardly seems worth it? TODO :: Research and experiment
      **/
      for(var field in update){
        line[field] = update[field];
      }
    }
    select.forEach(function(field){
      console.log(field);
      outStream.write(line[field] + ', '); //TODO: Trim Trailing ','
    });
    outStream.write('\n');
  }
  return lineMatch;
}
function deduplicate(){};
/**
*/
function comparison(){};
/**
  ProcessCSV performs the chosen operation on the data with a set of parameters
  @param  csvFile         string    File to read in the CSV data from
  @param  outFile         string    Where to write the resultant data to
  @param  operation       string    What opeartion type to perform on the data
  @param  operationParam  array     Array of operationParameters to run on the file

  @Example ::
    Query Params =
        [
          {'header' : HEADER_NAME, 'condition' : CONDITION, 'value' : VALUE},
          {'header' : HEADER_NAME, 'condition' : CONDITION, 'value' : VALUE},
          {'matchCondition' : ANY || ALL}
        ]

  @return count   integer   rows affected

**/
function processCSV(csvFile, outFile, operation, operationParam){
  //used to store temporary strings / chunks
  var buffer = '';
  //used to give a count of output records
  var count = 0;
  if(operation === 'FETCH' || 'UPDATE' || 'REMOVE'){
    var matchCondition = operationParam.queries.pop().matchCondition;
  }
  var readStream = fs.createReadStream(csvFile);
  var writeStream = fs.createWriteStream(outFile);

  var headers = false;

  readStream.setEncoding('UTF-8');
  readStream.on('data', function(chunk){
    //Add the incoming chunk to the existing buffer
    buffer += chunk;
    //split the buffer into a buffer of lines
    buffer = buffer.split('\n'); //TODO : Check type of line encoding
    //Slice off all the complete lines leaving the remainder in the buffer
    var lines = buffer.slice(0, buffer.length-1); //TODO : Set the number to remove to length of line ending (accounting for \r\n)
    //Let's get the headers if we don't already have them - this should only call on the first chunk
    if(headers === false){
      headers = [];
      //While we're getting the headers we need to trim white space.
      lines.shift().split(',').forEach(function(header){
        headers.push(header.trim());
      });
      //If we're doing a select just assign these
      if(operationParam.selectFields === '*'){
        operationParam.selectFields = headers;
      }
      //First things first - let's write the select headers to the out file
      writeStream.write(operationParam.selectFields.join(',')+'\n');
    }
    //loop over the complete lines in this chunk
    lines.forEach(function(line){

      //Create an object using the row values and the headers
      var rowValues = line.split(',');
      var row = {};
      headers.forEach(function(header){
        //Trim any whitespace from the value
        row[header] = rowValues.shift().trim(); //{'header' : 'value'}
      });
      switch(operation.toUpperCase()){
        case 'FETCH':
          //if our query returns true, then increment the rows affected counter
          if(queryLines(row, 'FETCH', operationParam, matchCondition, writeStream)){
            count++;
          }
          break;
        case 'UPDATE':
          //if our query returns true, then increment the rows affected counter
          if(queryLines(row, 'UPDATE', operationParam, matchCondition, writeStream)){
            count++;
          }
          break;
        case 'REMOVE':
          break;
        case 'DEDUPE':
          break;
        case 'COMPARE':
          break;
        default:
          break;
      }
    });
    //set the buffer to the remenants of last iteration
    //TODO :: Won't work on final iteration - need to check what's going on here?
    buffer = (buffer[buffer.length-1].length < 1) ? '' : buffer.pop();
  });
  readStream.on('end', function(){
    writeStream.end();
    console.log(count);
    console.log(Date());
  });
  readStream.on('error', function(e){
    console.error(e.message);
  });
  writeStream.on('error', function(e){
    console.error(e.message);
  });
}
// 3.1 FETCH, UPDATE, REMOVE
// 3.2 Deduplicate
// 3.3 Comparison

// 4. Return processed file

/**

  Notes:
    Operations -
                  //These all use the same process
                  Fetch where field(s) condition value(s), match on ANY or ALL
                  Update where field(s) condition value(s), match on ANY or ALL
                  Remove where field(s) condition value(s) match on ANY or ALL

                  Deduplicate CSV based on field(s)
                  Compare multiple CSVs, where is or isn't a match


**/
var queries = [
  {
    'header': 'email',
    'condition': 'contains',
    'value':'gmail.com'
  },
  {
    'header':'age',
    'condition': 'higher than',
    'value': '30'
  },
  {
    'header':'location',
    'condition' : 'contains',
    'value'  : 'London'
  },
  {
    'header' :'occupation',
    'condition' : 'contains',
    'value' : 'Student'
  },
  {
    'matchCondition' : 'ALL'
  }
];

var operationParam = {
  queries : queries,
  update : {'occupation' : 'Retired', 'location' : 'N/A'}, //?
  selectFields : '*'
}
console.log(Date());
processCSV('./CSV/demo.csv', './CSV/demo_result.csv','UPDATE', operationParam);
