// Problem :: Users can't easily parse and query CSV files
'use strict';
var fs = require('fs');

// 1. Upload CSV file to a storage location
// 2. Get the query from the User
// 3. Process CSV and inform user of Process
/**
  queries   array   array of queries, each with the format
                    [{
                      'header' : [VALID HEADER NAME]
                      'condition' : [CONDITION]
                      'value' : [VALUE]
                    },
                    {
                      'matchCondition' : 'ANY' || 'ALL' or 'ALL'
                    }]
*/
function queryLines(line, operation, queries, matchCondition, outStream){
  //If the line meets the query(s) then we'll set this to true
  var lineMatch = false;
  //Loop across the queries
  queries.forEach(function(query){
      switch(query.condition.toUpperCase()){
        //TODO: Add a handler for type?
        //Parse based on condition type
        case 'EQUAL':
          if(query.value === line[query.header]){
            if(matchCondition.toUpperCase() === 'ANY' || 'ALL'){
              lineMatch = true;
            }
          }
          else if(matchCondition.toUpperCase() === 'ALL'){
            lineMatch = false;
          }
          break;
        case 'LESS THAN':
          if(query.value < line[query.header]){
            if(matchCondition.toUpperCase() === 'ANY' || 'ALL'){
              lineMatch = true;
            }
          }
          else if(matchCondition.toUpperCase() === 'ALL'){
            lineMatch = false;
          }
          break;
        case 'MORE THAN':
          if(query.value > line[query.header]){
            if(matchCondition.toUpperCase() === 'ANY' || 'ALL'){
              lineMatch = true;
            }
          }
          else if(matchCondition.toUpperCase() === 'ALL'){
            lineMatch = false;
          }
          break;
        case 'NOT':
          if(query.value != line[query.header]){
            if(matchCondition.toUpperCase() === 'ANY' || 'ALL'){
              lineMatch = true;
            }
          }
          else if(matchCondition.toUpperCase() === 'ALL'){
            lineMatch = false;
          }
          break;
        case 'CONTAINS':
          if(line[query.header].indexOf(query.value) != -1){
            if(matchCondition.toUpperCase() === 'ANY' || 'ALL'){
              lineMatch = true;
            }
          }
          else if(matchCondition.toUpperCase() === 'ALL'){
            lineMatch = false;
          }
          break;
        case 'DOES NOT CONTAIN':
          if(line.indexOf(query.value) === -1){
            if(matchCondition.toUpperCase() === 'ANY' || 'ALL' || 'ALL'){
              lineMatch = true;
            }
          }
          else if(matchCondition.toUpperCase() === 'ALL'){
            lineMatch = false;
          }
          break;
      }
  });
  //Assuming the conditions are met, loop across the values and write them to the out file
  if(lineMatch){
    for(var header in line){
      outStream.write(line[header] + ', ');
    }
    outStream.write('\n');
  }
  return lineMatch;
}
function deduplicate(){};
/**
*/
function comparison(){};
/**
*/
function processCSV(csvFile, outFile, operation, operationParam){
  //used to store temporary strings / chunks
  var buffer = '';
  //used to give a count of output records
  var count = 0;
  if(operation === 'FETCH' || 'UPDATE' || 'REMOVE'){
    var matchCondition = operationParam.pop().matchCondition;
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
    if(headers === false){
      headers = lines.shift().split(',');
      writeStream.write(headers.join(',')+'\n');
    }
    //loop over the complete lines in this chunk
    lines.forEach(function(line){
      var rowValues = line.split(',');
      var row = {};
      headers.forEach(function(header){
        row[header.trim()] = rowValues.shift().trim(); //{'header' : 'value'}
      });
      switch(operation.toUpperCase()){
        case 'FETCH':
          if(queryLines(row, 'FETCH', operationParam, matchCondition, writeStream)){
            count++;
          }
          break;
        case 'UPDATE':
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
    'condition': 'greater_than',
    'value': '30'
  },
  {
    'header':'location',
    'condition' : 'equal',
    'value'  : 'London'
  },
  {
    'matchCondition' : 'ALL'
  }
];
console.log(Date());
processCSV('./CSV/demo.csv', './CSV/demo_result.csv','FETCH', queries);
