
'use strict';
/**
  * CSV Module allowing for querying
  * @license : MIT
  *
  * @category : Data Analysis
  * @package : CSV
  * @version : 0.1
  * @link : ....
  * @since: v0.1
  *
  * @author : Matt Barber
  * @created : 10th June 2015
  * @updated : 14th June 2015
**/


var CSV  =  function(){
  var fs = require('fs');
  var os = require('os');
  /**
   * Splits a string returning an array of headers
   * @param {line}  string    Line of a csv file represented as a string
   * @return array
   * @visibility : private

  **/
  function getHeaders(line){
    var headers = [];
    line.split(',').forEach(function(header){
      //trim the whitespace either side before pushing on to the array
      headers.push(header.trim());
    });
    return headers;
  }


  /**
   * Given the filename create a structure of streams
   * @param {fileName}  string  filename + path
   * @return object
   * @visibility private
  **/
  function createStreams(fileName){
    var streams = {
      read : fs.createReadStream(fileName),
      write : fs.createWriteStream(fileName+'test.csv') //TODO::validate
    };
    streams.read.setEncoding('UTF-8');
    return streams;
  };

  /**
   * Takes in a query as an object parameter, as well as a row object
   * @param query   object
   * @return boolean
   * @visibility  private
  **/
  function analyseQuery(query){
    //TODO: Add a handler for type?
    switch(query.condition.toUpperCase()){
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
  };
  /**
   * Takes in an operation, set of parameters (query), a match condition
   * and read stream, returns linematch / writes output to write stream
   * @param operation       string    Type of operation (SELECT, REMOVE, UPDATE)
   * @param row             object    A line of the CSV as {header : value}
   * @param params          object    Set of queries and added element called 'update' for that operation
   * @param matchCondition  string    ANY || ALL
   * @param writeStream     stream    A write stream for output
   *
   * @return boolean
   * @visibility private
  **/
  function processQuery(operation, row, params, matchCondition, writeStream){
    //Set the queries as a separate array
    var queries = params.queries;
    //Get the return fields, the "select"
    var select = params.select;
    //Only set this if it's declared - we only need it for an UPDATE
    if('undefined' !== typeof params.update){
      var update = params.update;
    }

    var lineMatch = false;

    if(matchCondition === 'ANY'){
      //We only need some of the results to be true
      lineMatch = queries.some(analyseQuery, row);
    }
    else if(matchCondition === 'ALL'){
      //We need every entry to be true
      lineMatch = queries.every(analyseQuery, row);
    }
    //If the operation is REMOVE we need to flip the result
    if(operation === 'REMOVE'){
      lineMatch = !lineMatch;
    }
    if(lineMatch){
      //If we have an UPDATE then modify the field with the values given
      if(operation === 'UPDATE'){
        update.forEach(function(field){
          row[field] = update[field];
        });
      }
      select.forEach(function(field, index, array){
        var writeString = (index < array.length-1) ? row[field] + ', ' : row[field] + os.EOL;
        writeStream.write(writeString);
      });
    }
    return lineMatch;
  };
  /**
   * Query CSV opens the read file, and pushes each line to the processQuery method
   * @param opeartion   string    SELECT || UPDATE || REMOVE
   * @param fileName    string    The path and name of the read file
   * @param params      object    Parameters for the query
   * @param callback    function  Callback for when the process completes
  **/
  function queryCSV(operation, fileName, params, callback){
      var streams = createStreams(fileName);
      var headers = false;
      var buffer  = '';
      var count = 0;
      var matchCondition = params.queries.pop().matchCondition;
      streams.read.on('data', function(chunk){
        //Add the next chunk on to the outstanding buffer, and split the buffer by the new line chatacters
        buffer += chunk;
        buffer = buffer.split(/\r\n|\r|\n/g)
        // The last element might not be complete in the chunk (best be on the safe side) - we'll handle this at the end
        var lines = buffer.slice(0, buffer.length-1)
        //If we haven't got the headers - then this will still be false, so we can just shift the first line off
        if(headers === false){
          headers = getHeaders(lines.shift());
          //If the SELECT is * then we need all the headers
          if(params.select === '*'){
            params.select = headers;
          }
          //Let's write the headers to the out file using the os.EOL to detect platform end of line
          streams.write.write(params.select.join(',') + os.EOL);
        }
        lines.forEach(function(line){
          var row = {};
          //REGEX to split the CSV correctly as documented here : http://stackoverflow.com/questions/8493195/how-can-i-parse-a-csv-string-with-javascript
          var values = line.match(/(?!\s*$)\s*(?:'([^'\\]*(?:\\[\S\s][^'\\]*)*)'|"([^"\\]*(?:\\[\S\s][^"\\]*)*)"|([^,'"\s\\]*(?:\s+[^,'"\s\\]+)*))\s*(?:,|$)/g);
          //Now let's turn the headers and values into a usable object.
          headers.forEach(function(header){
            row[header] = values.shift().trim().replace(/\,$/, '');
          });
          if(processQuery(operation, row, params, matchCondition, streams.write)){
            count++;
          }
        });
        buffer = (buffer[buffer.length-1].length < 1) ? '' : buffer.pop();
        return true; //I'm ready for the next data chunk
      });
      streams.read.on('end', function(){
        streams.write.end();
        callback(count);
        return count;
      });
      streams.write.on('error', function(e){
        console.error(e.message);
      });
      streams.read.on('error', function(e){
        console.error(e.message);
      });
  };
  /**
    Method performs a SELECT search on the filename using the given parameters
  */
  this.select = function(fileName, params, callback){
    queryCSV('SELECT', fileName, params, callback);
  };
  /**
    Method performs an UPDATE (search and replace) on the filename using the given params
  */
  this.update = function(fileName, params, callback){
    queryCSV('UPDATE', fileName, params, callback);
  };
  this.insert = function(fileName, params, callback){
    queryCSV('INSERT', fileName, params, callback);
  };
  this.remove = function(fileName, params, callback){
    queryCSV('REMOVE', fileName, params, callback);
  };
  this.deduplicate = '';
  this.compare = '';
  this.split = '';
  this.sample = '';
  this.random = '';
}

var test = new CSV();

var testQuery = {
  queries  : [
    {'header' : 'email', 'condition' : 'contains', 'value' : 'gmail.com'},
    {'header' : 'age', 'condition' : 'higher than', 'value' : '30'},
    {'matchCondition' : 'ALL'}],
  select : '*'
};
console.log(Date());
var showComplete = function(count){
  console.log(count);
  console.log(Date());
}
var counter = test.select('./CSV/demo.csv', testQuery, showComplete);

module.exports = new CSV();
