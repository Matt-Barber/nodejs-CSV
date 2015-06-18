
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
      select.forEach(function(field){
        writeStream.write(row[field] + ', '); //TODO :: Trim Trailing ','
      });
      writeStream.write('\n'); //TODO :: detect line ending
    }
    return lineMatch;
  };
  /**
   * Query CSV opens the read file, and pushes each line to the processQuery method
   * @param opeartion   string    SELECT || UPDATE || REMOVE
   * @param fileName    string    The path and name of the read file
   * @param params      object    Parameters for the query
  **/
  function queryCSV(operation, fileName, params){
      var streams = createStreams(fileName);
      var headers = false;
      var buffer  = '';
      var count = 0;
      var matchCondition = params.queries.pop().matchCondition;
      streams.read.on('data', function(chunk){
        buffer += chunk;
        buffer = buffer.split('\n') //TODO :: detect line ending
        var lines = buffer.slice(0, buffer.length-1) // TODO :: -1 is length of lineending
        if(headers === false){
          headers = getHeaders(lines.shift());
          if(params.select === '*'){
            params.select = headers;
          }
          streams.write.write(params.select.join(',') + '\n'); //TODO :: again use the lineending
        }
        lines.forEach(function(line){
          var row = {};
          var values = line.split(',');
          headers.forEach(function(header){
            row[header] = values.shift().trim();
          });
          if(processQuery(operation, row, params, matchCondition, streams.write)){
            count++;
          }
        });
        buffer = (buffer[buffer.length-1].length < 1) ? '' : buffer.pop();
      });
      streams.read.on('end', function(){
        streams.write.end();
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
  this.select = function(fileName, params){
    queryCSV('SELECT', fileName, params);
  };
  /**
    Method performs an UPDATE (search and replace) on the filename using the given params
  */
  this.update = function(fileName, params){
    queryCSV('UPDATE', fileName, params);
  };
  this.insert = function(fileName, params){
    queryCSV('INSERT', fileName, params);
  };
  this.remove = function(fileName, params){
    queryCSV('REMOVE', fileName, params);
  };
  this.deduplicate = '';
  this.compare = '';
  this.split = '';
  this.sample = '';
  this.random = '';
}

module.exports = new CSV();
