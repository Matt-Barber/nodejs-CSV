'use strict';
/**
  * CSV Module allowing for querying
  * @license : MIT
  *
  * @category : Data Analysis
  * @package : CSV
  * @version : 0.2.5
  * @link : ....
  * @since: v0.1.1
  *
  * @author : Matt Barber
  * @created : 10th June 2015
  * @updated : 8th July 2015
**/
var CSV = function(){
  //Set our global constants in this module
  var fs = require('fs'),
      os = require('os'),
      constants = require('./config/constants.js');
  /**
  * Generates a unique hash for user files / folders
  * @param  string    string    Part of the original string
  * @param  length    integer   The length of the unique string to return
  *
  * @return string
  * @visibility : private
  */
  function _generateUnique(string, length){
    var text = '',
        allowedCharacters = constants.AZ09_STR;
    while(text.length !== length){
      text += allowedCharacters.charAt(Math.floor(Math.random() * allowedCharacters.length));
    }
    return string.substring(0, string.length-3).toLowerCase() + '_' + text;
  }
  /**
   * Given the filename create a structure of streams
   * @param {fileName}  string  filename + path
   * @return object
   * @visibility private
  **/
  function _createStreams(readFile){
    var writeFile = '';
      //Generate a new write file name
    do{
      writeFile = _generateUnique(readFile, 8) + '.csv';
    }while(fs.stat(writeFile, function(err, stat){
      return (err !== null) ? true : false;
    }));
    //Allocate streams and filenames to streams object
    return {
      read: fs.createReadStream(readFile, {encoding: 'UTF-8'}),
      readFile : readFile,
      write: fs.createWriteStream(writeFile, { flags: 'a' }),
      writeFile: writeFile
    };
  }
  /**
   * Splits a string returning an array of headers
   * @param {line}  string    Line of a csv file represented as a string
   * @return array
   * @visibility : private
  **/
  function _getHeaders(line){
    var headers = [];
    line.split(',').forEach(function(header){
      //trim the whitespace either side before pushing on to the array
      headers.push(header.trim());
    });
    return headers;
  }
  /**
   * Turns headers and a single CSV string into an object
   * @param   headers     array     Array of headers from the CSV
   * @param   line        string    Valid CSV string
   *
   * @return object
   * @visibility : private
   */
  function _createRow(headers, line){
    var row = {},
        values = line.match(constants.CSV_REGEX);
    //Now let's turn the headers and values into a usable object.
    headers.forEach(function(header){
      row[header] = values.shift().trim().replace(/\,$/, '');
    });
    return row;
  }
  /**
   *
  **/
  function _analyse(WHERE){
    var row = this,
        result = false;
    switch(WHERE.condition.toUpperCase()){
      case '==':
        result = (WHERE.value === row[WHERE.field]) ? true : false;
      break;
      case '<':
        result = (WHERE.value > row[WHERE.field]) ? true : false;
      break;
      case '>':
        result = (WHERE.value < row[WHERE.field]) ? true : false;
      break;
      case '!':
        result = (WHERE.value !== row[WHERE.field]) ? true : false;
      break;
      case 'CONTAINS':
        result = (row[WHERE.field].indexOf(WHERE.value) !== -1) ? true : false;
      break;
      case '!CONTAINS':
        result = (row[WHERE.field].indexOf(WHERE.value) === -1) ? true : false;
      break;
    }
    return result;
  }
  /**
   * Takes in a query, a row and a stream, processes and writes out
   * @param query           object    Query to run on the row
   * @param row             object    A line of the CSV as key values
   * @param writeLocation   stream    Output stream
   *
   * @return boolean
  **/
  function _processQuery(query, row, writeLocation){
    var lineMatch = false,
        writeLine = false;
    //ANY is an OR, ALL is an AND
    if(query.MATCH === 'ANY'){
      lineMatch = query.WHERE.some(_analyse, row);
    }
    else if(query.MATCH === 'ALL'){
      lineMatch = query.WHERE.every(_analyse, row);
    }
    //If we have a REMOVE property, inverse the lineMatch
    if(query.hasOwnProperty('REMOVE')) lineMatch = !lineMatch;
    //If we have a match and we're UPDATING, then replace the parts of the row
    if(lineMatch && query.hasOwnProperty('UPDATE')){
      query.UPDATE.forEach(function(UPDATE){
        row[UPDATE.field] = row[UPDATE.field].replace(UPDATE.search, UPDATE.value);
      });
    }
    //If we have a lineMatch, or we're doing an UPDATE || INSERT then set writeLine to true
    if( lineMatch ||
        query.hasOwnProperty('UPDATE') ||
        query.hasOwnProperty('INSERT')){
      writeLine = true;
    }
    if(writeLine){
      //For each field in our SELECT, write out the row
      query.SELECT.forEach(function(field, index, array){
        var writeString = row[field];
        writeString += (index < array.length-1) ? ', ' : os.EOL;
        writeLocation.write(writeString);
      });
    }
    return lineMatch;
  }
  /**
   * Generates a set of queries given a comparison query. Queries are chunked.
   * @param query     object    comparison query
   * @return array
  **/
  function _generateQueries(query){
    var readStream = fs.createReadStream(query.WITH, {encoding: 'UTF-8'}),
        buffer = '',
        headers = false,
        queries = [];
    return new Promise(function(resolve, reject){
      readStream.on('data', function(chunk){
        var lines = [];
        //Create a query object for this chunk
        var newQuery = {
          SELECT: ['*'],
          FROM: query.COMPARE,
          WHERE: [],
          MATCH: 'ANY'
        };
        //Add the next chunk on to the outstanding buffer, and split the buffer by the new line chatacters
        buffer = (buffer + chunk).split(constants.NEWLINE_REGEX);
        // The last element might not be complete in the chunk (best be on the safe side) - we'll handle this at the end
        lines = (buffer.length > 2) ? buffer.slice(0, buffer.length-1) : buffer.slice(0, buffer.length);
        //If we haven't got the headers - then this will still be false, so we can just shift the first line off
        if(!headers){
          headers = _getHeaders(lines.shift());
        }
        //for each line, create a row and process
        lines.forEach(function(line){
          var row = _createRow(headers, line);
          //for each line, push on a new query on to the where property of the query object for this chunk
          query.WHERE.forEach(function(WHERE){
            newQuery.WHERE.push({
              field: WHERE.field,
              condition: WHERE.condition,
              value: row[WHERE.field]
            });
          });
        });
        //Once we're at the end of the chunk - push this onto the queries array
        queries.push(newQuery);
        buffer = (buffer[buffer.length-1].length < 1) ? '' : buffer.pop();
        return true; //Next chunk please
      });
      readStream.on('end', function(){
        //return the array of queries
        resolve(queries);
      });
      readStream.on('error', function(err){
        reject(err);
      });
    });
  }
  /**
   * Executes core queries such as SELECT, REMOVE, INSERT, UPDATE
   * @param query    object     A query object contain operation, source, and where conditions
   * @return object
  **/
  var executeQuery = function(query, _streams, append){
    //Globals for this method
    var streams = typeof streams !== 'undefined' ? _streams : _createStreams(query.FROM),
        buffer = '',
        headers = false,
        append = typeof headers !== 'undefined' ? append : false,
        count = 0;

    return new Promise(function(resolve, reject){
      streams.read.on('data', function(chunk){
        var lines = [];
        //Add the next chunk on to the outstanding buffer, and split the buffer by the new line chatacters
        buffer = (buffer + chunk).split(constants.NEWLINE_REGEX);
        // The last element might not be complete in the chunk (best be on the safe side) - we'll handle this at the end
        lines = (buffer.length > 2) ? buffer.slice(0, buffer.length-1) : buffer.slice(0, buffer.length);
        //If we haven't got the headers - then this will still be false, so we can just shift the first line off
        if(!headers){
          headers = _getHeaders(lines.shift());
          //If the SELECT is * then we need all the headers
          if( !query.hasOwnProperty('SELECT') ||
              query.SELECT[0] === '*') query.SELECT = headers;
          //Let's write the headers to the out file using the os.EOL to detect platform end of line
          if(!append) streams.write.write(query.SELECT.join(',') + os.EOL);
        }
        //for each line, create a row and process - if a match is found - increment the counter
        lines.forEach(function(line){
          var row = _createRow(headers, line);
          if(_processQuery(query, row, streams.write)) count++;
        });
        buffer = (buffer[buffer.length-1].length < 1) ? '' : buffer.pop();
      });
      //When the stream ends, close the write stream and resolve the data
      streams.read.on('end', function(){
        streams.write.end();
        resolve({
          writeFile: streams.writeFile,
          rows: count
        });
      });
      //if there is an error reject the promise
      streams.write.on('error', function(e){
        reject(e);
      });
      streams.read.on('error', function(e){
        reject(e);
      });
    });
  }
  /**
   * COMPARES two CSVs given the source, and comparison file, as well as what conditions to match on
   * @param cmp_query     object      Comparison query object
  **/
  var compare = function(cmp_query){
    var streams = _createStreams(cmp_query.COMPARE);
    return new Promise(function(resolve, reject){
      //Get the queries from the cmp_query object
      _generateQueries(cmp_query).then(function(queries){
        //Reduce the queries down (execute sequentially chaining promises)
        var result = queries.reduce(function(prev, curr, index, array){
          var append = (index > 0) ? true : false;
          //Execute the previous promise,
          //...then return the current promise
          return prev.then(function(){
            return executeQuery(curr, streams, append);
          });
        }, Promise.resolve().then(function(){
          resolve();
        }).catch(function(err){
          reject(err);
        }));

        //resolve the result.
        resolve(result);
      });
    });
  }

  return {
    query : executeQuery,
    compare : compare
  }
};

module.exports = new CSV();
