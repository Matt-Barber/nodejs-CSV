
'use strict';
/**
  * CSV Module allowing for querying
  * @license : MIT
  *
  * @category : Data Analysis
  * @package : CSV
  * @version : 0.1.1
  * @link : ....
  * @since: v0.1.1
  *
  * @author : Matt Barber
  * @created : 10th June 2015
  * @updated : 24th June 2015
**/


var CSV  =  function(){
  var fs = require('fs');
  var os = require('os');
  var NEW_LINE = /\r\n|\r|\n/g;
  var CSV_VALUES = /(?!\s*$)\s*(?:'([^'\\]*(?:\\[\S\s][^'\\]*)*)'|"([^"\\]*(?:\\[\S\s][^"\\]*)*)"|([^,'"\s\\]*(?:\s+[^,'"\s\\]+)*))\s*(?:,|$)/g;
  /**
  * Generates a unique hash for user files / folders
  * @param  string    string    Part of the original string
  * @param  length    integer   The length of the unique string to return
  *
  * @return string
  * @visibility : private
  */
  function generateUnique(string, length){
    var text = '';
    var allowedCharacters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    for(var i =0; i<length; i++){
      text+=allowedCharacters.charAt(Math.floor(Math.random() * allowedCharacters.length));
    }
    return string.substring(0, string.length/2).toLowerCase() + '_' + text;
  }
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
   * Turns headers and a single CSV string into an object
   * @param   headers     array     Array of headers from the CSV
   * @param   line        string    Valid CSV string
   *
   * @return object
   * @visibility : private
   */
  function createRow(headers, line){
    var row = {};
    //REGEX to split the CSV correctly as documented here : http://stackoverflow.com/questions/8493195/how-can-i-parse-a-csv-string-with-javascript
    var values = line.match(CSV_VALUES);
    //Now let's turn the headers and values into a usable object.
    headers.forEach(function(header){
      row[header] = values.shift().trim().replace(/\,$/, '');
    });
    return row;
  }
  /**
   * Given the filename create a structure of streams
   * @param {fileName}  string  filename + path
   * @return object
   * @visibility private
  **/
  function createStreams(readFile, writeFile){
    var streams = {
      read : fs.createReadStream(readFile, {'encoding' : 'UTF-8'}),
      write : fs.createWriteStream(writeFile) //TODO::validate
    };
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
   * @param streams     object     Read and write streams
   * @param params      object    Parameters for the query
   * @param callback    function  Callback for when the process completes
  **/
  function queryCSV(operation, streams, params, callback){
      var headers = false;
      var buffer  = '';
      var count = 0;
      var matchCondition = params.queries.pop().matchCondition;
      streams.read.on('data', function(chunk){
        //Add the next chunk on to the outstanding buffer, and split the buffer by the new line chatacters
        buffer += chunk;
        buffer = buffer.split(NEW_LINE);
        // The last element might not be complete in the chunk (best be on the safe side) - we'll handle this at the end
        var lines = buffer.slice(0, buffer.length-1);
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
          var row = createRow(headers, line);
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
    Builds the query for queryCSV call from the data in the comparison file
    @param source       string    path/to/file of source CSV
    @param comparison   string    path/to/file of comparison CSV
    @param fields       array     array of fields to match on (i.e. ['email', 'location'])
    @param condition    string    how to compare the files EQUAL, CONTAINS, NOT etc.


    TODO :: Fix the output stuff, method needs a rework - seems pretty naff.

  */
  function compareCSV(source, comparison, fields, condition){
    do{
      //Trim the .csv off and concatenate the current time in milliseconds
      var outfile = generateUnique(source, 5) + '.csv';
    }while(fs.existsSync(outfile));
    //Create a set of read and write stream, from the source, and to a destination
    var streams = createStreams(source, outfile);
    //Also allocate a stream for our comparison operator
    var comparisonStream = fs.createReadStream(comparison, {'encoding' : 'UTF-8'});

    //Function globals (need to be maintained between chunks in the data stream)
    var buffer = '';
    var headers = false;
    var matchCount = 0;
    //Start streaming data from the comparison file in chunks
    comparisonStream.on('data', function(chunk){
      //Add the next chunk on to the outstanding buffer, and split the buffer by the new line chatacters
      buffer += chunk;
      buffer = buffer.split(NEW_LINE);
      // The last element might not be complete in the chunk (best be on the safe side) - we'll handle this at the end
      var lines = buffer.slice(0, buffer.length-1);
      //If we haven't got the headers, get all of them from the source
      if(headers === false){
        if(fields.length == 0){
          headers = getHeaders(lines.shift());
        }
        else{
          lines.shift();
          headers = fields;
        }
      }
      //loop across each of the complete lines in the chunk
      lines.forEach(function(line){
        var comparisonQueries = {queries:[], select:'*'};
        var row = {};
        //REGEX to split the CSV correctly as documented here : http://stackoverflow.com/questions/8493195/how-can-i-parse-a-csv-string-with-javascript
        var values = line.match(CSV_VALUES);
        //Now let's turn the headers and values into a usable object.
        headers.forEach(function(header){
          //and push them onto the queries object
          comparisonQueries.queries.push(
            {
              header : header,
              condition : condition,
              value : values.shift().trim().replace(/\,$/, '')
            }
          );
        });
        //We want exact matches
        comparisonQueries.queries.push(
          {matchCondition : 'ALL'}
        );
        //Call queryCSV with out chunk of comparisonQueries TODO ::// suppress the outputting of headers if the flag for the stream is a
        queryCSV('SELECT', streams, comparisonQueries, function(counter){ matchCount += counter; });
        // streams.write = fs.createWriteStream(outfile, {
        //   'flags' : 'a'
        // });
      });
      buffer = (buffer[buffer.length-1].length < 1) ? '' : buffer.pop();
      return true; //I'm ready for the next data chunk
    });

    comparisonStream.on('error', function(e){
      console.error(e.message);
    });
  }
  /**
  * Split the CSV into a set of CSVs grouped by a field match value (i.e. email -> domain? regex? )
  * @param  fileName    string       path/to/source/csv/to/split
  * @param  params      object
  * @param  callback    function    what to do at the end of the async
  *
  * @visibility private
  */
  function splitCSV(fileName, params, callback){
    do{
      //Trim the .csv off and concatenate the current time in milliseconds
      var outDir = generateUnique('./CSV/splitTest/' + fileName, 5);
    }while(fs.existsSync(outDir));
    fs.mkdir(outDir, function(err){
      if(err){
        console.error(err);
        process.exit();
      }
    });
    var streams = {
      read : fs.createReadStream(fileName, {'encoding' : 'UTF-8'}),
      write : {}
    };
    //Function globals (need to be maintained between chunks in the data stream)
    var buffer = '';
    var headers = false;
    var matchCount = 0;
    streams.read.on('data', function(chunk){
      //Add the next chunk on to the outstanding buffer, and split the buffer by the new line chatacters
      buffer += chunk;
      buffer = buffer.split(NEW_LINE);
      // The last element might not be complete in the chunk (best be on the safe side) - we'll handle this at the end
      var lines = buffer.slice(0, buffer.length-1);
      //If we haven't got the headers, get all of them from the source
      if(headers === false){
          headers = getHeaders(lines.shift());
      }
      lines.forEach(function(line){
        var row = createRow(headers, line);
        var category = params.category(row[params.header]);
        if(streams.write.hasOwnProperty(category)){
          streams.write[category].write(line + os.EOL);
        }
        else{
          streams.write[category] = fs.createWriteStream(outDir + '/' + category + '.csv');
          streams.write[category].write(headers.join(',') + os.EOL);
          streams.write[category].write(line + os.EOL);
        }
      });
    });
    streams.read.on('end', function(){
      for(var stream in streams.write){
        if(streams.write.hasOwnProperty(stream)){
          streams.write[stream].end();
        }
      }
    });
  }
  /** Example PARAMS for select, update, insert, remove
   * fileName = './query/query.csv';
   * params   = {
   *    queries  : [
   *     {'header' : 'email', 'condition' : 'contains', 'value' : 'gmail.com'},
   *     {'header' : 'age', 'condition' : 'higher than', 'value' : '30'},
   *     {'matchCondition' : 'ALL'}],
   *   select : ['email', 'age']
   * };
   * callback = function(count){ console.log('Affected Records : '+count);};
   */

  /**
    Method performs a SELECT search on the filename using the given parameters
  */
  this.select = function(fileName, params, callback){
    var streams = createStreams(fileName, 'output_' + fileName);
    queryCSV('SELECT', streams, params, callback);
  };
  /**
    Method performs an UPDATE (search and replace) on the filename using the given params
  */
  this.update = function(fileName, params, callback){
    var streams = createStreams(fileName, 'output_' + fileName);
    queryCSV('UPDATE', streams, params, callback);
  };
  /**
    Method performs an INSERT at the end of the filename, returning the new larger file
  */
  this.insert = function(fileName, params, callback){
    var streams = createStreams(fileName, 'output_' + fileName);
    queryCSV('INSERT', streams, params, callback);
  };
  /**
    Method performs a REMOVE where the given params are met
  */
  this.remove = function(fileName, params, callback){
    var streams = createStreams(fileName, 'output_' + fileName);
    queryCSV('REMOVE', streams, params, callback);
  };
  /*
   * Example call for compare....
   * fileName     = './comparisonFiles/source.csv'
   * cmpFileName  = './comparisonFiles/comparison.csv'
   * fields       = ['email', 'location']
   * condition    = EQUAL
   */
  this.compare = function(fileName, cmpFileName, fields, condition){
    compareCSV(fileName, cmpFileName, fields, condition);
  };
  /**
   * Example params for split
   * fileName = './splitFiles/split.csv'
   * params   = {
   *              header : 'email',                           //Header to split on
   *              category : function(value){                 //how to split the value to find the grouping
   *                          return value.split('@').pop();
   *                      }
   *            }
   * callback = callback function on completion // TODO :: consider replacing this with promisess
   */
  this.split = function(fileName, params, callback){
    splitCSV(fileName, params, callback);
  };
  this.sample = '';
  this.random = '';
  this.deduplicate = '';
}

module.exports = new CSV();
