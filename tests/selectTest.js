var selectTest = function(){

  var CSV = require('../csv.js'),
      fs = require('fs'),
      chai = require('chai');

  function equals(query, expectedRows){

    describe('Select a string from a CSV given a query', function(){
      var persistResult = {};
      it("Should return a count of " + expectedRows + " row(s)", function(done){
        CSV.query(query).then(function(result){
            chai.assert.equal(expectedRows, result.rows);
            persistResult = result;
            done();
        }).catch(function(err){
          done(err);
        });
      });
      it("Should generate a single file", function(){
        fs.stat(persistResult.writeFile, function (err, stat) {
          var exists = (err === null) ? true : false;
          if(exists){
             fs.unlink(persistResult.writeFile);
          }
          chai.assert.equal(exists, true);
        });
      });
    });

    function stringTests(query, expectedRows)
    {
      describe('Perform a string select given a valid query', function(){

        var queryResult = {};
        /**
         * Let's get the results of the query done.
        **/
        beforeEach(function(done){
          CSV.query(query).then(function(result){
            queryResult = result;
            done();
          });
        });

        //shall we embed in an equals test? 
        query.condition = '==';
        it('It should return a valid result object', function(){
          var objectValid = {
            file : 'path/to/file',
            rows: 0,
            time: 0
          };
          chai.assert.notDeepEqual(objectValid, queryResult);
        });


        //EQUAL
        //CONTAINS
        //NOT EQUAL
        //BEGINS WITH
        //ENDS WITH ?
      });

      describe('Perform a select query on a csv given an invalid FROM csv', function(){
        //NO FROM given
        //INVALID PATH
        //MISSING EXTENSION
      });

      describe('Perform a string select query on a csv given valid matches', function(){
        //ONE MATCH
        //MULTIPLE MATCHES
        //ENTIRE MATCH
      });
      describe('Perform a string select query on a csv given no matches', function(){
        //NO MATCHES
      });

      describe('Perform a string select query on a csv given an invalid where field', function(){
        //INVALID WHERE FIELD - 1 where clause
        //INVALID WHERE FIELD - 1 of 2 where claueses (OR)
        //INVALID WHERE FIELD - 1 of 2 where clauses (AND)
      });
      describe('Perform a string select query on a csv given an invalid where value', function(){
        //NO VALUE
        //INVALID TYPE
        //INVALID VALUE
      });
      describe('Perform a string select query on a csv given an invalid type where condition', function(){
        //HIGHER THAN / LOWER THAN
        //AFTER [DATE]
      });
      return true; //completed
    }

    describe('Perform an integer select given a valid query', function(){
      //EQUAL
      //LOWER
      //HIGHER
      //NOT EQUAL
      //MODULOUS
      //BETWEEN
    });
    /**
     * INTEGER SPECIFIC TESTS
    **/
    describe('Perform an integer select query on a csv given valid matches', function(){});
    describe('Perform an integer query on a csv given no matches', function(){});

    describe('Perform an integer select given an invalid where field', function(){});
    describe('Perform an integer select given an invalid where value', function(){});
    describe('Perform an integer select given an invalid type where condition', function(){});

    describe('Perform a date select given a valid query', function(){

    });

    describe('Perform a date select query on a csv given an invalid where field', function(){});
    describe('Perform a date select query on a csv given an invalid where value', function(){});
    describe('Perform a date select query on a csv given an invalid type where condition', function(){});

    describe('Perform a minimum select size test - one field / one row', function(){});
    describe('Perform a maximum select stress test - 1000 fields, 1000000 rows', function(){});
  }

  this.execute = function(){
    var query = {
      SELECT: ['*'],
      FROM: './tests/csvs/sml_3_col.csv',
      WHERE: [
        { field: 'email', condition: '==', value: 'github@awesome.com' }
      ],
      MATCH: 'ANY'
    };
    expectedRows = 1;
    equals(query, expectedRows);
  }
}

module.exports = new selectTest();


//string
  //EQUAL

  //CONTAINS
  //LOWER THAN (Should fail gracefully)

//integer or float
  //EQUAL
  //LOWER THAN
  //CONTAINS (run - but warn)

//date
  //EQUAL
  //LOWER THAN
  //CONTAINS (run - but warn)
