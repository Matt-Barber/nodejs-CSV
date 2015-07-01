var CSV = require('../');
var fs = require('fs');
var chai = require('chai');

/**
 * Comparison Tests using Mocha and Chai
**/
describe('String Comparisons', function(){
  //Use the small csv for this test
  var testCSV = './csvs/sml_3_col.csv';
  var outputCSV = testCSV.substring(0, testCSV.length-3) + '_output.csv';
  //Tests for Select -> equals
  describe('select -> equals', function(){
    var params = {
        queries: [
          {'header' : 'email', 'condition' : 'equal', 'value' : 'github@awesome.com'},
          {'matchCondition' : 'ALL'}
        ],
        select: '*'
      };
    it("Should create a new file, of one row, with a count of 1 returned", function(done){
      CSV.select(testCSV, params, function(count){
        //assert that the counter returned from the async should be 1
        chai.assert.equal(count, 1);
        //and the new file should exist in the filesystem
        fs.exists(outputCSV, function (exists) {
          chai.assert.equal(exists, true);
        });
        done();
      });
    });

  })
})
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
