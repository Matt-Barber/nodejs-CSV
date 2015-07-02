var selectTest = function(){

  var CSV = require('../'),
      fs = require('fs'),
      chai = require('chai');

  function equals(testCSV, params, expectedRows){
    /** SELECT TESTS **/
    var test1 = params,
        test2 = params,
        test3 = params;

    describe('SELECT : EQUALS TESTS : 1', function(){
      var persistResult = {};
      it("Should return a count of " + expectedRows + " row(s)", function(done){
        CSV.select(testCSV, params).then(function(result){
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
          chai.assert(exists, true);
        });
      });
      /** Test string SELECT  - invalid param **/
      /** Test integer SELECT - valid param   **/
      /** Test integer SELECT - invalid param **/

    });
  }

  this.execute = function(){
    var params = {
        queries: [
          {header: 'email', condition: 'equal', value: 'github@awesome.com'},
        ],
        matchCondition : 'ALL',
        select: '*'
      },
      readFile = './tests/csvs/sml_3_col.csv',
      expectedRows = 1;
    equals(readFile, params, expectedRows);
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
