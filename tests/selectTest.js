var selectTest = function(){

  var CSV = require('../csv.js'),
      fs = require('fs'),
      chai = require('chai');

  function equals(query, expectedRows){
    /** SELECT TESTS **/
    describe('SELECT : EQUALS TESTS : 1', function(){
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
      /** Test string SELECT  - invalid param **/
      /** Test integer SELECT - valid param   **/
      /** Test integer SELECT - invalid param **/

    });
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
