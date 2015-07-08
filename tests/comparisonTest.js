var comparisonTest = function(){

  var CSV = require('../csv.js'),
      fs = require('fs'),
      chai = require('chai');

  function equals(query, expectedRows){
    /** Comparison Equals Test **/
    describe('COMPARE : EQUALS TESTS : 1', function(){
      var persistResult = {};
      it("Should return a count of " + expectedRows + " row(s)", function(done){
        CSV.compare(query).then(function(result){
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
      /** Test string compare  - invalid param **/
      /** Test integer compare - valid param   **/
      /** Test integer compare - invalid param **/

    });
  }

  this.execute = function(){
    var query = {
      COMPARE: './tests/csvs/sml_3_col.csv',
      WITH: './tests/csvs/sml_4_col.csv',
      WHERE: [
        {field: 'email', condition: 'CONTAINS'}
      ]
    },
      expectedRows = 2;
    equals(query, expectedRows);
  }
}

module.exports = new comparisonTest();
