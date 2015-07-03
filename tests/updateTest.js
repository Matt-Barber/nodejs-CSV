var updateTest = function(){
  var CSV = require('../'),
      fs = require('fs'),
      chai = require('chai');

  /**
    * Contains tests, searches each record for where a field contains a value
    * Updates the given fields, by searching and replacing the value in that record
    * Where the contains condition is met.
  **/
  function contains(testCSV, params, expectedRows){
    describe('UPDATE : CONTAINS : TEST 1', function(){
      var persistResult = {};
      it("Should return a count of " + expectedRows + " row(s)", function(done){
        CSV.update(testCSV, params).then(function(result){
          chai.assert.equal(expectedRows, result.rows);
          persistResult = result;
          done();
        }).catch(function(err){
          done(err);
        });
      });
      it("Should generate a single file", function(done){
        fs.stat(persistResult.writeFile, function (err, stat) {
          var exists = (err === null) ? true : false;
          chai.assert.equal(exists, true);
          done();
        });
      });
      it("Should not be the same", function(done){
        fs.stat(persistResult.writeFile, function(err, stat){
          if(err) done(err);
          if(stat.size>5000) done('Too Big!');
        });
        fs.readFile(testCSV, function (err, sourceData) {
          if(err) done(err);
          fs.readFile(persistResult.writeFile, function (err, newData) {
            if(err) done(err);
            chai.assert.notEqual(sourceData, newData);
          });
        });
        fs.unlink(persistResult.writeFile);
        done();
      });
    });

  }

  this.execute = function(){
    var params = {
        queries: [
          {header: 'email', condition: 'contains', value: 'awesome.com'},
        ],
        matchCondition : 'ALL',
        select: '*',
        updates: [
          {field: 'email', search: 'awesome.com', value: 'awesome.co.uk'}
        ]
      },
      readFile = './tests/csvs/sml_3_col.csv',
      expectedRows = 2;
    contains(readFile, params, expectedRows);

  }
}

module.exports = new updateTest();
