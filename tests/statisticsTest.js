var statisticsTest = function(){
  var CSV = require('../csv.js'),
      fs = require('fs'),
      chai = require('chai');

  function jsonData(statConfig, expectedObject){
    describe('STATS : JSON RESULT TEST', function(){
      var resultObject = {};
      it("Should return a " + expectedObject.TOTALS.length + " length array for totals and parcentages", function(done){
        CSV.stats(statConfig).then(function(result){
          resultObject = result;
          // ?
          result.forEach(function(element, index, array){
            chai.assert.equal(result.element, expectedObject.element);
          });
          done();
        }).catch(function(err){
          done(err);
        });
      });
    });
  }
}
