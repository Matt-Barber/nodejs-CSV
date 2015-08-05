var base64Test = function(){
  var CSV = require('../csv.js'),
      fs = require('fs'),
      chai = require('chai');

  function singleFile(fileName){
    describe('BASE64 : ENCODE SINGLE FILE', function(){
      it("Should generate a single file", function(done){
          CSV.base64(fileName).then(function(result){
            fs.stat(result.writeFile, function(err, stat){
              var exists = (err === null) ? true : false;
              if(exists){
                fs.unlink(fileName);
              }
              chai.assert(exists, true);
            });
        });
      });
    });
  }
  this.execute = function(){
    var fileName = './tests/csvs/sml_3_col.csv';
    singleFile(fileName);
  }
}

module.exports = new base64Test();
