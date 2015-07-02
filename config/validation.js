var Validation = function(){
  var fs = require('fs');

  this.isValidFile = function(fileName, extension){
    var valid = false;

    fs.stat(fileName, function (err, stat) {
      if(err === null && stat.isFile()){
        //The file exists, and is a file - great stuff
        if(fileName.split('.').pop() === extension){
          valid = true;
        }
      }
    }
    return valid;
  }


}

module.exports = new Validation();
