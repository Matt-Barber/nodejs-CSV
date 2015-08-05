/**
  Automated testing file, loops over the test modules and calls the execute command
  To add to this simply push on to end of tests object.
  Test modules should only expose a single execute method - this method calls the sub tests
**/
var tests = {
  select : require('./selectTest.js'),
  update : require('./updateTest.js'),
  compare: require('./comparisonTest.js'),
  base64 : require('./base64Test.js')
},
    test = '';

for(test in tests){
  if(tests.hasOwnProperty( test )){
    tests[test].execute();
  }
}
//TODO :: Synchronous testing.
console.log('Executing....');
