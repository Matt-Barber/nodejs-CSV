var chai = require('chai');

//Include a stacktrace on failing assertions
chai.config.includeStack = true;
//Globalise Chai's assertion and expect methods
global.expect = chai.expect;
global.AssertionError = chai.AssertionError;
global.Assertion = chai.Assertion;
global.assert = chai.assert;
