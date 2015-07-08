#CSV Query Module (NodeJS)

##Description
Parses a CSV file and creates a new file based on the conditions, or returns a JSON data structure for instance SELECT, REMOVE, UPDATE, INSERT, COMPARE, SPLIT, SAMPLE, DUMMY, STATS.

##Warnings

This is a very early build, very subject to change and is literally an experimental module - I wouldn't recommend using this in it's current state - but feedback, advice and or assistance welcome.

##Known Issues
I'll get round to linking these in once the functionality expands further. Currently - lots.

##Specification

Built using Node.JS, some methods return ES6 promises - these are documented

##Using the module

At the endpoint you'd like to use simply add the following PHP
```javascript
var CSV = require('./CSV.js'),
    query = {
      SELECT: ['*'],
      FROM: './tests/csvs/sml_3_col.csv',
      WHERE: [
        { field: 'email', condition: '==', value: 'github@awesome.com' }
      ],
      MATCH: 'ANY'
    };

//Select example
CSV.query(query).then(function(result){
  console.log('New file created : ' + result.writeFile);
  console.log('Row\s found : ' + result.rows);
}).catch(function(err){
  console.error(err.message);
  process.exit(1);
});
```
##Motivation

The primary motivation behind this project is having to deal with analysing and making sense of data exports from data platforms, before using them. For instance from social media sites, CRM systems, ESPs etc.

##Tests

Example tests are included in the testing folder, using the chai and mocha test driven frameworks.



*Licence:* GNU
