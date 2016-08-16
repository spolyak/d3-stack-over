var csv   = require("fast-csv"),
    fs    = require("fs"),
    async = require('async');

var map = {};

 function has(object, key) {
   return object ? hasOwnProperty.call(object, key) : false;
 }

function convertMovies(next) {
    count = 0;
    csv
	.fromPath('movies.csv')
	.on("data", function(data){
		var year = data[4];
		var title = data[2];
		if(count > 0) {
		  if(has(map,year)) {
              map[year].titles.push(title);
		  } else {
		  	  map[year] = {titles:[]};
			  map[year].titles.push(title);
		  }
		}
		count++;
	})
	.on("end", function(){
        return next();
    });
}

function outputData(next) {

  var header = 'year,';
  var years = Object.keys(map); 
  for (var i = 0; i < years.length; i++) {
  	var entry = map[years[i]];
  	for (var j = 0; j < entry.titles.length; j++) {
  	  header = header + '"' + entry.titles[j].trim() + '",';
  	}
  }
  console.log(header);

  for (var i = 0; i < years.length; i++) {
  	var yearDriver = years[i];
  	var record = yearDriver + ',';
    for (var j = 0; j < years.length; j++) {
  	  var entry = map[years[j]];
  	  var year = years[j];
  	  for (var k = 0; k < entry.titles.length; k++) {
  	    if(yearDriver === year) {
  	    	record = record + "1,"
  	    } else {
  	    	record = record + '0,'
  	    }
  	  }
    }
    console.log(record)
  }

}

async.series([
  function(next) {
  	convertMovies(next);
  },
  function(next) {
  	outputData(next)
  }
], function(err) {});
