var test = require('tape');
var dataloop = require('./common')
var writeStreams = require('../');
var through = require('through');

var sets = 100000;

test("writestream large values",function(t){

  stream = writeStreams(__dirname+'/db')

  var s = Date.now();
  var key = 'large';
  dataloop(key,stream.createWriteStream(),sets,function(){
    var ms = Date.now()-s;

    t.ok(1,key+' done. did '+sets+' in '+ms+' ms');
    t.ok(sets,((sets/ms)*1000).toFixed(4)+' per s');
    t.end();
  });

});

test("writestream medium values",function(t){

  stream = writeStreams(__dirname+'/db')

  var s = Date.now();
  var key = 'med'
  dataloop(key,stream.createWriteStream(),100000,function(){
    var ms = Date.now()-s;

    t.ok(1,key+' done. did '+sets+' in '+ms+' ms' );
    t.ok(sets,((sets/ms)*1000).toFixed(4)+' per s');
    t.end();
  });

});

test("writestream small values",function(t){

  stream = writeStreams(__dirname+'/db')

  var s = Date.now();
  var key = 'small'
  dataloop(key,stream.createWriteStream(),100000,function(){
    var ms = Date.now()-s;

    t.ok(1,key+' done. did '+sets+' in '+ms+' ms' );
    t.ok(sets,((sets/ms)*1000).toFixed(4)+' per s');
    t.end();
  });

});



test("writestream large values /dev/null",function(t){

  stream = writeStreams(__dirname+'/db',function(){
    return through();
  },{
    serializer:function(obj){
      return obj.key+'|'+obj.value;
    }
  })

  var s = Date.now();
  var key = 'large';
  dataloop(key,stream.createWriteStream(),sets,function(){
    var ms = Date.now()-s;

    t.ok(1,key+' done. did '+sets+' in '+ms+' ms');
    t.ok(sets,((sets/ms)*1000).toFixed(4)+' per s');
    t.end();
  });

});

