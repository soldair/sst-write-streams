
var fs = require('fs')
, EventEmitter = require('events').EventEmitter
, sortedbuckets = require('sorted-key-buckets') 
, monotonic = require('monotonic-timestamp')
, through = require('through')
, mergesort = require('mergesort-stream')

var fileId = 0;

// expects a stream of objects with key
// if the object has a batch key and it is an array find an sst where all keys sort less than all keys in the batch
// serialize batch and write it in one go.
// custom keys will be persisted in the sst. t is reserved for internal ordering

module.exports = function(path,stream,options){
  var em = new EventEmitter()
  , pauses = 0

  options = options||{};
  if(typeof stream != 'function'){
    options = stream||{};
    stream = false;
  }

  if(options.serializer === undefined) options.serializer = JSON.stringify;//jsonb.stringify;

  em.tables = [];
  em.compacting = false;

  var serializer = options.serializer;
  if(!stream) stream = function(path){
    var ws = fs.createWriteStream(path,'a+');
    return ws;
  }

  // compaction options.  
  em.createWriteStream = function(){
    // keep track of all write streams im writing too
    var streams = {}
    , s = through(function(obj){ 
      var z = this;
      obj.t = monotonic()
      
      //if(obj.batch) ... this is cool
      //deletes just have no value.

      var range = sortedbuckets(obj.key,em.tables);
      if(serializer) obj = serializer(obj);

      z.bytes += obj.length||0;
      z.count++;

      if(!range[1]) {
        var fpath = path+'/sst.'+(++fileId);
        range[1] = stream(fpath);
        range[1].sstpath = fpath;
      }

      if(!streams[range[1].sstpath]) refStream(range[1].sstpath,range[1]);

      // must write this tick.
      if(!range[1].write(obj+"\n")) {
        paused = true;
        if(!streams[range[1].sstpath].paused) {
          streams[range[1].sstpath].paused = true;
          pauses++;
        }
        z.pause();
      }
    },function(){
      cleanup()
      s.end = Date.now();
      this.queue(null);
    });
    s.bytes = 0;
    s.count = 0;
    s.start = Date.now();

    function refStream(path,ws){
      streams[path] = {
        paused:false,
        ws:ws,
        events:{
          drain:function(){
            if(streams[path].paused) {
              streams[path].paused = false;
              pauses--;
              if(!pauses) s.resume();
            }
          },
          end:function(){
            cleanup(path);
          }
        }
      };
      // handle listener warning as each active write stream will need a handle to each sst write stream it refrences
      ws.setMaxListeners(Infinity);
      ws.on('drain',streams[path].events.drain).on('end',streams[path].events.end);
    }

    function cleanup(path){
      if(!path) return Object.keys(streams).forEach(function(path){
        cleanup(path);
      });

      if(!streams[path]) return;
      streams[path].ws.removeListener('drain',streams[path].events.drain);
      streams[path].ws.removeListener('end',streams[path].events.end);
      if(streams[path].paused) {
        pauses--;
        if(!pauses) s.resume();
      }
      delete streams[path];
    }

    return s
  }

  return em;
}

