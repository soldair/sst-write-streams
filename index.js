/*
what are the results of this lib

write sorted data to disk.

specify how large a sst file may be. 
  files will almost always be one key value pair larger than limit in bytes.

only open a set number of file descriptors to "incomplete" files
  this means that if i have 4 files and 4 is the configured limit i must compact the files into one


writes always work so i will open file descriptors beyond the limit if pause is not respected.


*/



var fs = require('fs')
, EventEmitter = require('events').EventEmitter
, sortedbuckets = require('sorted-key-buckets') 
, monotonic = require('monotonic-timestamp')
, through = require('through')
, compaction = require('./compaction')

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

  if(options.serializer === undefined) options.serializer = JSON.stringify;

  // before compaction i will allow up to 10 open sst streams
  options.openTables = options.openTables||10;
  // when i reach openTables limit 
  // i will check if i have compactions running.
  // if i can start a compaction i do.
  // all tables being compacted are removed from openTables
  // if i still excede openTables i will pause the external writeable stream.
  options.concurrentCompactions = options.concurrentCompactions||1;

  em.tables = [];
  em.compacting = false;
  em.lastWrite = 0; // can be used to implement snapshotting


  em.writing = [];

  var serializer = options.serializer;

  if(!stream) stream = function(path){
    console.log('openeing write stream ',path)
    var ws = fs.createWriteStream(path,'a+');
    return ws;
  }

  em.batch = function(){

  }

  em.del = function(){

  }

  em.put = function(k,v,cb){
    
  }

  em._getStream = function(){
    // if the stream is already
    var fpath = path+'/sst.'+(++fileId);
    var s = stream(fpath);
    s.sstpath = fpath; 
    return s;
  }

  em._checkCompaction = function(){
    var z = this;
    // time to grow up boss
    if(z.tables.length > options.openTables) {
      
      z.compact();
      
    }
  }

  // compaction options.  
  em.createWriteStream = function(){
    // keep track of all write streams im writing too
    var streams = {}
    , s = through(function(obj){ 
      var z = this;
      em.lastWrite = obj.t = monotonic()
      
      //if(obj.batch) ... this is cool
      //deletes just have no value.

      var range = sortedbuckets(obj.key,em.tables);
      if(serializer) obj = serializer(obj);


      if(!range[1]) {

        em._checkCompaction();

        range[1] = em._getStream();
        range[1].startKey = obj.key;
        range[1].bytes = 0;

        if(em.compacting) {
          console.log('paused');
          z.pause();
        }


      }

      range[1].bytes = z.bytes += obj.length||0;
      range[1].count++;
      z.count++;
      
      range[1].endKey = obj.key;
      if(!streams[range[1].sstpath]) refStream(range[1].sstpath,range[1]);

      // must write this turn.
      console.log('writing ',range[1])
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
      // handle listener warning as each active write stream will need a listener bound to each sst write stream it refrences
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

    em.on('compaction',function(){
      console.log('compaction',arguments);
      process.nextTick(function(){
        if(s.paused) s.resume();
      });
    })

    return s
  }

  em.compact = function(cb){

    var z = this;
    if(z.compacting) {
      console.log('compaction queued');
      return z.compacting.push(cb);
    }
    // look at open tables array
    // take all tables that are open and remove them from the array
    var tables = z.tables;
    z.tables = [];

    var rs = options.readStream|| fs.createReadStream
    , closes = 0
    , streams = 0
    , readable = []
    , closed = function(){
      // create read streams to all of them.
      // get a new writeable stream

      // pass source readable streams to compaction piped to writeable
      
      compaction(readable)
      .pipe(em._getStream()).on('close',function(){
        var resultStream = this;

        // on end add writeable stream back to tables.
        // emit compaction {table: start: end:}
        //
        // add table
        sortedbuckets.insert([this.endKey,this],z.tables);
        // unlink the original tables.
 
       
        var todo = tables.length;
        for( var i=0;i<tables.length;++i){
          var errors = [];
          fs.unlink(tables[i][1].sstpath,function(err){
            if(err) errors.push(err)
            if(--todo) return;
            if(cb) cb(errors.length?errors:false,resultStream); 

            var cbs = em.compacting;
            em.compacting = false;

            z.emit('compaction',{table:this.sstpath,start:this.startKey,end:this.endKey});
            if(cbs.length) {
              // if compaction was queued. do one more.
              em.compact(function(err,data){
                while(cbs.length) { 
                  if(cb) cbs.shift()(err,data);
                }
              });
            }

          });
        }

        
        
      })

    }

    if(tables.length < 2) return process.nextTick(function(){
      cb(false,false);
    }); 


    em.compacting = [];
    // all tables should be open streams.
    for(var i=0;i<tables.length;++i){
      if(!tables[i][1]) continue;
      streams++;
      console.log('closing table ',i,tables[i][1])
      // call end on the streams which removes them as write targets from input write data.
      tables[i][1].once('close',function(){

        console.log('sstpath ',this.sstpath);
        // get a readble stream to each table
        readable.push(rs(this.sstpath));
        closes++;
        if(closes != streams) return;
        closed();
      }).end();
    }


  }

  em.end = function(cb){
    // prevent new write streams.
    // end all write streams
    // compact
    // cb
    // emit close.
  }

  em._parseStream = function(){

  }

  return em;
}

