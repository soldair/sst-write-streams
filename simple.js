var path = require('path')
, fs = require('fs')
, sortedbuckets = require('sorted-key-buckets') 
, monotonic = require('monotonic-timestamp')
//, bs = require('binarysearch')


module.exports = function(dir,serializer,deserializer,comparitor){

  var mem = [];
  var tables = [];
  var i = 0;

  var api = {
    dir:dir,
    buffered:0,
    comparitor:comparitor,
    serializer:serializer,
    deserializer:deserializer,
    put:function(key,value,cb){
      var z = this
      , range = sortedbuckets(key,tables)
      ;
      if(!range[1]) {
        // new table
        range[1] = [[key,value,cb]];
        fs.open(path.join(this.dir,"sst."+(i++)),'a+',function(err,fd){
          var cbs = range[1],cb,args;
          range[1] = fd;
          if(err) {
            z._removeTable(range);
            while(cbs.length) {
              cb = cbs.shift()[2];
              if(cb) cb(err);
            }
          }

          while(cbs.length) {
            args = cbs.shift();
            z._write(args[0],args[1],fd,args[2]);
          }
          
        });
        //return;
      } else if(Array.isArray(range[1])){
        range[1].push([key,value,cb]);
      } else {
        z._write(key,value,range[1],cb)
      }
    },
    _write:function(k,v,fd,cb){
      var z = this
      , serialized = z.serializer({key:k,value:v,t:monotonic()})

      if(!Buffer.isBuffer(serialized)) serialized = new Buffer(serialized+"\n");
      var  l = serialized.length;

      z._buffered += l;
      return fs.write(fd,serialized,0,l,null,function(err,bytes){
        z._buffered -= l;
        cb(err,bytes);
      })
    },
    _removeTable:function(range){
      for(var i=0;i<tables.length;++i){
        if(range === tables[i]) {
          tables[i].splice(i,1);
          return;
        }
      }
    }
  };

  return api;

}

