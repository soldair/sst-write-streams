var test = require('tape')
var size = require('prettysize');
var simple = require('../simple')

test('can write data simply',function(t){
  var dir = __dirname+'/db'  

  var writer = simple(dir,JSON.stringify,JSON.parse,function(o1,o2){
    if(o1.key > o2.key) return 1;
    else if(o1.key < o2.key) return -1;
    else {
      if(o1.t > o2.t) return 1;
      else if(o1.t < o2.t) return -1;
    }
    return 0;
  });

  var done = 0
  , spawned = 0
  , start = Date.now()
  , k = 0
  , target = 100000
  , _size = 0
  , _insize = 0
  , work = function(){
    _insize += 100+(++k+'').length;
    spawned++;;
    writer.put(k,'1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890',function(err,size){
      _size += size;
      done++;
      if(done === target) {
        report(_size,_insize,start,target);
        t.end();
      }else if(spawned < target)work();
    })
  };

  var i = 3;
  while(--i) work();
});


function report(_size,_insize,start,target){
  var end = Date.now();
  var ms = end-start;
  console.log('put ',target,'keys in ',ms,'ms. thats ',target/ms,' per ms. or',(target/ms)*1000,"per second");
  console.log(size(_insize)," data passed through put",size((_insize/ms)*1000),' per second');
  console.log('wrote ',size(_size),"to disk",size((_size/ms)*1000),' per second');
}




