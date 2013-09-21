var test = require('tape')

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

  writer.put('hello','file',function(err,data){

    console.log(err,data);
    t.end();
  })

});







