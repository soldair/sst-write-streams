


module.exports = dataloop;
module.exports.rand = randomKeys;
module.exports.data = data;

var str = {
  large : '123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890',
  medium : '1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890',
  small : '12345678901234567890'
}

var i = 0;
function data(key){
  return {key:randomKeys(++i+""),value:str[key]}
}

function dataloop(key,stream,limit,cb){
  var t = 0;
  var c = 0;
  (function fn(){
    t++;
    c++;
    if(!stream.write(data(key))){
      return stream.once('drain',function(){
        fn();
      })
    }

    if(t == limit) return cb();

    if(c < 100) fn();
    else setImmediate(function(){
      c = 0;
      fn();
    });
  }())
}

var crypto = require('crypto')
var dict = {}
function randomKeys(i){
  if(dict[i]) return dict[i];
  return crypto.createHash('sha1').update(i+'').digest('hex');
}

var t = Date.now();;
for(var i=0;i<100000;++i){
  dict[i] = randomKeys(i);
}
console.log('generated 1,000,000 random keys in ',Date.now()-t,'ms')
