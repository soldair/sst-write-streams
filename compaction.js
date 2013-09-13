var mergesort = require('mergesort-stream')
, through = require('through')
, undef

// expects data events that are objects with "key", "value" keys set. value must be undefined for deletes.
// the "t" key is compared to resolve unique ordering
module.exports = function(streams){
  var last;
  return mergesort(cmp,streams).pipe(through(function(data){
    this.endKey = last?last.key:data.key;
    if(!this.startKey) this.startKey = data.key;
    if(last && cmp(last,data) === 0) {
      // set last to the most recent version of the key
      if(last.t <= data.t) last = data
    } else {
      // if the item was not deleted
      if(last && last.value !== undef) this.queue(last)
      last = data
    }
  },function(){
    if(last && last.value !== undef) {
      this.queue(last);
    }
    this.queue(null)
  }))
}

function cmp(a,b){
    return a.key>b.key?1:a.key<b.key?-1:0
}  

