// DBCollection.prototype.pipe = function() {
//   print("pipe")
// }

var db = db.getSisterDB('t');

// Save the state of the find
var find = DBCollection.prototype.find;

//
// View Object
var View = function(collection, obj) {
  if(!obj.options) obj.options = {};
  var self = this;

  var addPredicate = function(_self, _obj) {
    return function(predicate) {
      if(_obj.query['$and']) {
        _obj.query['$and'].push(predicate);
        return _self;
      } 

      _obj.query = {'$and': [_obj.query, predicate]};
      return _self;
    }
  }

  var addQueryModifier = function(_self, _obj) {
    return function(name, value) {
      if(name[0] !== '$') throw new Error("A query modifier must begin with a $.");
      _obj.query[name] = value;
      return _self;      
    }
  }

  var batchSize = function(_self, _obj) {
    return function(size) {
      _obj.options['batchSize'] = size;
      return _self;      
    }
  }

  var comment = function(_self, _obj) {
    return function(comment) {
      _obj.query['$comment'] = comment;
      return _self;
    }
  }

  var project = function(_self, _obj) {
    return function(fields) {
      _obj.fields = fields;
      return _self;
    }
  }

  var sort = function(_self, _obj) {
    return function(sort) {
      _obj.sort = sort;
      return _self;
    }
  }

  var distinct = function(_self, _collection, _obj) {
    return function(value) {
      _obj.command = {
          distinct: _collection._shortName
        , key: value
        , query: _obj.query
      }

      return 'result';    
    }
  }

  var skip = function(_self, _obj) {
    return function(_skip) {
      _obj.options['skip'] = _skip;
      return {
          sort: sort(_self, _obj)
        , limit: limit(_self, _obj)
      }
    }
  }

  var limit = function(_self, _obj) {
    return function(_limit) {
      _obj.options['limit'] = _limit;
      var object = {
          sort: sort(_self, _obj)
        , limit: limit(_self, _obj)        
      }

      if(limit == 1) object['fetchOneThenRemove'] = fetchOneThenRemove(_self, collection, _obj);
      if(limit == 1) object['fetchOneThenReplace'] = fetchOneThenReplace(_self, collection, _obj);
      return object;
    }
  }

  var upsert = function(_self, _obj) {
    return function() {
      _obj.upsert = true;
      return _self;
    }
  }

  var fetchOneThenRemove = function(_self, _collection, _obj) {
    return function() {
      _obj.command = {
          findAndModify: _collection._shortName
        , query: _obj.query
        , remove: true
      }

      if(_obj.sort) _obj.command['sort'] = _obj.sort;
      if(_obj.fields) _obj.command['fields'] = _obj.fields;
      return "";
    }
  }

  var validateReplacementDoc = function(doc) {
    for(var key in doc) {
      if(key[0] == "$") {
        throw "Update operators are not allowed at the top-level of an replacement document.";
      }
    }    
  }

  var validateUpdateDoc = function(doc) {
    for(var key in doc) {
      if(key[0] != "$") {
          throw "Only update operators are allowed at the top-level of an update document.";
      }
    }    
  }

  var fetchOneThenReplace = function(_self, _collection, _obj) {
    return function(doc) {      
      validateReplacementDoc(doc);
      // Build command
      _obj.command = {
          findAndModify: _collection._shortName
        , query: _obj.query
        , update: doc
      }

      if(_obj.sort) _obj.command['sort'] = _obj.sort;
      if(_obj.fields) _obj.command['fields'] = _obj.fields;
      if(typeof _obj.upsert == 'boolean') _obj.command['upsert'] = _obj.upsert;
      return "";      
    }
  }

  var fetchOneThenUpdate = function(_self, _collection, _obj) {
    return function(doc) {      
      validateUpdateDoc(doc);
      // Build command
      _obj.command = {
          findAndModify: _collection._shortName
        , query: _obj.query
        , update: doc
      }

      if(_obj.sort) _obj.command['sort'] = _obj.sort;
      if(_obj.fields) _obj.command['fields'] = _obj.fields;
      if(typeof _obj.upsert == 'boolean') _obj.command['upsert'] = _obj.upsert;
      return "";      
    }    
  }

  //
  // Initial state
  this.addPredicate = addPredicate(this, obj);
  this.addQueryModifier = addQueryModifier(this, obj);
  this.batchSize = batchSize(this, obj);
  this.comment = comment(this, obj);
  this.distinct = distinct(this, collection, obj);
  this.skip = skip(this, obj);
  this.limit = limit(this, obj);
  this.project = project(this, obj);
  this.sort = sort(this, obj);
  this.fetchOneThenRemove = fetchOneThenRemove(this, collection, obj);
  this.fetchOneThenReplace = fetchOneThenReplace(this, collection, obj);
  this.fetchOneThenUpdate = fetchOneThenUpdate(this, collection, obj);
  this.upsert = upsert(this, obj);

  this.toQuery = function() {    
    return obj;
  }
}

// Set up our own fluent find
DBCollection.prototype.find = function(selector) {
  var query = {query: selector};
  return new View(this, query);
}

//
// Tests for Fluent API
//
function addPredicate() {  
  function adds_the_predicate_using_$and() {
    var query = db.fluent_api.find({x:2}).addPredicate({y:3}).toQuery();
    assert(JSON.stringify({$and: [{x:2}, {y:3}]}) == JSON.stringify(query.query));
  }

  adds_the_predicate_using_$and();
}

function addQueryModifier() {  
  function adds_a_first_modifier() {
    var query = db.fluent_api.find({x:2}).addQueryModifier("$hint", "myIndex").toQuery();
    assert(JSON.stringify({x:2, $hint: 'myIndex'}) == JSON.stringify(query.query));
  }

  function adds_a_second_modifier() {
    var query = db.fluent_api.find({x:2})
      .addQueryModifier("$hint", "myIndex")
      .addQueryModifier("$comment", "awesome").toQuery();
    assert(JSON.stringify({x:2, $hint: 'myIndex', $comment: 'awesome'}) == JSON.stringify(query.query));
  }

  function overwrites_an_existing_modifier() {
    var query = db.fluent_api.find({x:2})
      .addQueryModifier("$hint", "myIndex")
      .addQueryModifier("$hint", "myOtherIndex").toQuery();
    assert(JSON.stringify({x:2, $hint: 'myOtherIndex'}) == JSON.stringify(query.query));
  }

  function throw_an_error_when_provided_with_a_bad_modifier_name() {
    try {
      var query = db.fluent_api.find({x:2}).addQueryModifier("wrong", 42).toQuery();
      assert(false, 'Should throw on illegal query modifier');
    } catch(err) {}
  }

  adds_a_first_modifier();
  adds_a_second_modifier();
  overwrites_an_existing_modifier();
  throw_an_error_when_provided_with_a_bad_modifier_name();
}

function batchSize() {
  function should_track_batch_size() {
    var query = db.fluent_api.find({x:2}).batchSize(20).toQuery();
    assert(JSON.stringify({batchSize:20}) == JSON.stringify(query.options));
  }

  function should_use_the_last_batch_size_included() {
    var query = db.fluent_api.find({x:2}).batchSize(20).batchSize(30).toQuery();
    assert(JSON.stringify({batchSize:30}) == JSON.stringify(query.options));    
  }

  should_track_batch_size();
  should_use_the_last_batch_size_included();
}

function comment() {
  function adds_the_comment_query_modifier() {
    var query = db.fluent_api.find({x:2}).comment('awesome').toQuery();
    assert(JSON.stringify({x:2, $comment: 'awesome'}) == JSON.stringify(query.query));
  }

  function uses_the_last_comment_query_modifier() {
    var query = db.fluent_api.find({x:2})
      .comment('awesome').comment('even more so').toQuery();
    assert(JSON.stringify({x:2, $comment: 'even more so'}) == JSON.stringify(query.query));
  }

  function overwrites_previous_custom_setting_through_addQueryModifier() {
    var query = db.fluent_api.find({x:2})
      .addQueryModifier("$comment", "awesome").comment('even more so').toQuery();
    assert(JSON.stringify({x:2, $comment: 'even more so'}) == JSON.stringify(query.query));
  }

  function is_overwritten_by_subsequent_custom_setting_through_addQueryModifier() {
    var query = db.fluent_api.find({x:2})
      .comment('even more so').addQueryModifier("$comment", "awesome").toQuery();
    assert(JSON.stringify({x:2, $comment: 'awesome'}) == JSON.stringify(query.query));
  }

  adds_the_comment_query_modifier();
  uses_the_last_comment_query_modifier();
  overwrites_previous_custom_setting_through_addQueryModifier();
  is_overwritten_by_subsequent_custom_setting_through_addQueryModifier();
}

function count() {  
}

function cursorFlags() {  
}

function distinct() {
  function builds_the_correct_distinct_command() {
    var query = db.fluent_api.find({x:2});
    var result = query.distinct('x');
    assert(JSON.stringify({
            distinct: "fluent_api",
            key: "x",
            query: {x:2},
        }) == JSON.stringify(query.toQuery().command));
  }

  function throws_when_skip_was_specified() {
    try {
      db.fluent_api.find({x:2}).skip(10).distinct('x');
      assert(false, 'Should fail due to skip before distinct');
    } catch(err) {
    }
  }

  function throws_when_limit_was_specified() {
    try {
      db.fluent_api.find({x:2}).limit(10).distinct('x');
      assert(false, 'Should fail due to skip before distinct');
    } catch(err) {
    }
  }

  builds_the_correct_distinct_command();
  throws_when_skip_was_specified();
  throws_when_limit_was_specified();
}

function fetch() {
}

function fetchOne() {
}

function fetchOneThenRemove() {
  function builds_the_correct_findAndModify_command() {
    var query = db.fluent_api.find({x:2})
    query.project({x:1}).limit(1).sort({a:1}).fetchOneThenRemove();    
    assert(JSON.stringify({
        "findAndModify": "fluent_api",
        "query": {
          "x": 2
        },
        "remove": true,
        "sort": {
          "a": 1
        },
        "fields": {
          "x": 1
        }
      }) == JSON.stringify(query.toQuery().command));
  }

  function throws_when_skip_was_specified() {
    try {
      db.fluent_api.find({x:2}).skip(10).fetchOneThenRemove();
      assert(false, 'Should fail due to skip before fetchOneThenRemove');
    } catch(err) {      
    }
  }

  function throws_when_limit_other_than_1_was_specified() {
    try {
      db.fluent_api.find({x:2}).limit(2).fetchOneThenRemove();
      assert(false, 'Should fail due to limit before fetchOneThenRemove');
    } catch(err) {      
    }
  }

  builds_the_correct_findAndModify_command();
  throws_when_skip_was_specified();
  throws_when_limit_other_than_1_was_specified();
}

function fetchOneThenReplace() {
  function builds_the_correct_findAndModify_command_without_upsert() {    
    var query = db.fluent_api.find({x:2})
    query.project({x:1}).limit(1).sort({a:1}).fetchOneThenReplace({a:2});    
    assert(JSON.stringify({
      "findAndModify": "fluent_api",
      "query": {
        "x": 2
      },
      "update": {
        "a": 2
      },
      "sort": {
        "a": 1
      },
      "fields": {
        "x": 1
      }
    }) == JSON.stringify(query.toQuery().command));
  }

  function builds_the_correct_findAndModify_command_with_upsert() {    
    var query = db.fluent_api.find({x:2})
    query.project({x:1}).limit(1).sort({a:1}).upsert().fetchOneThenReplace({a:2});
    assert(JSON.stringify({
      "findAndModify": "fluent_api",
      "query": {
        "x": 2
      },
      "update": {
        "a": 2
      },
      "sort": {
        "a": 1
      },
      "fields": {
        "x": 1
      },
      upsert:true
    }) == JSON.stringify(query.toQuery().command));
  }

  function throws_when_the_replacement_document_is_invalid() {
    try {
      db.fluent_api.find({x:2}).fetchOneThenReplace({$a:2});
      assert(false, 'Should fail due to illegal fetchOneThenReplace');
    } catch(err) {}
  }

  function throws_when_skip_was_specified() {    
    try {
      db.fluent_api.find({x:2}).skip(10).fetchOneThenReplace({a:2});
      assert(false, 'Should fail due to skip specified');
    } catch(err) {}
  }

  function throws_when_limit_other_than_1_was_specified() {    
    try {
      db.fluent_api.find({x:2}).limit(2).fetchOneThenReplace({a:2});
      assert(false, 'Should fail due to limit larger than 1');
    } catch(err) {}
  }

  builds_the_correct_findAndModify_command_without_upsert();
  builds_the_correct_findAndModify_command_with_upsert();
  throws_when_the_replacement_document_is_invalid();
  throws_when_skip_was_specified();
  throws_when_limit_other_than_1_was_specified();
}

function fetchOneThenUpdate() {  
  function builds_the_correct_findAndModify_command_without_upsert() {    
    var query = db.fluent_api.find({x:2})
    query.project({x:1}).limit(1).sort({a:1}).fetchOneThenUpdate({$set: {a:2}});
    assert(JSON.stringify({
      "findAndModify": "fluent_api",
      "query": {
        "x": 2
      },
      "update": {
        "$set": {
          "a": 2
        }
      },
      "sort": {
        "a": 1
      },
      "fields": {
        "x": 1
      },
    }) == JSON.stringify(query.toQuery().command));
  }

  function builds_the_correct_findAndModify_command_with_upsert() {    
    var query = db.fluent_api.find({x:2})
    query.project({x:1}).limit(1).sort({a:1}).upsert().fetchOneThenUpdate({$set: {a:2}});
    assert(JSON.stringify({
      "findAndModify": "fluent_api",
      "query": {
        "x": 2
      },
      "update": {
        "$set": {
          "a": 2
        }
      },
      "sort": {
        "a": 1
      },
      "fields": {
        "x": 1
      },
      upsert:true
    }) == JSON.stringify(query.toQuery().command));
  }

  function throws_when_the_update_document_is_invalid() {
    try {
      db.fluent_api.find({x:2}).fetchOneThenUpdate({a:2});
      assert(false, 'Should fail due to illegal update specified');
    } catch(err) {}    
  }

  function throws_when_skip_was_specified() {    
    try {
      db.fluent_api.find({x:2}).skip(10).fetchOneThenUpdate({a:2});
      assert(false, 'Should fail due to skip');
    } catch(err) {}    
  }

  function throws_when_limit_other_than_1_was_specified() {    
    try {
      db.fluent_api.find({x:2}).limit(2).fetchOneThenUpdate({a:2});
      assert(false, 'Should fail due to limit');
    } catch(err) {}    
  }

  builds_the_correct_findAndModify_command_without_upsert();
  builds_the_correct_findAndModify_command_with_upsert();
  throws_when_the_update_document_is_invalid();
  throws_when_skip_was_specified();
  throws_when_limit_other_than_1_was_specified();
}

function limit() {  
    // print(JSON.stringify(query.toQuery().command, null, 2));
}

function maxTimeMS() {  
}

function project() {  
}

function remove() {
}

function removeOne() {  
}

function replaceOne() {  
}

function replaceOneThenFetch() {  
}

function skip() {  
}

function sort() { 
}

function update() {  
}

function updateOne() {  
}

function updateOneThenFetch() {  
}

function writeConcern() {  
}

//
// Execute All Tests
addPredicate();
addQueryModifier();
batchSize();
comment();
count();
cursorFlags();
distinct();
fetch();
fetchOne();
fetchOneThenRemove();
fetchOneThenReplace();
fetchOneThenUpdate();
limit();
maxTimeMS();
project();
remove();
removeOne();
replaceOne();
replaceOneThenFetch();
skip();
sort();
update();
updateOne();
updateOneThenFetch();
writeConcern();

//
// Cleanup
DBCollection.prototype.find = find;

//
// Strip test method
View.prototype.toQuery = undefined;