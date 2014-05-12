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
  if(!obj.modifiers) obj.modifiers = {};
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
      _obj.modifiers[name] = value;
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
      _obj.modifiers['$comment'] = comment;
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
    return function(_sort) {
      _obj.sort = _sort;

      return {
          addPredicate: addPredicate(_self, obj)
        , addQueryModifier: addQueryModifier(_self, obj)
        , batchSize: batchSize(_self, obj)
        , comment: comment(_self, obj)
        , distinct: distinct(_self, collection, obj)
        , skip: skip(_self, obj)
        , limit: limit(_self, obj)
        , project: project(_self, obj)
        , sort: sort(_self, obj)
        , fetchOneThenRemove: fetchOneThenRemove(_self, collection, obj)
        , fetchOneThenReplace: fetchOneThenReplace(_self, collection, obj)
        , fetchOneThenUpdate: fetchOneThenUpdate(_self, collection, obj)
        , replaceOneThenFetch: replaceOneThenFetch(_self, collection, obj)
        , updateOneThenFetch: updateOneThenFetch(_self, collection, obj)
        , upsert: upsert(_self, obj)
        , maxTimeMS: maxTimeMS(_self, obj)
      }
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

  var maxTimeMS = function(_self, _obj) {
    return function(_maxTimeMS) {
      _obj.modifiers['$maxTimeMS'] = _maxTimeMS;
      return _self;
    }
  }

  var skip = function(_self, _obj) {
    return function(_skip) {
      _obj.options['skip'] = _skip;
      return {
          sort: sort(_self, _obj)
        , limit: limit(_self, _obj)
        , skip: skip(_self, _obj)
        , maxTimeMS: maxTimeMS(_self, _obj)
      }
    }
  }

  var limit = function(_self, _obj) {
    return function(_limit) {
      _obj.options['limit'] = _limit;
      var object = {
          sort: sort(_self, _obj)
        , limit: limit(_self, _obj)        
        , maxTimeMS: maxTimeMS(_self, _obj)
      }

      if(_limit == 1) object['fetchOneThenRemove'] = fetchOneThenRemove(_self, collection, _obj);
      if(_limit == 1) object['fetchOneThenReplace'] = fetchOneThenReplace(_self, collection, _obj);
      if(_limit == 1 || _limit == 0) object['remove'] = remove(_self, collection, _obj);
      if(_limit == 1) object['removeOne'] = removeOne(_self, collection, _obj);
      if(_limit == 1) object['update'] = update(_self, collection, _obj);
      if(_limit == 1) object['updateOneThenFetch'] = updateOneThenFetch(_self, collection, _obj);
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
      for(var name in _obj.modifiers) _obj.command[name] = _obj.modifiers[name];
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
      for(var name in _obj.modifiers) _obj.command[name] = _obj.modifiers[name];
      return "";      
    }
  }

  var replaceOneThenFetch = function(_self, _collection, _obj) {
    return function(doc) {
      validateReplacementDoc(doc);
      // Build command
      _obj.command = {
          findAndModify: _collection._shortName
        , query: _obj.query
        , update: doc
        , new: true
      }

      if(_obj.sort) _obj.command['sort'] = _obj.sort;
      if(_obj.fields) _obj.command['fields'] = _obj.fields;
      if(typeof _obj.upsert == 'boolean') _obj.command['upsert'] = _obj.upsert;
      for(var name in _obj.modifiers) _obj.command[name] = _obj.modifiers[name];
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
      for(var name in _obj.modifiers) _obj.command[name] = _obj.modifiers[name];
      return "";      
    }    
  }

  var updateOneThenFetch = function(_self, _collection, _obj) {
    return function(doc) {      
      validateUpdateDoc(doc);
      // Build command
      _obj.command = {
          findAndModify: _collection._shortName
        , query: _obj.query
        , update: doc
        , new: true
      }

      if(_obj.sort) _obj.command['sort'] = _obj.sort;
      if(_obj.fields) _obj.command['fields'] = _obj.fields;
      if(typeof _obj.upsert == 'boolean') _obj.command['upsert'] = _obj.upsert;
      for(var name in _obj.modifiers) _obj.command[name] = _obj.modifiers[name];
      return "";      
    }
  }

  var remove = function(_self, _collection, _obj) {
    return function() {
      // Build command
      _obj.command = {
          delete: _collection._shortName
        , deletes: [{
          q: _obj.query
        }]
      }

      if(_obj.options.limit) _obj.command.deletes[0]['limit'] = _obj.options.limit;
      for(var name in _obj.modifiers) _obj.command[name] = _obj.modifiers[name];
      return "";      
    }    
  }

  var removeOne = function(_self, _collection, _obj) {
    return function() {
      // Build command
      _obj.command = {
          delete: _collection._shortName
        , deletes: [{
            q: _obj.query
          , limit: 1
        }]
      }

      for(var name in _obj.modifiers) _obj.command[name] = _obj.modifiers[name];
      return "";      
    }    
  }

  var replaceOne = function(_self, _collection, _obj) {
    return function(doc) {
      validateReplacementDoc(doc);      

      // Build command
      _obj.command = {
          update: _collection._shortName
        , updates: [{
            q: _obj.query
          , u: doc
          , multi: false
          , upsert: false
        }]
      }

      if(typeof _obj.upsert == 'boolean') _obj.command.updates[0]['upsert'] = _obj.upsert;
      for(var name in _obj.modifiers) _obj.command[name] = _obj.modifiers[name];
    }
  }

  var update = function(_self, _collection, _obj) {
    return function(doc) {
      validateUpdateDoc(doc);

      // Build command
      _obj.command = {
          update: _collection._shortName
        , updates: [{
            q: _obj.query
          , u: doc
          , multi: true
          , upsert: false
        }]
      }

      if(typeof _obj.upsert == 'boolean') _obj.command.updates[0]['upsert'] = _obj.upsert;
      if(_obj.options.limit == 1) _obj.command.updates[0].multi = false;
      for(var name in _obj.modifiers) _obj.command[name] = _obj.modifiers[name];      
    }
  }

  var updateOne = function(_self, _collection, _obj) {
    return function(doc) {
      validateUpdateDoc(doc);

      // Build command
      _obj.command = {
          update: _collection._shortName
        , updates: [{
            q: _obj.query
          , u: doc
          , multi: false
          , upsert: false
        }]
      }

      if(typeof _obj.upsert == 'boolean') _obj.command.updates[0]['upsert'] = _obj.upsert;
      for(var name in _obj.modifiers) _obj.command[name] = _obj.modifiers[name];            
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
  this.maxTimeMS = maxTimeMS(this, obj);
  this.remove = remove(this, collection, obj);
  this.removeOne = removeOne(this, collection, obj);
  this.replaceOne = replaceOne(this, collection, obj);
  this.replaceOneThenFetch = replaceOneThenFetch(this, collection, obj);
  this.updateOneThenFetch = updateOneThenFetch(this, collection,  obj);
  this.update = update(this, collection, obj);
  this.updateOne = updateOne(this, collection, obj);

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
    assert(JSON.stringify({$hint: 'myIndex'}) == JSON.stringify(query.modifiers));
  }

  function adds_a_second_modifier() {
    var query = db.fluent_api.find({x:2})
      .addQueryModifier("$hint", "myIndex")
      .addQueryModifier("$comment", "awesome").toQuery();
    assert(JSON.stringify({$hint: 'myIndex', $comment: 'awesome'}) == JSON.stringify(query.modifiers));
  }

  function overwrites_an_existing_modifier() {
    var query = db.fluent_api.find({x:2})
      .addQueryModifier("$hint", "myIndex")
      .addQueryModifier("$hint", "myOtherIndex").toQuery();
    assert(JSON.stringify({$hint: 'myOtherIndex'}) == JSON.stringify(query.modifiers));
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
    assert(JSON.stringify({$comment: 'awesome'}) == JSON.stringify(query.modifiers));
  }

  function uses_the_last_comment_query_modifier() {
    var query = db.fluent_api.find({x:2})
      .comment('awesome').comment('even more so').toQuery();
    assert(JSON.stringify({$comment: 'even more so'}) == JSON.stringify(query.modifiers));
  }

  function overwrites_previous_custom_setting_through_addQueryModifier() {
    var query = db.fluent_api.find({x:2})
      .addQueryModifier("$comment", "awesome").comment('even more so').toQuery();
    assert(JSON.stringify({$comment: 'even more so'}) == JSON.stringify(query.modifiers));
  }

  function is_overwritten_by_subsequent_custom_setting_through_addQueryModifier() {
    var query = db.fluent_api.find({x:2})
      .comment('even more so').addQueryModifier("$comment", "awesome").toQuery();
    assert(JSON.stringify({$comment: 'awesome'}) == JSON.stringify(query.modifiers));
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
  function should_track_limit() {    
    var query = db.fluent_api.find({x:2});
    query.limit(2);
    assert(query.toQuery().options.limit == 2);
  }

  function should_use_the_last_limit_specified() {    
    var query = db.fluent_api.find({x:2});
    query.limit(2).limit(10);
    assert(query.toQuery().options.limit == 10);
  }

  should_track_limit();
  should_use_the_last_limit_specified();
}

function maxTimeMS() {
  function adds_the_maxTimeMS_query_modifier() {    
    var query = db.fluent_api.find({x:2});
    query.maxTimeMS(20);
    assert(query.toQuery().modifiers['$maxTimeMS'] == 20);
  }

  function uses_the_last_maxTimeMS_query_modifier() {    
    var query = db.fluent_api.find({x:2});
    query.maxTimeMS(20).maxTimeMS(30);
    assert(query.toQuery().modifiers['$maxTimeMS'] == 30);
  }

  function overwrites_previous_custom_setting_through_addQueryModifier() {    
    var query = db.fluent_api.find({x:2});
    query.maxTimeMS(30).addQueryModifier("$maxTimeMS", 20);
    assert(query.toQuery().modifiers['$maxTimeMS'] == 20);
  }

  function is_overwritten_by_subsequent_custom_setting_through_addQueryModifier() {    
    var query = db.fluent_api.find({x:2});
    query.addQueryModifier("$maxTimeMS", 20).maxTimeMS(30);
    assert(query.toQuery().modifiers['$maxTimeMS'] == 30);
  }

  adds_the_maxTimeMS_query_modifier();
  uses_the_last_maxTimeMS_query_modifier();
  overwrites_previous_custom_setting_through_addQueryModifier();
  is_overwritten_by_subsequent_custom_setting_through_addQueryModifier();
}

function project() {  
  function should_track_the_projection() {    
    var query = db.fluent_api.find({x:2});
    query.project({y:1});
    assert(JSON.stringify({
      "y": 1
    }) == JSON.stringify(query.toQuery().fields));
  }

  function should_use_the_last_projection_specified() {    
    var query = db.fluent_api.find({x:2});
    query.project({y:1}).project({z:0});
    assert(JSON.stringify({
      "z": 0
    }) == JSON.stringify(query.toQuery().fields));
  }

  should_track_the_projection();
  should_use_the_last_projection_specified();
}

function remove() {
  function builds_the_correct_delete_command() {    
    var query = db.fluent_api.find({x:2})
    query.remove();
    assert(JSON.stringify({
      "delete": "fluent_api",
      "deletes": [{
          q: {x:2}
      }]
    }) == JSON.stringify(query.toQuery().command));
  }

  function builds_the_correct_delete_command_with_limit() {    
    var query = db.fluent_api.find({x:2})
    query.limit(1).remove();
    assert(JSON.stringify({
      "delete": "fluent_api",
      "deletes": [{
          q: {x:2}
        , limit: 1
      }]
    }) == JSON.stringify(query.toQuery().command));
  }

  function throws_when_skip_was_specified() {    
    try {
      db.fluent_api.find({x:2}).skip(10).remove();
      assert(false, 'Should fail due to skip');
    } catch(err) {}    
  }

  function throws_when_sort_was_specified() {    
    try {
      db.fluent_api.find({x:2}).sort({a:1}).remove();
      assert(false, 'Should fail due to sort');
    } catch(err) {}    
  }

  function throws_when_limit_other_than_1_was_specified() {    
    try {
      db.fluent_api.find({x:2}).limit(2).remove();
      assert(false, 'Should fail due to limit == 2');
    } catch(err) {}    
  }

  builds_the_correct_delete_command();
  builds_the_correct_delete_command_with_limit();
  throws_when_skip_was_specified();
  throws_when_sort_was_specified();
  throws_when_limit_other_than_1_was_specified();
}

function removeOne() {  
  function builds_the_correct_remove_command() {    
    var query = db.fluent_api.find({x:2})
    query.removeOne();
    assert(JSON.stringify({
      "delete": "fluent_api",
      "deletes": [{
          q: {x:2}
        , limit: 1
      }]
    }) == JSON.stringify(query.toQuery().command));
  }

  function throws_when_skip_was_specified() {    
    try {
      db.fluent_api.find({x:2}).skip(10).removeOne();
      assert(false, 'Should fail due to skip');
    } catch(err) {}    
  }

  function throws_when_sort_was_specified() {    
    try {
      db.fluent_api.find({x:2}).sort({a:1}).removeOne();
      assert(false, 'Should fail due to sort');
    } catch(err) {}    
  }

  builds_the_correct_remove_command();
  throws_when_skip_was_specified();
  throws_when_sort_was_specified();
}

function replaceOne() {  
  function builds_the_correct_update_command_without_upsert() {    
    var query = db.fluent_api.find({x:2})
    query.replaceOne({a:2})
    assert(JSON.stringify({
      "update": "fluent_api",
      "updates": [{
          q: {x:2}
        , u: {a:2}
        , multi: false
        , upsert: false
      }]
    }) == JSON.stringify(query.toQuery().command));
  }

  function builds_the_correct_update_command_with_upsert() {    
    var query = db.fluent_api.find({x:2})
    query.upsert().replaceOne({a:2})
    assert(JSON.stringify({
      "update": "fluent_api",
      "updates": [{
          q: {x:2}
        , u: {a:2}
        , multi: false
        , upsert: true
      }]
    }) == JSON.stringify(query.toQuery().command));
  }

  function throws_when_the_replacement_document_is_invalid() {    
    try {
      db.fluent_api.find({x:2}).replaceOne({$a:2});
      assert(false, 'Should fail due to illegal document');
    } catch(err) {}    
  }

  function throws_when_skip_was_specified() {    
    try {
      db.fluent_api.find({x:2}).skip(10).replaceOne({a:2});
      assert(false, 'Should fail due to skip');
    } catch(err) {}    
  }

  function throws_when_sort_was_specified() {    
    try {
      db.fluent_api.find({x:2}).sort({a:1}).replaceOne({a:2});
      assert(false, 'Should fail due to sort');
    } catch(err) {}    
  }

  builds_the_correct_update_command_without_upsert();
  builds_the_correct_update_command_with_upsert();
  throws_when_the_replacement_document_is_invalid();
  throws_when_skip_was_specified();
  throws_when_sort_was_specified();
}

function replaceOneThenFetch() {  
  function builds_the_correct_findAndModify_command_without_upsert() {
    var query = db.fluent_api.find({x:2})
    query.project({x:1}).limit(1).sort({a:1}).replaceOneThenFetch({a:2})
    assert(JSON.stringify( {
      findAndModify: "fluent_api",
      query: {x:2},
      update: {a:2},
      new: true,
      sort: {a:1},
      fields: {x:1}
    }) == JSON.stringify(query.toQuery().command));
  }

  function builds_the_correct_findAndModify_command_with_upsert() {      
    var query = db.fluent_api.find({x:2})
    query.project({x:1}).limit(1).sort({a:1}).upsert().replaceOneThenFetch({a:2})
    assert(JSON.stringify( {
      findAndModify: "fluent_api",
      query: {x:2},
      update: {a:2},
      new: true,
      sort: {a:1},
      fields: {x:1},
      upsert:true
    }) == JSON.stringify(query.toQuery().command));
  }

  function throws_when_the_replacement_document_is_invalid() {
    try {
      db.fluent_api.find({x:2}).replaceOneThenFetch({$a:2});
      assert(false, 'Should fail due to illegal document');
    } catch(err) {}    
  }

  function throws_when_skip_was_specified() {
    try {
      db.fluent_api.find({x:2}).skip(10).replaceOneThenFetch({a:2});
      assert(false, 'Should fail due to skip');
    } catch(err) {}    
  }

  function throws_when_limit_other_than_1_was_specified() {
    try {
      db.fluent_api.find({x:2}).limig(2).replaceOneThenFetch({a:2});
      assert(false, 'Should fail due to sort');
    } catch(err) {}    
  }

  builds_the_correct_findAndModify_command_without_upsert();
  builds_the_correct_findAndModify_command_with_upsert();
  throws_when_the_replacement_document_is_invalid();
  throws_when_skip_was_specified();
  throws_when_limit_other_than_1_was_specified();
}

function skip() {  
  function should_track_skip() {
    var query = db.fluent_api.find({x:2});
    query.skip(2);
    assert(query.toQuery().options.skip == 2);
  }

  function should_use_the_last_skip_specified() {
    var query = db.fluent_api.find({x:2});
    query.skip(2).skip(10);
    assert(query.toQuery().options.skip == 10);
  }

  should_track_skip();
  should_use_the_last_skip_specified();
}

function sort() {
  function should_track_sort() {
    var query = db.fluent_api.find({x:2});
    query.sort({a:1});
    assert(JSON.stringify({
      a:1
    }) == JSON.stringify(query.toQuery().sort));
  } 

  function should_use_the_latest_sort_specified() {
    var query = db.fluent_api.find({x:2});
    query.sort({a:1}).sort({b:1});
    assert(JSON.stringify({
      b:1
    }) == JSON.stringify(query.toQuery().sort));
  }

  should_track_sort();
  should_use_the_latest_sort_specified();
}

function update() {  
  function builds_the_correct_update_command_without_upsert() {
    var query = db.fluent_api.find({x:2})
    query.update({$set:{a:2}});
    assert(JSON.stringify( {
      update: "fluent_api",
      updates: [{
        q: {x:2},
        u: {$set:{a:2}},
        multi: true,
        upsert: false
      }]
    }) == JSON.stringify(query.toQuery().command));
  }

  function builds_the_correct_update_command_with_upsert() {
    var query = db.fluent_api.find({x:2})
    query.upsert().update({$set:{a:2}});
    assert(JSON.stringify( {
      update: "fluent_api",
      updates: [{
        q: {x:2},
        u: {$set:{a:2}},
        multi: true,
        upsert: true
      }]
    }) == JSON.stringify(query.toQuery().command));
  }

  function builds_the_correct_update_command_with_limit() {
    var query = db.fluent_api.find({x:2})
    query.limit(1).update({$set:{a:2}});
    assert(JSON.stringify( {
      update: "fluent_api",
      updates: [{
        q: {x:2},
        u: {$set:{a:2}},
        multi: false,
        upsert: false
      }]
    }) == JSON.stringify(query.toQuery().command));
  }

  function throws_when_the_update_document_is_invalid() {
    try {
      db.fluent_api.find({x:2}).update({a:2});
      assert(false, 'Should fail due to illegal document');
    } catch(err) {}    
  }

  function throws_when_skip_was_specified() {
    try {
      db.fluent_api.find({x:2}).skip(10).update({$set:{a:2}});
      assert(false, 'Should fail due to skip');
    } catch(err) {}    
  }

  function throws_when_sort_was_specified() {
    try {
      db.fluent_api.find({x:2}).sort({a:2}).update({$set:{a:2}});
      assert(false, 'Should fail due to limit == 2');
    } catch(err) {}    
  }

  function throws_when_limit_other_than_1_was_specified() {
    try {
      db.fluent_api.find({x:2}).limit(2).update({$set:{a:2}});
      assert(false, 'Should fail due to limit == 2');
    } catch(err) {}    
  }

  builds_the_correct_update_command_without_upsert();
  builds_the_correct_update_command_with_upsert();
  builds_the_correct_update_command_with_limit();
  throws_when_the_update_document_is_invalid();
  throws_when_skip_was_specified();
  throws_when_sort_was_specified();
  throws_when_limit_other_than_1_was_specified();
}

function updateOne() {  
  function builds_the_correct_update_command_without_upsert() {
    var query = db.fluent_api.find({x:2})
    query.updateOne({$set:{a:2}});
    assert(JSON.stringify( {
      update: "fluent_api",
      updates: [{
        q: {x:2},
        u: {$set:{a:2}},
        multi: false,
        upsert: false
      }]
    }) == JSON.stringify(query.toQuery().command));
  }

  function builds_the_correct_update_command_with_upsert() {
    var query = db.fluent_api.find({x:2})
    query.upsert().updateOne({$set:{a:2}});
    assert(JSON.stringify( {
      update: "fluent_api",
      updates: [{
        q: {x:2},
        u: {$set:{a:2}},
        multi: false,
        upsert: true
      }]
    }) == JSON.stringify(query.toQuery().command));
  }

  function throws_when_the_update_document_is_invalid() {
    try {
      db.fluent_api.find({x:2}).updateOne({a:2});
      assert(false, 'Should fail due to illegal document');
    } catch(err) {}    
  }

  function throws_when_skip_was_specified() {
    try {
      db.fluent_api.find({x:2}).skip(10).updateOne({$set:{a:2}});
      assert(false, 'Should fail due to skip');
    } catch(err) {}    
  }

  function throws_when_sort_was_specified() {
    try {
      db.fluent_api.find({x:2}).sort({a:2}).updateOne({$set:{a:2}});
      assert(false, 'Should fail due to limit == 2');
    } catch(err) {}    
  }

  function throws_when_limit_other_than_1_was_specified() {
    try {
      db.fluent_api.find({x:2}).limit(2).updateOne({$set:{a:2}});
      assert(false, 'Should fail due to limit == 2');
    } catch(err) {}    
  }

  builds_the_correct_update_command_without_upsert();
  builds_the_correct_update_command_with_upsert();
  throws_when_the_update_document_is_invalid();
  throws_when_skip_was_specified();
  throws_when_sort_was_specified();
  throws_when_limit_other_than_1_was_specified();
}

function updateOneThenFetch() {  
  function builds_the_correct_findAndModify_command_without_upsert() {
    var query = db.fluent_api.find({x:2})
    query.project({x:1}).limit(1).sort({a:1}).updateOneThenFetch({$set:{a:2}});
    assert(JSON.stringify( {
      findAndModify: "fluent_api",
      query: {x:2},
      update: {$set:{a:2}},
      new: true,
      sort: {a:1},
      fields: {x:1}
    }) == JSON.stringify(query.toQuery().command));
  }

  function builds_the_correct_findAndModify_command_with_upsert() {
    // print(JSON.stringify(query.toQuery(), null, 2))
    var query = db.fluent_api.find({x:2})
    query.project({x:1}).limit(1).sort({a:1}).upsert().updateOneThenFetch({$set:{a:2}});
    assert(JSON.stringify( {
      findAndModify: "fluent_api",
      query: {x:2},
      update: {$set:{a:2}},
      new: true,
      sort: {a:1},
      fields: {x:1},
      upsert: true
    }) == JSON.stringify(query.toQuery().command));
  }

  function throws_when_the_update_document_is_invalid() {
    try {
      db.fluent_api.find({x:2}).updateOneThenFetch({a:2});
      assert(false, 'Should fail due to illegal document');
    } catch(err) {}    
  }

  function throws_when_skip_was_specified() {
    try {
      db.fluent_api.find({x:2}).skip(10).updateOneThenFetch({$set:{a:2}});
      assert(false, 'Should fail due to skip');
    } catch(err) {}    
  }

  function throws_when_limit_other_than_1_was_specified() {
    try {
      db.fluent_api.find({x:2}).limit(2).updateOneThenFetch({$set:{a:2}});
      assert(false, 'Should fail due to limit == 2');
    } catch(err) {}    
  }

  builds_the_correct_findAndModify_command_without_upsert();
  builds_the_correct_findAndModify_command_with_upsert();
  throws_when_the_update_document_is_invalid();
  throws_when_skip_was_specified();
  throws_when_limit_other_than_1_was_specified();
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