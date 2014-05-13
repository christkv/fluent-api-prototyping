// DBCollection.prototype.pipe = function() {
//   print("pipe")
// }

var db = db.getSisterDB('t');

// Save the state of the find
var find = DBCollection.prototype.find;

//
// View Object
var View = function(collection, obj, qfields , qlimit , qskip, qbatchSize, qoptions) {
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
      if(_obj.writeConcern) _obj.command['writeConcern'] = _obj.writeConcern;
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
      if(_obj.writeConcern) _obj.command['writeConcern'] = _obj.writeConcern;
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
      if(_obj.writeConcern) _obj.command['writeConcern'] = _obj.writeConcern;
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
      if(_obj.writeConcern) _obj.command['writeConcern'] = _obj.writeConcern;
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
      if(_obj.writeConcern) _obj.command['writeConcern'] = _obj.writeConcern;
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
      if(_obj.writeConcern) _obj.command['writeConcern'] = _obj.writeConcern;
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

      if(_obj.writeConcern) _obj.command['writeConcern'] = _obj.writeConcern;
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
      if(_obj.writeConcern) _obj.command['writeConcern'] = _obj.writeConcern;
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
      if(_obj.writeConcern) _obj.command['writeConcern'] = _obj.writeConcern;
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
      if(_obj.writeConcern) _obj.command['writeConcern'] = _obj.writeConcern;
      for(var name in _obj.modifiers) _obj.command[name] = _obj.modifiers[name];            
    }
  }

  var writeConcern = function(_self, _obj) {
    return function(_writeConcern) {
      _obj.writeConcern = _writeConcern;
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
        , upsert: upsert(_self, obj)
        , maxTimeMS: maxTimeMS(_self, obj)
        // Terminating functions
        , fetchOneThenRemove: fetchOneThenRemove(_self, collection, obj)
        , fetchOneThenReplace: fetchOneThenReplace(_self, collection, obj)
        , fetchOneThenUpdate: fetchOneThenUpdate(_self, collection, obj)
        , remove: remove(_self, collection, obj)
        , removeOne: removeOne(_self, collection, obj)
        , replaceOneThenFetch: replaceOneThenFetch(_self, collection, obj)
        , updateOneThenFetch: updateOneThenFetch(_self, collection, obj)
        , updateOne: updateOne(_self, collection, obj)
        , update: update(_self, collection, obj)
      }
    }
  }

  //
  // Initial state
  this.addPredicate = addPredicate(this, obj);
  this.addQueryModifier = addQueryModifier(this, obj);
  this.batchSize = batchSize(this, obj);
  this.comment = comment(this, obj);
  this.skip = skip(this, obj);
  this.limit = limit(this, obj);
  this.project = project(this, obj);
  this.sort = sort(this, obj);
  this.upsert = upsert(this, obj);
  this.maxTimeMS = maxTimeMS(this, obj);
  this.writeConcern = writeConcern(this, obj);

  // Terminators
  this.distinct = distinct(this, collection, obj);
  this.fetchOneThenRemove = fetchOneThenRemove(this, collection, obj);
  this.fetchOneThenReplace = fetchOneThenReplace(this, collection, obj);
  this.fetchOneThenUpdate = fetchOneThenUpdate(this, collection, obj);
  this.remove = remove(this, collection, obj);
  this.removeOne = removeOne(this, collection, obj);
  this.replaceOne = replaceOne(this, collection, obj);
  this.replaceOneThenFetch = replaceOneThenFetch(this, collection, obj);
  this.updateOneThenFetch = updateOneThenFetch(this, collection,  obj);
  this.update = update(this, collection, obj);
  this.updateOne = updateOne(this, collection, obj);

  print("================================ COMMAND")
  print(JSON.stringify(obj.query, null, 2))
  print(_limit)
  print(_batchSize)

  // Build a cursor for backward compatibility
  var cursor = new DBQuery(
      collection._mongo , collection._db , collection
    , collection._fullName , collection._massageObject(obj.query) 
    , qfields , qlimit , qskip , qbatchSize , qoptions || collection.getQueryOptions());

  // Backward compatibility
  this.hasNext = function() { return cursor.hasNext(); }
  this.next = function() { return cursor.next(); }
  this.count = function() { return cursor.count(); }

  // Just debugging method removed at bootstrap
  this.toQuery = function() {    
    return obj;
  }
}

// Set up our own fluent find
DBCollection.prototype.find = function(query , fields , limit , skip, batchSize, options) {
  var _query = {query: query};
  return new View(this, _query, fields , limit , skip, batchSize, options);
}

DBCollection.prototype.writeConcern = function(writeConcern) {
  var self = this;

  return {
      find: function(selector) {
        var query = {query: selector, writeConcern: writeConcern};
        return new View(self, query);
      }
    , insert: DBCollection.prototype.insert
  }
}

//
// Fluent API tests
try {
  load("fluent_api_tests.js");
} catch(err) {}

//
// Legacy test
try {
  load("legacy_tests.js");  
} catch(err) {}

//
// Cleanup
DBCollection.prototype.find = find;

//
// Strip test method
View.prototype.toQuery = undefined;