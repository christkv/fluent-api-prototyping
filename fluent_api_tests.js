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
  function should_track_write_concern() {
    var query = db.fluent_api.find({x:2});
    query.writeConcern({w: "majority"}).updateOne({$set:{a:2}});
    assert(JSON.stringify( {
      update: "fluent_api",
      updates: [{
        q: {x:2},
        u: {$set:{a:2}},
        multi: false,
        upsert: false,
      }],
      writeConcern: {w: 'majority'}
    }) == JSON.stringify(query.toQuery().command));
  }

  function should_initiate_with_write_concern() {
    var query = db.fluent_api.writeConcern({w: "majority"}).find({x:2});
    query.updateOne({$set:{a:2}});
    assert(JSON.stringify( {
      update: "fluent_api",
      updates: [{
        q: {x:2},
        u: {$set:{a:2}},
        multi: false,
        upsert: false,
      }],
      writeConcern: {w: 'majority'}
    }) == JSON.stringify(query.toQuery().command));    
  }

  should_track_write_concern();
  should_initiate_with_write_concern();
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