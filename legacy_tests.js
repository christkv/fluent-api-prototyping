var t = db.getSisterDB('test');
t.legacy.drop();

// Insert some data used for the cursor tests
t.legacy.insert([{a:1}, {a:2}, {a:3}, {a:4}]);

// Perform a simple count
assert.eq(4, t.legacy.find().count());