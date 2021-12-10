
// migrations.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/
// https://docs.mongodb.com/manual/reference/operator/update/unset/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.removeOne/
// https://docs.mongodb.com/manual/reference/method/Bulk.insert/

// Operations to perform when V1 is deprecated and sync will be stop:
// - Remove _p key from all entities
// - Remove/replace _p logic in all functions
// - Sync should start to use _ key for partition
// - All V1 functions, triggers and schemes should be removed
// - 'divtracker' database should be removed
exports = async function() {
  context.functions.execute("utils");

  
};
