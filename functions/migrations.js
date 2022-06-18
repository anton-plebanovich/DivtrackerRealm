
// migrations.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.count/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/
// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/
// https://docs.mongodb.com/manual/reference/operator/update/unset/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeOrderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.removeOne/
// https://docs.mongodb.com/manual/reference/method/Bulk.insert/

// TODO: Move to old
// TODO: All splits update because we might have missing

exports = async function(migration) {
  context.functions.execute("utils");

  logVerbose = true;
  logData = true;

  try {
    if (migration === 'TODO') {
      await TODO();
    } else {
      throw `Unexpected migration: ${migration}`;
    }
    
  } catch(error) {
    console.error(error);
  }
};
