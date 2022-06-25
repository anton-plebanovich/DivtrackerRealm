
// migrations.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.count/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/
// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/
// https://docs.mongodb.com/manual/reference/operator/update/unset/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeOrderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.removeOne/
// https://docs.mongodb.com/manual/reference/method/Bulk.insert/

exports = async function(migration, arg) {
  context.functions.execute("utils");

  logVerbose = true;
  logData = true;

  try {
    if (migration === 'fetch_refid_for_IEX_dividends') {
      await fetch_refid_for_past_IEX_dividends();
    } else {
      throw `Unexpected migration: ${migration}`;
    }
    
  } catch(error) {
    console.error(error);
  }
};
