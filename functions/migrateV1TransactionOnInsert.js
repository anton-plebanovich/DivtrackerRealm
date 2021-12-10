
// migrateV1TransactionOnInsert.js

// https://docs.mongodb.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/

exports = async function(transactions) {
  context.functions.execute("utilsV2");

  // TODO:
};
