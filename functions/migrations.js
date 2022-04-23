
// migrations.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.count/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/
// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/
// https://docs.mongodb.com/manual/reference/operator/update/unset/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeOrderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.removeOne/
// https://docs.mongodb.com/manual/reference/method/Bulk.insert/

exports = async function() {
  context.functions.execute("utils");

  // Release new server
  // dtcheck backup --environment production --database fmp
  // dtcheck restore --environment production --database fmp --to-database fmp-tmp
  // dtcheck call-realm-function --environment production --function fmpUpdateSymbols --argument fmp-tmp
  // dtcheck call-realm-function --environment production --function fmpLoadMissingData --argument fmp-tmp --retry-on-error 'execution time limit exceeded'
  // dtcheck backup --environment production --database fmp-tmp
  // dtcheck restore --environment production --database fmp-tmp --to-database fmp
  // dtcheck call-realm-function --environment production --function checkTransactionsV2
};
