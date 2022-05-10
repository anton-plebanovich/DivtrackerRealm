
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

  // Release new server with disabled FMP symbols update
  // dtcheck backup --environment production --database fmp
  // dtcheck restore --environment production --database fmp --to-database fmp-tmp
  // dtcheck call-realm-function --environment production --function fmpUpdateSymbols --argument fmp-tmp
  // dtcheck call-realm-function --environment production --function fmpLoadMissingData --argument fmp-tmp --retry-on-error 'execution time limit exceeded'
  // dtcheck backup --environment production --database fmp-tmp # just in case something goes wrong
  await adjustSymbolIDs();
  // dtcheck backup --environment production --database fmp-tmp
  // dtcheck restore --environment production --database fmp-tmp --to-database fmp
  // dtcheck call-realm-function --environment production --function checkTransactionsV2
  // Enable FMP symbols update
};

async function adjustSymbolIDs() {
  const fmpTmp = atlas.db("fmp-tmp");

  // 5 hours ago, just in case fetch will take so much
  const oldDate = new Date();
  oldDate.setUTCHours(oldDate.getUTCHours() - 5);

  const objectID = BSON.ObjectId.fromDate(oldDate);

  const symbolsCollection = fmpTmp.collection('symbols');
  const newSymbols = await symbolsCollection.find({ _id: { $gte: objectID } }).toArray();
  const newObjectIDs = newSymbols.map(x => x._id);

  // 10 mins in the future so we have time to release an update
  const newDate = new Date();
  newDate.setUTCMinutes(newDate.getUTCMinutes() + 10);

  // Delete old
  const bulk = symbolsCollection.initializeUnorderedBulkOp();
  for (const newObjectID of newObjectIDs) {
    bulk.find({ _id: newObjectID }).remove();
  }
  
  // Modify and insert new
  const hexSeconds = Math.floor(newDate/1000).toString(16);
  for (const newSymbol of newSymbols) {
    const hex = newSymbol._id.hex();
    const newID = BSON.ObjectId.fromDate(newDate, hex);
    newSymbol._id = newID;
    bulk.insert(newSymbol);
  }
  
  await bulk.execute();
}