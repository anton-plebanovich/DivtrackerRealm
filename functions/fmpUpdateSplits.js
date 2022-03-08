
// fmpUpdateSplits.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeUnorderedBulkOp/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write

exports = async function() {
  context.functions.execute("fmpUtils");
  
  const shortSymbols = await getShortSymbols();
  if (shortSymbols.length <= 0) {
    console.log(`No symbols. Skipping update.`);
    return;
  }

  const splits = await fetchSplits(shortSymbols);
  const collection = fmp.collection("splits");
  await collection.safeUpdateMany(splits);

  await setUpdateDate("fmp-splits");
};
