
// fmpUpdateQuotes.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeUnorderedBulkOp/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write
// https://docs.mongodb.com/realm/mongodb/actions/collection.find/

exports = async function(database) {
  context.functions.execute("fmpUtils", database);
  database = getFMPDatabaseName(database);

  const shortSymbols = await getShortSymbols();
  if (shortSymbols.length <= 0) {
    console.log(`No symbols. Skipping update.`);
    return;
  }

  const collection = fmp.collection("quotes");
  await fetchQuotes(shortSymbols, async (quotes, symbolIDs) => {
    await collection.safeUpsertMany(quotes, '_id');
    await updateStatus("quotes", symbolIDs);
    checkExecutionTimeoutAndThrow();
  });

  await setUpdateDate(`${database}-quotes`);

  console.log(`SUCCESS`);
};
