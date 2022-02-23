
// fmpUpdateSymbolsV2.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeOrderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.update/
// https://docs.mongodb.com/manual/reference/operator/query-logical/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write

/**
{
  "symbol" : "GAZP.ME",
  "name" : "Public Joint Stock Company Gazprom",
  "price" : 310.81,
  "exchange" : "MCX",
  "exchangeShortName" : "MCX",
  "type" : "stock"
}
 */

/**
 * @note FMP update happens at 8am, 9am, 12pm, 1pm UTC
 */
exports = async function() {
  context.functions.execute("fmpUtils");

  await updateFMPSymbols();
  await setUpdateDate("fmp-symbols");
  
  console.log(`SUCCESS`);
};

async function updateFMPSymbols() {
  console.log(`Updating FMP symbols`);

  const newSymbols = await fetchSymbols();
  const fmpCollection = fmp.collection("symbols");
  await fmpCollection.safeUpsertMany(newSymbols, 't');
  
  // We do not delete old symbols but instead mark them as disabled to be able to display user transactions.
  const newSymbolIDs = newSymbols.map(x => x.symbol);
  const allSymbolIDs = await fmpCollection.distinct('t');
  const symbolsIDsToDisable = allSymbolIDs.filter(symbolID =>
    !newSymbolIDs.includes(symbolID)
  );

  if (symbolsIDsToDisable.length) {
    console.log(`Disabling FMP symbols: ${symbolsIDsToDisable}`);
    await fmpCollection.updateMany(
      { symbol: { $in: symbolsIDsToDisable } },
      { $set: { isEnabled: false } }
    );
  }
}
