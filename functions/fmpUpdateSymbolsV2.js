
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
  await fmpCollection.safeUpdateMany(newSymbols, undefined, 't');
  
  // We do not delete old symbols but instead mark them as disabled to be able to display user transactions.
  const newTickers = newSymbols.map(x => x.t);
  const allTickers = await fmpCollection.distinct('t');
  const tickersToDisable = allTickers.filter(symbolID =>
    !newTickers.includes(symbolID)
  );

  if (tickersToDisable.length) {
    console.log(`Disabling FMP symbols: ${tickersToDisable}`);
    await fmpCollection.updateMany(
      { t: { $in: tickersToDisable } },
      { $set: { isEnabled: false } }
    );
  }
}
