
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

  // Currently, we support 'MCX' only.
  const newSymbols = await fmpFetch("/v3/stock/list")
    .then(symbols => 
      symbols.filter(symbol => symbol.exchangeShortName === "MCX")
    );

  const fmpCollection = fmp.collection("symbols");
  const count = await fmpCollection.count({});
  if (count === 0) {
    console.log(`No FMP symbols. Just inserting all records.`);
    await fmpCollection.insertMany(newMCXSymbols);
    return;
  }

  const newSymbolIDs = newSymbols.map(x => x.symbol);

  // We drop `_id` field so we can compare old and new objects.
  // All updates are done through other fields anyway.
  const oldSymbols = await fmpCollection.find({}, { "_id": 0 }).toArray();
  const oldSymbolByTicker = oldSymbols.toDictionary('symbol');

  const bulk = fmpCollection.initializeUnorderedBulkOp();
  for (const newSymbol of newSymbols) {
    if (update('symbol', bulk, oldSymbolByTicker, oldSymbols, newSymbol)) {
      continue;
    }

    // If we were unable to find what to update we just insert new symbol
    console.log(`Inserting FMP ${newSymbol.symbol}`);
    bulk.insert(newSymbol);
  }

  // We need to execute update first because symbols might be renamed
  await bulk.safeExecute();

  // We do not delete old symbols but instead mark them as disabled to be able to display user transactions.
  const allSymbolIDs = await fmpCollection.distinct('symbol');
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

// Checks if update is required for `newSymbol` using provided `fields`.
// Adds bulk operation if needed and returns `true` if update check passed.
// Returns `false` is update check wasn't possible.
function update(field, bulk, oldSymbolByTicker, oldSymbols, newSymbol) {

  // First, we try to use dictionary because in most cases symbols will just match
  // and so we can increase performance.
  const newSymbolFieldValue = newSymbol[field];
  let oldSymbol = oldSymbolByTicker[newSymbol.symbol];
  if (field !== 'symbol' && (!oldSymbol || oldSymbol[field] !== newSymbolFieldValue)) {
    // Perform search since our assumption failed. This one is slower.
    oldSymbol = oldSymbols.find(oldSymbol => {
      return oldSymbol[field] === newSymbolFieldValue;
    });
  }

  if (oldSymbol == null) {
    return false;

  } else if (isFMPSandbox && newSymbol.isEnabled === oldSymbol.isEnabled) {
    // It takes too much time to update every symbol on sandbox so we just skip update if enabled field didn't change
    return true

  } else {
    bulk.findAndUpdateIfNeeded(newSymbol, oldSymbol, field);
    return true;
  }
}
