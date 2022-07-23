
// updateDividendsFuture.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeUnorderedBulkOp/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write
// https://docs.mongodb.com/realm/mongodb/actions/collection.distinct/

/**
 * @note IEX update happens at 9am UTC
 */
exports = async function() {
  context.functions.execute("iexUtils");
  const shortSymbols = await getInUseShortSymbols();
  const collection = db.collection("dividends");

  if (shortSymbols.length <= 0) {
    console.log(`No symbols. Skipping update.`);
    return;
  }

  // Future dividends update.

  // We remove symbols that already have future dividends and update only those that don't.
  // On Monday we update all symbols because future dividends data may change over time.
  const find = {};
  const now = new Date();
  if (now.getDay !== 1) {
    find.e = { $gte: now };
  }

  const upToDateSymbolIDs = await collection.distinct("s", find);
  const upToDateSymbolIDByID = upToDateSymbolIDs.toDictionary();
  const shortSymbolsToUpdate = shortSymbols.filter(x => upToDateSymbolIDByID[x._id] == null);

  console.log(`Fetching future dividends (${shortSymbolsToUpdate.length}) for '${shortSymbolsToUpdate.stringify()}' IDs`);
  const futureDividends = await fetchDividends(shortSymbolsToUpdate, true);
  
  await collection.safeUpdateMany(futureDividends, null, 'i', true, true);

  await setUpdateDate(db, "dividends-future");
};
