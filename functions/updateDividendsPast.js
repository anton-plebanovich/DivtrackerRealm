
// updateDividendsPast.js

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
  logVerbose = false;
  logData = false;

  const inUseShortSymbols = await getInUseShortSymbols();
  const collection = db.collection("dividends");

  const weekAgo = new Date();
  weekAgo.setUTCDate(weekAgo.getUTCDate() - 7);
  const weekAgoDayString = weekAgo.dayString();
  const weekAgoDividendsDate = getOpenDate(weekAgoDayString);
  const longFetchSymbolIDs = await collection.distinct('s', { e: weekAgoDividendsDate });
  const longFetchShortSymbols = await getShortSymbols(longFetchSymbolIDs);
  console.log(`Long fetch symbol IDs (${longFetchSymbolIDs.length})`);
  console.logData(`Long fetch symbol IDs (${longFetchSymbolIDs.length})`, longFetchSymbolIDs);
  
  // We do not need to short-fetch long-fetch symbols
  const longFetchSymbolIDByID = longFetchSymbolIDs.toDictionary();
  const shortFetchShortSymbols = inUseShortSymbols.filter(x => longFetchSymbolIDByID[x._id] == null);
  console.log(`Short fetch symbol IDs (${shortFetchShortSymbols.length})`);
  console.logData(`Short fetch symbol IDs (${shortFetchShortSymbols.length})`, shortFetchShortSymbols);

  if (shortFetchShortSymbols.length <= 0 && longFetchSymbolIDByID.length <= 0) {
    console.log(`No symbols. Skipping update.`);
    return;
  }

  // Fetch
  const longFetchRange = '1y';
  const days = 4;
  console.log(`Fetching past dividends for the last ${days} days`);
  const shortFetchRange = `${days}d`;
  const [longFetchDividends, shortFetchDividends] = await Promise.all([
    fetchDividends(longFetchShortSymbols, false, longFetchRange, 1),
    fetchDividends(shortFetchShortSymbols, false, shortFetchRange),
  ]);

  const pastDividends = longFetchDividends.concat(shortFetchDividends);
  await collection.safeUpdateMany(pastDividends, null, 'i', true, true);

  await setUpdateDate(db, "dividends-past");
};
