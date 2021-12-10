
// updateQuotesV2.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeUnorderedBulkOp/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write
// https://docs.mongodb.com/realm/mongodb/actions/collection.find/

/**
 * Ideal update times:
 * X - exchange open, 9:30 ET
 * Y - exchange close, 16:00 ET
 * Х + 1 - first update (10:30 ET, 15:30 GMT)
 * X + (Y - X + 1) / 2 - second update (13:15 ET, 18:15 GMT)
 * Y - third update (16:00 ET, 21:00 GMT)
 * 
 * Real update times: 10:00 ET, 13:00 ET, 16:00 ET (+17 mins)
 * 
 * @note IEX update happens at 4:30am-8pm ET Mon-Fri
 */
exports = async function() {
  context.functions.execute("utilsV2");
  const shortSymbols = await getInUseShortSymbols();

  if (shortSymbols.length <= 0) {
    console.log(`No symbols. Skipping update.`);
    return;
  }

  const quotes = await fetchQuotes(shortSymbols);
  const quotesCollection = db.collection("quotes");
  const existingQuotes = await quotesCollection.find().toArray();
  await quotesCollection.safeUpdateMany(quotes, existingQuotes);

  console.log(`SUCCESS`);
};
