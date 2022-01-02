
// updatePricesV2.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeUnorderedBulkOp/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write

/**
 * Updates previous day prices and on 2nd day of each month also inserts historical price.
 * @note IEX update happens at 4am ET Tue-Sat
 * @note Since we lazily fetch historical prices with 21 day period on a day of the first usage
 * there is a difference between series before the first usage and after the first usage and also 
 * there is a period gap.
 */
 exports = async function() {
  context.functions.execute("utilsV2");
  const shortSymbols = await getInUseShortSymbols();

  if (shortSymbols.length <= 0) {
    console.log(`No symbols. Skipping update.`);
    return;
  }

  // Update previous day prices
  const previousDayPrices = await fetchPreviousDayPrices(shortSymbols);
  const previousDayPricesCollection = db.collection("previous-day-prices");
  const previousDayPricesBulk = previousDayPricesCollection.initializeUnorderedBulkOp();
  for (const previousDayPrice of previousDayPrices) {
    console.logVerbose(`Updating previous day price for '${previousDayPrice._id}' with '${previousDayPrice.c}'`);
    previousDayPricesBulk.find({ _id: previousDayPrice._id })
      .upsert()
      .updateOne({ $set: previousDayPrice });
  }
  previousDayPricesBulk.safeExecute();

  // Insert historical price record if more than 29 days passed from the previous one till yesterday
  const yesterdayCloseDate = getCloseDate(Date.yesterday());
  const monthAgoCloseDate = new Date(yesterdayCloseDate);
  monthAgoCloseDate.setUTCDate(monthAgoCloseDate.getUTCDate() - 29);

  const historicalPricesCollection = db.collection("historical-prices");
  
  const upToDateSymbolIDsAggregation = [
    { $group: { _id: "$s", d: { $max: "$d" } } },
    { $match: { d: { $gt: monthAgoCloseDate } } },
    { $project: { _id: 1 } }
  ];

  const upToDateSymbolIDs = await historicalPricesCollection
    .aggregate(upToDateSymbolIDsAggregation)
    .toArray()
    // Extract IDs from [{ _id: ObjectId }]
    .then(x => x.map(x => x._id.toString()));

  console.log(`Up to date unique IDs (${upToDateSymbolIDs.length}) for '${monthAgoCloseDate}' date`);
  console.logData(`Up to date unique IDs (${upToDateSymbolIDs.length}) for '${monthAgoCloseDate}' date`, upToDateSymbolIDs);

  const historicalPrices = previousDayPrices
    .filter(x => !upToDateSymbolIDs.includes(x._id.toString()))
    .map(previousDayPrice => {
      const historicalPrice = {};
      historicalPrice.c = previousDayPrice.c;
      historicalPrice.d = yesterdayCloseDate;
      historicalPrice.s = previousDayPrice._id;

      return historicalPrice;
    });

  if (historicalPrices.length) {
    const historicalPriceIDs = historicalPrices.map(x => x.s);
    console.log(`Inserting historical prices (${historicalPrices.length}) for IDs`);
    console.logData(`Inserting historical prices (${historicalPrices.length}) for IDs`, historicalPriceIDs);
    await historicalPricesCollection.insertMany(historicalPrices);
  } else {
    console.log(`Historical prices are up to date.`);
  }
};
