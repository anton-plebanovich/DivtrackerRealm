
// pricesUpdate.js

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
  context.functions.execute("utils");
  const uniqueIDs = await getUniqueIDs();

  if (uniqueIDs.length <= 0) {
    console.log(`No uniqueIDs. Skipping update.`);
    return;
  }

  // Update previous day prices
  const previousDayPrices = await fetchPreviousDayPrices(uniqueIDs);
  const previousDayPricesCollection = db.collection("previous-day-prices");
  const previousDayPricesBulk = previousDayPricesCollection.initializeUnorderedBulkOp();

  const previousDayPricesCount = previousDayPrices.length;
  for (let i = 0; i < previousDayPricesCount; i += 1) {
    const previousDayPrice = previousDayPrices[i];
    
    console.logVerbose(`Updating previous day price for '${previousDayPrice._id}' with '${previousDayPrice.c}'`);
    previousDayPricesBulk.find({ _id: previousDayPrice._id })
      .updateOne({ $set: { c: previousDayPrice.c } });
  }
  
  previousDayPricesBulk.execute();

  // Insert historical price record if more than 29 days passed from the previous one till yesterday
  const yesterdayCloseDate = getCloseDate(Date.yesterday());
  const monthAgoCloseDate = new Date(yesterdayCloseDate);
  monthAgoCloseDate.setUTCDate(monthAgoCloseDate.getUTCDate() - 29);

  const historicalPricesCollection = db.collection("historical-prices");
  
  const upToDateUniqueIDsAggregation = [
    { $group: { _id: "$_i", d: { $max: "$d" } } },
    { $match: { d: { $gt: monthAgoCloseDate } } },
    { $project: { _id: 1 } }
  ];

  const upToDateUniqueIDs = await historicalPricesCollection
    .aggregate(upToDateUniqueIDsAggregation)
    .toArray()
    // Extract IDs from [{ _id: "MSFT:NAS" }]
    .then(x => x.map(x => x._id));

  console.log(`Up to date unique IDs (${upToDateUniqueIDs.length}) for '${monthAgoCloseDate}' date: ${upToDateUniqueIDs}.`);

  const historicalPrices = previousDayPrices
    .filter(x => !upToDateUniqueIDs.includes(x._id))
    .map(previousDayPrice => {
      const historicalPrice = {};
      historicalPrice._i = previousDayPrice._id;
      historicalPrice._p = previousDayPrice._p;
      historicalPrice.c = previousDayPrice.c;
      historicalPrice.d = yesterdayCloseDate;

      return historicalPrice;
    });

  if (historicalPrices.length) {
    const historicalPriceIDs = historicalPrices.map(x => x._i);
    console.log(`Inserting historical prices (${historicalPrices.length}) for IDs: ${historicalPriceIDs}.`);
    await historicalPricesCollection.insertMany(historicalPrices);
  } else {
    console.log(`Historical prices are up to date.`);
  }
};
