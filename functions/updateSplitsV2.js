
// updateSplitsV2.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeUnorderedBulkOp/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write

/**
 * @note IEX update happens at 9am UTC
 */
exports = async function() {
  context.functions.execute("iexUtils");
  const shortSymbols = await getInUseShortSymbols();
  if (shortSymbols.length <= 0) {
    console.log(`No symbols. Skipping update.`);
    return;
  }

  const days = 3;
  const daysParam = `${days}d`;
  const collection = db.collection("splits");

  // Future
  const futureSplits = await fetchSplits(shortSymbols, daysParam, true);
  await collection.safeInsertMissing(futureSplits, 'i');

  // Past
  // We do not update to prevent date adjust on duplicated splits.
  console.log(`Fetching splits for the last ${days} days`);
  const splits = await fetchSplits(shortSymbols, daysParam, false);
  if (splits.length) {
    console.log(`Inserting missed`);

    await collection.safeInsertMissing(splits, 'i');

    console.log(`SUCCESS`);

  } else {
    console.log(`Splits are empty for symbols: '${shortSymbols.map(x => x.t)}'`);
  }

  await setUpdateDate("splits");
};
