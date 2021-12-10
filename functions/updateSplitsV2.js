
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
  context.functions.execute("utilsV2");
  const shortSymbols = await getInUseShortSymbols();
  const days = 3;

  if (shortSymbols.length <= 0) {
    console.log(`No symbols. Skipping update.`);
    return;
  }

  console.log(`Fetching splits for the last ${days} days`);
  const daysParam = `${days}d`;
  const splits = await fetchSplits(shortSymbols, daysParam);
  if (splits.length) {
    console.log(`Inserting missed`);

    const collection = db.collection("splits");
    const bulk = collection.initializeUnorderedBulkOp();
    for (const split of splits) {
      console.log(`Checking '${split.s}' for '${split.e}' ex date`);
      bulk.find({ e: split.e, s: split.s })
        .upsert()
        .updateOne({ $setOnInsert: split });
    }
    bulk.execute();

    console.log(`SUCCESS`);

  } else {
    console.log(`Splits are empty for symbols: '${shortSymbols.map(x => x.t)}'`);
  }
};
