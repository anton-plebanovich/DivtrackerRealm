
// splitsUpdate.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeUnorderedBulkOp/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write

exports = async function() {
  context.functions.execute("utils");
  await computeDistinctSymbols();
  const days = 3;

  if (distinctSymbols.length <= 0) {
    console.log(`No symbols. Skipping update.`);
    return;
  }

  console.log(`Fetching splits for the last ${days} days`);
  const daysParam = `${days}d`;
  const splits = await fetchSplits(distinctSymbols, daysParam);
  if (splits.length) {
    console.log(`Inserting missed`);

    const collection = db.collection("splits");
    const bulk = collection.initializeUnorderedBulkOp();

    const splitsCount = splits.length;
    for (let i = 0; i < splitsCount; i += 1) {
      const split = splits[i];
      
      console.log(`Checking '${split._i}' for '${split.e}' ex date`);
      bulk.find({ _i: split._i, d: split.e })
        .upsert()
        .updateOne({ $setOnInsert: split });
    }

    bulk.execute();

    console.log(`SUCCESS`);

  } else {
    console.log(`Splits are empty for symbols '${distinctSymbols}'`);
  }
};
