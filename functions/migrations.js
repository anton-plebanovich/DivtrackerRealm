
// migrations.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.count/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/
// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/
// https://docs.mongodb.com/manual/reference/operator/update/unset/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeOrderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.removeOne/
// https://docs.mongodb.com/manual/reference/method/Bulk.insert/

exports = async function(migration) {
  context.functions.execute("utils");

  logVerbose = true;
  logData = true;

  try {
    if (migration === 'refetch_IEX_splits') {
      await refetch_IEX_splits();
    } else {
      throw `Unexpected migration: ${migration}`;
    }
    
  } catch(error) {
    console.error(error);
  }
};

async function refetch_IEX_splits() {
  context.functions.execute("iexUtils");
  const shortSymbols = await getInUseShortSymbols();
  if (shortSymbols.length <= 0) {
    console.log(`No symbols. Skipping update.`);
    return;
  }

  const collection = db.collection("splits");
  const splits = await fetchSplits(shortSymbols, null, false);
  if (splits.length) {
    console.log(`Inserting missed`);
    await collection.safeInsertMissing(splits, 'i');
    console.log(`SUCCESS`);
  } else {
    console.log(`Historical splits are empty for symbols: '${shortSymbols.map(x => x.t)}'`);
  }

  await setUpdateDate("splits");
}
