
// migrations.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.count/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/
// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/
// https://docs.mongodb.com/manual/reference/operator/update/unset/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeOrderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.removeOne/
// https://docs.mongodb.com/manual/reference/method/Bulk.insert/

exports = async function() {
  return await fetch_refid_for_IEX_splits()
};

////////////////////////////////////////////////////// 2022-06-XX IEX refid for splits

async function fetch_refid_for_IEX_splits() {
  context.functions.execute("iexUtils");

  const shortSymbols = await getInUseShortSymbols();
  const range = '10y';
  const splits = await fetchSplits(shortSymbols, range);
  const collection = db.collection("splits");
  return await collection.safeUpdateMany(splits, null, ['e', 's'], true);
}
