
// syncV1toV2Transaction.js

// https://docs.mongodb.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/
// https://docs.mongodb.com/realm/mongodb/actions/collection.deleteOne/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/

/**
{
  "_id": {
    "_data": "8261B4C4580000000B2B022C0100296E5A1004AB87601FFD4649B0800C6173B36640D946645F6964006461B4C448B64021DA8C6200000004"
  },
  "operationType": "delete",
  "clusterTime": {
    "$timestamp": {
      "t": 1639236696,
      "i": 11
    }
  },
  "ns": {
    "db": "iex",
    "coll": "symbols"
  },
  "documentKey": {
    "_id": "61b4c448b64021da8c620000"
  }
}
*/
exports = async function(changeEvent) {
  context.functions.execute("utilsV2");

  console.log(`Fixing transaction for change event: ${changeEvent.stringify()}`)
  const transactionsCollection = db.collection("transactions")

  // Delete V2 transaction on delete
  if (changeEvent.operationType === 'delete') {
    const id = changeEvent.documentKey._id;
    await transactionsCollection.deleteOne({ _id: id });
    return;
  }
  
  // Update/Insert V2 transaction
  const v1Transaction = changeEvent.fullDocument;
  const symbolsCollection = db.collection("symbols");
  const symbol = await symbolsCollection.findOne({ t: v1Transaction.s });
  const symbolID = symbol._id;

  const v2Transaction = {};
  v2Transaction._id = v1Transaction._id;
  v2Transaction._p = "2";
  v2Transaction.a = v1Transaction.a;
  v2Transaction.c = v1Transaction.c;
  v2Transaction.d = v1Transaction.d;
  v2Transaction.p = v1Transaction.p;
  v2Transaction.s = symbolID;

  console.log(`New V2 transaction: ${v2Transaction.stringify()}`);
  
  return await transactionsCollection.safeUpdateMany([v2Transaction]);
};
