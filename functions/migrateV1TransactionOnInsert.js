
// migrateV1TransactionOnInsert.js

// https://docs.mongodb.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/

exports = async function(transaction) {
  context.functions.execute("utilsV2");

  const symbolsCollection = db.collection("symbols");
  const symbol = await symbolsCollection.findOne({ t: transaction.s });
  const symbolID = symbol._id;

  const transactionV2 = {};
  transactionV2._id = transaction._id;
  transactionV2.a = transaction.a;
  transactionV2.c = transaction.c;
  transactionV2.d = transaction.d;
  transactionV2.p = transaction.p;
  transactionV2.s = symbolID;
  
  const transactionsCollection = db.collection("transactions");
  return await transactionsCollection.insertOne(transactionV2);
};
