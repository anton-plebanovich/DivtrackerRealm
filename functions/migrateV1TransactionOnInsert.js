
// migrateV1TransactionOnInsert.js

// https://docs.mongodb.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/

exports = async function(changeEvent) {
  context.functions.execute("utilsV2");

  const transaction = changeEvent.fullDocument;
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
  let existingTransactionV2 = await transactionsCollection.findOne({ _id: transactionsCollection._id });
  let existingTransactionV2s = null;
  if (existingTransactionV2 != null) {
    existingTransactionV2s = [existingTransactionV2];
  }

  return await transactionsCollection.safeUpdateMany([transactionV2], existingTransactionV2s);
};
