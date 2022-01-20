
// migrations.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.count/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/
// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/
// https://docs.mongodb.com/manual/reference/operator/update/unset/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeOrderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.removeOne/
// https://docs.mongodb.com/manual/reference/method/Bulk.insert/

exports = async function() {
  context.functions.execute("utilsV2");

  try {
    positiveCommissionsMigration();
  } catch(error) {
    console.error(error);
  }
};

async function positiveCommissionsMigration() {
  const transactionsCollection = db.collection("transactions");
  const oldTransactions = await transactionsCollection.find({ c: { $lt: 0 } });
  const newTransactions = [];
  for (const oldTransaction of oldTransactions) {
    const newTransaction = Object.assign({}, oldTransaction);
    newTransaction.c = newTransaction.c * -1;
    newTransactions.push(newTransaction);
  }

  await transactionsCollection.safeUpdateMany(newTransactions, oldTransactions);
}
