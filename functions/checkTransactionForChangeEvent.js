
// checkTransactionForChangeEvent.js

exports = async function(changeEvent) {
  context.functions.execute("utils");
  const transaction = changeEvent.fullDocument;
  const userID = transaction._p;

  try {
    return await context.functions.execute("checkUserTransactions", userID, [transaction]);
  } catch(error) {
    await deleteTransaction(transaction);
    throw error;
  }
};

async function deleteTransaction(transaction) {
  console.log(`Erasing: ${transaction.stringify()}`);
  const transactionsCollection = db.collection("transactions");
  return transactionsCollection.deleteOne({ _id: transaction._id });
}

// exports({ fullDocument: { _id: BSON.ObjectId("607afe635ed931675acfff1c"), s: "AAPL", e: "NAS" } });
