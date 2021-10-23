
// checkTransactions.js

/**
 * Checks all existing transactions to be valid.
 */
 exports = async function() {
  context.functions.execute("utils");
  const transactionsCollection = db.collection("transactions");
  const transactions = await transactionsCollection.find({}).toArray();
  if (!transactions.length) {
    console.log(`Nothing to verify. Transactions are empty.`);
    return;
  }

  const symbolsCollection = db.collection("symbols");
  const validExchanges = await symbolsCollection.distinct("e");
  const validSymbols = await symbolsCollection.distinct("s");

  // Check transactions
  const errors = [];
  for (const transaction of transactions) {
    try {
      checkTransaction(transaction, validExchanges, validSymbols)
    } catch(error) {
      errors.push(error);
    }
  }

  if (errors.length) {
    throw(errors);

  } else {
    console.log(`All trasactions are valid!`);
  }
};

const requiredTransactionKeys = [
  "_id",
  "_p",
  "a",
  "d",
  "e",
  "p",
  "s"
];

const optionalTransactionKeys = [
  "c"
];

function checkTransaction(transaction, validExchanges, validSymbols) {
    // Checking that all required keys are present
    for (const requiredKey of requiredTransactionKeys) {
      if (typeof transaction[requiredKey] === 'undefined') {
        logAndThrow(`Transaction required key '${requiredKey}' not found for transaction: ${transaction.stringify()}`);
      }
    }

    // Checking that no excessive keys are present
    const entries = Object.entries(transaction);
    for (const [key, value] of entries) {
      if (!requiredTransactionKeys.includes(key) && !optionalTransactionKeys.includes(key)) {
        logAndThrow(`Found excessive transaction key '${key}' for transaction: ${transaction.stringify()}`);
      }
    }

    if (!transaction._p.length) {
      logAndThrow(`Transaction partition is absent: ${transaction.stringify()}`);
    }
    
    if (!transaction.e.length) {
      logAndThrow(`Transaction exchange is absent: ${transaction.stringify()}`);
    }
  
    if (!transaction.s.length) {
      logAndThrow(`Transaction symbol is absent: ${transaction.stringify()}`);
    }

    if (!validExchanges.includes(transaction.e)) {
      logAndThrow(`Unknown transaction exchange: ${transaction.stringify()}`);
    }
  
    if (!validSymbols.includes(transaction.s)) {
      logAndThrow(`Unknown transaction symbol: ${transaction.stringify()}`);
    }

    // TODO: Check that values are of proper type
}
