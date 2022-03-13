
// checkTransactionsV2.js

/**
 * Checks all existing transactions to be valid.
 */
 exports = async function() {
  context.functions.execute("iexUtils");
  const transactionsCollection = db.collection("transactions");
  const transactions = await transactionsCollection.find({}).toArray();
  if (!transactions.length) {
    console.log(`Nothing to verify. Transactions are empty.`);
    return;
  }

  // Check transactions
  const symbolIDs = await db.collection("symbols").distinct("_id");
  const symbolIDBySymbolID = symbolIDs.toDictionary(x => x.toString());
  const errors = [];
  for (const transaction of transactions) {
    try {
      checkTransaction(transaction, symbolIDBySymbolID);
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
  "_",
  "a",
  "d",
  "p",
  "s"
];

const optionalTransactionKeys = [
  "c"
];

function checkTransaction(transaction, symbolIDBySymbolID) {
    // Checking that all required keys are present
    for (const requiredKey of requiredTransactionKeys) {
      if (typeof transaction[requiredKey] === 'undefined') {
        logAndThrow(`Transaction required key '${requiredKey}' not found for transaction: ${transaction.stringify()}`);
      }
    }

    // Checking that no excessive keys are present
    for (const [key, value] of Object.entries(transaction)) {
      if (!requiredTransactionKeys.includes(key) && !optionalTransactionKeys.includes(key)) {
        logAndThrow(`Found excessive transaction key '${key}' for transaction: ${transaction.stringify()}`);
      }
    }

    if (!transaction._.length) {
      logAndThrow(`Transaction partition is absent: ${transaction.stringify()}`);
    }
  
    if (transaction.s.toString().length !== 24) {
      logAndThrow(`Transaction symbol ID format is invalid. It should be 24 characters ObjectId: ${transaction.stringify()}`);
    }
  
    if (symbolIDBySymbolID[transaction.s.toString()] == null) {
      logAndThrow(`Unknown transaction symbol: ${transaction.stringify()}`);
    }

    if (transaction.c != null && transaction.c < 0) {
      logAndThrow(`The commission cannot be negative: ${transaction.stringify()}`);
    }

    // TODO: Check that values are of proper type
}
