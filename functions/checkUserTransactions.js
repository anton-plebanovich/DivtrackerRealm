
// checkUserTransactions.js

/**
 * Safety checks for inserting transactions. 
 * - It should have all required values.
 * - It shouldn't have excessive values.
 * - The _p key should equal userID.
 * - It should have valid and existing symbol.
 * - It should have valid and existing exchange.
 * - There should be less than 1_000 unique symbols per user.
 * - There should be less than 1_000_000 transactions per user.
 * @param {string} arg1 User ID
 * @param {[Transaction]} arg2 The same user transactions.
 * @example exports('61ae5154d9b3cb9ea55ec5c6', [{"_p": "61ae5154d9b3cb9ea55ec5c6","p":300.1,"d":new Date(1635895369641).toString(),"e":"NYS","s":"MA","a":1.1}]);
 */ 
 exports = async function(arg1, arg2) {
  context.functions.execute("utils");

  const userID = throwIfUndefinedOrNull(arg1);
  const transactions = throwIfUndefinedOrNull(arg2);
  if (!transactions.length) {
    console.log(`Nothing to verify. Transactions are empty.`);
    return;
  }

  const requiredTransactionKeys = [
    "_p",
    "a",
    "d",
    "e",
    "p",
    "s"
  ];

  const optionalTransactionKeys = [
    "_id",
    "c"
  ];

  const [validSymbols, validExchanges] = await getUniqueSymbolsAndExchanges();
  const transactionsCollection = db.collection("transactions");
  // Check transactions
  for (const transaction of transactions) {

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

    if (transaction._p !== userID) {
      logAndThrow(`Transaction partition '${transaction._p}' should match user ID '${userID}': ${transaction.stringify()}`);
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

  const transactionsCount = await transactionsCollection.count({ _p: userID }) + transactions.length;
  if (transactionsCount >= 1000000) {
    logAndThrow(`Maximum number of 1000000 unique transactions for user is reached: ${transactionsCount}`);
  }

  const existingDistinctSymbols = await transactionsCollection.distinct("s", { _p: userID });
  const insertingDistinctSymbols = transactions.map(x => x.s);
  const distinctSymbols = existingDistinctSymbols
    .concat(insertingDistinctSymbols)
    .distinct();

  if (distinctSymbols.length >= 1000) {
    logAndThrow(`Maximum number of 1000 unique companies for user is reached: ${distinctSymbols.length}`);
  }

  console.log(`Verification success. Transactions (${transactions.length}) are valid.`);
};
