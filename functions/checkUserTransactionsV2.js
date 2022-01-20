
// checkUserTransactionsV2.js

/**
 * Safety checks for inserting transactions. 
 * - It should have all required values.
 * - It shouldn't have excessive values.
 * - The _ key should equal userID.
 * - It should have valid and existing symbol.
 * - It should have valid and existing exchange.
 * - There should be less than 1_000 unique symbols per user.
 * - There should be less than 1_000_000 transactions per user.
 * @param {string} userID User ID
 * @param {[Transaction]} arg2 The same user transactions.
 * @example exports('619c69468f19308bae927d4e', [{ "_": "619c69468f19308bae927d4e","p":300.1,"d":"2021-12-08T21:00:00.000+00:00","s":new BSON.ObjectId("61b102c0048b84e9c13e4564"),"a":1.1}]);
 */ 
 exports = async function(userID, transactions) {
  context.functions.execute("utilsV2");

  throwIfEmptyArray(
    transactions, 
    `Please pass non-empty transactions array as the second argument.`
  );

  const requiredTransactionKeys = [
    "_",
    "a",
    "d",
    "p",
    "s"
  ];

  const optionalTransactionKeys = [
    "_id",
    "c"
  ];

  const supportedSymbolIDs = await getSupportedSymbolIDs();
  const sybmolIDBySymbolID = Object.assign({}, ...supportedSymbolIDs.map(x => ({ [x]: x })))
  const transactionsCollection = db.collection("transactions");
  // Check transactions
  for (const transaction of transactions) {
    
    // Checking that all required keys are present
    for (const requiredKey of requiredTransactionKeys) {
      if (typeof transaction[requiredKey] === 'undefined') {
        return logAndReject(`Transaction required key '${requiredKey}' not found for transaction`, transaction.stringify());
      }
    }

    // Checking that no excessive keys are present
    for (const [key, value] of Object.entries(transaction)) {
      if (!requiredTransactionKeys.includes(key) && !optionalTransactionKeys.includes(key)) {
        return logAndReject(`Found excessive transaction key '${key}' for transaction`, transaction.stringify());
      }
    }

    if (!transaction._.length) {
      return logAndReject(`Transaction partition is absent`, transaction.stringify());
    }

    if (transaction._ !== userID) {
      return logAndReject(`Transaction partition '${transaction._}' should match user ID '${userID}'`, transaction.stringify());
    }
  
    if (transaction.s.toString().length !== 24) {
      return logAndReject(`Transaction symbol ID format is invalid. It should be 24 characters ObjectId`, transaction.stringify());
    }
  
    if (sybmolIDBySymbolID[transaction.s] == null) {
      return logAndReject(`Unknown transaction symbol`, transaction.stringify());
    }

    if (transaction.c != null && transaction.c < 0) {
      return logAndReject(`The commission should be a positive value`, transaction.stringify());
    }

    // TODO: Check that values are of proper type
  }

  const transactionsCount = await transactionsCollection.count({ p: userID }) + transactions.length;
  if (transactionsCount >= 1000000) {
    return logAndReject(`Maximum number of 1000000 unique transactions for user is reached`, transactionsCount);
  }

  const existingDistinctSymbols = await transactionsCollection.distinct("s", { p: userID });
  const insertingDistinctSymbols = transactions.map(x => x.s);
  const distinctSymbols = existingDistinctSymbols
    .concat(insertingDistinctSymbols)
    .distinct();

  if (distinctSymbols.length >= 1000) {
    return logAndReject(`Maximum number of 1000 unique companies for user is reached`, distinctSymbols.length);
  }

  console.log(`Verification success. Transactions (${transactions.length}) are valid.`);
};
