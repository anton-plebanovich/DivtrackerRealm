
// addTransactions.js

// https://docs.mongodb.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/

/**
 * @example 
   context.user.id = '615955b47e2079114597d16c';
   exports([{"a":1.1,"d":new Date(1636089825657),"e":"NYS","p":320.1,"s":"LMT"}]);
 */
exports = async function(transactions) {
  context.functions.execute("utils");

  if (typeof transactions === 'undefined') {
    throw new UserError(`Transaction parameter is undefined`);
  } else if (transactions === null) {
    throw new UserError(`Transaction parameter is null`);
  }

  const transactionsType = Object.prototype.toString.call(transactions);
  if (transactionsType !== '[object Array]') {
    throw new UserError(`First argument should be an array of transactions. Instead, received '${transactionsType}': ${transactions.stringify()}`);
  }

  if (!transactions.length) {
    throw new UserError(`Transactions array is empty`);
  }

  const userID = context.user.id;
  console.log(`Adding transactions (${transactions.length}) for user '${userID}'`);

  // Add `_p` key if missing
  transactions.forEach(transaction => {
    if (typeof transaction._p === 'undefined') {
      transaction._p = userID;
    }
  });

  // Check
  await context.functions
    .execute("checkUserTransactions", userID, transactions)
    .mapErrorToUser();

  // Load missing data first
  await context.functions.execute("loadMissingData", transactions);

  // Data is loaded we can safely insert our transactions
  const transactionsCollection = db.collection("transactions");
  const returnResult = await transactionsCollection.insertMany(transactions).mapErrorToSystem();
  console.log(`return result: ${returnResult.stringify()}`);

  return returnResult;
};
