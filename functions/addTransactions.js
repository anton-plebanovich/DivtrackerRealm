
// addTransactions.js

// https://docs.mongodb.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/

/**
 * @example 
   context.user.id = '618600d6cab8724b39b6df4b';
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

  // Insert and load missing data together so we can speed up transaction display on UI
  const transactionsCollection = db.collection("transactions");

  //////////// ON HOLD UNTIL ANDROID IS UPDATED
  // const result = await Promise.safeAllAndUnwrap([
  //   transactionsCollection.insertMany(transactions).mapErrorToSystem(),
  //   context.functions.execute("loadMissingData", transactions)
  // ]);

  // console.log(`result: ${result.stringify()}`);

  // const returnResult = result[0];
  // console.log(`return result: ${returnResult.stringify()}`);
  //////////////////////////////////////////////
  await context.functions.execute("loadMissingData", transactions);
  const returnResult = await transactionsCollection.insertMany(transactions).mapErrorToSystem();
  console.log(`return result: ${returnResult.stringify()}`);
  //////////////////////////////////////////////

  return returnResult;
};
