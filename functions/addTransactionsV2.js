
// addTransactionsV2.js

// https://docs.mongodb.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/

/**
 * @example 
   context.user.id = '619c69468f19308bae927d4e';
   exports([{"a":1.1,"d":"2021-12-08T21:00:00.000+00:00","p":320.1,"s":new BSON.ObjectId("61b102c0048b84e9c13e4564")}]);
 */
exports = async function(transactions) {
  context.functions.execute("utilsV2");

  throwIfEmptyArray(
    transactions, 
    `Please pass non-empty transactions array as the first argument.`, 
    UserError
  );

  const userID = context.user.id;
  console.log(`Adding transactions (${transactions.length}) for user '${userID}'`);

  // Add `_p` key if missing
  transactions.forEach(transaction => {
    if (transaction._p == null) {
      transaction._p = userID;
    }
  });

  // Check
  await context.functions
    .execute("checkUserTransactionsV2", userID, transactions)
    .mapErrorToUser();

  // Load missing data first
  await context.functions.execute("loadMissingDataV2", transactions);

  // Data is loaded we can safely insert our transactions
  const transactionsCollection = db.collection("transactions");
  const returnResult = await transactionsCollection.insertMany(transactions).mapErrorToSystem();
  console.log(`return result: ${returnResult.stringify()}`);

  return returnResult;
};
