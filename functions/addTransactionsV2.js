
// addTransactionsV2.js

// https://docs.mongodb.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/

/**
 * @example 
   context.user.id = '615955b47e2079114597d16c';
   exports([{"a":1.1,"d":"2021-12-08T21:00:00.000+00:00","p":320.1,"s":new BSON.ObjectId("61b102c0048b84e9c13e4564")}]);
 */
exports = async function(transactions, replace) {
  context.functions.execute("iexUtils");

  throwIfEmptyArray(
    transactions, 
    `Please pass non-empty transactions array as the first argument.`, 
    UserError
  );

  if (replace == null) {
    replace = false;
  }

  const userID = context.user.id;

  // Delete old if needed
  const transactionsCollection = db.collection("transactions");
  if (replace) {
    if (userID != null, userID.length === 24) {
      console.log(`Removing old user transactions`);
      await transactionsCollection.deleteMany({ _: userID });
    } else {
      throw new UserError(`Unable to replace transactions for userID: ${userID}`);
    }
  } else {
    console.log(`Replace is not needed`);
  }

  console.log(`Adding transactions (${transactions.length}) for user '${userID}'`);

  // Add `_` key if missing
  transactions.forEach(transaction => {
    if (transaction._ == null) {
      transaction._ = userID;
    }

    // USD is assumed by default
    if (transaction.y == 'USD') {
      delete transaction.y;
    }
  });

  // Check
  await context.functions
    .execute("checkUserTransactionsV2", userID, transactions)
    .mapErrorToUser();

  // Load missing data first
  await context.functions.execute("loadMissingDataV2", transactions);

  // Data is loaded we can safely insert our transactions
  const returnResult = await transactionsCollection.insertMany(transactions).mapErrorToSystem();
  console.logVerbose(`Result: ${returnResult.stringify()}`);
};
