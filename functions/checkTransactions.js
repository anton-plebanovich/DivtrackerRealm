
// checkTransactions.js

/**
 * Checks all existing transactions to be valid.
 */
 exports = async function() {
  context.functions.execute("utils");
  const transactionsCollection = db.collection("transactions");
  const userIDs = await transactionsCollection.distinct("_p");

  for (const userID of userIDs) {
    checkExecutionTimeout();
    
    console.log(`Checking user '${userID}' transactions...`);
    const userTransactions = await transactionsCollection.find({ _p: userID }).toArray();
    await context.functions.execute("checkUserTransactions", userID, userTransactions);
  }

  console.log(`All trasactions are valid!`);
};
