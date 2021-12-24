
// testAddTransactions.js

/**
 * @example exports();
 */
 exports = async function() {
  context.functions.execute("testUtils");

  // Cleanup environment
  await cleanup();

  const transactions = await generateRandomTransactions();
  await context.functions.execute("addTransactionsV2", transactions);

  return await checkData(transactions);
};
