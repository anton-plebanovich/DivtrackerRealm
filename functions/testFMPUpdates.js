
// testFMPUpdates.js

/**
 * @example
   exports();
 */
 exports = async function() {
  context.functions.execute("testUtils");

  // Cleanup environment
  await cleanup();

  const transactions = await generateRandomTransactions();
  await context.functions.execute("addTransactionsV2", transactions);

  // TODO: Make it more complex and move to separate test case
  await Promise.all([
    context.functions.execute("fmpUpdateCompanies"),
    context.functions.execute("fmpUpdateDividends"),
    context.functions.execute("fmpUpdatePrices"),
    context.functions.execute("fmpUpdateQuotes"),
    context.functions.execute("fmpUpdateSplits"),
    context.functions.execute("fmpUpdateSymbols"),
  ]);

  await checkData(transactions);
};
