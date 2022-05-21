
// testIEXUpdates.js

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
    context.functions.execute("updateCompaniesV2"),
    context.functions.execute("updateDividendsFuture"),
    context.functions.execute("updateDividendsPast"),
    context.functions.execute("updatePricesV2"),
    context.functions.execute("updateQuotesV2"),
    context.functions.execute("updateSplitsV2"),
    context.functions.execute("updateSymbolsV2"),
  ]);

  await checkData(transactions);
};
