
// testUpdates.js

/**
 * @example
   exports();
 */
 exports = async function() {
  context.functions.execute("testUtils");

  // We do not cleanup since it's better to perform update in the non-empty environment

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
};
