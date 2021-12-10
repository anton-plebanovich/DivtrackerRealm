
// testUpdates.js

/**
 * @example
   exports();
 */
 exports = async function() {
  context.functions.execute("utilsV2");

  // TODO: Make it more complex and move to separate test case
  await Promise.all([
    context.functions.execute("updateCompaniesV2"),
    context.functions.execute("updateDividendsV2"),
    context.functions.execute("updatePricesV2"),
    context.functions.execute("updateQuotesV2"),
    context.functions.execute("updateSplitsV2"),
    context.functions.execute("updateSymbolsV2"),
  ]);
  
  if (errors.length) {
    throw errors;
  } else {
    console.log("SUCCESS!");
  }
};
