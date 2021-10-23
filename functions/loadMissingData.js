
// loadMissingData.js

// TODO: We need to load missing data using batch API
exports = async function() {
  context.functions.execute("utils");
  await computeDistinctSymbols();

  // Set proper value and uncomment if everything can't be fetched in one run.
  // resumeOn = "AAPL";
  
  // It takes ~2s to check fetched symbol and ~3s to fully fetch one.
  // So this function can work only until we have less that ~30-45 symbols.
  for (const symbol of distinctSymbols) {
    if (typeof resumeOn !== 'undefined' && symbol < resumeOn) {
      console.logVerbose(`Skipping: ${symbol}`);
      continue;
    }

    try {
      checkExecutionTimeout();
    } catch (error) {
      console.error(`Stopped on: ${symbol}`);
      return;
    }
    
    console.log(`Loading missing data for symbol: ${symbol}`);
    // await context.functions.execute("loadMissingDividendsForSymbol", symbol, distinctSymbolsDictionary[symbol]);
    await context.functions.execute("loadMissingDataForSymbol", symbol, distinctSymbolsDictionary[symbol]);
  }
};
