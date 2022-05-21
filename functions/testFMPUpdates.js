
// testFMPUpdates.js

/**
 * @example
   exports();
 */
 exports = async function() {
  context.functions.execute("fmpUtils");

  // Cleanup FMP environment
  await Promise.all([
    fmp.collection('companies').deleteMany({}),
    fmp.collection('data-status').deleteMany({}),
    fmp.collection('dividends').deleteMany({}),
    fmp.collection('historical-prices').deleteMany({}),
    fmp.collection('quotes').deleteMany({}),
    fmp.collection('splits').deleteMany({}),
    fmp.collection('symbols').deleteMany({}),
  ]);
  
  // Add some symbols
  const symbols = await fetchSymbols();
  const symbolsToAdd = symbols.getRandomElements(20);
  await fmp.collection('symbols').insertMany(symbolsToAdd);

  await Promise.all([
    context.functions.execute("fmpUpdateCompanies"),
    context.functions.execute("fmpUpdateDividends"),
    context.functions.execute("fmpUpdatePrices"),
    context.functions.execute("fmpUpdateQuotes"),
    context.functions.execute("fmpUpdateSplits"),
    context.functions.execute("fmpUpdateSymbols"),
  ]);
};
