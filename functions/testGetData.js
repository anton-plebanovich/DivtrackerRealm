
// testGetData.js

exports = async function() {
  context.functions.execute("testUtils");
  
  // Prepate environment
  const transactions = await generateRandomTransactions(10);
  const symbolIDs = transactions.map(x => x.s);
  await context.functions.execute("addTransactionsV2", transactions);

  // TODO: Add data verification

  // Base data fetch
  await context.functions.execute("getData", null, ["exchange-rates", "symbols", "updates"], null, null);

  // Symbols data fetch
  await context.functions.execute("getData", null, ["companies", "dividends", "historical-prices", "quotes", "splits"], symbolIDs, null);

  // Data update
  await context.functions.execute("getData", new Date('2020-01-01'), null, symbolIDs, ["exchange-rates", "symbols", "updates"]);
};
