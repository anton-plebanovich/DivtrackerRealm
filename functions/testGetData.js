
// testGetData.js

exports = async function() {
  context.functions.execute("testUtils");
  
  // Prepate environment
  const transactions = await generateRandomTransactions(10);
  const symbolIDs = transactions.map(x => x.s);
  await context.functions.execute("addTransactionsV2", transactions);

  // TODO: Add more data verifications
  let response;

  // Base data fetch
  response = await context.functions.execute("getData", null, ["exchange-rates", "symbols", "updates"], null, null);
  verifyResponse(response, ["exchange-rates", "symbols", "updates"]);

  // Symbols data fetch
  response = await context.functions.execute("getData", null, ["companies", "dividends", "historical-prices", "quotes", "splits"], symbolIDs, null);
  verifyResponse(response, ["companies", "dividends", "historical-prices", "quotes", "splits"]);

  // Data update
  response = await context.functions.execute("getData", new Date('2020-01-01'), null, symbolIDs, ["exchange-rates", "symbols", "updates"]);
  verifyResponse(response, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"]);
};

function verifyResponse(response, collections) {
  if (response.lastUpdateDate == null) {
    throw `lastUpdateDate is absent in the response: ${response.stringify()}`;
  }

  const hasRequiredCollections = collections.reduce((success, collection) => success && response[collection] != null, true);
  const keys = Object.keys(response);
  if (!hasRequiredCollections) {
    throw `Response does not have all required collections. Collections: ${collections}. Keys: ${keys}`;
  }

  // collections + lastUpdateDate
  const expectedKeysCount = collections.length + 1;
  if (expectedKeysCount !== keys.length) {
    throw `Unexpected response keys number: ${keys.length} !== ${collections.length + 1}`;
  }
}