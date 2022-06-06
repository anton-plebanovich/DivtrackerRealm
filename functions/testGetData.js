
// testGetData.js

exports = async function() {
  context.functions.execute("testUtils");
  
  // Prepate environment
  const transactions = await generateRandomTransactions(10);
  await context.functions.execute("addTransactionsV2", transactions);

  await testGetDataV1(transactions);
  await testGetDataV2(transactions);
};

//////////////////////////// TESTS V1

async function testGetDataV1(transactions) {
  const symbolIDs = transactions.map(x => x.s);

  let response;

  // Base data fetch
  response = await context.functions.execute("getData", null, ["exchange-rates", "symbols", "updates"], null, null);
  verifyResponseV1(response, ["exchange-rates", "symbols", "updates"]);

  // Symbols data fetch
  response = await context.functions.execute("getData", null, ["companies", "dividends", "historical-prices", "quotes", "splits"], symbolIDs, null);
  verifyResponseV1(response, ["companies", "dividends", "historical-prices", "quotes", "splits"]);

  // Data update
  response = await context.functions.execute("getData", new Date('2020-01-01'), null, symbolIDs, ["exchange-rates", "symbols", "updates"]);
  verifyResponseV1(response, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"]);
}

function verifyResponseV1(response, collections) {
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

//////////////////////////// TESTS V2

async function testGetDataV2(transactions) {
  const symbolIDs = transactions.map(x => x.s);

  let response;

  // Base data fetch
  response = await context.functions.execute("getDataV2", null, ["exchange-rates", "symbols", "updates"], null, null);
  verifyResponseV2(response, ["exchange-rates", "symbols", "updates"]);

  // Symbols data fetch
  response = await context.functions.execute("getDataV2", null, ["companies", "dividends", "historical-prices", "quotes", "splits"], symbolIDs, null);
  verifyResponseV2(response, ["companies", "dividends", "historical-prices", "quotes", "splits"]);

  // Data update
  response = await context.functions.execute("getDataV2", new Date('2020-01-01'), null, symbolIDs, ["exchange-rates", "symbols", "updates"]);
  verifyResponseV2(response, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"]);
}

function verifyResponseV2(response, collections) {
  if (response.lastUpdateDate == null) {
    throw `lastUpdateDate is absent in the response: ${response.stringify()}`;
  }

  const hasRequiredCollections = collections.reduce((success, collection) => success && response.updates[collection] != null, true);
  const keys = Object.keys(response.updates);
  if (!hasRequiredCollections) {
    throw `Response does not have all required collections. Collections: ${collections}. Keys: ${keys}`;
  }

  // TODO: Add more data verifications
}

//////////////////////////// HELPERS
