
// testGetData.js

exports = async function() {
  context.functions.execute("testUtils");
  
  // Prepate environment
  const transactions = await generateRandomTransactions(1);
  await context.functions.execute("addTransactionsV2", transactions);

  try {
    await testGetDataV1(transactions);
  } catch(error) {
    console.error(error);
    throw error;
  }

  try {
    await testGetDataV2(transactions);
  } catch(error) {
    console.error(error);
    await restoreSymbols();
    throw error;
  }
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
  verifyResponseV2(response, ["exchange-rates", "symbols", "updates"], transactions.length);

  // Symbols data fetch
  response = await context.functions.execute("getDataV2", null, ["companies", "dividends", "historical-prices", "quotes", "splits"], symbolIDs, null);
  verifyResponseV2(response, ["companies", "dividends", "historical-prices", "quotes", "splits"], transactions.length);

  // Data update
  response = await context.functions.execute("getDataV2", new Date('2020-01-01').getTime(), null, symbolIDs, ["exchange-rates", "symbols", "updates"]);
  verifyResponseV2(response, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"], transactions.length);

  await test_getDataV2_refetch();
  await test_getDataV2_FMP();
  await test_getDataV2_FMP_and_IEX(symbolIDs);
  await test_getDataV2_errors();
  await restoreSymbols();
}

async function test_getDataV2_refetch() {
  let response;

  await cleanupSymbols();

  // We fetch lower priority source symbols first and then higher priority and so there should be multiple source symbols in the end
  await context.functions.execute("updateSymbolsV2"); 

  // Get timestamp just before we create refetch symbols
  const timestamp = new Date().getTime();

  // Create and get refetch symbol
  await context.functions.execute("fmpUpdateSymbols");
  const refetchSymbol = await atlas.db("merged").collection("symbols").findOne({ "r": { $ne: null } });
  if (refetchSymbol == null) {
    throw `Unable to get refetch symbol`;
  }

  response = await context.functions.execute("getDataV2", timestamp, null, [refetchSymbol._id], ["exchange-rates", "symbols", "updates"]);
  verifyResponseV2(response, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"], 1);
  verifyRefetchResponse(response, timestamp, ["exchange-rates", "symbols", "updates"]);

  response = await context.functions.execute("getDataV2", lastUpdateTimestamp, null, [refetchSymbol._id], null);
  verifyResponseV2(response, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"], 1);
  verifyRefetchResponse(response, timestamp);
}

async function test_getDataV2_FMP() {
  const fmpSymbol = await atlas.db("merged").collection("symbols").findOne({ "m.s": "f" });
  const response = await context.functions.execute("getDataV2", null, null, [fmpSymbol._id], ["exchange-rates", "symbols", "updates"]);
  verifyResponseV2(response, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"], 1);
}

async function test_getDataV2_FMP_and_IEX(_symbolIDs) {
  const fmpSymbol = await atlas.db("merged").collection("symbols").findOne({ "m.s": "f" });
  const symbolIDs = [..._symbolIDs];
  symbolIDs.push(fmpSymbol);
  const response = await context.functions.execute("getDataV2", null, null, symbolIDs, ["exchange-rates", "symbols", "updates"]);
  verifyResponseV2(response, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"], symbolIDs.length);
}

async function test_getDataV2_errors() {
  try {
    await expectGetDataError(new Date(), null, null, null);
  } catch(error) {
    verifyError(error, 'TODO');
  }
  
  try {
    await expectGetDataError(new Date().getTime() + 1000, null, null, null);
  } catch(error) {
    verifyError(error, 'TODO');
  }
  
  try {
    await expectGetDataError(null, "parameter", null, null);
  } catch(error) {
    verifyError(error, 'TODO');
  }
  
  try {
    await expectGetDataError(null, [], null, null);
  } catch(error) {
    verifyError(error, 'TODO');
  }
  
  try {
    await expectGetDataError(null, [1], null, null);
  } catch(error) {
    verifyError(error, 'TODO');
  }
  
  try {
    await expectGetDataError(null, ["parameter"], null, null);
  } catch(error) {
    verifyError(error, 'TODO');
  }
  
  try {
    await expectGetDataError(null, null, [], null);
  } catch(error) {
    verifyError(error, 'TODO');
  }
  
  try {
    await expectGetDataError(null, null, "parameter", null);
  } catch(error) {
    verifyError(error, 'TODO');
  }
  
  try {
    await expectGetDataError(null, null, ["parameter"], null);
  } catch(error) {
    verifyError(error, 'TODO');
  }
  
  try {
    await expectGetDataError(null, null, ['companies', 'dividends', 'historical-prices', 'quotes', 'splits'], null);
  } catch(error) {
    verifyError(error, 'TODO');
  }
  
  try {
    const symbolIDs = Array(1001).map(x => new BSON.ObjectId());
    await expectGetDataError(null, null, symbolIDs, null);
  } catch(error) {
    verifyError(error, 'TODO');
  }

  
  try {
    await expectGetDataError(null, null, null, "parameter");
  } catch(error) {
    verifyError(error, 'TODO');
  }
  
  try {
    await expectGetDataError(null, null, null, [1]);
  } catch(error) {
    verifyError(error, 'TODO');
  }
  
  try {
    await expectGetDataError(null, null, null, ["parameter"]);
  } catch(error) {
    verifyError(error, 'TODO');
  }
}

async function expectGetDataError(timestamp, collectionNames, symbolIDs, fullFetchCollections) {
  await context.functions.execute("getDataV2", timestamp, collectionNames, symbolIDs, fullFetchCollections);
  throw `getDataV2 did not throw an error`;
}

function verifyResponseV2(response, collections, length) {
  console.logData(`response: `, response);

  if (response.lastUpdateTimestamp == null) {
    throw `'lastUpdateTimestamp' field is absent in the response: ${response.stringify()}`;
  }

  if (response.updates == null) {
    throw `'updates' field is absent in the response: ${response.stringify()}`;
  }

  const hasRequiredCollections = collections.reduce((success, collection) => success && response.updates[collection] != null, true);
  const keys = Object.keys(response.updates);
  if (!hasRequiredCollections) {
    throw `Response does not have all required collections. Collections: ${collections}. Keys: ${keys}`;
  }

  if (length != null) {
    requiredDataCollections.forEach(collection => {
      if (!collections.includes(collection)) {
        return;
      }

      const data = response.updates[collection];
      if (data == null) {
        throw `'updates' data for '${collection}' collection is null`;
      }

      const dataLength = data.length;
      if (dataLength < length) {
        throw `Unexpected data length '${dataLength} < ${length}' for '${collection}' collection`;
      }
    });
  }

  // TODO: Add more data verifications
}

function verifyRefetchResponse(response, timestamp, fullFetchCollections) {
  const updates = response.updates;
  if (updates == null) {
    throw `Refetch update is null`;
  }
  if (response.lastUpdateTimestamp == null || response.lastUpdateTimestamp < timestamp) {
    throw `Unexpected lastUpdateTimestamp: ${response.lastUpdateTimestamp}. Should be higher that timestamp: ${timestamp}`;
  }
  if (response.cleanups?.length !== 1) {
    throw `Wrong cleanups length: ${response.cleanups}`;
  }

  requiredDataCollections.forEach(collection => {
    if (updates[collection] == null) {
      throw `Refetch updates for '${collection}' collection are null`;
    }
    if (updates[collection].length == 0) {
      throw `Refetch updates for '${collection}' collection are empty`;
    }
  });

  if (fullFetchCollections != null) {
    fullFetchCollections.forEach(collection => {
      const length = updates[collection].length;
      if (length <= 1) {
        throw `Unexpected '${collection}' collection length: ${length}`;
      }
    });
  }
}

function verifyError(error, message) {
  if (error !== message) {
    throw `Error '${error}' expected to be equal to '${message}'`;
  }
}

//////////////////////////// CONSTANTS

const requiredDataCollections = [
  'companies',
  'exchange-rates',
  'quotes',
  'symbols',
  'updates',
];
