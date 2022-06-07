
// testGetData.js

exports = async function() {
  context.functions.execute("testUtils");

  const transactions = await generateRandomTransactions(1);
  const symbolIDs = transactions.map(x => x.s);
  await context.functions.execute("addTransactionsV2", transactions);

  try {
    await testGetDataV1(transactions);
  } catch(error) {
    console.error(error);
    throw error;
  }
  
  try {
    await test_getDataV2_base_fetch(transactions);
    await test_getDataV2_symbols_fetch(transactions, symbolIDs);
    await test_getDataV2_update_fetch(transactions, symbolIDs);
    await test_getDataV2_FMP();
    await test_getDataV2_FMP_and_IEX();
    await test_getDataV2_errors();
    await test_getDataV2_refetch();
  } catch(error) {
    console.error(error);
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

async function test_getDataV2_base_fetch(transactions) {
  console.log("test_getDataV2_base_fetch");
  const response = await context.functions.execute("getDataV2", null, ["exchange-rates", "symbols", "updates"], null, null);
  verifyResponseV2(response, ["exchange-rates", "symbols", "updates"], transactions.length);
}

async function test_getDataV2_symbols_fetch(transactions, symbolIDs) {
  console.log("test_getDataV2_symbols_fetch");
  const response = await context.functions.execute("getDataV2", null, ["companies", "dividends", "historical-prices", "quotes", "splits"], symbolIDs, null);
  verifyResponseV2(response, ["companies", "dividends", "historical-prices", "quotes", "splits"], transactions.length);
}

async function test_getDataV2_update_fetch(transactions, symbolIDs) {
  console.log("test_getDataV2_update_fetch");
  const response = await context.functions.execute("getDataV2", new Date('2020-01-01').getTime(), null, symbolIDs, ["exchange-rates", "symbols", "updates"]);
  verifyResponseV2(response, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"], transactions.length);
}

async function test_getDataV2_FMP() {
  console.log("test_getDataV2_FMP");
  const fmpSymbol = await atlas.db("merged").collection("symbols").findOne({ "m.s": "f" });
  const response = await context.functions.execute("getDataV2", null, null, [fmpSymbol._id], null);
  verifyResponseV2(response, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"], 1);
}

async function test_getDataV2_FMP_and_IEX() {
  console.log("test_getDataV2_FMP_and_IEX");
  const transactions = await generateRandomTransactions(1);
  const symbolIDs = transactions.map(x => x.s);
  await context.functions.execute("addTransactionsV2", transactions);

  const fmpSymbol = await atlas.db("merged").collection("symbols").findOne({ "m.s": "f" });
  symbolIDs.push(fmpSymbol._id);
  const response = await context.functions.execute("getDataV2", null, null, symbolIDs, null);
  verifyResponseV2(response, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"], symbolIDs.length);
}

async function test_getDataV2_errors() {
  console.log("test_getDataV2_errors");
  try {
    await expectGetDataError(new Date(), null, null, null);
  } catch(error) {
    verifyError(error, `{"type":"user","message":"Argument should be of the 'number' type. Instead, received '[object Date] (object)'. Please pass timestamp in milliseconds as the first argument."}`);
  }
  
  try {
    await expectGetDataError(new Date().getTime() + 1000, null, null, null);
  } catch(error) {
    verifyError(error, RegExp(`{"type":"user","message":"Invalid last update timestamp parameter. Passed timestamp '[0-9]*' is higher than the new update timestamp '[0-9]*'"}`));
  }
  
  try {
    await expectGetDataError(null, "parameter", null, null);
  } catch(error) {
    verifyError(error, `{"type":"user","message":"Argument should be of the 'Array' type. Instead, received '[object String]'. Please pass collections array as the second argument. It may be null but must not be empty. Valid collections are: companies,dividends,exchange-rates,historical-prices,quotes,splits,symbols,updates."}`);
  }
  
  try {
    await expectGetDataError(null, [], null, null);
  } catch(error) {
    verifyError(error, `{"type":"user","message":"Array is empty. Please pass collections array as the second argument. It may be null but must not be empty. Valid collections are: companies,dividends,exchange-rates,historical-prices,quotes,splits,symbols,updates."}`);
  }
  
  try {
    await expectGetDataError(null, [1], null, null);
  } catch(error) {
    verifyError(error, `{"type":"user","message":"Argument should be of the 'string' type. Instead, received '[object Number]'. Please pass collections array as the second argument. Valid collections are: companies,dividends,exchange-rates,historical-prices,quotes,splits,symbols,updates."}`);
  }
  
  try {
    await expectGetDataError(null, ["parameter"], null, null);
  } catch(error) {
    verifyError(error, `{"type":"user","message":"Invalid collections array as the second argument: parameter. Valid collections are: companies,dividends,exchange-rates,historical-prices,quotes,splits,symbols,updates."}`);
  }
  
  try {
    await expectGetDataError(null, null, [], null);
  } catch(error) {
    verifyError(error, `{"type":"user","message":"Array is empty. Please pass symbol IDs array as the third argument. It may be null but must not be empty."}`);
  }
  
  try {
    await expectGetDataError(null, null, "parameter", null);
  } catch(error) {
    verifyError(error, `{"type":"user","message":"Argument should be of the 'Array' type. Instead, received '[object String]'. Please pass symbol IDs array as the third argument. It may be null but must not be empty."}`);
  }
  
  try {
    await expectGetDataError(null, null, ["parameter"], null);
  } catch(error) {
    verifyError(error, `{"type":"user","message":"Argument should be of the 'ObjectId' type. Instead, received '[object String]'. Please pass symbol IDs array as the third argument."}`);
  }
  
  try {
    await expectGetDataError(null, null, ['companies', 'dividends', 'historical-prices', 'quotes', 'splits'], null);
  } catch(error) {
    verifyError(error, `{"type":"user","message":"Argument should be of the 'ObjectId' type. Instead, received '[object String]'. Please pass symbol IDs array as the third argument."}`);
  }
  
  try {
    const symbolIDs = [...Array(1001).keys()].map(x => new BSON.ObjectId());
    await expectGetDataError(null, null, symbolIDs, null);
  } catch(error) {
    verifyError(error, `{"type":"user","message":"Max collections count '1000' is exceeded. Please make sure there are less than 1000 unique symbols in the portfolio."}`);
  }

  
  try {
    await expectGetDataError(null, null, null, "parameter");
  } catch(error) {
    verifyError(error, `{"type":"user","message":"Argument should be of the 'Array' type. Instead, received '[object String]'. Please pass full symbol collections array as the fourth argument. It may be null. Valid collections are: companies,dividends,exchange-rates,historical-prices,quotes,splits,symbols,updates."}`);
  }
  
  try {
    await expectGetDataError(null, null, null, [1]);
  } catch(error) {
    verifyError(error, `{"type":"user","message":"Argument should be of the 'string' type. Instead, received '[object Number]'. Please pass full symbol collections array as the fourth argument. Valid collections are: companies,dividends,exchange-rates,historical-prices,quotes,splits,symbols,updates."}`);
  }
  
  try {
    await expectGetDataError(null, null, null, ["parameter"]);
  } catch(error) {
    verifyError(error, `{"type":"user","message":"Invalid full symbol collections array as the fourth argument: parameter. Valid full symbol collections are: exchange-rates,symbols,updates."}`);
  }
}

async function expectGetDataError(timestamp, collectionNames, symbolIDs, fullFetchCollections) {
  await context.functions.execute("getDataV2", timestamp, collectionNames, symbolIDs, fullFetchCollections);
  throw `getDataV2 did not throw an error`;
}

async function test_getDataV2_refetch() {

  // Create and get refetch symbol
  const refetchSymbol = await atlas.db("merged").collection("symbols").findOne({ "r": { $ne: null } });
  if (refetchSymbol == null) {
    throw `Unable to get refetch symbol`;
  }

  // Get timestamp just before refetch date
  const timestamp = refetchSymbol.r.getTime() - 1;

  let response;

  console.log("test_getDataV2_refetch.update_default");
  response = await context.functions.execute("getDataV2", timestamp, null, [refetchSymbol._id], ["exchange-rates", "symbols", "updates"]);
  verifyResponseV2(response, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"], 1);
  verifyRefetchResponse(response, timestamp, ["exchange-rates", "symbols", "updates"]);

  console.log("test_getDataV2_refetch.update_symbol");
  response = await context.functions.execute("getDataV2", timestamp, null, [refetchSymbol._id], null);
  verifyResponseV2(response, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"], 1);
  verifyRefetchResponse(response, timestamp);
}

//////////////////////////// VERIFICATION V2

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
        throw `Unexpected data length '${dataLength}' lower than '${length}' for '${collection}' collection`;
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
  if (message instanceof RegExp) {
    if (!message.test(error)) {
      throw `Error '${error}' expected to be equal to '${message}'`;
    }
  } else {
    if (error != message) {
      throw `Error '${error}' expected to be equal to '${message}'`;
    }
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
