
// testGetData.js

exports = async function() {
  context.functions.execute("testUtils");

  // verboseLogEnabled = true;
  // dataLogEnabled = true;

  await prepareFMPData();

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
    await test_getDataV2_base_fetch();
    await test_getDataV2_symbols_fetch(symbolIDs);
    await test_getDataV2_update_fetch(symbolIDs);
    await test_getDataV2_FMP();
    await test_getDataV2_FMP_and_IEX(symbolIDs);
    await test_getDataV2_errors();
    await test_getDataV2_refetch();
    await test_getDataV2_full_fetch_deleted(symbolIDs);
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

async function test_getDataV2_base_fetch() {
  console.log("test_getDataV2_base_fetch");
  const response = await context.functions.execute("getDataV2", null, ["exchange-rates", "symbols", "updates"], null, null);
  verifyResponseV2(response, null, ["exchange-rates", "symbols", "updates"], null);
}

async function test_getDataV2_symbols_fetch(symbolIDs) {
  console.log("test_getDataV2_symbols_fetch");
  const response = await context.functions.execute("getDataV2", null, ["companies", "dividends", "historical-prices", "quotes", "splits"], symbolIDs, null);
  verifyResponseV2(response, null, ["companies", "dividends", "historical-prices", "quotes", "splits"], symbolIDs);
}

async function test_getDataV2_update_fetch(symbolIDs) {
  console.log("test_getDataV2_update_fetch");
  const response = await context.functions.execute("getDataV2", new Date('2020-01-01').getTime(), null, symbolIDs, ["exchange-rates", "symbols", "updates"]);
  verifyResponseV2(response, null, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"], symbolIDs, ["exchange-rates", "symbols", "updates"]);
}

async function test_getDataV2_FMP() {
  console.log("test_getDataV2_FMP");
  const fmpSymbol = await atlas.db("merged").collection("symbols").findOne({ "m.s": "f" });
  const symbolIDs = [fmpSymbol._id];
  const response = await context.functions.execute("getDataV2", null, null, symbolIDs, null);
  verifyResponseV2(response, null, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"], symbolIDs);
}

async function test_getDataV2_FMP_and_IEX(_symbolIDs) {
  console.log("test_getDataV2_FMP_and_IEX");
  const fmpSymbol = await atlas.db("merged").collection("symbols").findOne({ "m.s": "f" });
  const symbolIDs = [..._symbolIDs];
  symbolIDs.push(fmpSymbol._id);
  const response = await context.functions.execute("getDataV2", null, null, symbolIDs, null);
  verifyResponseV2(response, null, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"], symbolIDs);
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
    await expectGetDataError(new Date(-1).getTime(), null, null, null);
  } catch(error) {
    verifyError(error, RegExp(`{"type":"user","message":"Invalid last update timestamp parameter. Timestamp '-[0-9]*' should be greater than zero"}`));
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
  const symbolIDs = [refetchSymbol._id];

  let response;

  console.log("test_getDataV2_refetch.update_default");
  response = await context.functions.execute("getDataV2", timestamp, null, symbolIDs, ["exchange-rates", "symbols", "updates"]);
  verifyResponseV2(response, timestamp, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"], symbolIDs, ["exchange-rates", "symbols", "updates"]);
  verifyRefetchResponse(response, timestamp, ["exchange-rates", "symbols", "updates"]);

  console.log("test_getDataV2_refetch.update_symbol");
  response = await context.functions.execute("getDataV2", timestamp, null, symbolIDs, null);
  verifyResponseV2(response, timestamp, ["companies", "dividends", "historical-prices", "quotes", "splits", "exchange-rates", "symbols", "updates"], symbolIDs);
  verifyRefetchResponse(response, timestamp);
}

async function test_getDataV2_full_fetch_deleted(symbolIDs) {
  console.log("test_getDataV2_full_fetch_deleted");

  let response;
  let deletedObjectID;

  response = await context.functions.execute("getDataV2", null, ["historical-prices"], symbolIDs, null);
  const historicalPrice = response.updates["historical-prices"][0];
  const id = historicalPrice._id;
  const idString = id.toString();
  const symbolID = historicalPrice.s;
  const db = sourceByName.iex.db;
  const collection = db.collection('historical-prices');

  // Delete historical price
  const timestamp = new Date().getTime();
  await collection.updateOne({ _id: id }, { $set: { x: true }, $currentDate: { u: true } });

  response = await context.functions.execute("getDataV2", null, ["historical-prices"], [symbolID], null);
  const updatedObject = response.updates?.["historical-prices"]?.find(x => x._id.toString() === idString);
  if (updatedObject != null) {
    throw `Deleted object is returned during full fetch: ${updatedObject.stringify()}`;
  }

  deletedObjectID = response.deletions?.["historical-prices"]?.find(x => x.toString() === idString);
  if (deletedObjectID != null) {
    throw `Deleted object ID returned during full fetch: ${response.deletions?.stringify()}`;
  }

  response = await context.functions.execute("getDataV2", timestamp, ["historical-prices"], [symbolID], null);
  deletedObjectID = response.deletions?.["historical-prices"]?.find(x => x.toString() === idString);
  if (deletedObjectID == null) {
    throw `Deleted object ID is not returned during update fetch: ${response.deletions?.stringify()}`;
  }

  // Restore environment
  await collection.updateOne({ _id: id }, { $unset: { x: "" } });
}

//////////////////////////// VERIFICATION V2

function verifyResponseV2(response, timestamp, collections, symbolIDs, fullFetchCollections) {
  console.logData(`response`, response);

  if (response.lastUpdateTimestamp == null) {
    throw `'lastUpdateTimestamp' field is absent in the response: ${response.stringify()}`;
  }

  if (response.updates == null) {
    throw `'updates' field is absent in the response: ${response.stringify()}`;
  }

  const updates = response.updates;
  const collectionsToCheck = collections.filter(x => requiredCollections.includes(x));
  const hasRequiredCollections = collectionsToCheck.reduce((success, collection) => success && updates[collection] != null, true);
  const updateCollections = Object.keys(updates);
  if (timestamp == null && !hasRequiredCollections) {
    throw `Response does not have all required collections. Collections: ${collectionsToCheck}. Update collections: ${updateCollections}`;
  }

  if (symbolIDs != null) {
    const symbolIDStrings = symbolIDs.map(x => x.toString());
    for (const collection of updateCollections) {
      if (nonSearchableIDCollections.includes(collection)) { continue; }
      if (fullFetchCollections?.includes(collection)) { continue; }
  
      const objects = updates[collection];
      if (singularSymbolCollections.includes(collection)) {
        objects.forEach(object => { 
          const idString = object._id.toString();
          if (!symbolIDStrings.includes(idString)) {
            throw `Collection '${collection}' update data ID '${idString}' expected to be included in requested symbol IDs: ${symbolIDStrings}. Data: ${object.stringify()}`;
          }
        });
      } else {
        objects.forEach(update => { 
          const idString = update.s.toString();
          if (!symbolIDStrings.includes(idString)) {
            throw `Collection '${collection}' update symbol ID '${idString}' expected to be included in requested symbol IDs: ${symbolIDStrings}. Data: ${object.stringify()}`;
          }
        });
      }
    }
  }

  const length = symbolIDs?.length;
  if (length != null && timestamp == null) {
    requiredCollections.forEach(collection => {
      if (!collections.includes(collection)) {
        return;
      }

      const data = response.updates[collection];
      if (data == null) {
        throw `'updates' data for '${collection}' collection is null`;
      }

      if (timestamp == null) {
        const dataLength = data.length;
        if (dataLength < length) {
          throw `Unexpected data length '${dataLength}' lower than '${length}' for '${collection}' collection`;
        }
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

  requiredCollections.forEach(collection => {
    if (nonSearchableIDCollections.includes(collection)) { return; }

    if (updates[collection] == null) {
      throw `Refetch updates for '${collection}' collection are null`;
    }
    if (updates[collection].length == 0) {
      throw `Refetch updates for '${collection}' collection are empty`;
    }
  });

  if (fullFetchCollections != null && timestamp == null) {
    fullFetchCollections.forEach(collection => {
    if (nonSearchableIDCollections.includes(collection)) { return; }

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

const requiredCollections = [
  'companies',
  'quotes',
];

const singularSymbolCollections = [
  'companies',
  'quotes',
  'symbols',
];

const nonSearchableIDCollections = [
  'exchange-rates',
  'updates',
];
