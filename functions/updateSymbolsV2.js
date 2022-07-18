
// updateSymbolsV2.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeOrderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.update/
// https://docs.mongodb.com/manual/reference/operator/query-logical/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write

/**
{
  "symbol": "AAPL",
  "exchange": "XNAS",
  "exchangeSuffix": "",
  "exchangeName": "Nasdaq All Markets",
  "exchangeSegment": "XNGS",
  "exchangeSegmentName": "Nasdaq Ngs Global Select Market",
  "name": "Apple Inc",
  "date": "2021-12-05",
  "type": "cs",
  "iexId": "IEX_4D48333344362D52",
  "region": "US",
  "currency": "USD",
  "isEnabled": true,
  "figi": "BBG000B9XRY4",
  "cik": "0000320193",
  "lei": "HWUPKR0MPOU8FGXBT394"
}
 */

/**
 * @note IEX update happens at 8am, 9am, 12pm, 1pm UTC
 */
exports = async function(__reserved, updateName) {
  context.functions.execute("iexUtils");

  if (updateName == null) {
    const date = new Date();
    await updateIEXSymbols();
    await setUpdateDate(iex, "symbols");
  
    await updateDivtrackerSymbols();
    await setUpdateDate(db, "symbols");
  
    await context.functions.execute("mergedUpdateSymbols", date, "iex");

  } else if (updateName === 'updateIEXSymbols') {
    await updateIEXSymbols();
    await setUpdateDate(iex, "symbols");

  } else if (updateName === 'updateDivtrackerSymbols') {
    await updateDivtrackerSymbols();
    await setUpdateDate(db, "symbols");

  } else {
    throw `Unknown '${updateName}' update name`;
  }
  
  console.log(`SUCCESS`);
};

async function updateIEXSymbols() {
  console.log(`Updating IEX symbols`);

  const newSymbols = await fetchSymbols();

  // We remove date field to prevent excessive updates each day
  newSymbols.forEach(x => delete x.date);

  const iexCollection = iex.collection("symbols");
  const count = await iexCollection.count({});
  if (count === 0) {
    console.log(`No IEX symbols. Just inserting all records.`);
    await iexCollection.insertMany(newSymbols);
    return;
  }

  const newSymbolIDs = newSymbols.map(x => x.symbol);
  const oldSymbols = await iexCollection.fullFind({});

  // iexId - is always unique, small coverage
  // figi - mostly unique but may have duplicates, big coverage
  // lei - a lot of duplicates and small coverage
  // cik - huge amount of duplicates but big coverage

  const newSymbolsByIexId = newSymbols.toBuckets('iexId');
  const newSymbolsByFigi = newSymbols.toBuckets('figi');
  const newSymbolsByLei = newSymbols.toBuckets('lei');
  const newSymbolsByCik = newSymbols.toBuckets('cik');
  const newSymbolsByTicker = newSymbols.toBuckets('symbol');

  const oldSymbolsByIexId = oldSymbols.toBuckets('iexId');
  const oldSymbolsByFigi = oldSymbols.toBuckets('figi');
  const oldSymbolsByLei = oldSymbols.toBuckets('lei');
  const oldSymbolsByCik = oldSymbols.toBuckets('cik');
  const oldSymbolsByTicker = oldSymbols.toBuckets('symbol');

  const bulk = iexCollection.initializeUnorderedBulkOp();
  for (const newSymbol of newSymbols) {
    // We try to update all symbold using IDs that allow us to track symbol renames.
    if (isIEXProduction && update('iexId', bulk, newSymbol, newSymbolsByIexId, oldSymbolsByIexId)) {
      continue;
    } else if (isIEXProduction && update('figi', bulk, newSymbol, newSymbolsByFigi, oldSymbolsByFigi)) {
      continue;
    } else if (isIEXProduction && update('lei', bulk, newSymbol, newSymbolsByLei, oldSymbolsByLei)) {
      continue;
    } else if (isIEXProduction && update('cik', bulk, newSymbol, newSymbolsByCik, oldSymbolsByCik)) {
      continue;
    } else if (update('symbol', bulk, newSymbol, newSymbolsByTicker, oldSymbolsByTicker)) {
      continue;
    }

    // If we were unable to find what to update we just insert new symbol
    console.log(`Inserting IEX ${newSymbol.symbol}`);
    bulk.insert(newSymbol);
  }

  // We need to execute update first because symbols might be renamed
  await bulk.safeExecute();

  // We do not delete old symbols but instead mark them as disabled to be able to display user transactions.
  const allSymbolIDs = await iexCollection.distinct('symbol');
  const symbolsIDsToDisable = allSymbolIDs.filter(symbolID =>
    !newSymbolIDs.includes(symbolID)
  );

  if (symbolsIDsToDisable.length) {
    console.log(`Disabling IEX symbols: ${symbolsIDsToDisable}`);
    await iexCollection.updateMany(
      { symbol: { $in: symbolsIDsToDisable }, isEnabled: true },
      { $set: { isEnabled: false }, $currentDate: { u: true } }
    );
  }
}

// Checks if update is required for `newSymbol` using provided `fields`.
// Adds bulk operation if needed and returns `true` if update check passed.
// Returns `false` is update check wasn't possible.
function update(field, bulk, newSymbol, newSymbolsByField, oldSymbolsByField) {
  const newSymbolFieldValue = newSymbol[field];
  if (newSymbolFieldValue == null) {
    return false;
  }

  let oldSymbol;

  // If we have 1 size old and new buckets we just update
  // If we have bigger size buckets we compare by ticker name
  const oldBucket = oldSymbolsByField[newSymbolFieldValue];
  const newBucket = newSymbolsByField[newSymbolFieldValue];
  if (oldBucket?.length === 1 && newBucket?.length === 1) {
    oldSymbol = oldBucket[0];
  } else if (oldBucket?.length) {
    oldSymbol = oldBucket.find(x => x.symbol === newSymbol.symbol);
  } else {
    return false;
  }

  if (oldSymbol == null) {
    return false;

  } else if (isIEXSandbox && newSymbol.isEnabled === oldSymbol.isEnabled) {
    // It takes too much time to update every symbol for IEX sandbox so we just skip update if enabled field didn't change
    return true;

  } else {
    bulk.findAndUpdateIfNeeded(newSymbol, oldSymbol, field, true);
    return true;
  }
}

async function updateDivtrackerSymbols() {
  console.log(`Updating Divtracker symbols`);
  const iexCollection = iex.collection("symbols");

  const iexSymbols = await iexCollection.fullFind({});
  const newSymbols = iexSymbols.map(iexSymbol => {
    const symbol = {};
    symbol._id = iexSymbol._id;
    symbol.setIfNotNullOrUndefined('c', iexSymbol.exchangeSegment);
    symbol.setIfNotNullOrUndefined('n', iexSymbol.name);
    symbol.setIfNotNullOrUndefined('t', iexSymbol.symbol);

    // Enable flag should be only set if it's `false`
    if (!iexSymbol.isEnabled) {
      symbol.e = false;
    }

    return symbol;
  });

  const divtrackerCollection = db.collection("symbols");
  const oldCount = await divtrackerCollection.count({});
  if (oldCount === 0) {
    console.log(`No Divtracker symbols. Just inserting all records.`);
    await divtrackerCollection.insertMany(newSymbols);
    return;
  }

  const oldSymbols = await divtrackerCollection.fullFind({});

  // Sanity check that allows us to skip removing of excessive documents.
  if (oldSymbols.count > newSymbols.count) {
    throw 'Invalid Divtracker symbols collections state. Old objects count should never be greater than new objects count.';
  }

  const oldSymbolsDictionary = oldSymbols.toDictionary('_id');

  const bulk = divtrackerCollection.initializeUnorderedBulkOp();
  for (const newSymbol of newSymbols) {
    const oldSymbol = oldSymbolsDictionary[newSymbol._id];
    bulk.findAndUpdateOrInsertIfNeeded(newSymbol, oldSymbol, '_id', true);
  }

  await bulk.safeExecute();
}
