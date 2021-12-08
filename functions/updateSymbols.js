
// updateSymbols.js

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
exports = async function() {
  context.functions.execute("utils");

  await updateIEXSymbols();
  await updateDivtrackerSymbols();
  
  console.log(`SUCCESS`);
};

async function updateIEXSymbols() {
  console.log(`Updating IEX symbols`);

  // https://sandbox.iexapis.com/stable/ref-data/symbols?token=Tpk_581685f711114d9f9ab06d77506fdd49
  const newSymbols = await fetch("/ref-data/symbols");

  // We remove date field to prevent excessive updates each day
  newSymbols.forEach(x => delete x.date);

  const iexCollection = iex.collection("symbols");
  const count = await iexCollection.count({});
  if (count == 0) {
    console.log(`No IEX symbols. Just inserting all records.`);
    await iexCollection.insertMany(newSymbols);
    return;
  }

  const newSymbolIDs = newSymbols.map(x => x.symbol);

  // We drop `_id` field so we can compare old and new objects.
  // All updates are done through other fields anyway.
  const oldSymbols = await iexCollection.find({}, { "_id": 0 }).toArray();
  const oldSymbolsDictionary = oldSymbols.toDictionary('symbol');

  const bulk = iexCollection.initializeUnorderedBulkOp();
  for (const newSymbol of newSymbols) {
    // We try to update all symbold using IDs that allow us to track symbol renames.
    if (isProduction && newSymbol.iexId != null && update('iexId', bulk, oldSymbolsDictionary, oldSymbols, newSymbol)) {
      continue;
    }
    if (isProduction && newSymbol.figi != null && update('figi', bulk, oldSymbolsDictionary, oldSymbols, newSymbol)) {
      continue;
    }
    if (update('symbol', bulk, oldSymbolsDictionary, oldSymbols, newSymbol)) {
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
    console.log(`Disabling IEX ${symbolsIDsToDisable}`);
    await iexCollection.updateMany(
      { symbol: { $in: symbolsIDsToDisable } },
      { $set: { isEnabled: false } }
    );
  }
}

// Checks if update is required for `newSymbol` using provided `fields`.
// Adds bulk operation if needed and returns `true` if update check passed.
// Returns `false` is update check wasn't possible.
function update(field, bulk, oldSymbolsDictionary, oldSymbols, newSymbol) {

  // First, we try to use dictionary because in most cases symbols will just match
  // and so we can increase performance.
  const newSymbolFieldValue = newSymbol[field];
  let oldSymbol = oldSymbolsDictionary[newSymbol.symbol];
  if (field !== 'symbol' && (!oldSymbol || oldSymbol[field] !== newSymbolFieldValue)) {
    // Perform search since our assumption failed. This one is slower.
    oldSymbol = oldSymbols.find(oldSymbol => {
      return oldSymbol[field] === newSymbolFieldValue;
    });
  }

  if (oldSymbol == null) {
    return false;

  } else {
    // Skip update on sandbox
    // if (isProduction) {
      bulk.findAndUpdateIfNeeded(newSymbol, oldSymbol, field);
    // }

    return true;
  }
}

async function updateDivtrackerSymbols() {
  console.log(`Updating Divtracker symbols`);
  const iexCollection = iex.collection("symbols");

  const iexSymbols = await iexCollection.find({}).toArray();
  const newSymbols = iexSymbols.map(iexSymbol => {
    const symbol = {};
    symbol._id = iexSymbol._id;

    // TODO: Remove '_p' later
    symbol._p = "2";
    
    symbol._ = "2";
    symbol.s = iexSymbol.symbol;
    symbol.n = iexSymbol.name;

    // Disable flag should be only set if it's `true`
    if (iexSymbol.d) {
      symbol.d = iexSymbol.d;
    }

    return symbol;
  });

  const divtrackerCollection = db.collection("symbols");
  const oldCount = await divtrackerCollection.count({});
  if (oldCount == 0) {
    console.log(`No Divtracker symbols. Just inserting all records.`);
    await divtrackerCollection.insertMany(newSymbols);
    return;
  }

  const oldSymbols = await divtrackerCollection.find({}).toArray();

  // Sanity check that allows us to skip removing of excessive documents.
  if (oldSymbols.count > newSymbols.count) {
    throw 'Invalid Divtracker symbols collections state. Old objects count should never be greater than new objects count.';
  }

  const oldSymbolsDictionary = oldSymbols.toDictionary('_id');

  const bulk = divtrackerCollection.initializeUnorderedBulkOp();
  for (const newSymbol of newSymbols) {
    const oldSymbol = oldSymbolsDictionary[newSymbol._id];
    bulk.findAndUpdateOrInsertIfNeeded(newSymbol, oldSymbol);
  }

  await bulk.safeExecute();
}
