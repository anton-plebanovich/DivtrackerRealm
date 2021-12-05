
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
  // https://sandbox.iexapis.com/stable/ref-data/symbols?token=Tpk_581685f711114d9f9ab06d77506fdd49
  const newSymbols = await fetch("/ref-data/symbols");

  // We remove date field to prevent excessive updates each day
  newSymbols.forEach(x => delete x.date);

  const iexCollection = context.services.get("mongodb-atlas").db("iex").collection("symbols");
  const count = await iexCollection.count({});
  if (count == 0) {
    console.log(`No IEX data. Just inserting all records.`);
    await iexCollection.insertMany(newSymbols);
    return;
  }

  const newSymbolIDs = newSymbols.map(x => x.symbol);

  const oldSymbols = await iexCollection.find({}).toArray();
  const oldSymbolsDictionary = oldSymbols.toDictionary('symbol');
  const oldSymbolIDs = Object.keys(oldSymbolsDictionary);

  const bulk = iexCollection.initializeUnorderedBulkOp();
  
  console.log(`Updating existing IEX symbols and inserting missing`);
  for (const newSymbol of newSymbols) {
    // We try to update all symbold using IDs that allow us to track symbol renames.
    if (newSymbol.iexId != null && update('iexId', bulk, oldSymbolsDictionary, oldSymbols, newSymbol)) {
      continue;
    }
    if (newSymbol.figi != null && update('figi', bulk, oldSymbolsDictionary, oldSymbols, newSymbol)) {
      continue;
    }
    if (update('symbol', bulk, oldSymbolsDictionary, oldSymbols, newSymbol)) {
      continue;
    }

    // If we were unable to find what to update we just insert new symbol
    console.log(`Inserting IEX ${newSymbol.symbol}`);
    bulk.insert(newSymbol);
  }

  // We do not delete old symbols but instead mark them as disabled to be able to display user transactions.
  const symbolsIDsToDisable = oldSymbolIDs.filter(oldSymbolID =>
    !newSymbolIDs.includes(oldSymbolID)
  );

  if (symbolsIDsToDisable.length) {
    // TODO: throw error later to catch disabled symbols? Not sure if there are any.
    console.log(`Disabling IEX ${symbolsIDsToDisable}`);
    bulk.find({ "symbol": { $in: symbolsIDsToDisable } })
      .update({ $set: { isEnabled: false } });
  }
  
  await bulk.safeExecute();
}

// Checks if update is required for `newSymbol` using provided `fields`.
// Adds bulk operation if needed and returns `true` if update check passed.
// Returns `false` is update check wasn't possible.
function update(field, bulk, oldSymbolsDictionary, oldSymbols, newSymbol) {

  // First, we try to use dictionary because in most cases symbols will just match
  // and so we can increase performance.
  const newSymbolFieldValue = newSymbol[field];
  let oldSymbol = oldSymbolsDictionary[newSymbol.symbol];
  if (oldSymbol[field] !== newSymbolFieldValue) {
    // Perform search since our assumption failed. This one is slower.
    oldSymbol = oldSymbols.find(oldSymbol => {
      return oldSymbol[field] === newSymbolFieldValue;
    });
  }

  if (typeof oldSymbol === 'undefined') {
    return false;

  } else {
    if (newSymbol.symbol !== oldSymbol.symbol || 
      newSymbol.exchange !== oldSymbol.exchange || 
      newSymbol.name !== oldSymbol.name || 
      newSymbol.isEnabled !== oldSymbol.isEnabled) {

      console.log(`Updating ${oldSymbol.symbol} -> ${newSymbol.symbol}`);
      bulk.find({ [field]: newSymbolFieldValue })
        .updateOne({ $set: newSymbol });
    }

    return true;
  }
}

async function updateDivtrackerSymbols() {
  console.log(`Updating divtracker data`);
  const iexCollection = context.services.get("mongodb-atlas").db("iex").collection("symbols");

  const iexSymbols = await iexCollection.find({}).toArray();
  const newSymbols = iexSymbols.map(iexSymbol => {
    const symbol = {};
    symbol._id = iexSymbol._id;
    symbol._p = "P";
    symbol.s = iexSymbol.symbol;
    symbol.n = iexSymbol.name;

    // Disable flag should be only set if it's `true`
    if (iexSymbol.d) {
      symbol.d = iexSymbol.d;
    }

    return symbol;
  });

  const divtrackerCollection = context.services.get("mongodb-atlas").db("divtracker").collection("symbols-v2");
  const oldCount = await divtrackerCollection.count({});
  if (oldCount == 0) {
    console.log(`No Divtracker data. Just inserting all records.`);
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
    if (typeof oldSymbol === 'undefined') {
      console.log(`Inserting Divtracker ${newSymbol.s}`);
      bulk.insert(newSymbol);

    } else if (!newSymbol.isEqual(oldSymbol)) {
      console.log(`Updating Divtracker ${oldSymbol.s} -> ${newSymbol.s}`);

      // Disable flag should be only set if it's `true`
      if (newSymbol.d) {
        bulk.find({ _id: newSymbol._id })
          .updateOne({ $set: newSymbol });
      } else {
        bulk.find({ _id: newSymbol._id })
          .updateOne({ $set: newSymbol, $unset: { d: "" } });
      }
    }
  }

  await bulk.safeExecute();
}
