
// loadMissingDataV2.js

// https://docs.mongodb.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/

/**
 * @example
 * exports();
 * exports([{"s":new BSON.ObjectId("61b102c0048b84e9c13e4564")}]);
 */
exports = async function loadMissingData(transactions) {
  context.functions.execute("utilsV2");
  
  let shortSymbols;
  if (Object.prototype.toString.call(transactions) === '[object Array]') {
    const symbolIDs = transactions
      .map(x => x.s)
      .distinct();

    shortSymbols = await getShortSymbols(symbolIDs);

  } else {
    shortSymbols = await getInUseShortSymbols();
  }

  const tickers = shortSymbols.map(x => x.t);
  console.log(`Loading missing data for tickers (${tickers.length}): ${tickers}`);
  
  return Promise.safeAllAndUnwrap([
    loadMissingCompanies(shortSymbols).mapErrorToSystem(),
    loadMissingDividends(shortSymbols).mapErrorToSystem(),
    loadMissingHistoricalPrices(shortSymbols).mapErrorToSystem(),
    loadMissingPreviousDayPrices(shortSymbols).mapErrorToSystem(),
    loadMissingQuotes(shortSymbols).mapErrorToSystem(),
    loadMissingSplits(shortSymbols).mapErrorToSystem()
  ]);
};

//////////////////////////////////////////////////////////////////// Companies

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingCompanies(shortSymbols) {
  const collection = db.collection('companies');
  const missingShortSymbols = await getMissingShortSymbols(collection, '_id', shortSymbols);
  if (missingShortSymbols.length) {
    console.log(`Found missing companies for tickers: ${missingShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No missing companies. Skipping loading.`);
    return;
  }
  
  const companies = await fetchCompanies(missingShortSymbols);
  if (!companies.length) {
    console.log(`No companies. Skipping insert.`);
    return;
  }

  const operations = [];
  for (const company of companies) {
    const filter = { _id: company._id };
    const update = { $setOnInsert: company };
    const updateOne = { filter: filter, update: update, upsert: true };
    const operation = { updateOne: updateOne };
    operations.push(operation);
  }
  
  return collection.bulkWrite(operations);
}

//////////////////////////////////////////////////////////////////// Dividends

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingDividends(shortSymbols) {
  const collection = db.collection('dividends');
  const missingShortSymbols = await getMissingShortSymbols(collection, 's', shortSymbols);
  if (missingShortSymbols.length) {
    console.log(`Found missing dividends for tickers: ${missingShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No missing dividends. Skipping loading.`);
    return;
  }

  const futureDividends = await fetchDividends(missingShortSymbols, true);
  const pastDividends = await fetchDividends(missingShortSymbols, false);
  const dividends = futureDividends.concat(pastDividends);
  if (!dividends.length) {
    console.log(`No dividends. Skipping insert.`);
    return;
  }

  const bulk = collection.initializeUnorderedBulkOp();
  for (const dividend of dividends) {
    const query = { e: dividend.e, a: dividend.a, f: dividend.f, s: dividend.s };
    const update = { $setOnInsert: dividend };
    bulk.find(query).upsert().updateOne(update);
  }
  
  return bulk.execute();
}

//////////////////////////////////////////////////////////////////// Historical Prices

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingHistoricalPrices(shortSymbols) {
  const collection = db.collection('historical-prices');
  const missingShortSymbols = await getMissingShortSymbols(collection, 's', shortSymbols);
  if (missingShortSymbols.length) {
    console.log(`Found missing historical prices for tickers: ${missingShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No missing historical prices. Skipping loading.`);
    return;
  }
  
  const historicalPrices = await fetchHistoricalPrices(missingShortSymbols);
  if (!historicalPrices.length) {
    console.log(`No historical prices. Skipping insert.`);
    return;
  }

  const bulk = collection.initializeUnorderedBulkOp();
  for (const historicalPrice of historicalPrices) {
    const query = { d: historicalPrice.d, s: historicalPrice.s };
    const update = { $setOnInsert: historicalPrice };
    bulk.find(query).upsert().updateOne(update);
  }
  
  return bulk.execute();
}

//////////////////////////////////////////////////////////////////// Previous Day Prices

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingPreviousDayPrices(shortSymbols) {
  const collection = db.collection('previous-day-prices');
  const missingShortSymbols = await getMissingShortSymbols(collection, '_id', shortSymbols);
  if (missingShortSymbols.length) {
    console.log(`Found missing previous day prices for tickers: ${missingShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No missing previous day prices. Skipping loading.`);
    return;
  }
  
  const previousDayPrices = await fetchPreviousDayPrices(missingShortSymbols);
  if (!previousDayPrices.length) {
    console.log(`No previous day prices. Skipping insert.`);
    return;
  }

  const bulk = collection.initializeUnorderedBulkOp();
  for (const previousDayPrice of previousDayPrices) {
    const query = { _id: previousDayPrice._id };
    const update = { $setOnInsert: previousDayPrice };
    bulk.find(query).upsert().updateOne(update);
  }
  
  return bulk.execute();
}

//////////////////////////////////////////////////////////////////// Quote

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingQuotes(shortSymbols) {
  const collection = db.collection('quotes');
  const missingShortSymbols = await getMissingShortSymbols(collection, '_id', shortSymbols);
  if (missingShortSymbols.length) {
    console.log(`Found missing qoutes for tickers: ${missingShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No missing quotes. Skipping loading.`);
    return;
  }
  
  const quotes = await fetchQuotes(missingShortSymbols);
  if (!quotes.length) {
    console.log(`No quotes. Skipping insert.`);
    return;
  }

  const bulk = collection.initializeUnorderedBulkOp();
  for (const quote of quotes) {
    const query = { _id: quote._id };
    const update = { $setOnInsert: quote };
    bulk.find(query).upsert().updateOne(update);
  }
  
  return bulk.execute();
}

//////////////////////////////////////////////////////////////////// Splits

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingSplits(shortSymbols) {
  const collection = db.collection('splits');
  const missingShortSymbols = await getMissingShortSymbols(collection, 's', shortSymbols);
  if (missingShortSymbols.length) {
    console.log(`Found missing splits for tickers: ${missingShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No missing splits. Skipping loading.`);
    return;
  }
  
  const splits = await fetchSplits(missingShortSymbols);
  if (!splits.length) {
    console.log(`No splits. Skipping insert.`);
    return;
  }

  const bulk = collection.initializeUnorderedBulkOp();
  for (const split of splits) {
    const query = { e: split.e, s: split.s };
    const update = { $setOnInsert: split };
    bulk.find(query).upsert().updateOne(update);
  }
  
  return bulk.execute();
}

//////////////////////////////////////////////////////////////////// Helpers

/**
 * Returns only missing IDs from `shortSymbols`.
 */
async function getMissingShortSymbols(collection, field, shortSymbols) {
  const symbolIDs = shortSymbols.map(x => x._id);
  const existingIDs = await collection
    .distinct(field, { [field]: { $in: symbolIDs } });
    
  const missingShortSymbols = shortSymbols.filter(
    x => !existingIDs.includesObject(x._id)
  );
  
  return missingShortSymbols;
}
