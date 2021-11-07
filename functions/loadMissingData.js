
// loadMissingData.js

// https://docs.mongodb.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/

/**
 * @example
 * exports();
 * exports([{"e":"NYS","s":"LMT"}]);
 */
exports = async function loadMissingData(transactions) {
  context.functions.execute("utils");
  
  let uniqueIDs;
  if (transactions && Object.prototype.toString.call(transactions) === '[object Array]') {
    uniqueIDs = transactions
      .map(x => `${x.s}:${x.e}`)
      .distinct();

  } else {
    uniqueIDs = await getUniqueTransactionIDs();
  }
  
  console.log(`Loading missing data for unique transaction IDs (${uniqueIDs.length}): ${uniqueIDs}`);
  
  return Promise.safeAllAndUnwrap([
    loadMissingCompanies(uniqueIDs).mapErrorToSystem(),
    loadMissingDividends(uniqueIDs).mapErrorToSystem(),
    loadMissingHistoricalPrices(uniqueIDs).mapErrorToSystem(),
    loadMissingPreviousDayPrices(uniqueIDs).mapErrorToSystem(),
    loadMissingQuotes(uniqueIDs).mapErrorToSystem(),
    loadMissingSplits(uniqueIDs).mapErrorToSystem()
  ]);
};

//////////////////////////////////////////////////////////////////// Companies

/**
 * @param {[string]} uniqueIDs Unique IDs to check.
 */
async function loadMissingCompanies(uniqueIDs) {
  const collection = db.collection('companies');
  const missingIDs = await getMissingIDs(collection, '_id', uniqueIDs);
  if (missingIDs.length) {
    console.log(`Found missing companies for IDs: ${missingIDs}`);
  } else {
    console.log(`No missing companies. Skipping loading.`);
    return;
  }
  
  const companies = await fetchCompanies(missingIDs);
  if (!companies.length) {
    console.log(`No companies. Skipping insert.`);
    return;
  }

  // bulkWrite example
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
 * @param {[string]} uniqueIDs Unique IDs to check.
 */
async function loadMissingDividends(uniqueIDs) {
  const collection = db.collection('dividends');
  const missingIDs = await getMissingIDs(collection, '_i', uniqueIDs);
  if (missingIDs.length) {
    console.log(`Found missing dividends for IDs: ${missingIDs}`);
  } else {
    console.log(`No missing dividends. Skipping loading.`);
    return;
  }

  const futureDividends = await fetchDividends(missingIDs, true);
  const pastDividends = await fetchDividends(missingIDs, false);
  const dividends = futureDividends.concat(pastDividends);
  if (!dividends.length) {
    console.log(`No dividends. Skipping insert.`);
    return;
  }

  const bulk = collection.initializeUnorderedBulkOp();
  for (const dividend of dividends) {
    const query = { _i: dividend._i, e: dividend.e, a: dividend.a, f: dividend.f };
    const update = { $setOnInsert: dividend };
    bulk.find(query).upsert().updateOne(update);
  }
  
  return bulk.execute();
}

//////////////////////////////////////////////////////////////////// Historical Prices

/**
 * @param {[string]} uniqueIDs Unique IDs to check.
 */
async function loadMissingHistoricalPrices(uniqueIDs) {
  const collection = db.collection('historical-prices');
  const missingIDs = await getMissingIDs(collection, '_i', uniqueIDs);
  if (missingIDs.length) {
    console.log(`Found missing historical prices for IDs: ${missingIDs}`);
  } else {
    console.log(`No missing historical prices. Skipping loading.`);
    return;
  }
  
  const historicalPrices = await fetchHistoricalPrices(missingIDs);
  if (!historicalPrices.length) {
    console.log(`No historical prices. Skipping insert.`);
    return;
  }

  const bulk = collection.initializeUnorderedBulkOp();
  for (const historicalPrice of historicalPrices) {
    const query = { _i: historicalPrice._i, d: historicalPrice.d };
    const update = { $setOnInsert: historicalPrice };
    bulk.find(query).upsert().updateOne(update);
  }
  
  return bulk.execute();
}

//////////////////////////////////////////////////////////////////// Previous Day Prices

/**
 * @param {[string]} uniqueIDs Unique IDs to check.
 */
async function loadMissingPreviousDayPrices(uniqueIDs) {
  const collection = db.collection('previous-day-prices');
  const missingIDs = await getMissingIDs(collection, '_id', uniqueIDs);
  if (missingIDs.length) {
    console.log(`Found missing previous day prices for IDs: ${missingIDs}`);
  } else {
    console.log(`No missing previous day prices. Skipping loading.`);
    return;
  }
  
  const previousDayPrices = await fetchPreviousDayPrices(missingIDs);
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
 * @param {[string]} uniqueIDs Unique IDs to check.
 */
async function loadMissingQuotes(uniqueIDs) {
  const collection = db.collection('quotes');
  const missingIDs = await getMissingIDs(collection, '_id', uniqueIDs);
  if (missingIDs.length) {
    console.log(`Found missing qoutes for IDs: ${missingIDs}`);
  } else {
    console.log(`No missing quotes. Skipping loading.`);
    return;
  }
  
  const quotes = await fetchQuotes(missingIDs);
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
 * @param {[string]} uniqueIDs Unique IDs to check.
 */
async function loadMissingSplits(uniqueIDs) {
  const collection = db.collection('splits');
  const missingIDs = await getMissingIDs(collection, '_i', uniqueIDs);
  if (missingIDs.length) {
    console.log(`Found missing splits for IDs: ${missingIDs}`);
  } else {
    console.log(`No missing splits. Skipping loading.`);
    return;
  }
  
  const splits = await fetchSplits(missingIDs);
  if (!splits.length) {
    console.log(`No splits. Skipping insert.`);
    return;
  }

  const bulk = collection.initializeUnorderedBulkOp();
  for (const split of splits) {
    const query = { _i: split._i, e: split.e };
    const update = { $setOnInsert: split };
    bulk.find(query).upsert().updateOne(update);
  }
  
  return bulk.execute();
}

//////////////////////////////////////////////////////////////////// Helpers

/**
 * Returns only missing IDs from `uniqueIDs`.
 */
async function getMissingIDs(collection, fieldName, uniqueIDs) {
  const existingIDs = await collection.distinct(fieldName, { [fieldName]: { $in: uniqueIDs } });
  const missingIDs = uniqueIDs.filter(x => !existingIDs.includes(x));
  
  return missingIDs;
}
