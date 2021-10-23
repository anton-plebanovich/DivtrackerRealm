
// addTransactions.js

exports = async function(transactions) {
  context.functions.execute("utils");
  const userID = context.user.id;
  console.log(`Adding transactions (${transactions.length}) for user '${userID}'`);
  
  // Check
  await context.functions.execute("checkUserTransactions", userID, transactions);

  // Load missing data
  await loadMissingData(transactions);

  // Insert
  const transactionsCollection = db.collection("transactions");
  return transactionsCollection.insertMany(transactions);

  // TODO: Remove triggers on insertion
};

async function loadMissingData(transactions) {
  const uniqueIDs = transactions
    .map(x => `${x.s}:${x.e}`)
    .distinct();

  await loadMissingCompanies(uniqueIDs);
  await loadMissingDividends(uniqueIDs);
  await loadMissingPreviousDayPrices(uniqueIDs);
  await loadMissingHistoricalPrices(uniqueIDs);
  await loadMissingQuotes(uniqueIDs);
  await loadMissingSplits(uniqueIDs);
}

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
  
  const companies = await fetchCompanies(uniqueIDs);
  if (!companies.length) {
    console.log(`No companies. Skipping insert.`);
    return;
  }

  return collection.insertMany(companies);
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

  return collection.insertMany(dividends);
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

  return collection.insertMany(previousDayPrices);
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

  return collection.insertMany(historicalPrices);
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

  return collection.insertMany(quotes);
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

  return collection.insertMany(splits);
}

//////////////////////////////////////////////////////////////////// Helpers

/**
 * Maps array and filters `null` elements.
 * @param {[string]} uniqueIDs Unique IDs to check.
 */
async function getMissingIDs(collection, fieldName, uniqueIDs) {
  const existingIDs = await collection.distinct(fieldName, { [fieldName]: { $in: uniqueIDs } });
  const missingIDs = uniqueIDs.filter(x => !existingIDs.includes(x));
  
  return missingIDs;
}
