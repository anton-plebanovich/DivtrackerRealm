
// fmpLoadMissingData.js

// https://docs.mongodb.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/

/**
 * @example
 * exports();
 */
exports = async function(database) {
  context.functions.execute("fmpUtils", database);
  
  try {
    await run();
  } catch(error) {
    if (error.message !== executionTimeoutErrorMessage) {
      throw error;
    } else {
      return error;
    }
  }
};

async function run() {
  const shortSymbols = await getShortSymbols();
  const tickers = shortSymbols.map(x => x.t);
  console.log(`Loading missing data for tickers (${tickers.length}): ${tickers}`);

  await loadMissingQuotes(shortSymbols).mapErrorToSystem();
  await loadMissingCompanies(shortSymbols).mapErrorToSystem();
  await loadMissingSplits(shortSymbols).mapErrorToSystem();
  await loadMissingDividends(shortSymbols).mapErrorToSystem();
  await loadMissingHistoricalPrices(shortSymbols).mapErrorToSystem();
}

//////////////////////////////////////////////////////////////////// Companies

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingCompanies(shortSymbols) {
  const collectionName = 'companies';
  const collection = fmp.collection(collectionName);
  const missingShortSymbols = await getMissingShortSymbols(collectionName, shortSymbols);
  if (missingShortSymbols.length) {
    console.log(`Found missing companies for tickers: ${missingShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No missing companies. Skipping loading.`);
    return;
  }
  
  await fetchCompanies(missingShortSymbols, async (companies, symbolIDs) => {
    await collection.safeInsertMissing(companies, '_id');
    await updateStatus(collectionName, symbolIDs);
    checkExecutionTimeoutAndThrow();
  });
}

//////////////////////////////////////////////////////////////////// Dividends

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingDividends(shortSymbols) {
  const collectionName = 'dividends';
  const collection = fmp.collection(collectionName);
  const missingShortSymbols = await getMissingShortSymbols(collectionName, shortSymbols);
  if (missingShortSymbols.length) {
    console.log(`Found missing dividends for tickers: ${missingShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No missing dividends. Skipping loading.`);
    return;
  }

  const callbackBase = async (historical, dividends, symbolIDs) => {
    await collection.safeInsertMissing(dividends, ['s', 'e', 'a']);
    if (historical) {
      await updateStatus(collectionName, symbolIDs);
    }
    checkExecutionTimeoutAndThrow();
  };

  const calendarCallback = async (dividends, symbolIDs) => {
    callbackBase(false, dividends, symbolIDs);
  };

  const historicalCallback = async (dividends, symbolIDs) => {
    callbackBase(true, dividends, symbolIDs);
  };

  await Promise.all([
    fetchDividendsCalendar(missingShortSymbols, calendarCallback),
    fetchDividends(missingShortSymbols, null, historicalCallback)
  ]);
}

//////////////////////////////////////////////////////////////////// Historical Prices

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingHistoricalPrices(shortSymbols) {
  const collectionName = 'historical-prices';
  const collection = fmp.collection(collectionName);
  const missingShortSymbols = await getMissingShortSymbols(collectionName, shortSymbols);
  if (missingShortSymbols.length) {
    console.log(`Found missing historical prices for tickers: ${missingShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No missing historical prices. Skipping loading.`);
    return;
  }
  
  await fetchHistoricalPrices(missingShortSymbols, null, async (historicalPrices, symbolIDs) => {
    await collection.safeInsertMissing(historicalPrices, ['s', 'd']);
    await updateStatus(collectionName, symbolIDs);
    checkExecutionTimeoutAndThrow();
  });
}

//////////////////////////////////////////////////////////////////// Quote

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingQuotes(shortSymbols) {
  const collectionName = 'quotes';
  const collection = fmp.collection(collectionName);
  const missingShortSymbols = await getMissingShortSymbols(collectionName, shortSymbols);
  if (missingShortSymbols.length) {
    console.log(`Found missing qoutes for tickers: ${missingShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No missing quotes. Skipping loading.`);
    return;
  }
  
  await fetchQuotes(missingShortSymbols, async (quotes, symbolIDs) => {
    await collection.safeInsertMissing(quotes, '_id');
    await updateStatus(collectionName, symbolIDs);
    checkExecutionTimeoutAndThrow();
  });
}

//////////////////////////////////////////////////////////////////// Splits

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingSplits(shortSymbols) {
  const collectionName = 'splits';
  const collection = fmp.collection(collectionName);
  const missingShortSymbols = await getMissingShortSymbols(collectionName, shortSymbols);
  if (missingShortSymbols.length) {
    console.log(`Found missing splits for tickers: ${missingShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No missing splits. Skipping loading.`);
    return;
  }
  
  await fetchSplits(missingShortSymbols, async (splits, symbolIDs) => {
    await collection.safeInsertMissing(splits, ['s', 'e']);
    await updateStatus(collectionName, symbolIDs);
    checkExecutionTimeoutAndThrow();
  });
}

//////////////////////////////////////////////////////////////////// Helpers

const statusCollectionName = 'data-status';

/**
 * Returns only missing IDs from `shortSymbols`.
 */
async function getMissingShortSymbols(collectionName, shortSymbols) {
  const statusCollection = fmp.collection(statusCollectionName)
  const existingIDs = await statusCollection
    .find({ [collectionName]: { $ne: null } }, { _id: 1 })
    .toArray()

  const idByID = existingIDs.toDictionary('_id');
  const missingShortSymbols = shortSymbols.filter(
    x => idByID[x._id] == null
  );
  
  return missingShortSymbols;
}

async function updateStatus(collectionName, symbolIDs) {
  const statusCollection = fmp.collection(statusCollectionName)
  
  const bulk = statusCollection.initializeUnorderedBulkOp();
  for (const symbolID of symbolIDs) {
    bulk
      .find({ _id: symbolID })
      .upsert()
      .updateOne({ $currentDate: { [collectionName]: true } });
  }

  await bulk.safeExecute();
}