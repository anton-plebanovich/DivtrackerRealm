
// fmpUpdateData.js

// https://docs.mongofmp.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongofmp.com/realm/mongodb/actions/collection.bulkWrite/

const statusCollectionName = 'data-status';
const updatesCollectionName = 'updates';

/**
 * @example
 * exports();
 */
exports = async function(_databaseName) {
  context.functions.execute("fmpUtils", _databaseName);

  try {
    await run();
  } catch(error) {
    if (error !== executionTimeoutError) {
      throw error;
    }
  }
};

async function run() {
  await updateSymbolsDaily();

  console.log(`Checking missing data`);
  await context.functions.execute("fmpLoadMissingData", databaseName);

  const shortSymbols = await getShortSymbols();
  const tickers = shortSymbols.map(x => x.t);
  console.log(`Updating data for tickers (${tickers.length}): ${tickers}`);

  // await loadMissingSplits(shortSymbols).mapErrorToSystem();
  if (checkExecutionTimeout()) { return; }

  // await loadMissingDividends(shortSymbols).mapErrorToSystem();
  if (checkExecutionTimeout()) { return; }

  await updateCompaniesDaily(shortSymbols).mapErrorToSystem();
  if (checkExecutionTimeout()) { return; }

  // await loadMissingHistoricalPrices(shortSymbols).mapErrorToSystem();
  if (checkExecutionTimeout()) { return; }
}

//////////////////////////////////////////////////////////////////// Symbols

async function updateSymbolsDaily() {
  const collectionName = 'symbols';
  const minDate = Date.today();
  const isUpToDate = await checkIfUpToDate(collectionName, minDate);
  if (isUpToDate) { return; }

  await context.functions.execute("fmpUpdateSymbols", databaseName);
}

//////////////////////////////////////////////////////////////////// Companies

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function updateCompaniesDaily(shortSymbols) {
  const collectionName = 'companies';
  const minDate = Date.today();
  const isUpToDate = await checkIfUpToDate(collectionName, minDate);
  if (isUpToDate) { return; }

  const outdatedShortSymbols = await getOutdatedShortSymbols(collectionName, shortSymbols, minDate);
  if (outdatedShortSymbols.length) {
    console.log(`Found outdated companies for tickers: ${outdatedShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No outdated companies. Skipping update.`);
    return;
  }
  
  const collection = fmp.collection(collectionName);
  await fetchCompanies(outdatedShortSymbols, async (companies, symbolIDs) => {
    checkExecutionTimeoutAndThrow(-1);
    
    if (!companies.length) {
      console.log(`No companies. Skipping update.`);
      await updateStatus(collectionName, symbolIDs);
      return;
    }

    await collection.safeUpdateMany(companies, undefined, '_id');
    await updateStatus(collectionName, symbolIDs);
  });

  await setUpdateDate(`${databaseName}-${collectionName}`);
}

//////////////////////////////////////////////////////////////////// Dividends

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingDividends(shortSymbols) {
  const collectionName = 'dividends';
  const minDate = Date.today();
  const isUpToDate = await checkIfUpToDate(collectionName, minDate);
  if (isUpToDate) { return; }

  const outdatedShortSymbols = await getOutdatedShortSymbols(collectionName, shortSymbols, minDate);
  if (outdatedShortSymbols.length) {
    console.log(`Found outdated dividends for tickers: ${outdatedShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No outdated dividends. Skipping update.`);
    return;
  }

  const collection = fmp.collection(collectionName);
  const callbackBase = async (historical, dividends, symbolIDs) => {
    if (!dividends.length) {
      if (historical) {
        console.log(`No historical dividends. Skipping update.`);
        await updateStatus(collectionName, symbolIDs);
      } else {
        // We should not update status for calendar dividends and instead rely on historical dividends.
        console.log(`No calendar dividends. Skipping update.`);
      }
      return;
    }
  
    const bulk = collection.initializeUnorderedBulkOp();
    for (const dividend of dividends) {
      const query = {};
      if (dividend.e != null) {
        query.e = dividend.e;
      } else {
        console.error(`Invalid ex date: ${dividend.stringify()}`);
      }
  
      if (dividend.a != null) {
        query.a = dividend.a;
      } else {
        console.error(`Invalid amount: ${dividend.stringify()}`);
      }

      // We do not check frequency because it might be changed when we delete dividends and recompute it for all of them
  
      if (dividend.s != null) {
        query.s = dividend.s;
      } else {
        console.error(`Invalid symbol: ${dividend.stringify()}`);
      }
  
      const update = { $set: dividend };
      bulk.find(query).upsert().updateOne(update);
    }
  
    await bulk.execute();
    if (historical) {
      await updateStatus(collectionName, symbolIDs);
    }
  };

  const calendarCallback = async (dividends, symbolIDs) => {
    callbackBase(false, dividends, symbolIDs);
  };

  const historicalCallback = async (dividends, symbolIDs) => {
    callbackBase(true, dividends, symbolIDs);
  };

  await Promise.all([
    fetchDividendsCalendar(outdatedShortSymbols, calendarCallback),
    fetchDividends(outdatedShortSymbols, null, historicalCallback)
  ]);

  await setUpdateDate(`${databaseName}-${collectionName}`);
}

//////////////////////////////////////////////////////////////////// Historical Prices

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingHistoricalPrices(shortSymbols) {
  const collectionName = 'historical-prices';
  const minDate = Date.today();
  const isUpToDate = await checkIfUpToDate(collectionName, minDate);
  if (isUpToDate) { return; }

  const outdatedShortSymbols = await getOutdatedShortSymbols(collectionName, shortSymbols, minDate);
  if (outdatedShortSymbols.length) {
    console.log(`Found outdated historical prices for tickers: ${outdatedShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No outdated historical prices. Skipping update.`);
    return;
  }
  
  const collection = fmp.collection(collectionName);
  await fetchHistoricalPrices(outdatedShortSymbols, null, async (historicalPrices, symbolIDs) => {
    if (!historicalPrices.length) {
      console.log(`No historical prices. Skipping update.`);
      await updateStatus(collectionName, symbolIDs);
      return;
    }
  
    const bulk = collection.initializeUnorderedBulkOp();
    for (const historicalPrice of historicalPrices) {
      const query = { d: historicalPrice.d, s: historicalPrice.s };
      const update = { $set: historicalPrice };
      bulk.find(query).upsert().updateOne(update);
    }
  
    await bulk.execute();
    await updateStatus(collectionName, symbolIDs);
  });

  await setUpdateDate(`${databaseName}-${collectionName}`);
}

//////////////////////////////////////////////////////////////////// Splits

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingSplits(shortSymbols) {
  const collectionName = 'splits';
  const minDate = Date.today();
  const isUpToDate = await checkIfUpToDate(collectionName, minDate);
  if (isUpToDate) { return; }
  
  const outdatedShortSymbols = await getOutdatedShortSymbols(collectionName, shortSymbols, minDate);
  if (outdatedShortSymbols.length) {
    console.log(`Found outdated splits for tickers: ${outdatedShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No outdated splits. Skipping update.`);
    return;
  }
  
  const collection = fmp.collection(collectionName);
  await fetchSplits(outdatedShortSymbols, async (splits, symbolIDs) => {
    if (!splits.length) {
      console.log(`No splits. Skipping update.`);
      await updateStatus(collectionName, symbolIDs);
      return;
    }
  
    const bulk = collection.initializeUnorderedBulkOp();
    for (const split of splits) {
      const query = { e: split.e, s: split.s };
      const update = { $set: split };
      bulk.find(query).upsert().updateOne(update);
    }
  
    await bulk.execute();
    await updateStatus(collectionName, symbolIDs);
  });

  await setUpdateDate(`${databaseName}-${collectionName}`);
}

//////////////////////////////////////////////////////////////////// Helpers

/**
 * Checks if all data in a collection is up to date.
 */
async function checkIfUpToDate(collectionName, minDate) {
  const objectID = `${databaseName}-${collectionName}`;
  const updatesCollection = db.collection(updatesCollectionName);
  const update = await updatesCollection.findOne({ _id: objectID });

  if (update == null || update.d < minDate) {
    console.log(`Collection '${collectionName}' is outdated`);
    return false;

  } else {
    console.log(`Collection '${collectionName}' is up to date`);
    return true;
  }
}

/**
 * Returns only outdated IDs from `shortSymbols`.
 */
async function getOutdatedShortSymbols(collectionName, shortSymbols, minDate) {
  const statusCollection = fmp.collection(statusCollectionName)
  const outdatedIDs = await statusCollection
  // We do not check for `null` here because date should be always set by `fmpLoadMissingData` first.
  // If the date is missing it means the data is not loaded and we can't update yet or we'll prevent missing data from load.
    .find({ [collectionName]: { $lt: minDate } }, { _id: 1 })
    .toArray()

  const idByID = outdatedIDs.toDictionary('_id');
  const outdatedShortSymbols = shortSymbols.filter(
    x => idByID[x._id] == null
  );
  
  return outdatedShortSymbols;
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
