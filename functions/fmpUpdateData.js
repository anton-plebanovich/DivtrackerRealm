
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
    if (error.message !== executionTimeoutErrorMessage) {
      throw error;
    } else {
      return error;
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

  await updateCompaniesDaily(shortSymbols)
    .mapErrorToSystem();

  // await updateSplitsDaily(shortSymbols)
  //   .mapErrorToSystem();

  // await updateDividends(shortSymbols)
  //   .mapErrorToSystem();

  // await updateHistoricalPricesDaily(shortSymbols)
  //   .mapErrorToSystem();
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

  await fetchCompanies(outdatedShortSymbols, async (companies, symbolIDs) => {
    await collection.safeUpsertMany(companies, '_id');
    await updateStatus(collectionName, symbolIDs);
    checkExecutionTimeoutAndThrow();
  });

  await setUpdateDate(`${databaseName}-${collectionName}`);
}

//////////////////////////////////////////////////////////////////// Dividends

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function updateDividends(shortSymbols) {
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

  let existingDividends;
  const collection = fmp.collection(collectionName);
  if (outdatedShortSymbols.length < ENV.maxFindInSize) {
    const symbolIDs = outdatedShortSymbols.map(x => x._id);
    existingDividends = await collection.find({ s: { $in: symbolIDs } });
  } else {
    existingDividends = await collection.fullFind();
  }

  const existingDividendsBySymbolID = existingDividends.toBuckets('s');
  const fields = ['s', 'e', 'a'];
  const existingDividendByFields = existingDividends
    .sortedDeletedToTheStart()
    .toDictionary(fields);

  const callbackBase = async (historical, dividends, symbolIDs) => {
    dividends = fixDividends(dividends, existingDividendsBySymbolID);
    await collection.safeUpdateMany(dividends, existingDividendByFields, fields);

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

function fixDividends(dividends, existingDividendsBySymbolID) {
  if (!dividends.length) { return []; }

  const dividendsBySymbolID = dividends.toBuckets(x => x.s);
  const fixedDividends = [];
  for (const [symbolID, dividends] of Object.entries(dividendsBySymbolID)) {
    const existingDividends = existingDividendsBySymbolID[symbolID];

    // Check if there is nothing to fix
    if (existingDividends == null) {
      // There were no dividends but we have them now. 
      // It's hard to say if that's the first record or the whole set was added so asking to fix manually.
      // dt data-status -e <ENV> -d fmp -c dividends --id <ID1> && dt call-realm-function -e <ENV> -f fmpLoadMissingData --verbose
      console.error(`Missing existing dividends for: ${symbolID}. It's better to load missing dividends data for this.`);
      continue;
    }

    // We remove new dividends from existing to allow update in case something was changed.
    // We remove deleted dividends from existing to do not count them during frequency computations.
    const deduplicatedExistingDividends = [];
    for (const existingDividend of existingDividends) {
      const matchedDividendIndex = dividends.findIndex(dividend => 
        existingDividend.a == dividend.a && compareOptionalDates(existingDividend.e, dividend.e)
      );
      
      if (matchedDividendIndex === -1) {
        // No match, add existing if not deleted
        if (existingDividend.x != true) {
          deduplicatedExistingDividends.push(existingDividend);
        }

      } else if (existingDividend.x == true) {
        // Deleted dividend match, exclude from new
        dividends.splice(matchedDividendIndex, 1);

      } else {
        // Match, will be added later
      }
    }

    // Frequency fix using all known dividends
    let _fixedDividends = deduplicatedExistingDividends
      .concat(dividends)
      .sorted((l, r) => l.e - r.e);
    
    _fixedDividends = removeDuplicatedDividends(_fixedDividends);
    _fixedDividends = updateDividendsFrequency(_fixedDividends);

    _fixedDividends = _fixedDividends
      .filter(fixedDividend => 
        existingDividends.find(dividend => 
          fixedDividend.a == dividend.a && 
          fixedDividend.f == dividend.f && 
          compareOptionalDates(fixedDividend.d, dividend.d) && 
          compareOptionalDates(fixedDividend.e, dividend.e) && 
          compareOptionalDates(fixedDividend.p, dividend.p)
        ) == null
      );

    // Push result to others
    fixedDividends.push(..._fixedDividends);
  }

  return fixedDividends;
}

//////////////////////////////////////////////////////////////////// Historical Prices

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function updateHistoricalPricesDaily(shortSymbols) {
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

  const previousMonthStart = Date.previousMonthStart().dayString();
  const previousMonthEnd = Date.previousMonthEnd().dayString();
  const query = { from: previousMonthStart, to: previousMonthEnd };
  const collection = fmp.collection(collectionName);
  await fetchHistoricalPrices(outdatedShortSymbols, query, async (historicalPrices, symbolIDs) => {
    await collection.safeUpsertMany(historicalPrices, ['s', 'd']);
    await updateStatus(collectionName, symbolIDs);
    checkExecutionTimeoutAndThrow();
  });

  await setUpdateDate(`${databaseName}-${collectionName}`);
}

//////////////////////////////////////////////////////////////////// Splits

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function updateSplitsDaily(shortSymbols) {
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
    await collection.safeUpdateMany(splits, null, ['s', 'e']);
    await updateStatus(collectionName, symbolIDs);
    checkExecutionTimeoutAndThrow();
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
  const upToDateIDs = await statusCollection
  // We do not check for `null` here because date should be always set by `fmpLoadMissingData` first.
  // If the date is missing it means the data is not loaded and we can't update yet or we'll prevent missing data from load.
    .find({ [collectionName]: { $gte: minDate } }, { _id: 1 })
    .toArray()

  const upToDateIDByID = upToDateIDs.toDictionary('_id');
  const outdatedShortSymbols = shortSymbols.filter(
    x => upToDateIDByID[x._id] == null
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
