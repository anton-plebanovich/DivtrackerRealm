
// fmpUpdateData.js

// https://docs.mongofmp.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongofmp.com/realm/mongodb/actions/collection.bulkWrite/

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

  await updateSplitsDaily(shortSymbols)
    .mapErrorToSystem();

  await updateDividends(shortSymbols)
    .mapErrorToSystem();

  await updateHistoricalPricesDaily(shortSymbols)
    .mapErrorToSystem();
}

//////////////////////////////////////////////////////////////////// Symbols

async function updateSymbolsDaily() {
  const collectionName = 'symbols';
  const minDate = Date.today();
  const isUpToDate = await checkIfUpToDate(databaseName, collectionName, minDate);
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
  const isUpToDate = await checkIfUpToDate(databaseName, collectionName, minDate);
  if (isUpToDate) { return; }

  const outdatedShortSymbols = await getOutdatedShortSymbols(collectionName, shortSymbols, minDate);
  if (outdatedShortSymbols.length) {
    console.log(`Found outdated companies for tickers: ${outdatedShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No outdated companies. Skipping update.`);
    return;
  }

  const collection = fmp.collection(collectionName)
  const existingCompanies = await collection.fullFind();
  const fields = ['_id'];
  const existingCompaniesByFields = existingCompanies.toDictionary(fields);

  await fetchCompanies(outdatedShortSymbols, async (companies, symbolIDs) => {
    await collection.safeUpdateMany(companies, existingCompaniesByFields, fields);
    await updateStatus(collectionName, symbolIDs);
    checkExecutionTimeoutAndThrow();
  });

  await setUpdateDate(fmp, `${databaseName}-${collectionName}`);
}

//////////////////////////////////////////////////////////////////// Dividends

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function updateDividends(shortSymbols) {
  const collectionName = 'dividends';
  const minDate = Date.today();
  const isUpToDate = await checkIfUpToDate(databaseName, collectionName, minDate);
  if (isUpToDate) { return; }

  const outdatedShortSymbols = await getOutdatedShortSymbols(collectionName, shortSymbols, minDate);
  if (outdatedShortSymbols.length) {
    console.log(`Found outdated dividends for tickers: ${outdatedShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No outdated dividends. Skipping update.`);
    return;
  }

  const collection = fmp.collection(collectionName)
  const existingDividends = await collection.fullFind();
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
    checkExecutionTimeoutAndThrow();
  };

  const calendarCallback = async (dividends, symbolIDs) => {
    await callbackBase(false, dividends, symbolIDs);
  };

  const historicalCallback = async (dividends, symbolIDs) => {
    await callbackBase(true, dividends, symbolIDs);
  };

  await Promise.all([
    fetchDividendsCalendar(outdatedShortSymbols, calendarCallback),
    fetchDividends(outdatedShortSymbols, null, historicalCallback)
  ]);

  await setUpdateDate(fmp, `${databaseName}-${collectionName}`);
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
  const isUpToDate = await checkIfUpToDate(databaseName, collectionName, minDate);
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
  const existingHistoricalPrices = await collection.fullFind({ d: { $gte: previousMonthStart, $lte: previousMonthEnd } });

  const fields = ['s', 'd'];
  const existingHistoricalPricesByFields = existingHistoricalPrices
    .sortedDeletedToTheStart()
    .toDictionary(fields);

  await fetchHistoricalPrices(outdatedShortSymbols, query, async (historicalPrices, symbolIDs) => {
    await collection.safeUpdateMany(historicalPrices, existingHistoricalPricesByFields, fields);
    await updateStatus(collectionName, symbolIDs);
    checkExecutionTimeoutAndThrow();
  });

  await setUpdateDate(fmp, `${databaseName}-${collectionName}`);
}

//////////////////////////////////////////////////////////////////// Splits

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function updateSplitsDaily(shortSymbols) {
  const collectionName = 'splits';
  const minDate = Date.today();
  const isUpToDate = await checkIfUpToDate(databaseName, collectionName, minDate);
  if (isUpToDate) { return; }
  
  const outdatedShortSymbols = await getOutdatedShortSymbols(collectionName, shortSymbols, minDate);
  if (outdatedShortSymbols.length) {
    console.log(`Found outdated splits for tickers: ${outdatedShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No outdated splits. Skipping update.`);
    return;
  }
  
  const collection = fmp.collection(collectionName)
  const existingSplits = await collection.fullFind();
  const fields = ['s', 'e'];
  const existingSplitsByFields = existingSplits
    .sortedDeletedToTheStart()
    .toDictionary(fields);

  await fetchSplits(outdatedShortSymbols, async (splits, symbolIDs) => {
    await collection.safeUpdateMany(splits, existingSplitsByFields, fields);
    await updateStatus(collectionName, symbolIDs);
    checkExecutionTimeoutAndThrow();
  });

  await setUpdateDate(fmp, `${databaseName}-${collectionName}`);
}

//////////////////////////////////////////////////////////////////// Helpers

/**
 * Returns only outdated IDs from `shortSymbols`.
 */
async function getOutdatedShortSymbols(collectionName, shortSymbols, minDate) {
  const statusCollection = fmp.collection('data-status')
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
