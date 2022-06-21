
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
  
  await updateSymbolsDaily(databaseName);

  console.log(`Checking missing symbols`);
  await context.functions.execute("fmpLoadMissingData", databaseName);

  const shortSymbols = await getShortSymbols();
  const tickers = shortSymbols.map(x => x.t);
  console.log(`Loading missing data for tickers (${tickers.length}): ${tickers}`);

  // await loadMissingSplits(shortSymbols).mapErrorToSystem();
  if (checkExecutionTimeout(90)) { return; }

  // await loadMissingDividends(shortSymbols).mapErrorToSystem();
  if (checkExecutionTimeout(90)) { return; }

  await updateCompaniesDaily(shortSymbols).mapErrorToSystem();
  if (checkExecutionTimeout(90)) { return; }
  
  // await loadMissingHistoricalPrices(shortSymbols).mapErrorToSystem();
  if (checkExecutionTimeout(90)) { return; }
};

//////////////////////////////////////////////////////////////////// Symbols

async function updateSymbolsDaily(databaseName) {
  const isOutdated = await isOutdated(databaseName, 'symbols', minDate);
  if (isOutdated) {
    await context.functions.execute("fmpUpdateSymbols", databaseName);
  }
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
    if (!companies.length) {
      console.log(`No companies. Skipping insert.`);
      await updateStatus(collectionName, symbolIDs);
      return;
    }
  
    const operations = [];
    for (const company of companies) {
      const filter = { _id: company._id };
      const update = { $setOnInsert: company, $currentDate: { u: true } };
      const updateOne = { filter: filter, update: update, upsert: true };
      const operation = { updateOne: updateOne };
      operations.push(operation);
    }
  
    const options = { ordered: false };
    await collection.bulkWrite(operations, options);
    await updateStatus(collectionName, symbolIDs);
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
    if (!dividends.length) {
      if (historical) {
        console.log(`No historical dividends. Skipping insert.`);
        await updateStatus(collectionName, symbolIDs);
      } else {
        // We should not update status for calendar dividends and instead rely on historical dividends.
        console.log(`No calendar dividends. Skipping insert.`);
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
  
      const update = { $setOnInsert: dividend };
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
    if (!historicalPrices.length) {
      console.log(`No historical prices. Skipping insert.`);
      await updateStatus(collectionName, symbolIDs);
      return;
    }
  
    const bulk = collection.initializeUnorderedBulkOp();
    for (const historicalPrice of historicalPrices) {
      const query = { d: historicalPrice.d, s: historicalPrice.s };
      const update = { $setOnInsert: historicalPrice };
      bulk.find(query).upsert().updateOne(update);
    }
  
    await bulk.execute();
    await updateStatus(collectionName, symbolIDs);
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
    if (!splits.length) {
      console.log(`No splits. Skipping insert.`);
      await updateStatus(collectionName, symbolIDs);
      return;
    }
  
    const bulk = collection.initializeUnorderedBulkOp();
    for (const split of splits) {
      const query = { e: split.e, s: split.s };
      const update = { $setOnInsert: split };
      bulk.find(query).upsert().updateOne(update);
    }
  
    await bulk.execute();
    await updateStatus(collectionName, symbolIDs);
  });
}

//////////////////////////////////////////////////////////////////// Helpers

async function isOutdated(databaseName, collectionName, minDate) {
  const objectID = `${databaseName}-${collectionName}`;
  const updatesCollection = db.collection(updatesCollectionName);
  const update = await updatesCollection.findOne({ _id: objectID });
  const date = update.d;

  if (date < minDate) {
    console.log(`Collection '${collectionName}' is outdated`);
    return true;

  } else {
    console.log(`Collection '${collectionName}' is up to date`);
    return false;
  }
}

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
