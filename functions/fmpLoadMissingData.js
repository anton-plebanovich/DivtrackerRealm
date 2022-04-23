
// fmpLoadMissingData.js

// https://docs.mongofmp.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongofmp.com/realm/mongodb/actions/collection.bulkWrite/

/**
 * @example
 * exports();
 */
exports = async function(database) {
  context.functions.execute("fmpUtils");

  if (database != null) {
    throwIfNotString(database);
    fmp = atlas.db(database);
  }
  
  const shortSymbols = await getShortSymbols();
  const tickers = shortSymbols.map(x => x.t);
  console.log(`Loading missing data for tickers (${tickers.length}): ${tickers}`);

  // Fetch huge requests first
  await loadMissingCompanies(shortSymbols).mapErrorToSystem();
  await loadMissingQuotes(shortSymbols).mapErrorToSystem();

  // Fetch huge but rarely missing data
  await loadMissingHistoricalPrices(shortSymbols).mapErrorToSystem();

  // Dividends are averagely missing
  await loadMissingDividends(shortSymbols).mapErrorToSystem();

  // Splits are frequently missing
  await loadMissingSplits(shortSymbols).mapErrorToSystem();
};

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
      const update = { $setOnInsert: company, $currentDate: { "u": true } };
      const updateOne = { filter: filter, update: update, upsert: true };
      const operation = { updateOne: updateOne };
      operations.push(operation);
    }
  
    await collection.bulkWrite(operations);
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

  const callback = async (dividends, symbolIDs) => {
    if (!dividends.length) {
      console.log(`No dividends. Skipping insert.`);
      await updateStatus(collectionName, symbolIDs);
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
  
      if (dividend.f != null) {
        query.f = dividend.f;
      } else {
        console.error(`Invalid frequency: ${dividend.stringify()}`);
      }
  
      if (dividend.s != null) {
        query.s = dividend.s;
      } else {
        console.error(`Invalid symbol: ${dividend.stringify()}`);
      }
  
      const update = { $setOnInsert: dividend };
      bulk.find(query).upsert().updateOne(update);
    }
  
    await bulk.execute();
    await updateStatus(collectionName, symbolIDs);
  };

  await Promise.all([
    fetchDividendsCalendar(missingShortSymbols, callback),
    fetchDividends(missingShortSymbols, null, callback)
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
    if (!quotes.length) {
      console.log(`No quotes. Skipping insert.`);
      await updateStatus(collectionName, symbolIDs);
      return;
    }
    
    const bulk = collection.initializeUnorderedBulkOp();
    for (const quote of quotes) {
      const query = { _id: quote._id };
      const update = { $setOnInsert: quote, $currentDate: { "u": true } };
      bulk.find(query).upsert().updateOne(update);
    }
    
    await bulk.execute();
    await updateStatus(collectionName, symbolIDs);
  })
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