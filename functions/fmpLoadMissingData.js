
// fmpLoadMissingData.js

// https://docs.mongofmp.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongofmp.com/realm/mongodb/actions/collection.bulkWrite/

/**
 * @example
 * exports();
 */
exports = async function loadMissingData() {
  context.functions.execute("fmpUtils");
  
  const shortSymbols = await getShortSymbols();
  const tickers = shortSymbols.map(x => x.t);
  console.log(`Loading missing data for tickers (${tickers.length}): ${tickers}`);

  const symbolIDs = shortSymbols.map(x => x._id);

  // Fetch huge requests separately
  await loadMissingCompanies(shortSymbols, symbolIDs).mapErrorToSystem();
  await loadMissingQuotes(shortSymbols, symbolIDs).mapErrorToSystem();

  await Promise.safeAllAndUnwrap([
    loadMissingDividends(shortSymbols, symbolIDs).mapErrorToSystem(),
    loadMissingHistoricalPrices(shortSymbols, symbolIDs).mapErrorToSystem(),
    loadMissingSplits(shortSymbols, symbolIDs).mapErrorToSystem()
  ]);
};

//////////////////////////////////////////////////////////////////// Companies

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingCompanies(shortSymbols, symbolIDs) {
  const collection = fmp.collection('companies');
  const missingShortSymbols = await getMissingShortSymbols(collection, '_id', shortSymbols, symbolIDs);
  if (missingShortSymbols.length) {
    console.log(`Found missing companies for tickers: ${missingShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No missing companies. Skipping loading.`);
    return;
  }
  
  await fetchCompanies(missingShortSymbols, (companies) => {
    if (!companies.length) {
      console.log(`No companies. Skipping insert.`);
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
  });
}

//////////////////////////////////////////////////////////////////// Dividends

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingDividends(shortSymbols, symbolIDs) {
  const collection = fmp.collection('dividends');
  const missingShortSymbols = await getMissingShortSymbols(collection, 's', shortSymbols, symbolIDs);
  if (missingShortSymbols.length) {
    console.log(`Found missing dividends for tickers: ${missingShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No missing dividends. Skipping loading.`);
    return;
  }

  const callback = async (dividends) => {
    if (!dividends.length) {
      console.log(`No dividends. Skipping insert.`);
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
async function loadMissingHistoricalPrices(shortSymbols, symbolIDs) {
  const collection = fmp.collection('historical-prices');
  const missingShortSymbols = await getMissingShortSymbols(collection, 's', shortSymbols, symbolIDs);
  if (missingShortSymbols.length) {
    console.log(`Found missing historical prices for tickers: ${missingShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No missing historical prices. Skipping loading.`);
    return;
  }
  
  await fetchHistoricalPrices(missingShortSymbols, null, async (historicalPrices) => {
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
  
    await bulk.execute();
  });
}

//////////////////////////////////////////////////////////////////// Quote

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingQuotes(shortSymbols, symbolIDs) {
  const collection = fmp.collection('quotes');
  const missingShortSymbols = await getMissingShortSymbols(collection, '_id', shortSymbols, symbolIDs);
  if (missingShortSymbols.length) {
    console.log(`Found missing qoutes for tickers: ${missingShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No missing quotes. Skipping loading.`);
    return;
  }
  
  await fetchQuotes(missingShortSymbols, (quotes) => {
    if (!quotes.length) {
      console.log(`No quotes. Skipping insert.`);
      return;
    }
    
    const bulk = collection.initializeUnorderedBulkOp();
    for (const quote of quotes) {
      const query = { _id: quote._id };
      const update = { $setOnInsert: quote, $currentDate: { "u": true } };
      bulk.find(query).upsert().updateOne(update);
    }
    
    await bulk.execute();
  })
}

//////////////////////////////////////////////////////////////////// Splits

/**
 * @param {[ShortSymbol]} shortSymbols
 */
async function loadMissingSplits(shortSymbols, symbolIDs) {
  const collection = fmp.collection('splits');
  const missingShortSymbols = await getMissingShortSymbols(collection, 's', shortSymbols, symbolIDs);
  if (missingShortSymbols.length) {
    console.log(`Found missing splits for tickers: ${missingShortSymbols.map(x => x.t)}`);
  } else {
    console.log(`No missing splits. Skipping loading.`);
    return;
  }
  
  await fetchSplits(missingShortSymbols, async (splits) => {
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
  
    await bulk.execute();
  });
}

//////////////////////////////////////////////////////////////////// Helpers

/**
 * Returns only missing IDs from `shortSymbols`.
 */
async function getMissingShortSymbols(collection, field, shortSymbols, symbolIDs) {
  const existingIDs = await collection
    .distinct(field, { [field]: { $in: symbolIDs } });

  const idByID = existingIDs.toDictionary();
  const missingShortSymbols = shortSymbols.filter(
    x => idByID[x._id] == null
  );
  
  return missingShortSymbols;
}
