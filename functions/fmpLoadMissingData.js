
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
  
  await Promise.safeAllAndUnwrap([
    loadMissingCompanies(shortSymbols, symbolIDs).mapErrorToSystem(),
    loadMissingDividends(shortSymbols, symbolIDs).mapErrorToSystem(),
    loadMissingHistoricalPrices(shortSymbols, symbolIDs).mapErrorToSystem(),
    loadMissingQuotes(shortSymbols, symbolIDs).mapErrorToSystem(),
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

  console.log(`Performing ${companies.length} update operations for companies.`);
  await collection.bulkWrite(operations);
  console.log(`Performed ${companies.length} update operations for companies.`);
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

  const futureDividends = await fetchDividendsCalendar(missingShortSymbols);
  const pastDividends = await fetchDividends(missingShortSymbols);
  const dividends = futureDividends.concat(pastDividends);
  if (!dividends.length) {
    console.log(`No dividends. Skipping insert.`);
    return;
  }

  const bulk = collection.initializeUnorderedBulkOp();
  for (const dividend of dividends) {
    const query = {}
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

  // ~2s for Sergey portfolio on tests environment, 1680 entities
  console.log(`Performing ${dividends.length} update operations for dividends.`);
  await bulk.execute();
  console.log(`Performed ${dividends.length} update operations for dividends.`);
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

  // TODO: Next target for optimization
  // ~9s for Sergey portfolio on tests environment, 4622 entities
  console.log(`Performing ${historicalPrices.length} update operations for historical prices.`);
  await bulk.execute();
  console.log(`Performed ${historicalPrices.length} update operations for historical prices.`);
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

  console.log(`Performing ${quotes.length} update operations for quotes.`);
  await bulk.execute();
  console.log(`Performed ${quotes.length} update operations for quotes.`);
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

  console.log(`Performing ${splits.length} update operations for splits.`);
  await bulk.execute();
  console.log(`Performed ${splits.length} update operations for splits.`);
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