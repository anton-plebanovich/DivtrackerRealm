
// migrationsOld.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/
// https://docs.mongodb.com/manual/reference/operator/update/unset/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.removeOne/
// https://docs.mongodb.com/manual/reference/method/Bulk.insert/

exports = function() {
  // 2022-06-26
  fetch_refid_for_all_past_IEX_dividends;

  // 2022-06-25
  refetch_IEX_splits;
  fix_FMP_dividends_v2;

  // 2022-06-18
  null_fields_cleanup_migration;
  old_data_delete_migration;
  old_date_format_splits_migration;
  old_date_format_dividends_migration;
  fetch_refid_for_IEX_splits;
  fetch_refid_for_future_IEX_dividends;
  fetch_refid_for_past_IEX_dividends;
  delete_duplicated_IEX_dividends;
  fix_FMP_dividends;

  // 2022-06-11
  checkSameTickers;

  // 2022-05-15
  adjustSymbolIDs;

  // 2022-01-23
  positiveCommissionsMigration;

  // 2022-01-03
  partitionKeyMigration;

  // 2021-12-25
  v2DatabaseFillMigration;

  // 2021-12-03
  exchangesFix_03122021;

  // 2021-11-24
  removeExcessiveFieldsInSymbols_24112021;
  truncateDividendsFrequency_24112021;

  // 2021-04-15
  executeTransactionsMigration_15042021;
};

////////////////////////////////////////////////////// 2022-06-26

async function fetch_refid_for_all_past_IEX_dividends() {
  context.functions.execute("iexUtils");

  const collection = db.collection("dividends");
  const symbolIDs = await collection.distinct('s', { i: null });
  const shortSymbols = await getShortSymbols(symbolIDs);
  console.log(`Refetching dividends for '${symbolIDs.length}' symbols: ${symbolIDs.stringify()}`)

  const minFetchDate = '2016-01-01';
  const defaultRange = `${(new Date().getUTCFullYear() - new Date(minFetchDate).getUTCFullYear()) * 12 + new Date().getMonth() + 1}m`;
  const pastDividends = await fetch_IEX_dividends_with_duplicates(shortSymbols, false, defaultRange, null);

  const now = new Date();
  const exDateFind = { $lt: now };
  const find = { i: null, e: exDateFind };
  await find_and_update_IEX_dividends(pastDividends, find);
}

////////////////////////////////////////////////////// 2022-06-25

async function refetch_IEX_splits() {
  context.functions.execute("iexUtils");
  const shortSymbols = await getInUseShortSymbols();
  if (shortSymbols.length <= 0) {
    console.log(`No symbols. Skipping update.`);
    return;
  }

  const collection = db.collection("splits");
  const splits = await fetchSplits(shortSymbols, null, false);
  if (splits.length) {
    console.log(`Inserting missed`);
    await collection.safeInsertMissing(splits, 'i');
    console.log(`SUCCESS`);
  } else {
    console.log(`Historical splits are empty for symbols: '${shortSymbols.map(x => x.t)}'`);
  }

  await setUpdateDate(db, "splits");
}

/**
 * Deletes duplicated FMP dividends and fixes frequency where needed
 */
 async function fix_FMP_dividends_v2(iteration) {
  context.functions.execute("fmpUtils");
  throwIfNotNumber(iteration, `Iteration should be a number parameter with proper iteration value`);

  const collection = fmp.collection('dividends');

  const symbols = await collection
    .distinct('s', { x: { $ne: true } })
    .then(symbols => symbols.sorted((l, r) => l.toString().localeCompare(r.toString())));
  
  const iterationSize = 1000;
  const iterationSymbols = symbols.splice(iteration * iterationSize, iterationSize);
  if (!iterationSymbols.length) {
    console.error(`No symbols to iterate for '${iteration}' iteration. Symbols length: ${symbols.length}`);
    return;
  }

  const oldDividends = await collection.fullFind({ s: { $in: iterationSymbols }, x: { $ne: true } });
  const dividendsBySymbolID = oldDividends.toBuckets('s');
  const newDividends = [];
  for (const symbolDividends of Object.values(dividendsBySymbolID)) {
    let fixedDividends = symbolDividends.sorted((l, r) => l.e - r.e);
    fixedDividends = removeDuplicatedDividends(fixedDividends);
    fixedDividends = updateDividendsFrequency(fixedDividends);
    newDividends.push(...fixedDividends);

    const dividendsToDelete = symbolDividends.filter(dividend => !fixedDividends.includes(dividend))
    dividendsToDelete.forEach(dividend => dividend.x = true);
    newDividends.push(...dividendsToDelete);
  }

  await collection.safeUpdateMany(newDividends);
  console.log(`Deleted objects: ${newDividends.filter(x => x.x == true).length}`);
}

////////////////////////////////////////////////////// 2022-06-18 Null and undefined fields cleanup

async function null_fields_cleanup_migration() {
  context.functions.execute("iexUtils");
  context.functions.execute("fmpUtils");

  const databaseNames = [
    'divtracker-v2',
    'fmp',
  ];

  const operations = [];
  for (const databaseName of databaseNames) {
    const collection = atlas.db(databaseName).collection('companies');
    operations.push(collection.updateMany({ i: null }, { $unset: { i: "" } }));
    operations.push(collection.updateMany({ t: null }, { $unset: { t: "" } }));
  }

  for (const databaseName of databaseNames) {
    const collection = atlas.db(databaseName).collection('dividends');
    operations.push(collection.updateMany({ c: null }, { $unset: { c: "" } }));
    operations.push(collection.updateMany({ d: null }, { $unset: { d: "" } }));
    operations.push(collection.updateMany({ f: null }, { $unset: { f: "" } }));
    operations.push(collection.updateMany({ p: null }, { $unset: { p: "" } }));
  }

  for (const databaseName of databaseNames) {
    const collection = atlas.db(databaseName).collection('quotes');
    operations.push(collection.updateMany({ p: null }, { $unset: { p: "" } }));
  }

  await Promise.all(operations);
}

////////////////////////////////////////////////////// 2022-06-18 Old data deletion

/**
 * Delete dividends, historical prices and splits before 2016-01-01
 */
async function old_data_delete_migration() {
  context.functions.execute("iexUtils");

  const databaseNames = [
    'divtracker-v2',
    'fmp',
  ];

  const collectionNames = [
    'dividends',
    'historical-prices',
    'splits',
  ];

  const fieldByCollectionName = {
    'dividends': 'e',
    'historical-prices': 'd',
    'splits': 'e',
  }

  const operations = [];
  const lowerDate = new Date('2016-01-01');
  for (const databaseName of databaseNames) {
    const atalsDB = atlas.db(databaseName);
    for (const collectionName of collectionNames) {
      const collection = atalsDB.collection(collectionName);
      const field = fieldByCollectionName[collectionName];
      const find = { [field]: { $lt: lowerDate }, x: { $ne: true } };
      const update = { $set: { x: true }, $currentDate: { u: true } };
      const operation = collection.updateMany(find, update)
      operations.push(operation);
    }
  }

  return await Promise.all(operations)
}

////////////////////////////////////////////////////// 2022-06-18 IEX splits old date format fix

/**
 * 2021-07-08T12:30:00.000+00:00 -> 2021-07-08T14:30:00.000+00:00
 */
async function old_date_format_splits_migration() {
  context.functions.execute("iexUtils");
  
  const splitsCollection = db.collection("splits");
  const splits = await splitsCollection.fullFind({ $expr: { $eq: [{ $hour: "$e" }, 12] } });
  splits.forEach(split => {
    split.e.setUTCHours(14);
  });

  await splitsCollection.safeUpdateMany(splits, null, '_id', true, false);
}

////////////////////////////////////////////////////// 2022-06-18 IEX dividends old date format fix

async function old_date_format_dividends_migration() {
  context.functions.execute("iexUtils");
  
  const dividendsCollection = db.collection("dividends");
  const dividends = await dividendsCollection.fullFind({ $expr: { $eq: [{ $hour: "$e" }, 12] } });
  dividends.forEach(dividend => {
    dividend.e.setUTCHours(14);
    if (dividend.p?.getUTCHours() === 12) {
      dividend.p.setUTCHours(14);
    }
    if (dividend.d?.getUTCHours() === 12) {
      dividend.d.setUTCHours(14);
    }
  });

  await dividendsCollection.safeUpdateMany(dividends, null, '_id', true, false);
}

////////////////////////////////////////////////////// 2022-06-18 IEX refid for splits

async function fetch_refid_for_IEX_splits() {
  context.functions.execute("iexUtils");
  
  const shortSymbols = await get_all_IEX_short_symbols();
  const range = '10y';
  const splits = await fetch_IEX_splits_with_duplicates(shortSymbols, range, false);
  const collection = db.collection("splits");

  console.log(`First, set refid on existing splits`);
  await collection.safeUpdateMany(splits, null, ['e', 's'], true, false);

  console.log(`Second, update with deduped on 'i' field. This may fix split date if duplicate was previously deleted.`);
  const splitsByRefid = splits.toBuckets('i');
  const dedupedSplits = [];
  const dupedSplits = [];
  for (const splits of Object.values(splitsByRefid)) {
    const sortedSplits = splits.sorted((l, r) => r.e - l.e);
    dedupedSplits.push(sortedSplits.shift());
    dupedSplits.push(...sortedSplits);
  }

  await collection.safeUpdateMany(dedupedSplits, null, 'i', true, false);

  // This is dangerous because it might delete second split record if the first one is already deleted.
  // console.log(`Third, delete duplicates if any left`)
  // const bulk = collection.initializeUnorderedBulkOp();
  // for (const dupedSplit of dupedSplits) {
  //   const find = ['e', 's'].reduce((find, field) => {
  //     return Object.assign(find, { [field]: dupedSplit[field] });
  //   }, {});

  //   bulk
  //     .find(find)
  //     .updateOne({ $set: { x: true }, $currentDate: { u: true } });
  // }

  // return await bulk.safeExecute();
}

async function get_all_IEX_short_symbols() {
  // We combine transactions and companies distinct IDs. 
  // Idealy, we should be checking all tables but we assume that only two will be enough.
  // All symbols have company record so company DB contains all ever fetched symbols.
  // Meanwhile transactions may contain not yet fetched symbols or have less symbols than we already should be updating (transactions may be deleted).
  // So by combining we have all current + all future symbols. Idealy.
  const companiesCollection = db.collection("companies");
  const transactionsCollection = db.collection("transactions");
  const [companyIDs, distinctTransactionSymbolIDs] = await Promise.all([
    companiesCollection.distinct("_id", {}),
    transactionsCollection.distinct("s", {}),
  ]);

  const objectIDByID = companyIDs.toDictionary(x => x.toString());
  const additionalIDs = distinctTransactionSymbolIDs.filter(x => objectIDByID[x.toString()] == null);
  const symbolIDs = companyIDs.concat(additionalIDs);

  return await getShortSymbols(symbolIDs);
}

async function fetch_IEX_splits_with_duplicates(shortSymbols, range, isFuture) {
  throwIfUndefinedOrNull(shortSymbols, `fetchSplitsWithDuplicates shortSymbols`);
  throwIfUndefinedOrNull(isFuture, `fetchSplitsWithDuplicates isFuture`);

  if (range == null) {
    range = defaultRange;
  }
  
  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);
  const parameters = { range: range };
  if (isFuture) {
    parameters.calendar = 'true';
  }

  // https://cloud.iexapis.com/stable/stock/market/batch?types=splits&token=pk_9f1d7a2688f24e26bb24335710eae053&range=6y&symbols=AAPL,AAP
  // https://sandbox.iexapis.com/stable/stock/market/batch?types=splits&token=Tpk_581685f711114d9f9ab06d77506fdd49&range=6y&symbols=AAPL,AAP
  return await iexFetchBatchAndMapArray('splits', tickers, idByTicker, fix_IEX_splits_with_duplicates, parameters);
}

function fix_IEX_splits_with_duplicates(iexSplits, symbolID) {
  try {
    throwIfUndefinedOrNull(iexSplits, `fixSplitsWithDuplicates splits`);
    throwIfUndefinedOrNull(symbolID, `fixSplitsWithDuplicates symbolID`);
    if (!iexSplits.length) { 
      console.logVerbose(`Splits are empty for ${symbolID}. Nothing to fix.`);
      return []; 
    }
  
    console.logVerbose(`Fixing splits for ${symbolID}`);

    return iexSplits
      .filterNullAndUndefined()
      .map(iexSplit => {
        const split = {};
        split.e = getOpenDate(iexSplit.exDate);
        split.i = iexSplit.refid;
        split.s = symbolID;

        if (iexSplit.ratio != null) {
          split.r = BSON.Double(iexSplit.ratio);
        }

        return split;
      });

  } catch (error) {
    console.error(`Unable to map splits: ${error}`);
    return [];
  }
}

////////////////////////////////////////////////////// 2022-06-18 IEX refid for future dividends

async function fetch_refid_for_future_IEX_dividends() {
  // We can't update all dividends because it will cost more than we can afford.
  // So instead, we update only future dividends and 1 past dividend.
  // There are also some known tickers with drifting value that we also update to fix their amount.
  context.functions.execute("iexUtils");
  
  const shortSymbols = await get_all_IEX_short_symbols();

  // We released to production on 2021-08-21 so we also cover all special tickers like 'BTI' and 'QCOM' with our 1 year behind refetch
  const futureDividends = await fetch_IEX_dividends_with_duplicates(shortSymbols, true, '10y', null);

  // Multiple update because we might have duplicates created by update which we can not distinguish during first run
  const collection = db.collection("dividends");
  const find = { i: null, e: { $gt: new Date() } };
  await find_and_update_IEX_dividends(futureDividends, find);
}
////////////////////////////////////////////////////// 2022-06-18 IEX refid for past dividends

async function fetch_refid_for_past_IEX_dividends() {
  // We can't update all dividends because it will cost more than we can afford.
  // So instead, we update only future dividends and 1 past dividend.
  // There are also some known tickers with drifting value that we also update to fix their amount.
  context.functions.execute("iexUtils");

  const shortSymbols = await get_all_IEX_short_symbols();

  // We released to production on 2021-08-21 so we also cover all special tickers like 'BTI' and 'QCOM' with our 1 year behind refetch
  const recentDividends = await fetch_IEX_dividends_with_duplicates(shortSymbols, false, '1y', null);

  const yearAgoDate = new Date();
  yearAgoDate.setUTCFullYear(yearAgoDate.getUTCFullYear() - 1);
  const exDateFind = { $lt: new Date(), $gt: yearAgoDate };
  const find = { i: null, e: exDateFind };
  await find_and_update_IEX_dividends(recentDividends, find);
}

async function find_and_update_IEX_dividends(dividends, find) {
  const collection = db.collection("dividends");
  let oldDividends = await collection.fullFind(find);
  let oldLength = 0;
  while (oldLength !== oldDividends.length) {
    oldLength = oldDividends.length;
    await update_IEX_dividends(dividends, oldDividends);
    oldDividends = await collection.fullFind(find);
  }
}

async function update_IEX_dividends(dividends, oldDividends) {
  console.log(`Updating refid field for '${dividends.length}' dividends`);

  console.log(`First, set refid on existing dividends`);
  const collection = db.collection("dividends");
  const fields = ['s', 'e'];
  const buckets = oldDividends.toBuckets(fields);
  
  // Try to prevent 'pending promise returned that will never resolve/reject uncaught promise rejection: &{0xc1bac2aa90 0xc1bac2aa80}' error by splitting batch operations to chunks
  const chunkSize = 1000;
  const chunkedNewDividends = dividends.chunkedBySize(chunkSize);
  for (const [i, newDividendsChunk] of chunkedNewDividends.entries()) {
    console.log(`Updating dividends: ${i * chunkSize + newDividendsChunk.length}/${dividends.length}`);
    const bulk = collection.initializeUnorderedBulkOp();
    for (const newDividend of newDividendsChunk) {
      const bucket = fields.reduce((buckets, field) => {
        if (buckets != null) {
          return buckets[newDividend[field]];
        } else {
          return null;
        }
      }, buckets);
      
      let existingDividend; 
      if (!bucket?.length) {
        existingDividend = null;
      } else if (bucket.length === 1) {
        existingDividend = bucket[0];
      } else {
        // Using the most strict validation first and then lower our expectations
        existingDividend = bucket.find(dividend => dividend.f === newDividend.f && dividend.a.valueOf() === newDividend.a.valueOf());
        if (existingDividend == null) {
          existingDividend = bucket.find(dividend => dividend.f === newDividend.f);
        }
        if (existingDividend == null) {
          const lowerAmount = newDividend.a.valueOf() * 0.9;
          const upperAmount = newDividend.a.valueOf() * 1.1;
          existingDividend = bucket.find(dividend => dividend.a.valueOf() > lowerAmount && dividend.a.valueOf() < upperAmount);
        }
        if (existingDividend == null) {
          existingDividend = bucket.find(dividend => compareOptionalDates(dividend.p, newDividend.p));
        }
        if (existingDividend == null) {
          console.error(`Bucket`);
          for (const dividend of bucket) {
            console.error(dividend.stringify());
          }
          throw `Unable to determine existingDividend for newDividend: ${newDividend.stringify()}`;
        }
      }
  
      if (existingDividend != null) {
        // Remove, so it won't be used by some other record
        bucket.remove(existingDividend);

        bulk.findAndUpdateOrInsertIfNeeded(newDividend, existingDividend, fields, true, false);
      }
    }
  
    await bulk.safeExecute();
  }

  console.log(`Success refid field update for '${dividends.length}' dividends`);
}

async function fetch_IEX_dividends_with_duplicates(shortSymbols, isFuture, range, limit) {
  throwIfUndefinedOrNull(shortSymbols, `fetchDividends shortSymbols`);
  throwIfUndefinedOrNull(isFuture, `fetchDividends isFuture`);
  if (!shortSymbols.length) { return []; }

  if (range == null) {
    range = defaultRange;
  }

  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);

  const parameters = { range: range };
  if (isFuture) {
    parameters.calendar = 'true';
  }
  if (limit != null) {
    parameters.limit = limit;
  }
  
  // https://cloud.iexapis.com/stable/stock/market/batch?token=pk_9f1d7a2688f24e26bb24335710eae053&types=dividends&symbols=AAPL,AAP&range=6y&calendar=true
  // https://cloud.iexapis.com/stable/stock/market/batch?token=pk_9f1d7a2688f24e26bb24335710eae053&types=dividends&symbols=AAPL,AAP&range=6y
  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_581685f711114d9f9ab06d77506fdd49&types=dividends&symbols=AAPL,AAP&range=6y&calendar=true
  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_581685f711114d9f9ab06d77506fdd49&types=dividends&symbols=AAPL,AAP&range=6y
  return await iexFetchBatchAndMapArray('dividends', tickers, idByTicker, fix_IEX_dividends_with_duplicates, parameters);
}

function fix_IEX_dividends_with_duplicates(iexDividends, symbolID) {
  try {
    throwIfUndefinedOrNull(iexDividends, `fixDividendsWithDuplicates iexDividends`);
    throwIfUndefinedOrNull(symbolID, `fixDividendsWithDuplicates uniqueID`);
    if (!iexDividends.length) { 
      console.logVerbose(`IEX dividends are empty for ${symbolID}. Nothing to fix.`);
      return []; 
    }

    console.logVerbose(`Mapping '${iexDividends.length}' IEX dividends for ${symbolID}`);
    const dividends = iexDividends
      .filterNullAndUndefined()
      .map(iexDividend => {
        const dividend = {};
        dividend.d = getOpenDate(iexDividend.declaredDate);
        dividend.e = getOpenDate(iexDividend.exDate);
        dividend.p = getOpenDate(iexDividend.paymentDate);
        dividend.i = iexDividend.refid;
        dividend.s = symbolID;

        if (iexDividend.amount != null) {
          dividend.a = BSON.Double(iexDividend.amount);
        }

        // We add only the first letter of a frequency
        if (iexDividend.frequency != null) {
          dividend.f = iexDividend.frequency.charAt(0);
        }
    
        // We do not add `USD` frequencies to the database.
        if (iexDividend.currency != null && iexDividend.currency !== "USD") {
          dividend.c = iexDividend.currency.toUpperCase();
        }
    
        return dividend;
      });

    console.logVerbose(`Returning '${dividends.length}' dividends for ${symbolID}`);
    return dividends;

  } catch(error) {
    console.error(`Unable to map dividends: ${error}`);
    return [];
  }
}

////////////////////////////////////////////////////// 2022-06-18 Duplicated IEX dividends deletion

async function delete_duplicated_IEX_dividends() {
  const collection = db.collection("dividends");
  const dividends = await collection.fullFind({ x: { $ne: true }, i: { $ne: null } });
  const duplicatedIDs = get_duplicated_IEX_dividend_IDs(dividends);

  console.log(`Deleting '${duplicatedIDs.length}' duplicate dividends`);
  console.logData(`duplicatedIDs`, duplicatedIDs);
  const find = { _id: { $in: duplicatedIDs } };
  const set = { $set: { x: true } };
  await collection.updateMany(find, set);
}

function get_duplicated_IEX_dividend_IDs(dividends) {
  const buckets = dividends.toBuckets('i');
  const duplicateIDs = [];
  for (const bucket of Object.values(buckets)) {
    // Prefer:
    // - Amount greater than zero
    // - Payment date not null
    // - Earlier ones (descending order)
    const sortedBucket = bucket.sorted((l, r) => {
      if (l.a.valueOf() <= 0) {
        return -1;
      } else if (r.a.valueOf() <= 0) {
        return 1;
      } else if (l.p == null) {
        return -1;
      } else if (r.p == null) {
        return 1;
      } else {
        return r.e - l.e;
      }
    });

    // Different frequency dividends are not duplicates
    const bucketByFrequency = sortedBucket.toBuckets('f');
    for (const frequencyBucket of Object.values(bucketByFrequency)) {
      if (frequencyBucket.length > 1) {
        const duplicates = frequencyBucket.slice(0, frequencyBucket.length - 1);
        console.error(`Duplicate dividends for ${duplicates[0].s}: ${duplicates.stringify()}`);
        const ids = duplicates.map(x => x._id);
        duplicateIDs.push(...ids);
      }
    }
  }

  return duplicateIDs;
}

////////////////////////////////////////////////////// 2022-06-18 FMP dividends data fix

/**
 * Deletes duplicated FMP dividends and fixes frequency where needed
 */
async function fix_FMP_dividends() {
  context.functions.execute("fmpUtils");

  const collection = fmp.collection('dividends');

  const oldDividends = await collection.fullFind({ x: { $ne: true } });
  const dividendsBySymbolID = oldDividends.toBuckets('s');
  const newDividends = [];
  for (const symbolDividends of Object.values(dividendsBySymbolID)) {
    let fixedDividends = symbolDividends.sorted((l, r) => l.e - r.e);
    fixedDividends = removeDuplicatedDividends(fixedDividends);
    fixedDividends = updateDividendsFrequency(fixedDividends);
    newDividends.push(...fixedDividends);

    const dividendsToDelete = symbolDividends.filter(dividend => !fixedDividends.includes(dividend))
    dividendsToDelete.forEach(dividend => dividend.x = true);
    newDividends.push(...dividendsToDelete);
  }

  await collection.safeUpdateMany(newDividends);
  console.log(`Deleted objects: ${newDividends.filter(x => x.x == true).length}`);
}

////////////////////////////////////////////////////// 2022-06-11 Merged symbols and getDataV2

// Checks that there is no transactions for IEX and FMP conflicting tickers
async function checkSameTickers() {
  // Same ticker symbols check
  const fmpSymbols = await fmp.collection("symbols").find({}).toArray();
  const iexSymbols = await db.collection("symbols").find({}).toArray();
  const allSymbols = fmpSymbols.concat(iexSymbols);
  const symbolsByTicker = allSymbols.toBuckets('t');
  for (const [ticker, symbols] of Object.entries(symbolsByTicker)) {
    if (symbols.length > 1) {
      console.log(`Conflicting ticker: ${ticker}`);
      const symbolIDs = symbols.map(x => x._id);
      const transactions = await db.collection('transactions').find({ s: { $in: symbolIDs } });
      if (transactions.length) {
        const transactionIDs = transactions.map(x => x._id);
        console.error(`Conflicting transactions: ${transactionIDs}`);
      } else {
        console.log(`No conflicting transactions for symbols: ${symbolIDs}`);
      }
    }
  }
}

////////////////////////////////////////////////////// 15-05-2022 FMP Funds

// Release new server with disabled FMP symbols update
// dt backup --environment production --database fmp
// dt restore --allow-production --environment production --database fmp --to-database fmp-tmp
// dt call-realm-function --environment production --function fmpUpdateSymbols --argument fmp-tmp --verbose
// dt call-realm-function --environment production --function fmpLoadMissingData --argument fmp-tmp --retry-on-error 'execution time limit exceeded'
// dt backup --environment production --database fmp-tmp
// dt restore --environment local --backup-source-environment production --database fmp-tmp
// Execute symbols migration
// dt backup --environment local --database fmp-tmp
// dt restore --allow-production --environment production --backup-source-environment local --database fmp-tmp
// Check data count
// dt call-realm-function --environment production --function fmpLoadMissingData --argument fmp-tmp --verbose
// dt call-realm-function --environment production --function fmpUpdateSymbols --argument fmp-tmp --verbose
// dt call-realm-function --environment production --function fmpUpdateCompanies --argument fmp-tmp --verbose
// dt call-realm-function --environment production --function fmpUpdateDividends --argument fmp-tmp --verbose
// dt call-realm-function --environment production --function fmpUpdateHistoricalPrices --argument fmp-tmp --verbose
// dt call-realm-function --environment production --function fmpUpdateQuotes --argument fmp-tmp --verbose
// dt call-realm-function --environment production --function fmpUpdateSplits --argument fmp-tmp --verbose
// Check data count
// dt backup --environment production --database fmp-tmp
// dt restore --allow-production --environment production --database fmp-tmp --to-database fmp
// dt call-realm-function --environment production --function checkTransactionsV2 --verbose
// Enable FMP symbols update

async function adjustSymbolIDs() {
  const fmpTmp = atlas.db("fmp-tmp");

  // 5 hours ago, just in case fetch will take so much
  const oldDate = new Date();
  oldDate.setUTCHours(oldDate.getUTCHours() - 5);

  const objectID = BSON.ObjectId.fromDate(oldDate);

  const symbolsCollection = fmpTmp.collection('symbols');
  const newSymbols = await symbolsCollection.find({ _id: { $gte: objectID } }).toArray();
  const newObjectIDs = newSymbols.map(x => x._id);

  // 10 mins in the future so we have time to release an update
  const newDate = new Date();
  newDate.setUTCMinutes(newDate.getUTCMinutes() + 10);

  // Delete old
  const bulk = symbolsCollection.initializeUnorderedBulkOp();
  for (const newObjectID of newObjectIDs) {
    bulk.find({ _id: newObjectID }).remove();
  }
  
  // Modify and insert new
  const hexSeconds = Math.floor(newDate/1000).toString(16);
  for (const newSymbol of newSymbols) {
    const hex = newSymbol._id.hex();
    const newID = BSON.ObjectId.fromDate(newDate, hex);
    newSymbol._id = newID;
    bulk.insert(newSymbol);
  }
  
  await bulk.execute();
}

////////////////////////////////////////////////////// 23-01-2022 Positive commission

async function positiveCommissionsMigration() {
  const transactionsCollection = db.collection("transactions");
  const oldTransactions = await transactionsCollection.find({ c: { $lt: 0 } }).toArray();
  const newTransactions = [];
  for (const oldTransaction of oldTransactions) {
    console.log(`Fixing: ${oldTransaction.stringify()}`);
    const newTransaction = Object.assign({}, oldTransaction);
    newTransaction.c = newTransaction.c * -1;
    console.log(`Fixed: ${newTransaction.stringify()}`);
    newTransactions.push(newTransaction);
  }

  if (newTransactions.length) {
    await transactionsCollection.safeUpdateMany(newTransactions, oldTransactions);
  } else {
    console.log("Noting to migrate");
  }
}

////////////////////////////////////////////////////// 03-01-2022 Partition key migration

async function partitionKeyMigration() {
  const operations = [];

  // Deleting _p key in all data collections
  const dataCollections = [
    db.collection('companies'),
    db.collection('dividends'),
    db.collection('exchange-rates'),
    db.collection('historical-prices'),
    db.collection('previous-day-prices'),
    db.collection('quotes'),
    db.collection('splits'),
    db.collection('symbols'),
  ];

  for (const collection of dataCollections) {
    const query = {};
    const update = { $unset: { "_p": "" } };
    const options = { "upsert": false };
    operations.push(
      collection.updateMany(query, update, options)
    );
  }

  // Renaming _p key to _ in all user collections
  const userCollections = [
    db.collection('settings'),
    db.collection('transactions'),
  ];

  for (const collection of userCollections) {
    const query = {};
    const update = { $rename: { _p: "_" } };
    const options = { "upsert": false };
    operations.push(
      collection.updateMany(query, update, options)
    );
  }

  return Promise.all(operations);
}

////////////////////////////////// 25-12-2021

async function v2DatabaseFillMigration() {
  const v2SymbolsCollection = db.collection('symbols');

  // Fetch V2 exchange rates and symbols if needed
  if (await v2SymbolsCollection.count() <= 0) {
    // Insert disabled PCI symbol
    const pciJSON = EJSON.parse('{"_id":{"$oid":"61b102c0048b84e9c13e6429"},"symbol":"PCI","exchange":"XNYS","exchangeSuffix":"","exchangeName":"New York Stock Exchange Inc","exchangeSegment":"XNYS","exchangeSegmentName":"New York Stock Exchange Inc","name":"PIMCO Dynamic Credit and Mortgage Income Fund","type":"cs","iexId":"IEX_5151505A56372D52","region":"US","currency":"USD","isEnabled":false,"figi":"BBG003GFZWD9","cik":"0001558629","lei":"549300Q41U0QIEXCOU14"}');
    const iexSymbols = iex.collection('symbols');
    await iexSymbols.insertOne(pciJSON);

    await Promise.all([
      context.functions.execute("updateExchangeRatesV2"),
      context.functions.execute("updateSymbolsV2")
    ]);
  }

  checkExecutionTimeoutAndThrow();
  const v2Symbols = await v2SymbolsCollection.find().toArray();
  const idByTicker = {};
  for (const v2Symbol of v2Symbols) {
    const ticker = v2Symbol.t;
    idByTicker[ticker] = v2Symbol._id;
  }

  const invalidEntitesFind = { $regex: ":(NAS|NYS|POR|USAMEX|USBATS|USPAC)" };

  checkExecutionTimeoutAndThrow();
  await fillV2CompanyCollectionMigration(idByTicker, invalidEntitesFind);
  checkExecutionTimeoutAndThrow();
  await fillV2DividendsCollectionMigration(idByTicker, invalidEntitesFind);
  checkExecutionTimeoutAndThrow();
  await fillV2HistoricalPricesCollectionMigration(idByTicker, invalidEntitesFind);
  checkExecutionTimeoutAndThrow();
  await fillV2PreviousDayPricesCollectionMigration(idByTicker, invalidEntitesFind);
  checkExecutionTimeoutAndThrow();
  await fillV2QoutesCollectionMigration(idByTicker, invalidEntitesFind);
  checkExecutionTimeoutAndThrow();
  await fillV2SettingsCollectionMigration(idByTicker, invalidEntitesFind);
  checkExecutionTimeoutAndThrow();
  await fillV2SplitsCollectionMigration(idByTicker, invalidEntitesFind);
  checkExecutionTimeoutAndThrow();
  await fillV2TransactionsCollectionMigration(idByTicker, invalidEntitesFind);
}

async function fillV2CompanyCollectionMigration(idByTicker, invalidEntitesFind) {
  const v1Collection = atlas.db("divtracker").collection('companies');

  // Check that data is valid
  const invalidEntities = await v1Collection.count({ _id: invalidEntitesFind });
  if (invalidEntities > 0) {
    throw `Found ${invalidEntities} invalid entities for V1 companies`;
  }
  
  const v1Companies = await v1Collection.find().toArray();
  /** V1
  {
    "_id": "AAPL:NAS",
    "_p": "P",
    "n": "Apple Inc",
    "s": "ap u MftrtEniiclorCrecmuc oaengtnu",
    "t": "cs"
  }
  */

  /** V2
  {
    "_id": {
      "$oid": "61b102c0048b84e9c13e4564"
    },
    "_p": "2",
    "i": "uric nrro uaetnMotuial etnCcgcmfpE",
    "n": "Apple Inc",
    "t": "cs"
  }
  */
  const v2Companies = v1Companies
    .map(v1Company => {
      // AAPL:XNAS -> AAPL
      const ticker = v1Company._id.split(':')[0];
      const symbolID = idByTicker[ticker];
      if (symbolID == null) {
        throw `Unknown V1 ticker '${ticker}' for company ${v1Company.stringify()}`;
      }

      const v2Company = {};
      v2Company._id = symbolID;
      v2Company._p = "2";
      v2Company.i = v1Company.s;
      v2Company.n = v1Company.n;
      v2Company.t = v1Company.t;

      return v2Company;
    });

  const v2Collection = db.collection('companies');

  // Delete existing if any to prevent duplication
  await v2Collection.deleteMany({});

  return await v2Collection.insertMany(v2Companies);
}

async function fillV2DividendsCollectionMigration(idByTicker, invalidEntitesFind) {
  const v1Collection = atlas.db("divtracker").collection('dividends');

  // Check that data is valid
  const invalidEntities = await v1Collection.count({ _i: invalidEntitesFind });
  if (invalidEntities > 0) {
    throw `Found ${invalidEntities} invalid entities for V1 dividends`;
  }

  const v1Dividends = await v1Collection.find().toArray();
  /** V1
  {
    "_i": "ABBV:XNYS",
    "_id": {
      "$oid": "61b4ec78e9bd6c27860a5b47"
    },
    "_p": "P",
    "a": {
      "$numberDouble": "1.42"
    },
    "d": {
      "$date": {
        "$numberLong": "1635085800000"
      }
    },
    "e": {
      "$date": {
        "$numberLong": "1641825000000"
      }
    },
    "f": "q",
    "p": {
      "$date": {
        "$numberLong": "1644589800000"
      }
    }
  }
  */
  
  /** V2
  {
    "_id": {
      "$oid": "61b4ec78e9bd6c27860a5b47"
    },
    "_p": "2",
    "a": {
      "$numberDouble": "1.42"
    },
    "d": {
      "$date": {
        "$numberLong": "1635085800000"
      }
    },
    "e": {
      "$date": {
        "$numberLong": "1641825000000"
      }
    },
    "f": "q",
    "p": {
      "$date": {
        "$numberLong": "1644589800000"
      }
    },
    "s": {
      "$oid": "61b102c0048b84e9c13e456f"
    }
  }
  */
  const v2Dividends = v1Dividends
    .map(v1Dividend => {
      // AAPL:XNAS -> AAPL
      const ticker = v1Dividend._i.split(':')[0];
      const symbolID = idByTicker[ticker];
      if (symbolID == null) {
        throw `Unknown V1 ticker '${ticker}' for dividend ${v1Dividend.stringify()}`;
      }

      const v2Dividend = {};
      v2Dividend._id = v1Dividend._id;
      v2Dividend._p = "2";
      v2Dividend.a = v1Dividend.a;
      v2Dividend.c = v1Dividend.c;
      v2Dividend.d = v1Dividend.d;
      v2Dividend.e = v1Dividend.e;
      v2Dividend.f = v1Dividend.f;
      v2Dividend.p = v1Dividend.p;
      v2Dividend.s = symbolID;

      return v2Dividend;
    });

  const v2Collection = db.collection('dividends');

  // Delete existing if any to prevent duplication
  await v2Collection.deleteMany({});

  return await v2Collection.insertMany(v2Dividends);
}

async function fillV2HistoricalPricesCollectionMigration(idByTicker, invalidEntitesFind) {
  const v1Collection = atlas.db("divtracker").collection('historical-prices');

  // Check that data is valid
  const invalidEntities = await v1Collection.count({ _i: invalidEntitesFind });
  if (invalidEntities > 0) {
    throw `Found ${invalidEntities} invalid entities for V1 historical prices`;
  }

  const v1HistoricalPrices = await v1Collection.find().toArray();
  /** V1
  {
    "_i": "AAPL:XNAS",
    "_id": {
      "$oid": "61b4ec77e9bd6c27860a4c2b"
    },
    "_p": "P",
    "c": {
      "$numberDouble": "28.516"
    },
    "d": {
      "$date": {
        "$numberLong": "1449867600000"
      }
    }
  }
  */
  
  /** V2
  {
    "_id": {
      "$oid": "61b4ec77e9bd6c27860a4c2b"
    },
    "_p": "2",
    "c": {
      "$numberDouble": "28.516"
    },
    "d": {
      "$date": {
        "$numberLong": "1449867600000"
      }
    },
    "s": {
      "$oid": "61b102c0048b84e9c13e4564"
    }
  }
  */
  const v2HistoricalPrices = v1HistoricalPrices
    .map(v1HistoricalPrice => {
      // AAPL:XNAS -> AAPL
      const ticker = v1HistoricalPrice._i.split(':')[0];
      const symbolID = idByTicker[ticker];
      if (symbolID == null) {
        throw `Unknown V1 ticker '${ticker}' for historical price ${v1HistoricalPrice.stringify()}`;
      }

      const v2HistoricalPrice = {};
      v2HistoricalPrice._id = v1HistoricalPrice._id;
      v2HistoricalPrice._p = "2";
      v2HistoricalPrice.c = v1HistoricalPrice.c;
      v2HistoricalPrice.d = v1HistoricalPrice.d;
      v2HistoricalPrice.s = symbolID;

      return v2HistoricalPrice;
    });

  const v2Collection = db.collection('historical-prices');

  // Delete existing if any to prevent duplication
  await v2Collection.deleteMany({});

  return await v2Collection.insertMany(v2HistoricalPrices);
}

async function fillV2PreviousDayPricesCollectionMigration(idByTicker, invalidEntitesFind) {
  const v1Collection = atlas.db("divtracker").collection('previous-day-prices');

  // Check that data is valid
  const invalidEntities = await v1Collection.count({ _id: invalidEntitesFind });
  if (invalidEntities > 0) {
    throw `Found ${invalidEntities} invalid entities for V1 previous day prices`;
  }

  const v1PreviousDayPrices = await v1Collection.find().toArray();
  /** V1
  {
    "_id": "AAPL:XNAS",
    "_p": "P",
    "c": {
      "$numberDouble": "182.37"
    }
  }
  */

  /** V2
  {
    "_id": {
      "$oid": "61b102c0048b84e9c13e4564"
    },
    "_p": "2",
    "c": {
      "$numberDouble": "182.37"
    }
  }
  */
  const v2PreviousDayPrices = v1PreviousDayPrices
    .map(v1PreviousDayPrice => {
      // AAPL:XNAS -> AAPL
      const ticker = v1PreviousDayPrice._id.split(':')[0];
      const symbolID = idByTicker[ticker];
      if (symbolID == null) {
        throw `Unknown V1 ticker '${ticker}' for previous day price ${v1PreviousDayPrice.stringify()}`;
      }

      const v2PreviousDayPrices = {};
      v2PreviousDayPrices._id = symbolID;
      v2PreviousDayPrices._p = "2";
      v2PreviousDayPrices.c = v1PreviousDayPrice.c;

      return v2PreviousDayPrices;
    });

  const v2Collection = db.collection('previous-day-prices');

  // Delete existing if any to prevent duplication
  await v2Collection.deleteMany({});

  return await v2Collection.insertMany(v2PreviousDayPrices);
}

async function fillV2QoutesCollectionMigration(idByTicker, invalidEntitesFind) {
  const v1Collection = atlas.db("divtracker").collection('quotes');

  // Check that data is valid
  const invalidEntities = await v1Collection.count({ _id: invalidEntitesFind });
  if (invalidEntities > 0) {
    throw `Found ${invalidEntities} invalid entities for V1 quotes`;
  }

  const v1Qoutes = await v1Collection.find().toArray();
  /** V1
  {
    "_id": "AAPL:XNAS",
    "_p": "P",
    "d": {
      "$date": {
        "$numberLong": "1653148017600"
      }
    },
    "l": {
      "$numberDouble": "179.87"
    },
    "p": {
      "$numberDouble": "16.17"
    }
  }
  */

  /** V2
  {
    "_id": {
      "$oid": "61b102c0048b84e9c13e4564"
    },
    "_p": "2",
    "l": {
      "$numberDouble": "179.87"
    },
    "p": {
      "$numberDouble": "16.17"
    }
  }
  */
  const v2Qoutes = v1Qoutes
    .map(v1Qoute => {
      // AAPL:XNAS -> AAPL
      const ticker = v1Qoute._id.split(':')[0];
      const symbolID = idByTicker[ticker];
      if (symbolID == null) {
        throw `Unknown V1 ticker '${ticker}' for quote ${v1Qoute.stringify()}`;
      }

      const v2Qoute = {};
      v2Qoute._id = symbolID;
      v2Qoute._p = "2";
      v2Qoute.l = v1Qoute.l;
      v2Qoute.p = v1Qoute.p;

      return v2Qoute;
    });

  const v2Collection = db.collection('quotes');

  // Delete existing if any to prevent duplication
  await v2Collection.deleteMany({});

  return await v2Collection.insertMany(v2Qoutes);
}

async function fillV2SettingsCollectionMigration(idByTicker, invalidEntitesFind) {
  const v1Collection = atlas.db("divtracker").collection('settings');

  // Check that data is valid
  const invalidEntities = await v1Collection.count({ "ts.i": invalidEntitesFind });
  if (invalidEntities > 0) {
    throw `Found ${invalidEntities} invalid entities for V1 settings`;
  }

  const v1Settings = await v1Collection.find().toArray();
  /** V1
  {
    "_id": {
      "$oid": "6107f99b395f4a2a8b092a1d"
    },
    "_p": "6107f89fc6a9d2fb9106915e",
    "ce": true,
    "g": {
      "$numberLong": "1000"
    },
    "t": {
      "$numberDouble": "0.13"
    },
    "te": true,
    "ts": [
      {
        "i": "ACN:XNYS",
        "t": {
          "$numberDouble": "0.25"
        }
      },
      {
        "i": "KNOP:XNYS",
        "t": {
          "$numberDouble": "0.3"
        }
      },
      {
        "i": "PBA:XNYS",
        "t": {
          "$numberDouble": "0.15"
        }
      },
      {
        "i": "ENB:XNYS",
        "t": {
          "$numberDouble": "0.15"
        }
      },
      {
        "i": "SPG:XNYS",
        "t": {
          "$numberDouble": "0.13"
        }
      }
    ]
  }
  */

  /** V2
  {
    "_id": {
      "$oid": "6107f99b395f4a2a8b092a1d"
    },
    "_p": "2",
    "ce": true,
    "g": {
      "$numberLong": "1000"
    },
    "t": {
      "$numberDouble": "0.13"
    },
    "te": true,
    "ts": [
      {
        "s": {
          "$oid": "61b102c0048b84e9c13e45b5"
        },
        "t": {
          "$numberDouble": "0.25"
        }
      },
      {
        "s": {
          "$oid": "61b102c0048b84e9c13e5cf1"
        },
        "t": {
          "$numberDouble": "0.3"
        }
      },
      {
        "s": {
          "$oid": "61b102c0048b84e9c13e63f6"
        },
        "t": {
          "$numberDouble": "0.15"
        }
      },
      {
        "s": {
          "$oid": "61b102c0048b84e9c13e51f7"
        },
        "t": {
          "$numberDouble": "0.15"
        }
      },
      {
        "s": {
          "$oid": "61b102c0048b84e9c13e6aca"
        },
        "t": {
          "$numberDouble": "0.13"
        }
      }
    ]
  }
  */
  const v2Settings = v1Settings
    .map(v1Settings => {
      const v2Settings = {};
      v2Settings._id = v1Settings._id;
      v2Settings._p = v1Settings._p;
      v2Settings.ce = v1Settings.ce;
      v2Settings.g = v1Settings.g;
      v2Settings.te = v1Settings.te;
      v2Settings.t = v1Settings.t;

      if (v1Settings.ts != null) {
        v2Settings.ts = v1Settings.ts.map(v1Taxes => {
          // AAPL:XNAS -> AAPL
          const ticker = v1Taxes.i.split(':')[0];
          const symbolID = idByTicker[ticker];
          if (symbolID == null) {
            throw `Unknown V1 ticker '${ticker}' for settings ${v1Settings.stringify()}`;
          }
  
          const v2Taxes = {};
          v2Taxes.s = symbolID;
          v2Taxes.t = v1Taxes.t;
  
          return v2Taxes;
        });
      }

      return v2Settings;
    });

  const v2Collection = db.collection('settings');

  // We execute everything in bulk to prevent insertions between delete and insert due to sync triggers
  const bulk = v2Collection.initializeOrderedBulkOp();

  // Delete existing if any to prevent duplication.
  bulk.find({}).remove();

  v2Settings.forEach(x => bulk.insert(x));

  return await bulk.safeExecute();
}

async function fillV2SplitsCollectionMigration(idByTicker, invalidEntitesFind) {
  const v1Collection = atlas.db("divtracker").collection('splits');

  // Check that data is valid
  const invalidEntities = await v1Collection.count({ _i: invalidEntitesFind });
  if (invalidEntities > 0) {
    throw `Found ${invalidEntities} invalid entities for V1 splits`;
  }

  const v1Splits = await v1Collection.find().toArray();
  /** V1
  {
    "_id": {
      "$oid": "61b4ec72e9bd6c27860a4627"
    },
    "_i": "AAPL:XNAS",
    "e": {
      "$date": {
        "$numberLong": "1597761000000"
      }
    },
    "_p": "P",
    "r": {
      "$numberDouble": "0.26"
    }
  }
  */

  /** V2
  {
    "_id": {
      "$oid": "61b4ec72e9bd6c27860a4627"
    },
    "_p": "2",
    "e": {
      "$date": {
        "$numberLong": "1597761000000"
      }
    },
    "r": {
      "$numberDouble": "0.26"
    },
    "s": {
      "$oid": "61b102c0048b84e9c13e4564"
    }
  }
  */
  const v2Splits = v1Splits
    .map(v1Split => {
      // AAPL:XNAS -> AAPL
      const ticker = v1Split._i.split(':')[0];
      const symbolID = idByTicker[ticker];
      if (symbolID == null) {
        throw `Unknown V1 ticker '${ticker}' for split ${v1Split.stringify()}`;
      }

      const v2Split = {};
      v2Split._id = v1Split._id;
      v2Split._p = "2";
      v2Split.e = v1Split.e;
      v2Split.r = v1Split.r;
      v2Split.s = symbolID;

      return v2Split;
    });

  const v2Collection = db.collection('splits');

  // Delete existing if any to prevent duplication
  await v2Collection.deleteMany({});

  return await v2Collection.insertMany(v2Splits);
}

async function fillV2TransactionsCollectionMigration(idByTicker, invalidEntitesFind) {
  const v1Collection = atlas.db("divtracker").collection('transactions');

  // Fixing regex
  invalidEntitesFind.$regex = `^${invalidEntitesFind.$regex.substring(1)}$`;

  // Check that data is valid.
  const invalidEntities = await v1Collection.count({ e: invalidEntitesFind });
  if (invalidEntities > 0) {
    throw `Found ${invalidEntities} invalid entities for V1 transactions`;
  }

  const v1Transactions = await v1Collection.find().toArray();
  /** V1
  {
    "_id": {
      "$oid": "61b4ec80b3083dd2cac32eb7"
    },
    "_p": "619c69468f19308bae927d4e",
    "a": {
      "$numberDouble": "25.1146"
    },
    "c": {
      "$numberDouble": "0.1"
    },
    "d": {
      "$date": {
        "$numberLong": "1596905100000"
      }
    },
    "e": "XNAS",
    "p": {
      "$numberDouble": "95.43"
    },
    "s": "AAPL"
  }
  */

  /** V2
  {
    "_id": {
      "$oid": "61b4ec80b3083dd2cac32eb7"
    },
    "_p": "2",
    "a": {
      "$numberDouble": "25.1146"
    },
    "c": {
      "$numberDouble": "0.1"
    },
    "d": {
      "$date": {
        "$numberLong": "1596905100000"
      }
    },
    "p": {
      "$numberDouble": "95.43"
    },
    "s": {
      "$oid": "61b102c0048b84e9c13e4564"
    }
  }
  */
  const v2Transactions = v1Transactions
    .map(v1Transactions => {
      // AAPL:XNAS -> AAPL
      const ticker = v1Transactions.s;
      const symbolID = idByTicker[ticker];
      if (symbolID == null) {
        throw `Unknown V1 ticker '${ticker}' for transaction ${v1Transactions.stringify()}`;
      }

      const v2Transactions = {};
      v2Transactions._id = v1Transactions._id;
      v2Transactions._p = v1Transactions._p;
      v2Transactions.a = v1Transactions.a;
      v2Transactions.c = v1Transactions.c;
      v2Transactions.d = v1Transactions.d;
      v2Transactions.p = v1Transactions.p;
      v2Transactions.s = symbolID;

      return v2Transactions;
    });

  const v2Collection = db.collection('transactions');

  // We execute everything in bulk to prevent insertions between delete and insert due to sync triggers
  const bulk = v2Collection.initializeOrderedBulkOp();

  // Delete existing if any to prevent duplication.
  bulk.find({}).remove();

  v2Transactions.forEach(x => bulk.insert(x));

  return await bulk.safeExecute();
}

////////////////////////////////// 03-12-2021

// IEX exchanges were renamed so we need to fix our data
async function exchangesFix_03122021() {
  await exchangesFix_id_Field_03122021('companies');
  await exchangesFix_i_Field_03122021('dividends');
  await exchangesFix_i_Field_03122021('historical-prices');
  await exchangesFix_id_Field_03122021('previous-day-prices');
  await exchangesFix_id_Field_03122021('quotes');
  await exchangesFixSettings_03122021();
  await exchangesFix_i_Field_03122021('splits');
  await exchangesFixTransactions_03122021();
}

async function exchangesFix_id_Field_03122021(collectionName) {
  const db = context.services.get("mongodb-atlas").db("divtracker");
  const collection = db.collection(collectionName);
  return await collection
    .find({})
    .toArray()
    .then(async entities => {
      const entitiesToMigrate = entities.filter(entity => {
        return isMigrationRequiredForID_03122021(entity._id);
      });

      if (!entitiesToMigrate.length) { return; }

      const bulk = collection.initializeUnorderedBulkOp();
      for (const entity of entitiesToMigrate) {
        const fixedID = fixExchangeNameInID_03122021(entity._id);
        const fixedEntity = Object.assign({}, entity);
        fixedEntity._id = fixedID;

        // Insert and replace new record if needed
        bulk.find({ _id: fixedID })
          .upsert()
          .replaceOne(fixedEntity);

        // Remove old
        bulk.find({ _id: entity._id })
          .removeOne();
      }
      return bulk.execute();
    });
}

async function exchangesFix_i_Field_03122021(collectionName) {
  const db = context.services.get("mongodb-atlas").db("divtracker");
  const collection = db.collection(collectionName);
  return await collection
    .find({}, { _id: 1, _i: 1 })
    .toArray()
    .then(async entities => {
      const entitiesToMigrate = entities.filter(entity => {
        return isMigrationRequiredForID_03122021(entity._i);
      });

      if (!entitiesToMigrate.length) { return; }

      if (entitiesToMigrate.length !== entities.length) {
        const migratedEntitiesIs = entities
          .filter(entity => {
            return !isMigrationRequiredForID_03122021(entity._i);
          })
          .map(x => x._i)
          .distinct();

        throw `Found conflict for '_i' entities for '${collectionName}' collection. Migrated entities: ${migratedEntitiesIs.stringify()}`
      }
      
      const bulk = collection.initializeUnorderedBulkOp();
      for (const entity of entitiesToMigrate) {
        const fixedID = fixExchangeNameInID_03122021(entity._i);
        bulk
          .find({ _id: entity._id })
          .updateOne({ $set: { _i: fixedID }});
      }
      return bulk.execute();
    });
}

async function exchangesFixSettings_03122021() {
  const db = context.services.get("mongodb-atlas").db("divtracker");
  const collection = db.collection('settings');
  return await collection
    .find({})
    .toArray()
    .then(async settings => {
      const settingsToMigrate = settings.filter(userSettings => {
        if (typeof userSettings.ts === 'undefined') {
          return false;
        } else if (typeof userSettings.ts[0] === 'undefined') {
          return false;
        } else {
          return isMigrationRequiredForID_03122021(userSettings.ts[0].i);
        }
      });

      if (!settingsToMigrate.length) { return; }
      
      const bulk = collection.initializeUnorderedBulkOp();
      for (const userSettings of settingsToMigrate) {
        for (const taxSettings of userSettings.ts) {
          const fixedExchange = fixExchangeNameInID_03122021(taxSettings.i);
          taxSettings.i = fixedExchange;
        }
        bulk
          .find({ _id: userSettings._id })
          .updateOne({ $set: userSettings });
      }
      return bulk.execute();
    });
}

async function exchangesFixTransactions_03122021() {
  const db = context.services.get("mongodb-atlas").db("divtracker");
  const collection = db.collection('transactions');
  return await collection
    .find({}, { _id: 1, e: 1 })
    .toArray()
    .then(async transactions => {
      const transactionsToMigrate = transactions.filter(transaction => {
        return isMigrationRequiredForExchange_03122021(transaction.e);
      });

      if (!transactionsToMigrate.length) { return; }
      
      const bulk = collection.initializeUnorderedBulkOp();
      for (const transaction of transactionsToMigrate) {
        const fixedExchange = fixExchangeName_03122021(transaction.e);
        bulk
          .find({ _id: transaction._id })
          .updateOne({ $set: { e: fixedExchange }});
      }
      return bulk.execute();
    });
}

function fixExchangeNameInID_03122021(_id) {
  const components = _id.split(':');
  const id = components[0];
  const exchange = components[1];
  const fixedExchange = fixExchangeName_03122021(exchange);
  return `${id}:${fixedExchange}`;
}

/**
 * NAS -> XNAS
 * NYS -> XNYS
 * POR -> XPOR
 * USAMEX -> XASE
 * USBATS -> BATS
 * USPAC -> ARCX
 */
function fixExchangeName_03122021(exchange) {
  if (exchange === 'NAS') {
    return 'XNAS';

  } else if (exchange === 'NYS') {
    return 'XNYS';

  } else if (exchange === 'POR') {
    return 'XPOR';

  } else if (exchange === 'USAMEX') {
    return 'XASE';

  } else if (exchange === 'USBATS') {
    return 'BATS';

  } else if (exchange === 'USPAC') {
    return 'ARCX';

  } else {
    console.error(`Already migrated exchange '${exchange}'?`);
    return exchange;
  }
}

function isMigrationRequiredForID_03122021(_id) {
  const exchange = _id.split(':')[1];
  return isMigrationRequiredForExchange_03122021(exchange);
}

function isMigrationRequiredForExchange_03122021(exchange) {
  if (exchange === 'NAS') {
    return true;

  } else if (exchange === 'NYS') {
    return true;

  } else if (exchange === 'POR') {
    return true;

  } else if (exchange === 'USAMEX') {
    return true;

  } else if (exchange === 'USBATS') {
    return true;

  } else if (exchange === 'USPAC') {
    return true;

  } else {
    return false;
  }
}

////////////////////////////////// 24-11-2021

// Remove `e` and `s` fields in the symbols collection
async function removeExcessiveFieldsInSymbols_24112021() {
  const db = context.services.get("mongodb-atlas").db("divtracker");
  const symbolsCollection = db.collection('symbols');

  const query = {};
  const update = { $unset: { e: "", s: "" } };
  const options = { "upsert": false };
  return symbolsCollection.updateMany(query, update, options);
}

// Truncates dividend frequency value to one letter
async function truncateDividendsFrequency_24112021() {
  const db = context.services.get("mongodb-atlas").db("divtracker");
  const dividendsCollection = db.collection('dividends');

  // Produce truncated dividends that will be used for bulk update
  // { _id: ObjectId("619d8187e3cabb8625968a99"), f: "q" }
  const project = { "f": { $substrBytes: [ "$f", 0, 1 ] } };
  const aggregation = [{ $project: project }];
  const dividends = await dividendsCollection
    .aggregate(aggregation)
    .toArray();

  // Find by `_id` and update `f`
  const operations = [];
  for (const dividend of dividends) {
    const filter = { _id: dividend._id };
    const update = { $set: { f: dividend.f } };
    const updateOne = { filter: filter, update: update, upsert: false };
    const operation = { updateOne: updateOne };
    operations.push(operation);
  }
  
  return dividendsCollection.bulkWrite(operations);
}

////////////////////////////////// 15-04-2021

// Add `exchange` field to all transactions
function executeTransactionsMigration_15042021() {
  const db = context.services.get("mongodb-atlas").db("divtracker");
  const transactionsCollection = db.collection('transactions');
  const symbolsCollection = db.collection('symbols');
  transactionsCollection
    .find({}, { symbol: 1 })
    .toArray()
    .then(async transactions => {
      const transactionsCount = transactions.length;
  
      if (transactionsCount) {
        const bulk = transactionsCollection.initializeUnorderedBulkOp();
        for (let i = 0; i < transactionsCount; i += 1) {
          const transaction = transactions[i];
          const symbol = await symbolsCollection
            .findOne({ symbol: transaction.symbol }, { exchange: 1 });
    
          if (!symbol) {
            console.error(`Skipping migration for '${transaction.symbol}' symbol`);
            continue;
          }
          
          const exchange = symbol.exchange;
          console.log(`Updating transaction '${transaction._id}' (${transaction.symbol}) with exchange: '${exchange}'`);
          bulk
            .find({ _id: transaction._id })
            .updateOne({ $set: { exchange: exchange }});
        }
        bulk.execute();
      }
    });
}
