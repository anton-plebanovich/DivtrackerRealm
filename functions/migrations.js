
// migrations.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.count/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/
// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/
// https://docs.mongodb.com/manual/reference/operator/update/unset/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeOrderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.removeOne/
// https://docs.mongodb.com/manual/reference/method/Bulk.insert/

exports = async function(migration) {
  context.functions.execute("utils");

  logVerbose = true;
  logData = true;

  try {
    if (migration === 'null_fields_cleanup_migration') {
      await null_fields_cleanup_migration();
    } else if (migration === 'old_data_delete_migration') {
      await old_data_delete_migration();
    } else if (migration === 'old_date_format_splits_migration') {
      await old_date_format_splits_migration();
    } else if (migration === 'old_date_format_dividends_migration') {
      await old_date_format_dividends_migration();
    } else if (migration === 'fetch_refid_for_IEX_splits') {
      await fetch_refid_for_IEX_splits();
    } else if (migration === 'fetch_refid_for_future_IEX_dividends') {
      await fetch_refid_for_future_IEX_dividends();
    } else if (migration === 'fetch_refid_for_past_IEX_dividends') {
      await fetch_refid_for_past_IEX_dividends();
    } else if (migration === 'delete_duplicated_IEX_dividends') {
      await delete_duplicated_IEX_dividends();
    } else if (migration === 'fix_FMP_dividends') {
      await fix_FMP_dividends();
    } else {
      throw `Unexpected migration: ${migration}`;
    }
    
  } catch(error) {
    console.error(error);
  }
};

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

////////////////////////////////////////////////////// 2022-06-XX Data range adjust

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

////////////////////////////////////////////////////// 2022-06-XX IEX refid for splits and dividends

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

async function fetch_refid_for_future_IEX_dividends() {
  // We can't update all dividends because it will cost more than we can afford.
  // So instead, we update only future dividends and 1 past dividend.
  // There are also some known tickers with drifting value that we also update to fix their amount.
  context.functions.execute("iexUtils");

  const shortSymbols = await get_all_IEX_short_symbols();

  // We released to production on 2021-08-21 so we also cover all special tickers like 'BTI' and 'QCOM' with our 1 year behind refetch
  const futureDividends = await fetch_IEX_dividends_with_duplicates(shortSymbols, true, '10y', null);

  await update_IEX_dividends(futureDividends);
}

async function fetch_refid_for_past_IEX_dividends() {
  // We can't update all dividends because it will cost more than we can afford.
  // So instead, we update only future dividends and 1 past dividend.
  // There are also some known tickers with drifting value that we also update to fix their amount.
  context.functions.execute("iexUtils");

  const shortSymbols = await get_all_IEX_short_symbols();

  // We released to production on 2021-08-21 so we also cover all special tickers like 'BTI' and 'QCOM' with our 1 year behind refetch
  const recentDividends = await fetch_IEX_dividends_with_duplicates(shortSymbols, false, '1y', null);

  await update_IEX_dividends(recentDividends);
}

async function update_IEX_dividends() {
  console.log(`Updating refid field for '${dividends.length}' dividends`);

  console.log(`First, set refid on existing dividends`);
  const collection = db.collection("dividends");
  const oldDividends = await collection
    .fullFind({})
    .then(x => x.sortedDeletedToTheStart());

  const fields = ['s', 'e'];
  const buckets = oldDividends.toBuckets(fields);
  
  // Try to prevent 'pending promise returned that will never resolve/reject uncaught promise rejection: &{0xc1bac2aa90 0xc1bac2aa80}' error by splitting batch operations to chunks
  const chunkSize = 1000;
  const chunkedNewDividends = dividends.chunked(chunkSize);
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
      if (bucket == null) {
        existingDividend = null;
      } else if (bucket.length === 1) {
        existingDividend = bucket[0];
      } else {
        existingDividend = bucket.find(dividend => dividend.f === newDividend.f);
        if (existingDividend == null) {
          const lowerAmount = newDividend.a * 0.9;
          const upperAmount = newDividend.a * 1.1;
          existingDividend = bucket.find(dividend => dividend.a > lowerAmount && dividend.a < upperAmount);
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
  
      bulk.findAndUpdateOrInsertIfNeeded(newDividend, existingDividend, fields, true, false);
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

function get_duplicated_IEX_dividend_IDs(dividends) {
  const buckets = dividends.toBuckets('i');
  const duplicateIDs = [];
  for (const bucket of Object.values(buckets)) {
    // Prefer:
    // - Amount greater than zero
    // - Payment date not null
    // - Earlier ones (descending order)
    const sortedBucket = bucket.sorted((l, r) => {
      if (l.a <= 0) {
        return -1;
      } else if (r.a <= 0) {
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
    const bucketByFrequency = bucket.toBuckets('f');
    for (const frequencyBucket of Object.values(bucketByFrequency)) {
      if (frequencyBucket.length > 1) {
        const duplicate = frequencyBucket[0];
        console.error(`Duplicate dividend for ${duplicate.s}: ${duplicate.stringify()}`);
        duplicateIDs.push(duplicate._id);
      }
    }
  }

  return duplicateIDs;
}

async function delete_duplicated_IEX_dividends() {
  const duplicatedIDs = get_duplicated_IEX_dividend_IDs(dividends);

  console.log(`Deleting '${duplicatedIDs.length}' duplicate dividends`);
  console.logData(`duplicatedIDs`, duplicatedIDs);
  const find = { _id: { $in: duplicatedIDs } };
  const set = { $set: { x: true } };
  await collection.updateMany(find, set);
}

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
