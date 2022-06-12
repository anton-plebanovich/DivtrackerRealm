
// migrations.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.count/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/
// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/
// https://docs.mongodb.com/manual/reference/operator/update/unset/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeOrderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.removeOne/
// https://docs.mongodb.com/manual/reference/method/Bulk.insert/

exports = async function() {
  try {
    return await refid_migration();
  } catch(error) {
    console.error(error);
  }
};

////////////////////////////////////////////////////// 2022-06-XX IEX refid for splits and dividends

async function refid_migration() {
  context.functions.execute("iexUtils");

  logVerbose = true;
  logData = true;

  return await Promise.all([
    fetch_refid_for_IEX_splits(),
    fetch_refid_for_IEX_dividends(),
  ]);
}

async function fetch_refid_for_IEX_splits() {

  const shortSymbols = await getAllShortSymbols();
  const range = '10y';
  const splits = await fetchSplitsWithDuplicates(shortSymbols, range, false);
  const collection = db.collection("splits");

  console.log(`First, set refid on existing splits`);
  await collection.safeUpdateMany(splits, null, ['e', 's'], true, false);

  console.log(`Second, backward compatibility, we had different hour previously so need to also check that`);
  const backwardCompatibilitySplits = splits.map(split => {
    const newSplit = Object.assign({}, split);
    const date = new Date(newSplit.e);
    date.setUTCHours(date.getUTCHours() - 2); // 2 hours difference with old splits
    newSplit.e = date;

    return newSplit;
  });
  await collection.safeUpdateMany(backwardCompatibilitySplits, null, ['e', 's'], true, false);

  console.log(`Third, update with deduped on 'i' field. This may fix split date if duplicate was previously deleted.`);
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
  // console.log(`Forth, delete duplicates if any left`)
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

async function getAllShortSymbols() {
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

async function fetchSplitsWithDuplicates(shortSymbols, range, isFuture) {
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
  return await iexFetchBatchAndMapArray('splits', tickers, idByTicker, fixSplitsWithDuplicates, parameters);
}

function fixSplitsWithDuplicates(iexSplits, symbolID) {
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

async function fetch_refid_for_IEX_dividends() {
  // We can't update all dividends because it will cost more than we can afford.
  // So instead, we update only future dividends and 1 past dividend.
  // There are also some known tickers with drifting value that we also update to fix their amount.

  const shortSymbols = await getAllShortSymbols();

  const specificTickers = ['BTI', 'QCOM'];
  const specificShortSymbols = shortSymbols.filter(x => specificTickers.includes(x.t));

  const [
    futureDividends,
    recentDividends,
    specificDividends,
  ] = await Promise.all([
    fetchDividendsWithDuplicates(shortSymbols, true, '10y', null),
    fetchDividendsWithDuplicates(shortSymbols, false, '1y', 1),
    fetchDividendsWithDuplicates(specificShortSymbols, false, '10y', null),
  ]);

  const dividends = futureDividends
    .concat(recentDividends)
    .concat(specificDividends);

  console.log(`Updating refid field for '${dividends.length}' dividends`);

  const collection = db.collection("dividends");

  console.log(`First, set refid on existing dividends`);
  await collection.safeUpdateMany(dividends, null, ['s', 'e', 'f'], true, false);

  console.log(`Second, update with deduped on 'i' field. This may fix dividend date if duplicate was previously deleted.`);
  const dedupedDividends = removeDuplicatedDividends(dividends);
  console.log(`Fixing dividend data for '${dedupedDividends.length}' dividends`);
  await collection.safeUpdateMany(dedupedDividends, null, 'i', true, false);

  console.log(`Success refid field update for '${dividends.length}' dividends`);
}

async function fetchDividendsWithDuplicates(shortSymbols, isFuture, range, limit) {
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
  return await iexFetchBatchAndMapArray('dividends', tickers, idByTicker, fixDividendsWithDuplicates, parameters);
}

function fixDividendsWithDuplicates(iexDividends, symbolID) {
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

function removeDuplicatedDividends(dividends) {
  const buckets = dividends.toBuckets('i');
  const result = [];
  for (const bucket of Object.values(buckets)) {
    // Prefer the one with payment date and earlier ones (descending order)
    const sortedBucket = bucket.sorted((l, r) => {
      if (l.p == null) {
        return -1;
      } else if (r.p == null) {
        return 1;
      } else {
        return r.e - l.e;
      }
    });

    if (sortedBucket.length > 1) {
      const duplicate = sortedBucket[0];
      console.error(`Duplicate dividend for ${duplicate.t}: ${duplicate.stringify()}`);
    }

    result.push(sortedBucket[sortedBucket.length - 1]);
  }

  return result;
}
