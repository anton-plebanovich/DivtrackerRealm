
// migrations.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.count/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/
// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/
// https://docs.mongodb.com/manual/reference/operator/update/unset/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeOrderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.removeOne/
// https://docs.mongodb.com/manual/reference/method/Bulk.insert/

exports = async function() {
  return await fetch_refid_for_IEX_splits();
};

////////////////////////////////////////////////////// 2022-06-XX IEX refid for splits

async function fetch_refid_for_IEX_splits() {
  context.functions.execute("iexUtils");

  logVerbose = true;
  logData = true;

  const shortSymbols = await getAllShortSymbols();
  const range = '10y';
  const splits = await fetchSplitsWithDuplicates(shortSymbols, range, false);
  const splitsByRefid = splits.toBuckets('refid');
  const dedupedSplits = [];
  const dupedSplits = [];
  for (const [refid, splits] of Object.entries(splitsByRefid)) {
    const sortedSplits = splits.sorted((l, r) => r.e - l.e);
    const split = sortedSplits[0];
    dedupedSplits.push(sortedSplits.shift());
    dupedSplits.push(...sortedSplits);
  }

  const collection = db.collection("splits");

  console.log(`First, set refid on existing splits`);
  await collection.safeUpdateMany(splits, null, ['e', 's'], true, false);

  // Backward compatibility, we had different hour previously so need to also check that
  const backwardCompatibilitySplits = splits.map(split => {
    const newSplit = Object.assign({}, split);
    const date = new Date(newSplit.e);
    date.setUTCHours(date.getUTCHours() - 2); // 2 hours difference with old splits
    newSplit.e = date;

    return newSplit;
  });
  await collection.safeUpdateMany(backwardCompatibilitySplits, null, ['e', 's'], true, false);

  console.log(`Second, update with deduped on 'i' field. This may fix split date if duplicate was previously deleted.`);
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
