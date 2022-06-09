
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

  const shortSymbols = await getInUseShortSymbols();
  const range = '10y';
  const splits = await fetchSplits(shortSymbols, range, false);
  const splitsByRefid = splits.toBuckets('refid');
  const dedupedSplits = [];
  for (const [refid, splits] of Object.entries(splitsByRefid)) {
    const split = splits.sorted((l, r) => r.e - l.e)[0];
    dedupedSplits.push(split);
  }

  const collection = db.collection("splits");

  // First, set refid on existing splits
  await collection.safeUpdateMany(splits, null, ['e', 's'], true, false);

  // Second, update with deduped on 'i' field
  await collection.safeUpdateMany(dedupedSplits, null, 'i', true, false);
}

async function fetchSplits(shortSymbols, range, isFuture) {
  throwIfUndefinedOrNull(shortSymbols, `fetchSplits shortSymbols`);
  throwIfUndefinedOrNull(isFuture, `fetchSplits isFuture`);

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
  return await iexFetchBatchAndMapArray('splits', tickers, idByTicker, fixSplits, parameters);
};

function fixSplits(iexSplits, symbolID) {
  try {
    throwIfUndefinedOrNull(iexSplits, `fixSplits splits`);
    throwIfUndefinedOrNull(symbolID, `fixSplits symbolID`);
    if (!iexSplits.length) { 
      console.logVerbose(`Splits are empty for ${symbolID}. Nothing to fix.`);
      return []; 
    }
  
    console.logVerbose(`Fixing splits for ${symbolID}`);

    return iexSplits
      .filterNullAndUndefined()
      .map(iexSplit => {
        const split = {};
        split.e = _getOpenDate(iexSplit.exDate);
        split.i = iexSplit.refid
        split.s = symbolID;

        if (iexSplit.ratio != null) {
          split.r = BSON.Double(iexSplit.ratio);
        }

        return split;
      });

  } catch (error) {
    console.logVerbose(`Unable to map splits: ${error}`);
    return [];
  }
};