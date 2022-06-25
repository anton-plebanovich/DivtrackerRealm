
// migrations.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.count/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/
// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/
// https://docs.mongodb.com/manual/reference/operator/update/unset/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeOrderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.removeOne/
// https://docs.mongodb.com/manual/reference/method/Bulk.insert/

exports = async function(migration, arg) {
  context.functions.execute("utils");

  logVerbose = true;
  logData = true;

  try {
    console.log(`Performing '${migration}' migration`);

    if (migration === 'fetch_new_symbols_for_fmp_tmp') {
      await fetch_new_symbols_for_fmp_tmp();
    } else if (migration === 'fetch_refid_for_past_IEX_dividends') {
      await fetch_refid_for_past_IEX_dividends();
    } else {
      throw `Unexpected migration: ${migration}`;
    }

    console.log(`SUCCESS!`);
    
  } catch(error) {
    console.error(error);
    console.error(`FAILURE!`);
  }
};

////////////////////////////////////////////////////// 2022-06-XX

async function fetch_new_symbols_for_fmp_tmp() {
  context.functions.execute("fmpUtils");
  
  const collection = fmp.collection("symbols");
  const symbols = await collection.fullFind();
  const symbolByTicker = symbols.toDictionary('t');
  console.log(`Existing symbols: ${symbols.length}`);

  const fetchedSymbols = await fetchSymbols();
  console.log(`Fetched symbols: ${fetchedSymbols.length}`);

  const newSymbols = fetchedSymbols.filter(x => symbolByTicker[x.t] == null);
  console.log(`New symbols: ${newSymbols.length}`);

  const tmpCollection = atlas.db('fmp-tmp').collection("symbols");
  await tmpCollection.insertMany(newSymbols);
}

////////////////////////////////////////////////////// 2022-06-XX

async function fetch_refid_for_past_IEX_dividends() {
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
