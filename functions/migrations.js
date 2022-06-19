
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
    if (migration === 'refetch_IEX_splits') {
      await refetch_IEX_splits();
    } else if (migration === 'fix_FMP_dividends') {
      await fix_FMP_dividends(arg);
    } else {
      throw `Unexpected migration: ${migration}`;
    }
    
  } catch(error) {
    console.error(error);
  }
};

////////////////////////////////////////////////////// 2022-06-XX

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

  await setUpdateDate("splits");
}

/**
 * Deletes duplicated FMP dividends and fixes frequency where needed
 */
 async function fix_FMP_dividends(iteration) {
  context.functions.execute("fmpUtils");

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