
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
    } else if (migration === 'fix_FMP_dividends_v3') {
      await fix_FMP_dividends_v3(arg);
    } else {
      throw `Unexpected migration: ${migration}`;
    }

    console.log(`SUCCESS!`);
    
  } catch(error) {
    console.error(error);
    console.error(`FAILURE!`);
    throw error;
  }
};

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

/**
 * Deletes duplicated FMP dividends and fixes frequency where needed
 */
 async function fix_FMP_dividends_v3(iteration) {
  context.functions.execute("fmpUtils");
  throwIfNotNumber(iteration, `Iteration should be a number parameter with proper iteration value`);

  const collection = fmp.collection('dividends');

  const symbols = await collection
    .distinct('s', { x: { $ne: true } })
    .then(symbols => symbols.sorted((l, r) => l.toString().localeCompare(r.toString())));

  
  const iterationSize = 1000;
  const iterationSymbols = symbols.splice(iteration * iterationSize, iterationSize);
  if (iterationSymbols.length) {
    const totalIterations = parseInt(symbols.length / iterationSize, 10);
    console.log(`Performing ${iteration}/${totalIterations} iteration`);
  } else {
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
