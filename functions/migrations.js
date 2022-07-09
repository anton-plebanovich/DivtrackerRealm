
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
    } else if (migration === 'fix_fmp_symbols_exchanges') {
      await fix_fmp_symbols_exchanges();
    } else if (migration === 'merged_symbols_fill_exchanges_migration') {
      await merged_symbols_fill_exchanges_migration();
    } else {
      throw `Unexpected migration: ${migration}`;
    }

    console.log(`SUCCESS!`);
    
  } catch(error) {
    console.error(error);
    console.error(`FAILURE!`);
  }
};

////////////////////////////////////////////////////// 2022-07-XX

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

////////////////////////////////////////////////////// 2022-07-XX

async function fix_fmp_symbols_exchanges() {
  context.functions.execute("fmpUtils");

  const newSymbols = await fetchSymbols();
  const fmpCollection = fmp.collection("symbols");
  await fmpCollection.safeUpdateMany(newSymbols, undefined, 't', false, false);

  const mergedSymbolsCollection = atlas.db("merged").collection("symbols");
  const fmpSymbolsCollection = atlas.db("fmp").collection('symbols');
  const [mergedSymbols, fmpSymbols] = await Promise.all([
    mergedSymbolsCollection.fullFind(),
    fmpSymbolsCollection.fullFind(),
  ]);

  const fmpSymbolsByID = fmpSymbols.toDictionary('_id');
  mergedSymbols.forEach(mergedSymbol => {
    if (mergedSymbol.f != null) {
      mergedSymbol.f.c = fmpSymbolsByID[mergedSymbol.f._id].c;
    }
  });

  await mergedSymbolsCollection.safeUpdateMany(mergedSymbols, null, '_id', false, false);
}

async function merged_symbols_fill_exchanges_migration() {
  context.functions.execute("utils");

  const mergedSymbolsCollection = atlas.db("merged").collection("symbols");
  const iexSymbolsCollection = atlas.db("divtracker-v2").collection('symbols');
  const fmpSymbolsCollection = atlas.db("fmp").collection('symbols');
  const [mergedSymbols, iexSymbols, fmpSymbols] = await Promise.all([
    mergedSymbolsCollection.fullFind(),
    iexSymbolsCollection.fullFind(),
    fmpSymbolsCollection.fullFind(),
  ]);

  const iexSymbolsByID = iexSymbols.toDictionary('_id');
  const fmpSymbolsByID = fmpSymbols.toDictionary('_id');
  mergedSymbols.forEach(mergedSymbol => {
    if (mergedSymbol.i != null) {
      mergedSymbol.i.c = iexSymbolsByID[mergedSymbol.i._id].c;
    }
    if (mergedSymbol.f != null) {
      mergedSymbol.f.c = fmpSymbolsByID[mergedSymbol.f._id].c;
    }
    mergedSymbol.m.c = mergedSymbol[mergedSymbol.m.s].c;
  });

  await mergedSymbolsCollection.safeUpdateMany(mergedSymbols, null, '_id', false, false);
}
