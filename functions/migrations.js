
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
    } else if (migration === 'fill_hardcoded_fmp_companies') {
      await fill_hardcoded_fmp_companies();
    } else {
      throw `Unexpected migration: ${migration}`;
    }

    console.log(`SUCCESS!`);
    
  } catch(error) {
    console.error(error);
    console.error(`FAILURE!`);
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

async function fill_hardcoded_fmp_companies() {
  context.functions.execute("fmpUtils");

  const symbols = await atlas.db('fmp-tmp').collection("symbols").find().toArray();
  const symbolByTicker = symbols.toDictionary('t');
  const fmpSymbols = await fetch(ENV.hostURL, '/fmp_mutual_funds.json');
  const companies = fmpSymbols.map(fmpSymbol => max_fmp_symbol_to_fmp_company(fmpSymbol, symbolByTicker[fmpSymbol.symbol]._id))
  await atlas.db('fmp-tmp').collection("companies").insertMany(companies);
}

function max_fmp_symbol_to_fmp_company(fmpSymbol, symbolID) {
  try {
    throwIfUndefinedOrNull(fmpSymbol, `max_fmp_symbol_to_fmp_company company`);
    throwIfUndefinedOrNull(symbolID, `max_fmp_symbol_to_fmp_company symbolID`);
  
    const company = {};
    company._id = symbolID;
    company.c = "USD";
    company.setIfNotNullOrUndefined('n', fmpSymbol.name);
    company.t = "mf";

    return company;
  } catch(error) {
    console.error(`Unable to fix company ${fmpSymbol.stringify()}: ${error}`);
    return null;
  }
}
