
// testSymbolUpdates.js

exports = async function() {
  context.functions.execute("testUtils");

  try {
    await test(test_IEX_insert);
    await test(test_FMP_insert);
    await test(test_FMP_to_IEX_merge);
    await test(test_IEX_to_FMP_merge);
    await test(test_singular_source_disable_and_enable);
    await test(test_singular_source_disable_attach_and_enable);
    await test(test_IEX_disable_enable_on_multiple_sources);
    await test(test_FMP_disable_enable_on_multiple_sources);
    await test(test_singular_source_update);
    await test(test_primary_source_update);
    await test(test_backup_source_update);
    await restoreSymbols();
  } catch(error) {
    console.log(error);
    await restoreSymbols();
    throw error;
  }
};

async function test(testFunction) {
  await cleanupSymbols();
  await testFunction();
}

//////////////////////////// TESTS

// Insert IEX
async function test_IEX_insert() {
  const [iexID, iexSymbol] = await add_IEX_symbol();
  await checkMergedSymbol('test_IEX_insert', iexSymbol, null, iexSymbol, iexID, 'i', null);
}

// Insert FMP
async function test_FMP_insert() {
  const [fmpID, fmpSymbol] = await add_FMP_symbol();
  await checkMergedSymbol('test_FMP_insert', null, fmpSymbol, fmpSymbol, fmpID, 'f', null);
}

// Merge FMP to IEX
async function test_FMP_to_IEX_merge() {
  const [iexID, iexSymbol] = await add_IEX_symbol();
  const [fmpID, fmpSymbol] = await add_FMP_symbol();
  await checkMergedSymbol('test_FMP_to_IEX_merge', iexSymbol, fmpSymbol, fmpSymbol, iexID, 'f', null);
}

// Merge IEX to FMP
async function test_IEX_to_FMP_merge() {
  const [fmpID, fmpSymbol] = await add_FMP_symbol();
  const [iexID, iexSymbol] = await add_IEX_symbol();
  await checkMergedSymbol('test_IEX_to_FMP_merge', iexSymbol, fmpSymbol, fmpSymbol, fmpID, 'f', null);
}

// Disable singular source symbol and enable it back
async function test_singular_source_disable_and_enable() {
  let date;

  date = new Date();
  const [fmpID, fmpSymbol] = await add_FMP_symbol(null, date);

  // Disable
  date = new Date();
  await disable_FMP_symbol(fmpSymbol, date);
  await checkMergedSymbol('test_singular_source_disable_and_enable.disable', null, fmpSymbol, fmpSymbol, fmpID, 'f', date);

  // Enable back
  date = new Date();
  await enable_FMP_symbol(fmpSymbol, date);
  await checkMergedSymbol('test_singular_source_disable_and_enable.enable', null, fmpSymbol, fmpSymbol, fmpID, 'f', date);
}

// Disable singular source symbol, attach new source and enable disabled source back
async function test_singular_source_disable_attach_and_enable() {
  let date;

  date = new Date();
  const [fmpID, fmpSymbol] = await add_FMP_symbol(null, date);

  // Disable
  date = new Date();
  fmpSymbol.e = false;
  await disable_FMP_symbol(fmpSymbol, date);
  await checkMergedSymbol('test_singular_source_disable_attach_and_enable.disable', null, fmpSymbol, fmpSymbol, fmpID, 'f', date);

  // Attach
  const [iexID, iexSymbol] = await add_IEX_symbol();
  await checkMergedSymbol('test_singular_source_disable_attach_and_enable.attach', iexSymbol, null, iexSymbol, fmpID, 'i', date, null, date, null);

  // Enable back
  date = new Date();
  await enable_FMP_symbol(fmpSymbol, date);
  await checkMergedSymbol('test_singular_source_disable_attach_and_enable.enable', iexSymbol, fmpSymbol, fmpSymbol, fmpID, 'f', date, null, date, null);
}

// Disable IEX source on multiple sources symbol and enable it back
async function test_IEX_disable_enable_on_multiple_sources() {
  let date;
  
  const [iexID, iexSymbol] = await add_IEX_symbol();
  const [fmpID, fmpSymbol] = await add_FMP_symbol();
  const updatesStartDate = new Date();
  
  // Disable
  date = new Date();
  await disable_IEX_symbol(iexSymbol, date);
  await checkMergedSymbol('test_IEX_disable_enable_on_multiple_sources.disable', null, fmpSymbol, fmpSymbol, iexID, 'f', null, updatesStartDate, null, updatesStartDate);

  // Enable back
  date = new Date();
  await enable_IEX_symbol(iexSymbol, date);
  await checkMergedSymbol('test_IEX_disable_enable_on_multiple_sources.enable', iexSymbol, fmpSymbol, fmpSymbol, iexID, 'f', null, updatesStartDate, null, updatesStartDate);
}

// Disable FMP source on multiple sources symbol and enable it back
async function test_FMP_disable_enable_on_multiple_sources() {
  let date;

  const [iexID, iexSymbol] = await add_IEX_symbol();
  const [fmpID, fmpSymbol] = await add_FMP_symbol();
  
  // Disable
  date = new Date();
  await disable_FMP_symbol(fmpSymbol, date);
  await checkMergedSymbol('test_FMP_disable_enable_on_multiple_sources.disable', iexSymbol, null, iexSymbol, iexID, 'i', date, null, date, null);

  // Enable back
  date = new Date();
  await enable_FMP_symbol(fmpSymbol, date);
  await checkMergedSymbol('test_FMP_disable_enable_on_multiple_sources.enable', iexSymbol, fmpSymbol, fmpSymbol, iexID, 'f', date, null, date, null);
}

// Update singular source symbol (ticker, name, nothing)
async function test_singular_source_update() {
  let date;
  
  const [iexID, iexSymbol] = await add_IEX_symbol();

  // Update ticker
  date = new Date();
  iexSymbol.t = "TICKER_NEW";
  await update_IEX_symbol(iexSymbol, date);
  await checkMergedSymbol('test_singular_source_update.ticker', iexSymbol, null, iexSymbol, iexID, 'i', date);

  // Update name
  date = new Date();
  iexSymbol.n = "NAME_NEW";
  await update_IEX_symbol(iexSymbol, date);
  await checkMergedSymbol('test_singular_source_update.name', iexSymbol, null, iexSymbol, iexID, 'i', date);

  // Update nothing
  date = new Date();
  await update_IEX_symbol(iexSymbol, date);
  await checkMergedSymbol('test_singular_source_update.nothing', iexSymbol, null, iexSymbol, iexID, 'i', null, date);
}

// Update primary source for multiple sources symbol (ticker, name, nothing)
async function test_primary_source_update() {
  let date;
  
  const [iexID, iexSymbol] = await add_IEX_symbol();
  const [fmpID, fmpSymbol] = await add_FMP_symbol();

  // Update ticker
  date = new Date();
  fmpSymbol.t = "TICKER_NEW";
  await update_FMP_symbol(fmpSymbol, date);
  await checkMergedSymbol('test_primary_source_update.ticker', iexSymbol, fmpSymbol, fmpSymbol, iexID, 'f', date);

  // Update name
  date = new Date();
  fmpSymbol.n = "FMP_NAME_NEW";
  await update_FMP_symbol(fmpSymbol, date);
  await checkMergedSymbol('test_primary_source_update.name', iexSymbol, fmpSymbol, fmpSymbol, iexID, 'f', date);

  // Update nothing
  date = new Date();
  await update_FMP_symbol(fmpSymbol, date);
  await checkMergedSymbol('test_primary_source_update.nothing', iexSymbol, fmpSymbol, fmpSymbol, iexID, 'f', null, date);
}

// Update backup source for multiple sources symbol (ticker, name, nothing)
async function test_backup_source_update() {
  let date;
  
  const [iexID, iexSymbol] = await add_IEX_symbol();
  const [fmpID, fmpSymbol] = await add_FMP_symbol();
  const updatesStartDate = new Date();

  // Update ticker
  date = new Date();
  iexSymbol.t = "TICKER_NEW";
  await update_IEX_symbol(iexSymbol, date);
  await checkMergedSymbol('test_backup_source_update.ticker', iexSymbol, fmpSymbol, fmpSymbol, iexID, 'f', null, updatesStartDate);

  // Update name
  date = new Date();
  iexSymbol.n = "NAME_NEW";
  await update_IEX_symbol(iexSymbol, date);
  await checkMergedSymbol('test_backup_source_update.name', iexSymbol, fmpSymbol, fmpSymbol, iexID, 'f', null, updatesStartDate);

  // Update nothing
  date = new Date();
  await update_IEX_symbol(iexSymbol, date);
  await checkMergedSymbol('test_backup_source_update.nothing', iexSymbol, fmpSymbol, fmpSymbol, iexID, 'f', null, updatesStartDate);
}

//////////////////////////// TESTS HELPERS

async function add_IEX_symbol(name, date) {
  return await add_symbol(name, date, "iex");
}

async function add_FMP_symbol(name, date) {
  return await add_symbol(name, date, "fmp");
}

async function add_symbol(name, date, sourceName) {
  const source = sourceByName[sourceName];
  const collection = source.db.collection('symbols');

  if (name == null) {
    name = `${sourceName}_NAME`;
  }

  const id = new BSON.ObjectId();
  const symbol = { _id: id, t: "TICKER", n: name };
  await collection.insertOne(symbol);
  await context.functions.execute("mergedUpdateSymbols", date, sourceName);

  return [id, symbol];
}

async function disable_IEX_symbol(symbol, date) {
  await disable_symbol(symbol, date, "iex");
}

async function disable_FMP_symbol(symbol, date) {
  await disable_symbol(symbol, date, "fmp");
}

async function disable_symbol(symbol, date, sourceName) {
  symbol.e = false;
  const source = sourceByName[sourceName];
  const collection = source.db.collection('symbols')
  await collection.updateOne({ _id: symbol._id }, { $set: { e: symbol.e }, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, sourceName);
}

async function enable_IEX_symbol(symbol, date) {
  await enable_symbol(symbol, date, "iex");
}

async function enable_FMP_symbol(symbol, date) {
  await enable_symbol(symbol, date, "fmp");
}

async function enable_symbol(symbol, date, sourceName) {
  delete symbol.e;
  const source = sourceByName[sourceName];
  const collection = source.db.collection('symbols');
  await collection.updateOne({ _id: symbol._id }, { $unset: { e: "" }, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, sourceName);
}

async function update_IEX_symbol(symbol, date) {
  await update_symbol(symbol, date, "iex");
}

async function update_FMP_symbol(symbol, date) {
  await update_symbol(symbol, date, "fmp");
}

async function update_symbol(symbol, date, sourceName) {
  const source = sourceByName[sourceName];
  const collection = source.db.collection('symbols');
  await collection.updateOne({ _id: symbol._id }, { $set: symbol, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, sourceName);
}

async function checkMergedSymbol(testName, iexSymbol, fmpSymbol, mainSymbol, id, source, lowerUpdateDate, upperUpdateDate, lowerRefetchDate, upperRefetchDate) {
  const mergedSymbol = await getMergedSymbol(testName);

  // IEX
  if (iexSymbol == null) {
    if (mergedSymbol.i != null) {
      throw `[${testName}] Merged symbol IEX source expected to be null: ${mergedSymbol.i.stringify()}`;
    }
  } else {
    if (!iexSymbol.isEqual(mergedSymbol.i)) {
      throw `[${testName}] Merged symbol IEX source '${mergedSymbol.i.stringify()}' expected to be equal to '${iexSymbol.stringify()}'`;
    }
  }
  
  // FMP
  if (fmpSymbol == null) {
    if (mergedSymbol.f != null) {
      throw `[${testName}] Merged symbol FMP source expected to be null: ${mergedSymbol.f.stringify()}`;
    }
  } else {
    if (!fmpSymbol.isEqual(mergedSymbol.f)) {
      throw `[${testName}] Merged symbol FMP source '${mergedSymbol.f.stringify()}' expected to be equal to '${fmpSymbol.stringify()}'`;
    }
  }
  
  // Main
  if (mainSymbol == null) {
    if (mergedSymbol.m != null) {
      throw `[${testName}] Merged symbol main source expected to be null: ${mergedSymbol.m.stringify()}`;
    }
  } else {
    // Delete '_id' field since main symbol should have '_id' from merged symbol
    const mainSymbolCopy = Object.assign({}, mainSymbol);
    delete mainSymbolCopy._id;
    const mergedSymbolMainCopy = Object.assign({}, mergedSymbol.m);
    delete mergedSymbolMainCopy._id;

    // Source will be compared separately
    delete mergedSymbolMainCopy.s;

    if (!mainSymbolCopy.isEqual(mergedSymbolMainCopy)) {
      throw `[${testName}] Merged symbol main source '${mergedSymbolMainCopy.stringify()}' expected to be equal to '${mainSymbolCopy.stringify()}'`;
    }
  }

  if (id != null) {
    const mergedSymbolIDString = mergedSymbol._id.toString();
    const idString = id.toString();
    if (mergedSymbolIDString !== idString) {
      throw `[${testName}] Merged symbol ID '${mergedSymbolIDString}' expected to be equal to '${idString}'`;
    }

    const mainSymbolIDString = mergedSymbol.m._id.toString();
    if (mainSymbolIDString !== idString) {
      throw `[${testName}] Main symbol ID '${mainSymbolIDString}' expected to be equal to '${idString}'`;
    }
  }

  if (mergedSymbol.m.s !== source) {
    throw `[${testName}] Merged symbol main source '${mergedSymbol.m.s}' expected to be equal to '${source}'`;
  }

  if (lowerUpdateDate != null) {
    if (mergedSymbol.u <= lowerUpdateDate) {
      throw `[${testName}] Merged symbol update date '${mergedSymbol.u.getTime()}' should be greater than or equal to lower update date '${lowerUpdateDate.getTime()}'`;
    }
  }

  if (upperUpdateDate != null) {
    if (mergedSymbol.u >= upperUpdateDate) {
      throw `[${testName}] Merged symbol update date '${mergedSymbol.u.getTime()}' should be lower than or equal to upper update date '${upperUpdateDate.getTime()}'`;
    }
  }

  if (lowerRefetchDate != null) {
    if (mergedSymbol.r <= lowerRefetchDate) {
      throw `[${testName}] Merged symbol update date '${mergedSymbol.r.getTime()}' should be greater than or equal lower refetch date '${lowerRefetchDate.getTime()}'`;
    }
  }

  if (upperRefetchDate != null) {
    if (mergedSymbol.r >= upperRefetchDate) {
      throw `[${testName}] Merged symbol update date '${mergedSymbol.r.getTime()}' should be lower than or equal to upper refetch date '${upperRefetchDate.getTime()}'`;
    }
  }
}

async function getMergedSymbol(testName) {
  const mergedSymbols = await atlas.db("merged").collection("symbols").find({}).toArray();
  if (mergedSymbols.length !== 1) {
    throw `[${testName}] Unexpected merged symbols length: ${mergedSymbols.stringify()}`;
  }

  return mergedSymbols[0];
}
