
// testSymbolUpdates.js

exports = async function() {
  context.functions.execute("testUtils");

  setup();

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
    await restore();
  } catch(error) {
    console.log(error);
    await restore();
    throw error;
  }
};

async function test(testFunction) {
  await cleanup();
  await testFunction();
}

//////////////////////////// TESTS

// Insert IEX
async function test_IEX_insert() {
  const id = new BSON.ObjectId();
  const iexSymbol = { _id: id, t: "TICKER", n: "NAME" };
  await iexSymbolsCollection.insertOne(iexSymbol);
  await context.functions.execute("mergedUpdateSymbols", null, "iex");
  await checkMergedSymbol('test_IEX_insert', iexSymbol, null, iexSymbol, id, 'i', null);
}

// Insert FMP
async function test_FMP_insert() {
  const id = new BSON.ObjectId();
  const fmpSymbol = { _id: id, t: "TICKER", n: "NAME" };
  await fmpSymbolsCollection.insertOne(fmpSymbol);
  await context.functions.execute("mergedUpdateSymbols", null, "fmp");
  await checkMergedSymbol('test_FMP_insert', null, fmpSymbol, fmpSymbol, id, 'f', null);
}

// Merge FMP to IEX
async function test_FMP_to_IEX_merge() {
  const iexID = new BSON.ObjectId();
  const iexSymbol = { _id: iexID, t: "TICKER", n: "IEX_NAME" };
  await iexSymbolsCollection.insertOne(iexSymbol);
  await context.functions.execute("mergedUpdateSymbols", null, "iex");

  const fmpID = new BSON.ObjectId();
  const fmpSymbol = { _id: fmpID, t: "TICKER", n: "FMP_NAME" };
  await fmpSymbolsCollection.insertOne(fmpSymbol);
  await context.functions.execute("mergedUpdateSymbols", null, "fmp");
  
  await checkMergedSymbol('test_FMP_to_IEX_merge', iexSymbol, fmpSymbol, fmpSymbol, iexID, 'f', null);
}

// Merge IEX to FMP
async function test_IEX_to_FMP_merge() {
  const fmpID = new BSON.ObjectId();
  const fmpSymbol = { _id: fmpID, t: "TICKER", n: "FMP_NAME" };
  await fmpSymbolsCollection.insertOne(fmpSymbol);
  await context.functions.execute("mergedUpdateSymbols", null, "fmp");

  const iexID = new BSON.ObjectId();
  const iexSymbol = { _id: iexID, t: "TICKER", n: "IEX_NAME" };
  await iexSymbolsCollection.insertOne(iexSymbol);
  await context.functions.execute("mergedUpdateSymbols", null, "iex");
  
  await checkMergedSymbol('test_IEX_to_FMP_merge', iexSymbol, fmpSymbol, fmpSymbol, fmpID, 'f', null);
}

// Disable singular source symbol and enable it back
async function test_singular_source_disable_and_enable() {
  let date;

  date = new Date();
  const fmpID = new BSON.ObjectId();
  const fmpSymbol = { _id: fmpID, t: "TICKER", n: "FMP_NAME" };
  await fmpSymbolsCollection.insertOne(fmpSymbol);
  await context.functions.execute("mergedUpdateSymbols", date, "fmp");

  // Disable
  date = new Date();
  fmpSymbol.e = false;
  await fmpSymbolsCollection.updateOne({ _id: fmpID }, { $set: { e: fmpSymbol.e }, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "fmp");
  await checkMergedSymbol('test_singular_source_disable_and_enable.disable', null, fmpSymbol, fmpSymbol, fmpID, 'f', date);

  // Enable back
  date = new Date();
  delete fmpSymbol.e;
  await fmpSymbolsCollection.updateOne({ _id: fmpID }, { $unset: { e: "" }, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "fmp");
  await checkMergedSymbol('test_singular_source_disable_and_enable.enable', null, fmpSymbol, fmpSymbol, fmpID, 'f', date);
}

// Disable singular source symbol, attach new source and enable disabled source back
async function test_singular_source_disable_attach_and_enable() {
  let date;

  date = new Date();
  const fmpID = new BSON.ObjectId();
  const fmpSymbol = { _id: fmpID, t: "TICKER", n: "FMP_NAME" };
  await fmpSymbolsCollection.insertOne(fmpSymbol);
  await context.functions.execute("mergedUpdateSymbols", date, "fmp");

  // Disable
  date = new Date();
  fmpSymbol.e = false;
  await fmpSymbolsCollection.updateOne({ _id: fmpID }, { $set: { e: fmpSymbol.e }, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "fmp");
  await checkMergedSymbol('test_singular_source_disable_attach_and_enable.disable', null, fmpSymbol, fmpSymbol, fmpID, 'f', date);

  // Attach
  const iexID = new BSON.ObjectId();
  const iexSymbol = { _id: iexID, t: "TICKER", n: "IEX_NAME" };
  await iexSymbolsCollection.insertOne(iexSymbol);
  await context.functions.execute("mergedUpdateSymbols", null, "iex");
  await checkMergedSymbol('test_singular_source_disable_attach_and_enable.attach', iexSymbol, null, iexSymbol, fmpID, 'i', date, null, date, null);

  // Enable back
  date = new Date();
  delete fmpSymbol.e;
  await fmpSymbolsCollection.updateOne({ _id: fmpID }, { $unset: { e: "" }, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "fmp");
  await checkMergedSymbol('test_singular_source_disable_attach_and_enable.enable', iexSymbol, fmpSymbol, fmpSymbol, fmpID, 'f', date, null, date, null);
}

// Disable IEX source on multiple sources symbol and enable it back
async function test_IEX_disable_enable_on_multiple_sources() {
  let date;
  
  const iexID = new BSON.ObjectId();
  const iexSymbol = { _id: iexID, t: "TICKER", n: "IEX_NAME" };
  await iexSymbolsCollection.insertOne(iexSymbol);
  await context.functions.execute("mergedUpdateSymbols", null, "iex");

  const fmpID = new BSON.ObjectId();
  const fmpSymbol = { _id: fmpID, t: "TICKER", n: "FMP_NAME" };
  await fmpSymbolsCollection.insertOne(fmpSymbol);
  await context.functions.execute("mergedUpdateSymbols", null, "fmp");

  const updatesStartDate = new Date();
  
  // Disable
  date = new Date();
  iexSymbol.e = false;
  await iexSymbolsCollection.updateOne({ _id: iexID }, { $set: { e: iexSymbol.e }, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "iex");
  await checkMergedSymbol('test_IEX_disable_enable_on_multiple_sources.disable', null, fmpSymbol, fmpSymbol, iexID, 'f', null, updatesStartDate, null, updatesStartDate);

  // Enable back
  date = new Date();
  delete iexSymbol.e;
  await iexSymbolsCollection.updateOne({ _id: iexID }, { $unset: { e: "" }, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "iex");
  await checkMergedSymbol('test_IEX_disable_enable_on_multiple_sources.enable', iexSymbol, fmpSymbol, fmpSymbol, iexID, 'f', null, updatesStartDate, null, updatesStartDate);
}

// Disable FMP source on multiple sources symbol and enable it back
async function test_FMP_disable_enable_on_multiple_sources() {
  let date;

  const iexID = new BSON.ObjectId();
  const iexSymbol = { _id: iexID, t: "TICKER", n: "IEX_NAME" };
  await iexSymbolsCollection.insertOne(iexSymbol);
  await context.functions.execute("mergedUpdateSymbols", null, "iex");

  const fmpID = new BSON.ObjectId();
  const fmpSymbol = { _id: fmpID, t: "TICKER", n: "FMP_NAME" };
  await fmpSymbolsCollection.insertOne(fmpSymbol);
  await context.functions.execute("mergedUpdateSymbols", null, "fmp");
  
  // Disable
  date = new Date();
  fmpSymbol.e = false;
  await fmpSymbolsCollection.updateOne({ _id: fmpID }, { $set: { e: fmpSymbol.e }, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "fmp");
  await checkMergedSymbol('test_FMP_disable_enable_on_multiple_sources.disable', iexSymbol, null, iexSymbol, iexID, date, 'i', null, date, null);

  // Enable back
  date = new Date();
  delete fmpSymbol.e;
  await fmpSymbolsCollection.updateOne({ _id: fmpID }, { $unset: { e: "" }, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "fmp");
  await checkMergedSymbol('test_FMP_disable_enable_on_multiple_sources.enable', iexSymbol, fmpSymbol, fmpSymbol, iexID, date, 'f', null, date, null);
}

// Update singular source symbol (ticker, name, nothing)
async function test_singular_source_update() {
  let date;
  
  const iexID = new BSON.ObjectId();
  const iexSymbol = { _id: iexID, t: "TICKER", n: "NAME" };
  await iexSymbolsCollection.insertOne(iexSymbol);
  await context.functions.execute("mergedUpdateSymbols", null, "iex");

  // Update ticker
  date = new Date();
  iexSymbol.t = "TICKER_NEW";
  await iexSymbolsCollection.updateOne({ _id: iexID }, { $set: iexSymbol, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "iex");
  await checkMergedSymbol('test_singular_source_update.ticker', iexSymbol, null, iexSymbol, iexID, 'i', date);

  // Update name
  date = new Date();
  iexSymbol.n = "NAME_NEW";
  await iexSymbolsCollection.updateOne({ _id: iexID }, { $set: iexSymbol, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "iex");
  await checkMergedSymbol('test_singular_source_update.name', iexSymbol, null, iexSymbol, iexID, 'i', date);

  // Update nothing
  date = new Date();
  await iexSymbolsCollection.updateOne({ _id: iexID }, { $set: iexSymbol, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "iex");
  await checkMergedSymbol('test_singular_source_update.nothing', iexSymbol, null, iexSymbol, iexID, 'i', null, date);
}

// Update primary source for multiple sources symbol (ticker, name, nothing)
async function test_primary_source_update() {
  let date;
  
  const iexID = new BSON.ObjectId();
  const iexSymbol = { _id: iexID, t: "TICKER", n: "IEX_NAME" };
  await iexSymbolsCollection.insertOne(iexSymbol);
  await context.functions.execute("mergedUpdateSymbols", null, "iex");

  const fmpID = new BSON.ObjectId();
  const fmpSymbol = { _id: fmpID, t: "TICKER", n: "FMP_NAME" };
  await fmpSymbolsCollection.insertOne(fmpSymbol);
  await context.functions.execute("mergedUpdateSymbols", null, "fmp");

  // Update ticker
  date = new Date();
  fmpSymbol.t = "TICKER_NEW";
  await fmpSymbolsCollection.updateOne({ _id: fmpID }, { $set: fmpSymbol, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "fmp");
  await checkMergedSymbol('test_primary_source_update.ticker', iexSymbol, fmpSymbol, fmpSymbol, iexID, 'f', date);

  // Update name
  date = new Date();
  fmpSymbol.n = "FMP_NAME_NEW";
  await fmpSymbolsCollection.updateOne({ _id: fmpID }, { $set: fmpSymbol, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "fmp");
  await checkMergedSymbol('test_primary_source_update.name', iexSymbol, fmpSymbol, fmpSymbol, iexID, 'f', date);

  // Update nothing
  date = new Date();
  await fmpSymbolsCollection.updateOne({ _id: fmpID }, { $set: fmpSymbol, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "fmp");
  await checkMergedSymbol('test_primary_source_update.nothing', iexSymbol, fmpSymbol, fmpSymbol, iexID, 'f', null, date);
}

// Update backup source for multiple sources symbol (ticker, name, nothing)
async function test_backup_source_update() {
  let date;
  
  const iexID = new BSON.ObjectId();
  const iexSymbol = { _id: iexID, t: "TICKER", n: "NAME" };
  await iexSymbolsCollection.insertOne(iexSymbol);
  await context.functions.execute("mergedUpdateSymbols", null, "iex");

  const fmpID = new BSON.ObjectId();
  const fmpSymbol = { _id: fmpID, t: "TICKER", n: "FMP_NAME" };
  await fmpSymbolsCollection.insertOne(fmpSymbol);
  await context.functions.execute("mergedUpdateSymbols", null, "fmp");

  const updatesStartDate = new Date();

  // Update ticker
  date = new Date();
  iexSymbol.t = "TICKER_NEW";
  await iexSymbolsCollection.updateOne({ _id: iexID }, { $set: iexSymbol, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "iex");
  await checkMergedSymbol('test_backup_source_update.ticker', iexSymbol, fmpSymbol, fmpSymbol, iexID, 'f', null, updatesStartDate);

  // Update name
  date = new Date();
  iexSymbol.n = "NAME_NEW";
  await iexSymbolsCollection.updateOne({ _id: iexID }, { $set: iexSymbol, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "iex");
  await checkMergedSymbol('test_backup_source_update.name', iexSymbol, fmpSymbol, fmpSymbol, iexID, 'f', null, updatesStartDate);

  // Update nothing
  date = new Date();
  await iexSymbolsCollection.updateOne({ _id: iexID }, { $set: iexSymbol, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "iex");
  await checkMergedSymbol('test_backup_source_update.nothing', iexSymbol, fmpSymbol, fmpSymbol, iexID, 'f', null, updatesStartDate);
}

//////////////////////////// TESTS HELPERS

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
    // Delete source field since original symbol does not have it
    const mergedSymbolMainCopy = Object.assign({}, mergedSymbol.m);
    delete mergedSymbolMainCopy.s;
    
    if (!mainSymbol.isEqual(mergedSymbolMainCopy)) {
      throw `[${testName}] Merged symbol main source '${mergedSymbolMainCopy.stringify()}' expected to be equal to '${mainSymbol.stringify()}'`;
    }
  }

  if (id != null) {
    const mergedSymbolIDString = mergedSymbol._id.toString();
    const idString = id.toString();
    if (mergedSymbolIDString !== idString) {
      throw `[${testName}] Merged symbol ID '${mergedSymbolIDString}' expected to be equal to '${idString}'`;
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
  const mergedSymbols = await mergedSymbolsCollection.find({}).toArray();
  if (mergedSymbols.length !== 1) {
    throw `[${testName}] Unexpected merged symbols length: ${mergedSymbols.stringify()}`;
  }

  return mergedSymbols[0];
}

//////////////////////////// ENVIRONMENT HELPERS

function setup() {
  iexSymbolsCollection = db.collection('symbols');
  fmpSymbolsCollection = atlas.db('fmp').collection('symbols');
  mergedSymbolsCollection = atlas.db("merged").collection("symbols");
}

async function cleanup() {
  await Promise.all([
    iexSymbolsCollection.deleteMany({}),
    fmpSymbolsCollection.deleteMany({}),
    mergedSymbolsCollection.deleteMany({}),
  ]);
}

async function restore() {
  await cleanup();
  await context.functions.execute("updateSymbolsV2");
  await context.functions.execute("fmpUpdateSymbols");
  await context.functions.execute("mergedUpdateSymbols");
}
