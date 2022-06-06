
// testSymbolUpdates.js

/**
 * @example
   exports();
 */
exports = async function() {
  context.functions.execute("testUtils");

  setup();

  try {
    await test(test_IEX_insert);
    await test(test_FMP_insert);
    await test(test_FMP_to_IEX_merge);
    await test(test_IEX_to_FMP_merge);
    await test(test_singular_source_disable_and_enable);
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
  await checkMergedSymbol('test_IEX_insert', iexSymbol, null, iexSymbol, id, null);
}

// Insert FMP
async function test_FMP_insert() {
  const id = new BSON.ObjectId();
  const fmpSymbol = { _id: id, t: "TICKER", n: "NAME" };
  await fmpSymbolsCollection.insertOne(fmpSymbol);
  await context.functions.execute("mergedUpdateSymbols", null, "fmp");
  await checkMergedSymbol('test_FMP_insert', null, fmpSymbol, fmpSymbol, id, null);
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
  
  await checkMergedSymbol('test_FMP_to_IEX_merge', iexSymbol, fmpSymbol, fmpSymbol, iexID, null);
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
  
  await checkMergedSymbol('test_IEX_to_FMP_merge', iexSymbol, fmpSymbol, fmpSymbol, fmpID, null);
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
  await checkMergedSymbol('test_singular_source_disable_and_enable', null, fmpSymbol, fmpSymbol, fmpID, date);

  // Enable back
  date = new Date();
  delete fmpSymbol.e;
  await fmpSymbolsCollection.updateOne({ _id: fmpID }, { $unset: { e: "" }, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "fmp");
  await checkMergedSymbol('test_singular_source_disable_and_enable', null, fmpSymbol, fmpSymbol, fmpID, date);
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
  
  // Disable
  date = new Date();
  iexSymbol.e = false;
  await iexSymbolsCollection.updateOne({ _id: iexID }, { $set: { e: iexSymbol.e }, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "iex");
  await checkMergedSymbol('test_IEX_disable_enable_on_multiple_sources', iexSymbol, fmpSymbol, fmpSymbol, iexID, date);

  // Enable back
  date = new Date();
  delete iexSymbol.e;
  await iexSymbolsCollection.updateOne({ _id: iexID }, { $unset: { e: "" }, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "iex");
  await checkMergedSymbol('test_IEX_disable_enable_on_multiple_sources', iexSymbol, fmpSymbol, fmpSymbol, iexID, date);
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
  await fmpSymbolsCollection.updateOne({ _id: iexID }, { $set: { e: fmpSymbol.e }, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "fmp");
  await checkMergedSymbol('test_FMP_disable_enable_on_multiple_sources', iexSymbol, null, iexSymbol, iexID, date);

  // Enable back
  date = new Date();
  delete fmpSymbol.e;
  await fmpSymbolsCollection.updateOne({ _id: iexID }, { $uset: { e: "" }, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "fmp");
  await checkMergedSymbol('test_FMP_disable_enable_on_multiple_sources', iexSymbol, fmpSymbol, fmpSymbol, iexID, date);
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
  await checkMergedSymbol('test_singular_source_update', iexSymbol, null, iexSymbol, iexID, date);

  // Update name
  date = new Date();
  iexSymbol.n = "NAME_NEW";
  await iexSymbolsCollection.updateOne({ _id: iexID }, { $set: iexSymbol, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "iex");
  await checkMergedSymbol('test_singular_source_update', iexSymbol, null, iexSymbol, iexID, date);

  // Update nothing
  date = new Date();
  await iexSymbolsCollection.updateOne({ _id: iexID }, { $set: iexSymbol, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "iex");
  await checkMergedSymbol('test_singular_source_update', iexSymbol, null, iexSymbol, iexID, null, date);
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
  await checkMergedSymbol('test_primary_source_update', iexSymbol, fmpSymbol, fmpSymbol, iexID, date);

  // Update name
  date = new Date();
  fmpSymbol.n = "FMP_NAME_NEW";
  await fmpSymbolsCollection.updateOne({ _id: fmpID }, { $set: fmpSymbol, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "fmp");
  await checkMergedSymbol('test_primary_source_update', iexSymbol, fmpSymbol, fmpSymbol, iexID, date);

  // Update nothing
  date = new Date();
  await fmpSymbolsCollection.updateOne({ _id: fmpID }, { $set: fmpSymbol, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "fmp");
  await checkMergedSymbol('test_primary_source_update', iexSymbol, fmpSymbol, fmpSymbol, iexID, null, date);
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
  await checkMergedSymbol('test_backup_source_update', iexSymbol, null, iexSymbol, iexID, null, updatesStartDate);

  // Update name
  date = new Date();
  iexSymbol.n = "NAME_NEW";
  await iexSymbolsCollection.updateOne({ _id: iexID }, { $set: iexSymbol, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "iex");
  await checkMergedSymbol('test_backup_source_update', iexSymbol, null, iexSymbol, iexID, null, updatesStartDate);

  // Update nothing
  date = new Date();
  await iexSymbolsCollection.updateOne({ _id: iexID }, { $set: iexSymbol, $currentDate: { "u": true } });
  await context.functions.execute("mergedUpdateSymbols", date, "iex");
  await checkMergedSymbol('test_backup_source_update', iexSymbol, null, iexSymbol, iexID, null, updatesStartDate);
}

//////////////////////////// TESTS HELPERS

async function checkMergedSymbol(testName, iexSymbol, fmpSymbol, mainSymbol, id, updateDate, noUpdateDate) {
  const mergedSymbol = await getMergedSymbol(testName);

  // IEX
  if (iexSymbol == null) {
    if (mergedSymbol.i != null) {
      throw `[${testName}] Merged symbol 'i' content expected to be null: ${mergedSymbol.i.stringify()}`;
    }
  } else {
    if (!iexSymbol.isEqual(mergedSymbol.i)) {
      throw `[${testName}] Merged symbol 'i' content '${mergedSymbol.i.stringify()}' expected to be equal to '${iexSymbol.stringify()}'`;
    }
  }
  
  // FMP
  if (fmpSymbol == null) {
    if (mergedSymbol.f != null) {
      throw `[${testName}] Merged symbol 'f' content expected to be null: ${mergedSymbol.f.stringify()}`;
    }
  } else {
    if (!fmpSymbol.isEqual(mergedSymbol.f)) {
      throw `[${testName}] Merged symbol 'f' content '${mergedSymbol.f.stringify()}' expected to be equal to '${fmpSymbol.stringify()}'`;
    }
  }
  
  // Main
  if (mainSymbol == null) {
    if (mergedSymbol.m != null) {
      throw `[${testName}] Merged symbol 'm' content expected to be null: ${mergedSymbol.m.stringify()}`;
    }
  } else {
    if (!mainSymbol.isEqual(mergedSymbol.m)) {
      throw `[${testName}] Merged symbol 'm' content '${mergedSymbol.m.stringify()}' expected to be equal to '${mainSymbol.stringify()}'`;
    }
  }

  if (id != null) {
    const mergedSymbolIDString = mergedSymbol._id.toString();
    const idString = id.toString();
    if (mergedSymbolIDString !== idString) {
      throw `[${testName}] Merged symbol ID '${mergedSymbolIDString}' expected to be equal to '${idString}'`;
    }
  }

  if (updateDate != null) {
    if (mergedSymbol.u <= updateDate) {
      throw `[${testName}] Merged symbol update date '${mergedSymbol.u}' should be greater than update date '${updateDate}'`;
    }
  }

  if (noUpdateDate != null) {
    if (mergedSymbol.u > noUpdateDate) {
      throw `[${testName}] Merged symbol update date '${mergedSymbol.u}' should be lower than no update date '${noUpdateDate}'`;
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
