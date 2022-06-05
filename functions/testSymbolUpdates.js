
// testSymbolUpdates.js

/**
 * @example
   exports();
 */
exports = async function() {
  context.functions.execute("testUtils");

  setup();

  try {
    await test(testIEXInsert);
  } catch(error) {
    console.log(error);
    await restore();
    throw error;
  }

  await restore();
};

async function test(testFunction) {
  await cleanup();
  await testFunction();
}

//////////////////////////// TESTS

// Insert IEX
async function testIEXInsert() {
  const id = new BSON.ObjectId();
  const symbol = { _id: id, t: "TICKER", n: "NAME" };
  await iexSymbolsCollection.insertOne(symbol);
  await context.functions.execute("mergedUpdateSymbols");
  const mergedSymbols = await mergedSymbolsCollection.find({}).toArray();
  if (mergedSymbols.length !== 1) {
    throw `[testIEXInsert] Unexpected merged symbols length: ${mergedSymbols.stringify()}`;
  }

  const mergedSymbol = mergedSymbols[0];
  if (!symbol.isEqual(mergedSymbol.m) || !symbol.isEqual(mergedSymbol.i) || mergedSymbol._id.toString() !== id.toString()) {
    throw `[testIEXInsert] Unexpected merged symbols content: ${mergedSymbol.stringify()}`;
  }
}

// Insert FMP
// Merge FMP to IEX
// Merge IEX to FMP
// Disable singular source symbol and enable it back
// Disable singular source symbol and attach new source to it
// Disable one source on multiple sources symbol and enable it back
// Update singular source symbol (ticker, company name, nothing)
// Update primary source for multiple sources symbol (ticker, company name, nothing)
// Update backup source for multiple sources symbol (ticker, company name, nothing)



//////////////////////////// HELPERS

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
}
