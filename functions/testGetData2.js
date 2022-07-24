
// testGetData2.js

exports = async function() {
  context.functions.execute("testUtils");

  // logVerbose = true;
  // logData = true;
  
  try {
    await test_getDataV2_symbols_length();
  } catch(error) {
    console.error(error);
    throw error;
  }
};

// Checks that total symbols length is more than 50k.
async function test_getDataV2_symbols_length() {
  console.log("test_getDataV2_symbols_length");
  
  // Prepare env
  const fmpSymbols = [];
  const mergedSymbols = [];
  for (let i = 0; i < 50001; i++) {
    const fmpSymbol = {};
    fmpSymbol._id = new BSON.ObjectId();
    fmpSymbol.c = "TEST";
    fmpSymbol.n = `TEST ${i}`;
    fmpSymbol.t = `TEST${i}`;

    const mergedSymbol = {};
    mergedSymbol._id = fmpSymbol._id;
    mergedSymbol.m = fmpSymbol;
    mergedSymbol.m.s = 'f';
    mergedSymbol.f = fmpSymbol;

    fmpSymbols.push(fmpSymbol);
    mergedSymbols.push(mergedSymbol);
  }

  await Promise.all([
    fmp.collection('symbols').insertMany(fmpSymbols),
    atlas.db("merged").collection("symbols").insertMany(mergedSymbols),
  ]);

  // Get data
  const response = await context.functions.execute("getDataV2", null, ["symbols"], null, null);

  // Restore env
  await Promise.all([
    fmp.collection('symbols').deleteMany({ c: "TEST" }),
    atlas.db("merged").collection("symbols").deleteMany({ 'm.c': "TEST" }),
  ]);

  // Check
  const symbolsLength = response.updates.symbols.length;
  if (symbolsLength <= 50000) {
    throw `Unexpected total symbols length: ${symbolsLength}`;
  }
}
