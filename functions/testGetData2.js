
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
  const symbols = [];
  for (let i = 0; i < 50001; i++) {
    const symbol = {};
    symbol.c = "TEST";
    symbol.n = `TEST ${i}`;
    symbol.t = `TEST${i}`;

    symbols.push(symbol);
  }
  await fmp.collection('symbols').insertMany(symbols);
  await context.functions.execute("mergedUpdateSymbols");

  // Get data
  const response = await context.functions.execute("getDataV2", null, ["symbols"], null, null);

  // Restore env
  Promise.all([
    fmp.collection('symbols').deleteMany({ c: "TEST" }),
    atlas.db("merged").collection("symbols").deleteMany({ 'm.c': "TEST" }),
  ]) 

  // Check
  const symbolsLength = response.updates.symbols.length;
  if (symbolsLength <= 50000) {
    throw `Unexpected total symbols length: ${symbolsLength}`;
  }
}
