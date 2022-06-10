
// testFMPUpdates.js

/**
 * @example
   exports();
 */
 exports = async function() {
  context.functions.execute("testUtils");

  try {
    await test();
  } catch(error) {
    console.log(error);
    throw error;
  }
};

async function test() {
  await context.functions.execute("fmpUpdateSymbols");
  await prepareFMPData();
  await Promise.all([
    context.functions.execute("fmpUpdateCompanies"),
    context.functions.execute("fmpUpdateDividends"),
    context.functions.execute("fmpUpdatePrices"),
    context.functions.execute("fmpUpdateQuotes"),
    context.functions.execute("fmpUpdateSplits"),
  ]);
}
