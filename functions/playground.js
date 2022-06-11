
// playground.js

exports = async function() {
  context.functions.execute("fmpUtils");
  
  await checkSameTickers();
};

////////////////////////////////////////////////////// 2022-06-XX Merged symbols and getDataV2

async function checkSameTickers() {
  // Same ticker symbols check
  const fmpSymbols = await fmp.collection("symbols").find({}).toArray();
  const iexSymbols = await db.collection("symbols").find({}).toArray();
  const allSymbols = fmpSymbols.concat(iexSymbols);
  const symbolsByTicker = allSymbols.toBuckets('t');
  for (const [ticker, symbols] of Object.entries(symbolsByTicker)) {
    if (symbols.length > 1) {
      console.log(`Conflicting ticker: ${ticker}`);
      const symbolIDs = symbols.map(x => x._id);
      const transactions = await db.collection('transactions').find({ s: { $in: symbolIDs } });
      if (transactions.length) {
        const transactionIDs = transactions.map(x => x._id);
        console.error(`Conflicting transactions: ${transactionIDs}`);
      } else {
        console.log(`No conflicting transactions for symbols: ${symbolIDs}`);
      }
    }
  }
}