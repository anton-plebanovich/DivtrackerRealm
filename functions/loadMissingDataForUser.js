
// loadMissingDataForUser.js

exports = async function(userID) {
  context.functions.execute("utils");
  const transactionsCollection = db.collection("transactions");
  const transactions = await transactionsCollection.find({ _p: userID }, { "e": 1, "s": 1 }).toArray();

  // We have only one fetch source atm so we don't need exchange actually.
  const distinctTransactions = transactions.distinct("s");
  
  const distinctSymbolsDictionary = {};
  for (const x of distinctTransactions) {
    distinctSymbolsDictionary[x.s] = `${x.s}:${x.e}`;
  }

  const symbols = Object.keys(distinctSymbolsDictionary);
  console.log(`Distinct user symbols (${symbols.length}): ${symbols.stringify()}`);
  
  for (const symbol of symbols) {
    console.log(`Loading missing user data for symbol: ${symbol}`);
    await context.functions.execute("loadMissingDataForSymbol", symbol, distinctSymbolsDictionary[symbol]);
  }
};

// exports('615955b47e2079114597d16c'); // Anton
