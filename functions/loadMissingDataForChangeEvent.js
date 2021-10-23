
// loadMissingDataForChangeEvent.js

exports = async function(changeEvent) {
  context.functions.execute("utils");
  const transaction = changeEvent.fullDocument;
  const symbol = transaction.s;
  const exchange = transaction.e;
  const uniqueID = `${symbol}:${exchange}`;
  console.log(`Transaction symbol '${symbol}', exchange '${exchange}', unique ID: ${uniqueID}`);
  
  await context.functions.execute("loadMissingDataForSymbol", symbol, uniqueID);
};

// exports({ fullDocument: { s: "AAPL", e: "NAS" } })
