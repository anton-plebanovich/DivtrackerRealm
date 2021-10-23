
// loadMissingDividendsForSymbol.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/bulk-write/
// https://docs.mongodb.com/realm/functions/json-and-bson/
// https://docs.meteor.com/api/ejson.html

///////////////////////////////////////////////////// EXPORTS

exports = async function(symbol, uniqueID) {
  context.functions.execute("utils");

  const periodParameter = "6y";

  // Get unique ID if needed
  if (!uniqueID) {
    const symbolsCollection = db.collection("symbols");
    const symbolObject = await symbolsCollection.findOne({ s: symbol });
    uniqueID = symbolObject._id;
  }

  console.log(`Symbol '${symbol}', unique ID '${uniqueID}'`);
  
  // Dividends
  try {
    const dividendsCollection = db.collection("dividends");
    if (await dividendsCollection.findOne({ _i: uniqueID })) {
      console.logVerbose(`Found existing dividend for unique ID '${uniqueID}'`);
      
    } else {
      // Future dividends
      console.logVerbose(`Fetching and fixing data for future dividends...`);
      const futureDividends = await fetch(
          `/stock/${symbol}/dividends/${periodParameter}`,
          { calendar: "true" }
        )
        .then(x => fixDividends(x, uniqueID));

      if (futureDividends.length) {
        console.logVerbose(`Inserting future dividends for unique ID '${uniqueID}'...`);
        dividendsCollection.insertMany(futureDividends);
        console.log(`Future dividends for unique ID '${uniqueID}' successfully inserted`);

      } else {
        console.logVerbose(`Future dividends are empty for unique ID '${uniqueID}'`);
      }

      // Past dividends
      console.logVerbose(`Dividends for unique ID '${uniqueID}' not found. Fetching past dividends and fixing data...`);
      const pastDividends = await fetch(`/stock/${symbol}/dividends/${periodParameter}`)
        .then(x => fixDividends(x, uniqueID));

      if (pastDividends.length) {
        console.logVerbose(`Inserting past dividends for unique ID '${uniqueID}'...`);
        dividendsCollection.insertMany(pastDividends);
        console.log(`Past dividends for unique ID '${uniqueID}' successfully inserted`);

      } else {
        console.logVerbose(`Past dividends are empty for unique ID '${uniqueID}'`);
      }
    }
  } catch(err) {
    console.error(`Unable to fetch dividends for '${uniqueID}', err: ${err}`);
  }
  
  console.log(`Fetched all missing dividends for the '${symbol}'`);
};

// exports('AAP');
// exports('AAP', 'NYS');
