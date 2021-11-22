
// loadMissingDataForSymbol.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/bulk-write/
// https://docs.mongodb.com/realm/mongodb/actions/collection.findOne/
// https://docs.mongodb.com/realm/functions/json-and-bson/
// https://docs.meteor.com/api/ejson.html

///////////////////////////////////////////////////// EXPORTS

exports = async function(symbol, uniqueID) {
  context.functions.execute("utils");

  const periodParameter = "6y";

  // Get unique ID if needed
  if (!uniqueID) {
    const symbolsCollection = db.collection("symbols");
    const symbolObject = await symbolsCollection.findOne({ _id: { "$regex": `^${symbol}:` } });
    uniqueID = symbolObject._id;
  }

  console.log(`Symbol '${symbol}', unique ID '${uniqueID}'`);
  const errors = [];
  
  // Company
  try {
    const companies = db.collection("companies");
    if (await companies.findOne({ "_id": uniqueID })) {
      console.logVerbose(`Found existing company with unique ID '${uniqueID}'`);
      
    } else {
      console.logVerbose(`Company for unique ID '${uniqueID}' not found. Fetching and fixing data...`);
      const company = await fetch(`/stock/${symbol}/company`)
        .then(x => fixCompany(x, uniqueID));

      if (company) {
        console.logVerbose(`Inserting company for unique ID '${uniqueID}'...`);
        companies.insertOne(company);
        console.log(`Company for unique ID '${uniqueID}' successfully inserted`);
      } else {
        console.error(`Unable to insert '${company}' company for unique ID '${uniqueID}'`);
      }
    }
  } catch(error) {
    console.error(`Unable to fetch company for '${uniqueID}', err: ${error}`);
    errors.push(error);
  }
  
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
  } catch(error) {
    console.error(`Unable to fetch dividends for '${uniqueID}', err: ${error}`);
    errors.push(error);
  }
  
  // Splits
  try {
    const splitsCollection = db.collection("splits");
    if (await splitsCollection.findOne({ _i: uniqueID })) {
      console.logVerbose(`Found existing split for unique ID '${uniqueID}'`);
      
    } else {
      console.logVerbose(`Splits for unique ID '${uniqueID}' not found. Fetching and fixing data...`);
      const splits = await fetch(`/stock/${symbol}/splits/${periodParameter}`)
        .then(x => fixSplits(x, uniqueID));

      if (splits.length) {
        console.logVerbose(`Inserting splits for unique ID '${uniqueID}'...`);
        splitsCollection.insertMany(splits);
        console.log(`Splits for unique ID '${uniqueID}' successfully inserted`);

      } else {
        console.logVerbose(`Splits are empty for unique ID '${uniqueID}'`);
      }
    }
  } catch(error) {
    console.error(`Unable to fetch splits for '${uniqueID}', err: ${error}`);
    errors.push(error);
  }
    
  // Historical price
  
  // Checking if there are records already
  var latestHistoricalPrice = null;
  const historicalPricesCollection = db.collection("historical-prices");
  try {
    const latestHistoricalPriceArray = await historicalPricesCollection
      .find({ _i: uniqueID })
      .sort({ d: -1 })
      .limit(1)
      .toArray();

    if (latestHistoricalPriceArray.length) {
      console.logVerbose(`Found existing historical price for unique ID '${uniqueID}'`);
      latestHistoricalPrice = latestHistoricalPriceArray[0];
      
    } else {
      // https://sandbox.iexapis.com/stable/stock/AAPL/chart/6y?token=Tpk_0a569a8bdc864334b284cb7112f579df&chartCloseOnly=true&chartInterval=21
      const historicalPrices = await fetch(`/stock/${symbol}/chart/${periodParameter}`, { 
          chartCloseOnly: true, 
          chartInterval: 21 
        })
        .then(x => fixHistoricalPrices(x, uniqueID));

      if (historicalPrices.length) {
        console.logVerbose(`Inserting historical prices for unique ID '${uniqueID}'...`);
        historicalPricesCollection.insertMany(historicalPrices);
        latestHistoricalPrice = historicalPrices[historicalPrices.length - 1];
        console.log(`Historical prices for unique ID '${uniqueID}' successfully inserted`);
        
      } else {
        console.error(`Received empty array of historical prices for unique ID '${uniqueID}'`);
      }
    }
  } catch(error) {
    console.error(`Unable to fetch historical price for '${uniqueID}', err: ${error}`);
    errors.push(error);
  }

  // Previous day price
  try {
    const previousDayPrices = db.collection("previous-day-prices");
    var previousDayPrice = await previousDayPrices.findOne({ "_id": uniqueID });
    if (previousDayPrice) {
      console.logVerbose(`Found existing previous day price for unique ID '${uniqueID}'`);

    } else {
      console.logVerbose(`Previous day price for unique ID '${uniqueID}' not found. Fetching and fixing data...`);
      previousDayPrice = await fetch(`/stock/${symbol}/previous`)
        .then (x => fixPreviousDayPrice(x, uniqueID));
      
      if (previousDayPrice) {
        console.logVerbose(`Inserting previous day price for unique ID '${uniqueID}'...`);
        previousDayPrices.insertOne(previousDayPrice);
        console.log(`Previous day price for unique ID '${uniqueID}' successfully inserted`);
      } else {
        console.error(`Unable to insert '${previousDayPrice}' previous day price for unique ID '${uniqueID}'`);
      }
    }

    // We might also need to update historical prices here because tomorrow during an update might be too late.
    const yesterdayCloseDate = getCloseDate(Date.yesterday());
    if (latestHistoricalPrice && previousDayPrice && yesterdayCloseDate.daysTo(latestHistoricalPrice.d) > 29) {
      const historicalPrice = {};
      historicalPrice._i = previousDayPrice._id;
      historicalPrice._p = previousDayPrice._p;
      historicalPrice.c = previousDayPrice.c;
      historicalPrice.d = yesterdayCloseDate;
      historicalPricesCollection.insertOne(historicalPrice);
      console.log(`Latest historical price for unique ID '${uniqueID}' successfully inserted: ${historicalPrice.stringify()}`);

    } else {
      console.logVerbose(`Historical prices for unique ID '${uniqueID}' is up to date`);
    }

  } catch(error) {
    console.error(`Unable to fetch previous day price for '${uniqueID}', err: ${error}`);
    errors.push(error);
  }

  // Quote
  try {
    const quotes = db.collection("quotes");
    var quote = await quotes.findOne({ "_id": uniqueID });
    if (quote) {
      console.logVerbose(`Found existing quote for unique ID '${uniqueID}'`);

    } else {
      console.logVerbose(`Quote for unique ID '${uniqueID}' not found. Fetching and fixing data...`);
      quote = await fetch(`/stock/${symbol}/quote`)
        .then (x => fixQuote(x, uniqueID));
      
      if (quote) {
        console.logVerbose(`Inserting quote for unique ID '${uniqueID}'...`);
        quotes.insertOne(quote);
        console.log(`Quote for unique ID '${uniqueID}' successfully inserted`);
      } else {
        console.error(`Unable to insert '${quote}' quote for unique ID '${uniqueID}'`);
      }
    }

  } catch(error) {
    console.error(`Unable to fetch quote for '${uniqueID}', err: ${error}`);
    errors.push(error);
  }
  
  if (errors.length) {
    throw errors;
  } else {
    console.log(`Fetched all missing data for the '${symbol}'`);
  }
};

// exports('AAP');
// exports('RPT-D', 'RPT-D:NYS');
