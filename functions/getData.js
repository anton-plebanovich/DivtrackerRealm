
// getData.js

// TODO: Improve response structure in V2
exports = async function(date, collections, symbols, fullSymbolsCollections) {
  context.functions.execute("utils");

  // Create new date and round to seconds because Realm date type does not support millis
  const lastUpdateDate = new Date();
  lastUpdateDate.setMilliseconds(0);

  const maxSymbolsCount = 1000;

  const allCollections = [
    'companies',
    'dividends',
    'exchange-rates',
    'historical-prices',
    'quotes',
    'splits',
    'symbols',
    'updates',
  ];

  const singularSymbolCollections = [
    'companies',
    'quotes',
    'symbols',
  ];
  
  const allowedFullSymbolsCollections = [
    "exchange-rates", 
    "symbols", 
    "updates"
  ];
  
  // Collections that have objects with non-searchable ID, e.g. 'USD' for 'exchange-rates'
  const nonSearchableIDCollections = [
    'exchange-rates',
    'updates',
  ];
  
  if (date != null) {
    throwIfNotDate(
      date, 
      `Please pass date as the first argument.`, 
      UserError
    );
    
    if (date > lastUpdateDate) {
      logAndThrow(`Invalid last update date parameter. Passed date '${date} (${date.getTime()})' is higher than the new update date: ${lastUpdateDate} (${lastUpdateDate.getTime()})`);
    }
  }

  if (collections != null) {
    throwIfEmptyArray(
      collections, 
      `Please pass collections array as the second argument. It may be null but must not be empty. Valid collections are: ${allCollections}.`, 
      UserError
    );

    throwIfNotString(
      collections[0], 
      `Please pass collections array as the second argument. Valid collections are: ${allCollections}.`, 
      UserError
    );

    const excessiveCollections = collections.filter(x => !allCollections.includes(x));
    if (excessiveCollections.length) {
      logAndThrow(`Invalid collections array as the second argument: ${excessiveCollections}. Valid collections are: ${allCollections}.`);
    }
    
  } else {
    collections = allCollections;
  }
  
  if (symbols != null) {
    throwIfEmptyArray(
      symbols, 
      `Please pass symbols array as the third argument. It may be null but must not be empty.`, 
      UserError
    );

    throwIfNotObjectId(
      symbols[0], 
      `Please pass symbols array as the third argument.`, 
      UserError
    );
  
    if (symbols.length > maxSymbolsCount) {
      logAndThrow(`Max collections count '${maxSymbolsCount}' is exceeded. Please make sure there are less than 1000 unique symbols in the portfolio.`);
    }
  }

  if (fullSymbolsCollections != null) {
    throwIfNotArray(
      fullSymbolsCollections, 
      `Please pass full symbol collections array as the fourth argument. It may be null. Valid collections are: ${allCollections}.`, 
      UserError
    );

    if (fullSymbolsCollections.length) {
      throwIfNotString(
        fullSymbolsCollections[0], 
        `Please pass full symbol collections array as the fourth argument. Valid collections are: ${allCollections}.`, 
        UserError
      );
  
      // Check that there are no unexpected all symbols collections
      const excessiveFullSymbolsCollections = fullSymbolsCollections.filter(x => !allowedFullSymbolsCollections.includes(x));
      if (excessiveFullSymbolsCollections.length) {
        logAndThrow(`Invalid full symbol collections array as the fourth argument: ${excessiveFullSymbolsCollections}. Valid full symbol collections are: ${allowedFullSymbolsCollections}.`);
      }
    }
  } else {
    fullSymbolsCollections = [];
  }

  // exchange-rates are always ignoring symbols when requested
  if (!fullSymbolsCollections.includes('exchange-rates')) {
    fullSymbolsCollections.push('exchange-rates');
  }

  // updates are always ignoring symbols when requested
  if (!fullSymbolsCollections.includes('updates')) {
    fullSymbolsCollections.push('updates');
  }

  // Check that we do not request all data for collections that do not support it
  if (symbols == null || symbols.length === 0) {
    const excessiveCollections = collections.filter(x => !allowedFullSymbolsCollections.includes(x));
    if (excessiveCollections.length > 0) {
      logAndThrow(`Invalid collections array as the second argument: ${collections}. Full data is not supported for: ${excessiveCollections}. Full data fetch allowed collections: ${allowedFullSymbolsCollections}. Please provide concrete symbols to fetch or remove not supported collections.`);
    }
  }

  const fmp = atlas.db("fmp");
  const iex = atlas.db("divtracker-v2");
  const projection = { u: false };
  const operations = collections.map(async collection => {
    const find = {};

    if (date != null) {
      // TODO: We might need to use date - 5 mins to prevent race conditions. I am not 100% sure because I don't know if MongoDB handles it properly.
      if (nonSearchableIDCollections.includes(collection)) {
        find.u = { $gte: date };

      } else {
        find.$or = [
          { _id: { $gte: BSON.ObjectId.fromDate(date) } },
          { u: { $gte: date } }
        ];
      }
    }

    if (symbols != null && !fullSymbolsCollections.includes(collection)) {
      if (singularSymbolCollections.includes(collection)) {
        find._id = { $in: symbols };
      } else {
        find.s = { $in: symbols };
      }
    }

    const [fmpObjects, iexObjects] = await Promise.all([
      fmp.collection(collection).find(find, projection).toArray(),
      iex.collection(collection).find(find, projection).toArray(),
    ]);

    // Add `f` flag to FMP symbols
    if (collection === 'symbols') {
      fmpObjects.forEach(x => x.f = true);
    }
    
    if (symbols == null && collection === 'symbols') {
      // Dedupe symbols when full data is returned to prevent collision.
      const enabledFMPTickers = fmpObjects
        .filter(x => x.e != false)
        .map(x => x.t);

      const filteredIEXObjects = iexObjects.filter(x => !enabledFMPTickers.includes(x.t));
      if (iexObjects.length != filteredIEXObjects.length) {
        const duplicates = iexObjects
          .filter(x => enabledFMPTickers.includes(x.t))
          .map(x => x.t);
          
        console.log(`Found duplicated tickers: ${duplicates}`)
      }

      return { [collection]: fmpObjects.concat(filteredIEXObjects) };
      
    } else {
      return { [collection]: fmpObjects.concat(iexObjects) };
    }
  });

  const operationResults = await Promise
    .all(operations)
    .mapErrorToSystem();

  const result = operationResults.reduce((result, operationResult) => {
    return Object.assign(result, operationResult);
  }, {});

  result.lastUpdateDate = lastUpdateDate;

  return result;
};
