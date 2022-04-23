
// getData.js

exports = async function(date, collections, symbols, fullSymbolsCollections) {
  context.functions.execute("utils");

  const find = {};
  if (date != null) {
    throwIfNotDate(
      date, 
      `Please pass date as the first argument.`, 
      UserError
    );

    // TODO: We might need to use date - 5 mins to prevent race conditions. I am not 100% sure because I don't know if MongoDB handles it properly.
    find.$or = [
      { _id: { $gte: BSON.ObjectId.fromDate(date) } },
      { u: { $gte: date } }
    ];
  }

  const allCollections = [
    'companies',
    'dividends',
    'historical-prices',
    'quotes',
    'splits',
    'symbols',
  ];

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
      _logAndThrow(`Invalid collections array as the second argument: ${excessiveCollections}. Valid collections are: ${allCollections}.`);
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
  
      const excessiveCollections = fullSymbolsCollections.filter(x => !allCollections.includes(x));
      if (excessiveCollections.length) {
        _logAndThrow(`Invalid full symbol collections array as the fourth argument: ${excessiveCollections}. Valid full symbol collections are: ${allCollections}.`);
      }
    }
  } else {
    fullSymbolsCollections = [];
  }

  const singularSymbolCollections = [
    'companies',
    'quotes',
    'symbols',
  ];

  const fmp = atlas.db("fmp");
  const iex = atlas.db("iex");
  const operations = collections.map(async collection => {
    const _find = Object.assign({}, find);
    if (symbols != null && !fullSymbolsCollections.includes(collection)) {
      if (singularSymbolCollections.includes(collection)) {
        _find._id = { $in: symbols };
      } else {
        _find.s = { $in: symbols };
      }
    }

    const [fmpObjects, iexObjects] = await Promise.all([
      fmp.collection(collection).find(_find).toArray(),
      iex.collection(collection).find(_find).toArray(),
    ]);
    
    return { [collection]: fmpObjects.concat(iexObjects) };
  });

  const operationResults = await Promise
    .all(operations)
    .mapErrorToSystem();

  const result = operationResults.reduce((result, operationResult) => {
    return Object.assign(result, operationResult);
  }, {});

  return result;
};
