
// fmpGetData.js

exports = async function(date, collections, symbols) {
  context.functions.execute("fmpUtils");

  if (date != null) {
    throwIfNotDate(
      date, 
      `Please pass date as the first argument.`, 
      UserError
    );
  }

  if (collections != null) {
    throwIfEmptyArray(
      symbols, 
      `Please pass collections array as the second argument. It may be null but must not be empty.`, 
      UserError
    );
  }

  if (symbols != null) {
    throwIfEmptyArray(
      symbols, 
      `Please pass symbols array as the third argument. It may be null but must not be empty.`, 
      UserError
    );
  }

  const find = {};
  if (date != null) {
    // TODO: We might need to use date - 5 mins to prevent race conditions. I am not 100% sure because I don't know if MongoDB handles it properly.

    find.$or = [
      { _id: { $gte: BSON.ObjectId.fromDate(date) } },
      { u: { $gte: date } }
    ];
  }

  if (collections == null) {
    collections = [
      'companies',
      'dividends',
      'historical-prices',
      'quotes',
      'splits',
      'symbols',
    ];
  }

  const singularSymbolCollections = [
    'companies',
    'quotes',
    'symbols',
  ]

  const operations = collections.map(async collection => {
    const _find = Object.assign({}, find);
    if (symbols != null) {
      if (singularSymbolCollections.includes(collection)) {
        _find._id = { $in: symbols }
      } else {
        _find.s = { $in: symbols }
      }
    }

    const objects = await fmp.collection(collection).find(_find).toArray();
    return { [collection]: objects };
  });

  const operationResults = await Promise
    .all(operations)
    .mapErrorToSystem();

  const result = operationResults.reduce((result, operationResult) => {
    return Object.assign(result, operationResult)
  }, {});

  return result;
};
