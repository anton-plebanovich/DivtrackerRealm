
// fmpGetData.js

exports = async function(date, collections) {
  context.functions.execute("fmpUtils");

  if (date != null) {
    throwIfNotDate(
      date, 
      `Please pass date as the first argument.`, 
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

  const operations = collections.map(async collection => {
    const objects = await fmp.collection(collection).find(find).toArray();
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
