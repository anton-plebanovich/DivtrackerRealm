
// fmpGetData.js

exports = async function(date) {
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
    find.$or = [
      { _id: { $gte: BSON.ObjectId.fromDate(date) } },
      { u: { $gte: date } }
    ];
  }

  const collections = [
    'companies',
    'dividends',
    'historical-prices',
    'quotes',
    'splits',
    'symbols',
  ];

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
