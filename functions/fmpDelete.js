
// fmpDelete.js

exports = async function(ticker, dayStrings) {
  context.functions.execute("fmpUtils");

  throwIfUndefinedOrNull(
    ticker, 
    `Please provide ticker as the first argument`,
    UserError,
  );

  throwIfEmptyArray(
    dayStrings, 
    `Please pass non-empty days array as the second argument. Each day should be in the 'yyyy-mm-dd' format, e.g. 2020-12-30.`, 
    UserError,
  );

  const symbolID = await fmp.collection("symbols")
    .findOne({ t: ticker })
    .then(x => x._id);

  const dates = dayStrings.map(x => new Date(`${x}T06:50:00.000+00:00`));
  
  const collection = fmp.collection("dividends");
  const existingDividends = await collection
    .find({ s: symbolID })
    .sort({ e: 1 })
    .toArray();
  
  let modifiedDivideds = existingDividends.map(x => Object.assign({}, x));
  for (const date of dates) {
    const existingDividend = modifiedDivideds.find(x => compareOptionalDates(x.e, date));
    if (existingDividend == null) {
      throw 'existingDividend is null';
    }
    
    existingDividend.x = true;
  }
  
  modifiedDivideds = updateDividendsFrequency(modifiedDivideds);
  
  collection.safeUpdateMany(modifiedDivideds, existingDividends, '_id', true);
};
