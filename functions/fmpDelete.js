
// fmpDelete.js
// exports('SBER.ME', ['2020-07-15', '2020-05-13'])

exports = async function(ticker, dayStrings) {
  context.functions.execute("fmpUtils");

  throwIfUndefinedOrNull(
    ticker, 
    `Please provide ticker as the first argument`,
    UserError,
  );

  if (Object.prototype.toString.call(dayStrings) === '[object Array]') {
    // ok
  } else if (typeof dayStrings === 'string') {
    dayStrings = dayStrings.split(",").map(x => x.trim());
  } else {
    _logAndThrow(
      `Please pass non-empty days array as the second argument. Each day should be in the 'yyyy-mm-dd' format, e.g. 2020-12-30.`, 
      UserError,
    );
  }

  const symbolID = await fmp.collection("symbols")
    .findOne({ t: ticker })
    .then(x => x._id);

  const dates = dayStrings.map(x => new Date(`${x}T06:50:00.000+00:00`));
  
  const collection = fmp.collection("dividends");
  const existingDividends = await collection
    .find({ s: symbolID, x: { $ne: true } })
    .sort({ e: 1 })
    .toArray();
  
  const modifiedDivideds = existingDividends.map(x => Object.assign({}, x));
  for (const date of dates) {
    const existingDividend = modifiedDivideds.find(x => compareOptionalDates(x.e, date));
    if (existingDividend == null) {
      throw 'existingDividend is null';
    }
    
    existingDividend.x = true;
  }
  
  updateDividendsFrequency(modifiedDivideds);
  
  collection.safeUpdateMany(modifiedDivideds, existingDividends, '_id', true);
};
