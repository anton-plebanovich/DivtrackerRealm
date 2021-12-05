
// testAddTransactions.js

/**
 * @example
   context.user.id = '618600d6cab8724b39b6df4b';
   exports();
 */
 exports = async function() {
  context.functions.execute("utils");

  // Cleanup environment
  await Promise.all([
    db.collection('companies').deleteMany( { } ),
    db.collection('dividends').deleteMany( { } ),
    db.collection('historical-prices').deleteMany( { } ),
    db.collection('previous-day-prices').deleteMany( { } ),
    db.collection('splits').deleteMany( { } ),
    db.collection('quotes').deleteMany( { } ),
    db.collection('transactions').deleteMany( { } )
  ]);

  // Add transactions
  const transactionsJSON = EJSON.parse('[{"s":"AAPL","e":"NAS","p":{"$numberDouble":"95.43"},"c":{"$numberDouble":"0.1"},"d":{"$date":{"$numberLong":"1596905100000"}},"a":{"$numberDouble":"25.1146"}},{"c":{"$numberDouble":"0.1"},"d":{"$date":{"$numberLong":"1596905100000"}},"p":{"$numberDouble":"95.4"},"e":"NYS","a":{"$numberDouble":"31.7138"},"s":"ABBV"},{"c":{"$numberDouble":"0.1"},"p":{"$numberDouble":"238.38"},"a":{"$numberDouble":"15"},"d":{"$date":{"$numberLong":"1596905100000"}},"s":"AMGN","e":"NAS"},{"p":{"$numberDouble":"224.83"},"e":"NYS","d":{"$date":{"$numberLong":"1596905100000"}},"c":{"$numberDouble":"0.1"},"a":{"$numberDouble":"12.1554"},"s":"AMT"},{"d":{"$date":{"$numberLong":"1636142399000"}},"s":"AVGO","p":{"$numberDouble":"335.84"},"e":"NAS","c":{"$numberDouble":"0.1"},"a":{"$numberDouble":"9.2854"}},{"s":"BABA","a":{"$numberDouble":"2"},"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"185.2"},"e":"NYS"},{"e":"NYS","d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"4.0744"},"p":{"$numberDouble":"249.13"},"s":"BDX"},{"p":{"$numberDouble":"53.12"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":"BMO","c":{"$numberDouble":"0.1"},"e":"NYS","a":{"$numberDouble":"8.289"}},{"s":"BMY","p":{"$numberDouble":"62.06"},"a":{"$numberDouble":"49.8607"},"d":{"$date":{"$numberLong":"1636142399000"}},"e":"NYS"},{"d":{"$date":{"$numberLong":"1636142399000"}},"s":"BTI","e":"NYS","p":{"$numberDouble":"35.48"},"a":{"$numberDouble":"87.4581"},"c":{"$numberDouble":"0.1"}},{"s":"CCI","p":{"$numberDouble":"160.82"},"c":{"$numberDouble":"0.1"},"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"8.2146"},"e":"NYS"},{"p":{"$numberDouble":"62.98"},"e":"NYS","d":{"$date":{"$numberLong":"1636142399000"}},"s":"CM","a":{"$numberDouble":"8.3819"}},{"s":"COST","d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"291.88"},"a":{"$numberDouble":"2.0692"},"e":"NAS"},{"e":"NYS","s":"CVX","a":{"$numberDouble":"16.6819"},"p":{"$numberDouble":"93.93"},"d":{"$date":{"$numberLong":"1636142399000"}}},{"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"5.0065"},"p":{"$numberDouble":"223.16"},"s":"DHR","e":"NYS"},{"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"49.54"},"s":"EMN","e":"NYS","a":{"$numberDouble":"10.375"}},{"p":{"$numberDouble":"29.95"},"d":{"$date":{"$numberLong":"1636142399000"}},"e":"NYS","s":"ENB","a":{"$numberDouble":"73.0022"}},{"a":{"$numberDouble":"3.1272"},"d":{"$date":{"$numberLong":"1636142399000"}},"e":"NYS","s":"ESS","p":{"$numberDouble":"199.72"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"25.5374"},"p":{"$numberDouble":"74.28"},"e":"NYS","s":"FRT"},{"e":"NYS","a":{"$numberDouble":"16.361"},"s":"GD","d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"157.85"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"s":"GILD","a":{"$numberDouble":"35.2918"},"p":{"$numberDouble":"63.89"},"e":"NAS"},{"d":{"$date":{"$numberLong":"1636142399000"}},"s":"HD","e":"NYS","p":{"$numberDouble":"266.78"},"a":{"$numberDouble":"12.15"}},{"e":"NYS","a":{"$numberDouble":"8.1551"},"s":"HRL","d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"41.84"}},{"a":{"$numberDouble":"6.3834"},"p":{"$numberDouble":"97.16"},"d":{"$date":{"$numberLong":"1636142399000"}},"e":"NYS","s":"IBM"},{"a":{"$numberDouble":"34.795"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":"INTC","p":{"$numberDouble":"48.14"},"e":"NAS"},{"s":"INTU","a":{"$numberDouble":"4.031"},"e":"NAS","p":{"$numberDouble":"257.26"},"d":{"$date":{"$numberLong":"1636142399000"}}},{"a":{"$numberDouble":"158.2324"},"e":"NYS","s":"IVZ","d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"9.28"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"131.07"},"a":{"$numberDouble":"15.4334"},"s":"JNJ","e":"NYS"},{"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"14.4227"},"e":"NYS","p":{"$numberDouble":"101.15"},"s":"JPM"},{"a":{"$numberDouble":"24.2797"},"e":"NYS","s":"KMB","d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"132.35"}},{"e":"NYS","s":"LMT","d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"347.51"},"a":{"$numberDouble":"15.297"}},{"s":"MA","d":{"$date":{"$numberLong":"1636142399000"}},"e":"NYS","a":{"$numberDouble":"1.0049"},"p":{"$numberDouble":"205.62"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"206.0397"},"s":"MAC","p":{"$numberDouble":"8.93"},"e":"NYS"},{"p":{"$numberDouble":"196.62"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":"MCD","e":"NYS","a":{"$numberDouble":"7.2698"}},{"a":{"$numberDouble":"4.1701"},"e":"NYS","p":{"$numberDouble":"156.08"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":"MMM"},{"s":"MPW","p":{"$numberDouble":"18.71"},"e":"NYS","d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"124.8775"}},{"p":{"$numberDouble":"72.4"},"e":"NYS","d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"42.4528"},"s":"MRK"},{"s":"MSFT","d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"11.1126"},"p":{"$numberDouble":"187.38"},"e":"NAS"},{"s":"MSM","a":{"$numberDouble":"17.2991"},"e":"NYS","d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"67.13"}},{"e":"NYS","a":{"$numberDouble":"32.4315"},"s":"NEE","p":{"$numberDouble":"71.02"},"d":{"$date":{"$numberLong":"1636142399000"}}},{"e":"NYS","a":{"$numberDouble":"26.7643"},"s":"NHI","p":{"$numberDouble":"46.16"},"d":{"$date":{"$numberLong":"1636142399000"}}},{"s":"NNN","d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"36.43"},"a":{"$numberDouble":"55.4463"},"e":"NYS"},{"a":{"$numberDouble":"82.7621"},"s":"O","d":{"$date":{"$numberLong":"1636142399000"}},"e":"NYS","p":{"$numberDouble":"63.61"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"89.4892"},"s":"OKE","p":{"$numberDouble":"27.98"},"e":"NYS"},{"a":{"$numberDouble":"41.8935"},"s":"OZK","e":"NAS","p":{"$numberDouble":"23.46"},"d":{"$date":{"$numberLong":"1636142399000"}}},{"a":{"$numberDouble":"207.5098"},"s":"PBA","d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"21.97"},"e":"NYS"},{"e":"NAS","p":{"$numberDouble":"136.47"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":"PEP","a":{"$numberDouble":"22.5504"}},{"s":"PFE","e":"NYS","d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"51.041"},"p":{"$numberDouble":"36.13"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"106.69"},"a":{"$numberDouble":"7.256"},"s":"PKG","e":"NYS"},{"s":"QCOM","d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"122.17"},"a":{"$numberDouble":"26.3479"},"e":"NAS"},{"s":"RPT-D","a":{"$numberDouble":"40"},"d":{"$date":{"$numberLong":"1636142399000"}},"e":"NYS","p":{"$numberDouble":"35.09"}},{"e":"NYS","p":{"$numberDouble":"56.37"},"s":"RTX","d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"27.6941"}},{"e":"NYS","a":{"$numberDouble":"10.3823"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":"SJM","p":{"$numberDouble":"102.51"}},{"a":{"$numberDouble":"22.1529"},"s":"SLG","e":"NYS","d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"56.89"}},{"s":"SOXX","e":"NAS","p":{"$numberDouble":"423.56"},"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"3.0035"}},{"e":"NYS","d":{"$date":{"$numberLong":"1636142399000"}},"s":"STOR","p":{"$numberDouble":"22"},"a":{"$numberDouble":"120.4095"}},{"e":"NYS","s":"SWCH","a":{"$numberDouble":"100.574"},"p":{"$numberDouble":"16.18"},"d":{"$date":{"$numberLong":"1636142399000"}}},{"d":{"$date":{"$numberLong":"1636142399000"}},"s":"SWKS","p":{"$numberDouble":"135.64"},"e":"NAS","a":{"$numberDouble":"18.1612"}},{"e":"NYS","s":"SWX","d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"61.54"},"a":{"$numberDouble":"27.3619"}},{"e":"NYS","a":{"$numberDouble":"4.0434"},"p":{"$numberDouble":"186.83"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":"SYK"},{"a":{"$numberDouble":"104.0041"},"p":{"$numberDouble":"30.37"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":"T","e":"NYS"},{"a":{"$numberDouble":"10.4002"},"p":{"$numberDouble":"41.79"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":"TD","e":"NYS"},{"p":{"$numberDouble":"313.4"},"s":"TMO","d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"2.005"},"e":"NYS"},{"p":{"$numberDouble":"30.04"},"a":{"$numberDouble":"57.3166"},"e":"NYS","d":{"$date":{"$numberLong":"1636142399000"}},"s":"UGI"},{"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"335.31"},"e":"NYS","s":"UNH","a":{"$numberDouble":"6.042"}},{"s":"V","e":"NYS","a":{"$numberDouble":"7.0515"},"p":{"$numberDouble":"182.19"},"d":{"$date":{"$numberLong":"1636142399000"}}},{"a":{"$numberDouble":"15.6669"},"s":"VZ","p":{"$numberDouble":"57.45"},"e":"NYS","d":{"$date":{"$numberLong":"1636142399000"}}},{"d":{"$date":{"$numberLong":"1636142399000"}},"s":"WBA","a":{"$numberDouble":"23.1947"},"e":"NAS","p":{"$numberDouble":"54.3"}},{"p":{"$numberDouble":"107.69"},"e":"NYS","a":{"$numberDouble":"6.1199"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":"WMT"},{"d":{"$date":{"$numberLong":"1636142399000"}},"e":"NYS","p":{"$numberDouble":"67.75"},"s":"WPC","a":{"$numberDouble":"15.9095"}},{"p":{"$numberDouble":"51.27"},"a":{"$numberDouble":"32.6848"},"e":"NYS","d":{"$date":{"$numberLong":"1596905100000"}},"s":"XOM"}]');
  await context.functions.execute("addTransactions", transactionsJSON);

  const errors = [];

  // Check that all data is inserted
  const transactionsCount = await db.collection('transactions').count({});
  if (transactionsCount != transactionsJSON.length) {
    errors.push([`Some transactions weren't inserted (${transactionsCount}/${transactionsJSON.length})`]);
  }

  const distinctCompaniesCount = transactionsJSON
    .map(x => x.s)
    .distinct()
    .length;

  const companiesCount = await db.collection('companies').count({});
  if (companiesCount != distinctCompaniesCount) {
    errors.push([`Some companies weren't inserted (${companiesCount}/${distinctCompaniesCount})`]);
  }

  const dividendsCount = await db.collection('dividends').count({});
  if (dividendsCount <= 0) {
    errors.push([`Dividends weren't inserted`]);
  }

  const historicalPricesCount = await db.collection('historical-prices').count({});
  if (historicalPricesCount <= 0) {
    errors.push([`Historical prices weren't inserted`]);
  }

  const previousDayPricesCount = await db.collection('previous-day-prices').count({});
  if (previousDayPricesCount != companiesCount) {
    errors.push([`Some previous day prices weren't inserted (${previousDayPricesCount}/${companiesCount})`]);
  }

  const splitsCount = await db.collection('splits').count({});
  if (splitsCount <= 0) {
    errors.push([`Splits weren't inserted`]);
  }

  const quotesCount = await db.collection('companies').count({});
  if (quotesCount != companiesCount) {
    errors.push([`Some quotes weren't inserted (${quotesCount}/${companiesCount})`]);
  }

  // Check update functions
  // TODO: Make it more complex and move to separate test case
  await Promise.all([
    context.functions.execute("updateCompanies"),
    context.functions.execute("updateDividends"),
    context.functions.execute("updatePrices"),
    context.functions.execute("updateQuotes"),
    context.functions.execute("updateSplits"),
    context.functions.execute("updateSymbols"),
  ]);
  
  if (errors.length) {
    throw errors;
  } else {
    console.log("SUCCESS!");
  }
};
