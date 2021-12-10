
// testAddTransactions.js

/**
 * @example
   context.user.id = '61ae5154d9b3cb9ea55ec5c6';
   exports();
 */
 exports = async function() {
  context.functions.execute("utilsV2");

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
  const transactionsJSON = EJSON.parse('[{"s":{"$oid":"61b102c0048b84e9c13e4564"},"p":{"$numberDouble":"95.43"},"c":{"$numberDouble":"0.1"},"d":{"$date":{"$numberLong":"1596905100000"}},"a":{"$numberDouble":"25.1146"}},{"c":{"$numberDouble":"0.1"},"d":{"$date":{"$numberLong":"1596905100000"}},"p":{"$numberDouble":"95.4"},"a":{"$numberDouble":"31.7138"},"s":{"$oid":"61b102c0048b84e9c13e456f"}},{"c":{"$numberDouble":"0.1"},"p":{"$numberDouble":"238.38"},"a":{"$numberDouble":"15"},"d":{"$date":{"$numberLong":"1596905100000"}},"s":{"$oid":"61b102c0048b84e9c13e4734"}},{"p":{"$numberDouble":"224.83"},"d":{"$date":{"$numberLong":"1596905100000"}},"c":{"$numberDouble":"0.1"},"a":{"$numberDouble":"12.1554"},"s":{"$oid":"61b102c0048b84e9c13e4759"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e48d0"},"p":{"$numberDouble":"335.84"},"c":{"$numberDouble":"0.1"},"a":{"$numberDouble":"9.2854"}},{"s":{"$oid":"61b102c0048b84e9c13e4924"},"a":{"$numberDouble":"2"},"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"185.2"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"4.0744"},"p":{"$numberDouble":"249.13"},"s":{"$oid":"61b102c0048b84e9c13e49a6"}},{"p":{"$numberDouble":"53.12"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e4a9d"},"c":{"$numberDouble":"0.1"},"a":{"$numberDouble":"8.289"}},{"s":{"$oid":"61b102c0048b84e9c13e4aa4"},"p":{"$numberDouble":"62.06"},"a":{"$numberDouble":"49.8607"},"d":{"$date":{"$numberLong":"1636142399000"}}},{"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e4b78"},"p":{"$numberDouble":"35.48"},"a":{"$numberDouble":"87.4581"},"c":{"$numberDouble":"0.1"}},{"s":{"$oid":"61b102c0048b84e9c13e4c30"},"p":{"$numberDouble":"160.82"},"c":{"$numberDouble":"0.1"},"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"8.2146"}},{"p":{"$numberDouble":"62.98"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e4d7b"},"a":{"$numberDouble":"8.3819"}},{"s":{"$oid":"61b102c0048b84e9c13e4e17"},"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"291.88"},"a":{"$numberDouble":"2.0692"}},{"s":{"$oid":"61b102c0048b84e9c13e4f05"},"a":{"$numberDouble":"16.6819"},"p":{"$numberDouble":"93.93"},"d":{"$date":{"$numberLong":"1636142399000"}}},{"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"5.0065"},"p":{"$numberDouble":"223.16"},"s":{"$oid":"61b102c0048b84e9c13e4fef"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"49.54"},"s":{"$oid":"61b102c0048b84e9c13e51ea"},"a":{"$numberDouble":"10.375"}},{"p":{"$numberDouble":"29.95"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e51f7"},"a":{"$numberDouble":"73.0022"}},{"a":{"$numberDouble":"3.1272"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e52a6"},"p":{"$numberDouble":"199.72"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"25.5374"},"p":{"$numberDouble":"74.28"},"s":{"$oid":"61b102c0048b84e9c13e5528"}},{"a":{"$numberDouble":"16.361"},"s":{"$oid":"61b102c0048b84e9c13e5621"},"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"157.85"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e5692"},"a":{"$numberDouble":"35.2918"},"p":{"$numberDouble":"63.89"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e5829"},"p":{"$numberDouble":"266.78"},"a":{"$numberDouble":"12.15"}},{"a":{"$numberDouble":"8.1551"},"s":{"$oid":"61b102c0048b84e9c13e58e5"},"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"41.84"}},{"a":{"$numberDouble":"6.3834"},"p":{"$numberDouble":"97.16"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e599d"}},{"a":{"$numberDouble":"34.795"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e5ac6"},"p":{"$numberDouble":"48.14"}},{"s":{"$oid":"61b102c0048b84e9c13e5acb"},"a":{"$numberDouble":"4.031"},"p":{"$numberDouble":"257.26"},"d":{"$date":{"$numberLong":"1636142399000"}}},{"a":{"$numberDouble":"158.2324"},"s":{"$oid":"61b102c0048b84e9c13e5b7f"},"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"9.28"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"131.07"},"a":{"$numberDouble":"15.4334"},"s":{"$oid":"61b102c0048b84e9c13e5c17"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"14.4227"},"p":{"$numberDouble":"101.15"},"s":{"$oid":"61b102c0048b84e9c13e5c33"}},{"a":{"$numberDouble":"24.2797"},"s":{"$oid":"61b102c0048b84e9c13e5ce2"},"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"132.35"}},{"s":{"$oid":"61b102c0048b84e9c13e5e28"},"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"347.51"},"a":{"$numberDouble":"15.297"}},{"s":{"$oid":"61b102c0048b84e9c13e5eb2"},"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"1.0049"},"p":{"$numberDouble":"205.62"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"206.0397"},"s":{"$oid":"61b102c0048b84e9c13e5eb7"},"p":{"$numberDouble":"8.93"}},{"p":{"$numberDouble":"196.62"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e5f0e"},"a":{"$numberDouble":"7.2698"}},{"a":{"$numberDouble":"4.1701"},"p":{"$numberDouble":"156.08"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e5ffc"}},{"s":{"$oid":"61b102c0048b84e9c13e6053"},"p":{"$numberDouble":"18.71"},"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"124.8775"}},{"p":{"$numberDouble":"72.4"},"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"42.4528"},"s":{"$oid":"61b102c0048b84e9c13e6063"}},{"s":{"$oid":"61b102c0048b84e9c13e6085"},"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"11.1126"},"p":{"$numberDouble":"187.38"}},{"s":{"$oid":"61b102c0048b84e9c13e608a"},"a":{"$numberDouble":"17.2991"},"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"67.13"}},{"a":{"$numberDouble":"32.4315"},"s":{"$oid":"61b102c0048b84e9c13e613f"},"p":{"$numberDouble":"71.02"},"d":{"$date":{"$numberLong":"1636142399000"}}},{"a":{"$numberDouble":"26.7643"},"s":{"$oid":"61b102c0048b84e9c13e6182"},"p":{"$numberDouble":"46.16"},"d":{"$date":{"$numberLong":"1636142399000"}}},{"s":{"$oid":"61b102c0048b84e9c13e61c9"},"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"36.43"},"a":{"$numberDouble":"55.4463"}},{"a":{"$numberDouble":"82.7621"},"s":{"$oid":"61b102c0048b84e9c13e62aa"},"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"63.61"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"89.4892"},"s":{"$oid":"61b102c0048b84e9c13e6300"},"p":{"$numberDouble":"27.98"}},{"a":{"$numberDouble":"41.8935"},"s":{"$oid":"61b102c0048b84e9c13e63ba"},"p":{"$numberDouble":"23.46"},"d":{"$date":{"$numberLong":"1636142399000"}}},{"a":{"$numberDouble":"207.5098"},"s":{"$oid":"61b102c0048b84e9c13e63f6"},"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"21.97"}},{"p":{"$numberDouble":"136.47"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e646b"},"a":{"$numberDouble":"22.5504"}},{"s":{"$oid":"61b102c0048b84e9c13e647e"},"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"51.041"},"p":{"$numberDouble":"36.13"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"106.69"},"a":{"$numberDouble":"7.256"},"s":{"$oid":"61b102c0048b84e9c13e64fe"}},{"s":{"$oid":"61b102c0048b84e9c13e667e"},"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"122.17"},"a":{"$numberDouble":"26.3479"}},{"s":{"$oid":"61b102c0048b84e9c13e6834"},"a":{"$numberDouble":"40"},"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"35.09"}},{"p":{"$numberDouble":"56.37"},"s":{"$oid":"61b102c0048b84e9c13e6852"},"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"27.6941"}},{"a":{"$numberDouble":"10.3823"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e69eb"},"p":{"$numberDouble":"102.51"}},{"a":{"$numberDouble":"22.1529"},"s":{"$oid":"61b102c0048b84e9c13e6a10"},"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"56.89"}},{"s":{"$oid":"61b102c0048b84e9c13e6ab1"},"p":{"$numberDouble":"423.56"},"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"3.0035"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e6b8e"},"p":{"$numberDouble":"22"},"a":{"$numberDouble":"120.4095"}},{"s":{"$oid":"61b102c0048b84e9c13e6be2"},"a":{"$numberDouble":"100.574"},"p":{"$numberDouble":"16.18"},"d":{"$date":{"$numberLong":"1636142399000"}}},{"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e6beb"},"p":{"$numberDouble":"135.64"},"a":{"$numberDouble":"18.1612"}},{"s":{"$oid":"61b102c0048b84e9c13e6bf3"},"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"61.54"},"a":{"$numberDouble":"27.3619"}},{"a":{"$numberDouble":"4.0434"},"p":{"$numberDouble":"186.83"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e6c00"}},{"a":{"$numberDouble":"104.0041"},"p":{"$numberDouble":"30.37"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e6c10"}},{"a":{"$numberDouble":"10.4002"},"p":{"$numberDouble":"41.79"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e6c64"}},{"p":{"$numberDouble":"313.4"},"s":{"$oid":"61b102c0048b84e9c13e6d1a"},"d":{"$date":{"$numberLong":"1636142399000"}},"a":{"$numberDouble":"2.005"}},{"p":{"$numberDouble":"30.04"},"a":{"$numberDouble":"57.3166"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e6e4f"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"335.31"},"s":{"$oid":"61b102c0048b84e9c13e6e7b"},"a":{"$numberDouble":"6.042"}},{"s":{"$oid":"61b102c0048b84e9c13e6ef5"},"a":{"$numberDouble":"7.0515"},"p":{"$numberDouble":"182.19"},"d":{"$date":{"$numberLong":"1636142399000"}}},{"a":{"$numberDouble":"15.6669"},"s":{"$oid":"61b102c0048b84e9c13e7052"},"p":{"$numberDouble":"57.45"},"d":{"$date":{"$numberLong":"1636142399000"}}},{"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e706b"},"a":{"$numberDouble":"23.1947"},"p":{"$numberDouble":"54.3"}},{"p":{"$numberDouble":"107.69"},"a":{"$numberDouble":"6.1199"},"d":{"$date":{"$numberLong":"1636142399000"}},"s":{"$oid":"61b102c0048b84e9c13e70e4"}},{"d":{"$date":{"$numberLong":"1636142399000"}},"p":{"$numberDouble":"67.75"},"s":{"$oid":"61b102c0048b84e9c13e70f1"},"a":{"$numberDouble":"15.9095"}},{"p":{"$numberDouble":"51.27"},"a":{"$numberDouble":"32.6848"},"d":{"$date":{"$numberLong":"1596905100000"}},"s":{"$oid":"61b102c0048b84e9c13e717d"}}]');
  await context.functions.execute("addTransactionsV2", transactionsJSON);

  const errors = [];

  // Check that all data is inserted
  const transactionsCount = await db.collection('transactions').count({});
  if (transactionsCount !== transactionsJSON.length) {
    errors.push([`Some transactions weren't inserted (${transactionsCount}/${transactionsJSON.length})`]);
  }

  const distinctCompaniesCount = transactionsJSON
    .map(x => x.s)
    .distinct()
    .length;

  const companiesCount = await db.collection('companies').count({});
  if (companiesCount !== distinctCompaniesCount) {
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
  if (previousDayPricesCount !== companiesCount) {
    errors.push([`Some previous day prices weren't inserted (${previousDayPricesCount}/${companiesCount})`]);
  }

  const splitsCount = await db.collection('splits').count({});
  if (splitsCount <= 0) {
    errors.push([`Splits weren't inserted`]);
  }

  const quotesCount = await db.collection('companies').count({});
  if (quotesCount !== companiesCount) {
    errors.push([`Some quotes weren't inserted (${quotesCount}/${companiesCount})`]);
  }
  
  if (errors.length) {
    throw errors;
  } else {
    console.log("SUCCESS!");
  }
};
