
// testAddTransactions.js

function randomDate(start, end) {
  return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
}

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

  // Generate random transactions
  // {
  //   "s": {
  //     "$oid": "61b102c0048b84e9c13e4564"
  //   },
  //   "p": {
  //     "$numberDouble": "95.43"
  //   },
  //   "c": {
  //     "$numberDouble": "0.1"
  //   },
  //   "d": {
  //     "$date": {
  //       "$numberLong": "1596905100000"
  //     }
  //   },
  //   "a": {
  //     "$numberDouble": "25.1146"
  //   }
  // }
  const symbols = await db.collection('symbols').find().toArray();
  const symbolsCount = symbols.length;
  const transactions = [];
  for(var i = 0; i < 200; i++){
    const symbolIndex = Math.floor(Math.random() * symbolsCount);
    const symbolID = symbols[symbolIndex]._id;
    const transaction = {};
    transaction.s = symbolID;
    transaction.p = BSON.Double(Math.random() * 100000);
    if (Math.random() >= 0.5) {
      transaction.c = BSON.Double(Math.random() * 1000);
    }

    transaction.d = randomDate(new Date(2016, 0, 1), new Date());
    transaction.a = BSON.Double(Math.random() * 10000);

    console.log(`Adding transaction: ${transaction.stringify()}`);
    transactions.push(transaction);
  }
  
  await context.functions.execute("addTransactionsV2", transactions);

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
