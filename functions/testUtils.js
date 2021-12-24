
// testUtils.js

//////////////////////////////////////////////////////////////////// Constants

const defaultTransactionsCount = 200;

const _defaultAsyncOperations = 10;
defaultAsyncOperations = _defaultAsyncOperations;

//////////////////////////////////////////////////////////////////// Functions

async function _cleanup() {
  const db = context.services.get("mongodb-atlas").db("divtracker-v2")

  return await Promise.all([
    db.collection('companies').deleteMany({}),
    db.collection('dividends').deleteMany({}),
    db.collection('historical-prices').deleteMany({}),
    db.collection('previous-day-prices').deleteMany({}),
    db.collection('quotes').deleteMany({}),
    db.collection('splits').deleteMany({}),
    db.collection('transactions').deleteMany({})
  ]);
};

cleanup = _cleanup;

/**
 * Generates random transactions
 */
async function _generateRandomTransactions(count) {
  if (count == null) {
    count = defaultTransactionsCount;
  }

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
  const symbols = await db.collection('symbols').find({ e: null }).toArray();
  const symbolsCount = symbols.length;
  const transactions = [];
  for(var i = 0; i < count; i++){
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

  return transactions;
}

generateRandomTransactions = _generateRandomTransactions;

function randomDate(start, end) {
  return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
}

/**
 * Checks data integrity
 */
async function _checkData(transactions) {
  const errors = [];

  // Check that all data is inserted
  const transactionsCount = await db.collection('transactions').count({});
  if (transactionsCount !== transactions.length) {
    errors.push([`Some transactions weren't inserted (${transactionsCount}/${transactions.length})`]);
  }

  const distinctSymbolIDs = transactions
    .map(x => x.s)
    .distinct();

  const distinctSymbolIDsCount = distinctSymbolIDs.length;

  const companiesCount = await db.collection('companies').count({});
  if (companiesCount !== distinctSymbolIDsCount) {
    const missedSymbolIDs = await getMissedSymbolIDs('companies', '_id', distinctSymbolIDs);
    errors.push([`Some companies weren't inserted (${companiesCount}/${distinctSymbolIDsCount}): ${missedSymbolIDs}`]);
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
  if (previousDayPricesCount !== distinctSymbolIDsCount) {
    const missedSymbolIDs = await getMissedSymbolIDs('previous-day-prices', '_id', distinctSymbolIDs);
    errors.push([`Some previous day prices weren't inserted (${previousDayPricesCount}/${distinctSymbolIDsCount}): ${missedSymbolIDs}`]);
  }

  const splitsCount = await db.collection('splits').count({});
  if (splitsCount <= 0) {
    errors.push([`Splits weren't inserted`]);
  }

  const quotesCount = await db.collection('quotes').count({});
  if (quotesCount !== distinctSymbolIDsCount) {
    const missedSymbolIDs = await getMissedSymbolIDs('quotes', '_id', distinctSymbolIDs);
    errors.push([`Some quotes weren't inserted (${quotesCount}/${distinctSymbolIDsCount}): ${missedSymbolIDs}`]);
  }
  
  if (errors.length) {
    throw errors;
  } else {
    console.log("SUCCESS!");
  }
}

checkData = _checkData;

async function getMissedSymbolIDs(collection, key, distinctSymbolIDs) {
  let actualSymbolIDs = await db.collection(collection).find({}).toArray();
  actualSymbolIDs.map(x => x[key]);
  return distinctSymbolIDs.filter(x => !actualSymbolIDs.includesObject(x));
}

//////////////////////////////////////////////////////////////////// Exports

exports = function() {
  context.functions.execute("utilsV2");
  
  context.user.id = '61ae5154d9b3cb9ea55ec5c6';
}
