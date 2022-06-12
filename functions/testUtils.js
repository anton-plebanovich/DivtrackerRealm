
// testUtils.js

//////////////////////////////////////////////////////////////////// Constants

const defaultTransactionsCount = 100;
const averagePrice = 109.02;

const _defaultAsyncOperations = 15;
defaultAsyncOperations = _defaultAsyncOperations;

const _defaultAsyncTransactionsCount = 15;
defaultAsyncTransactionsCount = _defaultAsyncTransactionsCount;

//////////////////////////////////////////////////////////////////// Functions

async function _cleanup() {
  await Promise.all([
    db.collection('companies').deleteMany({}),
    db.collection('dividends').deleteMany({}),
    db.collection('historical-prices').deleteMany({}),
    db.collection('previous-day-prices').deleteMany({}),
    db.collection('quotes').deleteMany({}),
    db.collection('splits').deleteMany({}),
    db.collection('transactions').deleteMany({})
  ]);
}

cleanup = _cleanup;

async function _cleanupSymbols() {
  const operations = sources.map(source => source.db.collection('symbols').deleteMany({}));
  const mergedSymbolsOperation = atlas.db("merged").collection("symbols").deleteMany({});
  operations.push(mergedSymbolsOperation);
  await Promise.all(operations);
}

cleanupSymbols = _cleanupSymbols;

async function _restoreSymbols() {
  await _cleanupSymbols();
  await context.functions.execute("updateSymbolsV2");
  await context.functions.execute("mergedUpdateSymbols");
  await context.functions.execute("fmpUpdateSymbols");
  await context.functions.execute("mergedUpdateSymbols");
}

restoreSymbols = _restoreSymbols;

async function _prepareFMPData() {
  if (environment !== 'tests') {
    return;
  }
  
  context.functions.execute("fmpUtils");

  // Cleanup FMP environment
  await Promise.all([
    fmp.collection('companies').deleteMany({}),
    fmp.collection('data-status').deleteMany({}),
    fmp.collection('dividends').deleteMany({}),
    fmp.collection('historical-prices').deleteMany({}),
    fmp.collection('quotes').deleteMany({}),
    fmp.collection('splits').deleteMany({}),
    fmp.collection('symbols').deleteMany({}),
    atlas.db("merged").collection("symbols").deleteMany({}),
  ]);
  
  // Insert 20 symbols
  const symbols = await fetchSymbols();
  const symbolsToAdd = symbols.getRandomElements(19);
  const pciSymbol = symbols.find(x => x.t === "DHROX");
  symbolsToAdd.push(pciSymbol);
  await fmp.collection('symbols').insertMany(symbolsToAdd);
  await context.functions.execute("mergedUpdateSymbols");
  await context.functions.execute("fmpLoadMissingData");

  context.functions.execute("iexUtils");
}

prepareFMPData = _prepareFMPData;

//////////////////////////////////////////////////////////////////// SYMBOLS OPERATIONS

/**
 * Generates random transactions
 */
async function _generateRandomTransactions(count, symbols) {
  if (count == null) {
    count = defaultTransactionsCount;
  }

  if (symbols == null) {
    symbols = await db.collection('symbols').find({ e: null }).toArray();
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
  const symbolsCount = symbols.length;
  const transactions = [];
  for(var i = 0; i < count; i++){
    const symbolIndex = Math.floor(Math.random() * symbolsCount);
    const symbolID = symbols[symbolIndex]._id;
    const transaction = {};
    transaction.s = symbolID;

    // 0.05 - 1.5 price coef
    const coef = 0.05 + 1.5 * Math.random();
    transaction.p = BSON.Double(coef * averagePrice);
    transaction.d = randomDate(new Date(2016, 0, 1), new Date());

    const random = Math.random();
    if (random <= 0.5) {
      const amount = Math.round(Math.random() * 100);
      transaction.a = BSON.Double(amount);

    } else {
      const amount = Math.round(Math.random() * 10000) / 100;
      transaction.a = BSON.Double(amount);
    }

    if (random >= 0.5) {
      transaction.c = BSON.Double(Math.random());
    }

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
    .map(x => x.s.toString())
    .distinct();

  const distinctSymbolIDsCount = distinctSymbolIDs.length;

  const companiesCount = await db.collection('companies').count({});
  if (companiesCount !== distinctSymbolIDsCount) {
    const missedSymbolIDs = await getMissedSymbolIDs('companies', '_id', distinctSymbolIDs);
    // Some symbols doesn't have company or other data. E.g. YSAC+
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
    // Using threshold because previous day prices for some symbols are actually just `null`
    const allowedMissingCount = Math.ceil(distinctSymbolIDsCount * 0.35);
    if (distinctSymbolIDsCount - previousDayPricesCount > allowedMissingCount) {
      errors.push([`Some previous day prices weren't inserted (${previousDayPricesCount}/${distinctSymbolIDsCount}): ${missedSymbolIDs}`]);
    } else {
      console.log(`Some previous day prices weren't inserted (${previousDayPricesCount}/${distinctSymbolIDsCount}): ${missedSymbolIDs}`);
    }
  }

  const splitsCount = await db.collection('splits').count({});
  if (splitsCount <= 0) {
    console.log(`No splits`);
  }

  const quotesCount = await db.collection('quotes').count({});
  if (quotesCount !== distinctSymbolIDsCount) {
    const missedSymbolIDs = await getMissedSymbolIDs('quotes', '_id', distinctSymbolIDs);
    // Using threshold because quotes for some symbols are actually just `null`. E.g. MULG
    const allowedMissingCount = Math.ceil(distinctSymbolIDsCount * 0.35);
    if (distinctSymbolIDsCount - quotesCount > allowedMissingCount) {
      errors.push([`Some quotes weren't inserted (${previousDayPricesCount}/${distinctSymbolIDsCount}): ${missedSymbolIDs}`]);
    } else {
      console.log(`Some quotes weren't inserted (${previousDayPricesCount}/${distinctSymbolIDsCount}): ${missedSymbolIDs}`);
    }
  }
  
  if (errors.length) {
    throw errors;
  } else {
    console.log("SUCCESS!");
  }
}

checkData = _checkData;

async function getMissedSymbolIDs(collection, key, distinctSymbolIDs) {
  const objects = await db.collection(collection).find({}, { [key]: 1 }).toArray();
  const actualSymbolIDs = objects.map(x => x[key].toString());
  return distinctSymbolIDs.filter(x => !actualSymbolIDs.includes(x));
}

//////////////////////////////////////////////////////////////////// Exports

exports = function() {
  context.functions.execute("fmpUtils");
  context.functions.execute("iexUtils");
  
  context.user.id = '61ae5154d9b3cb9ea55ec5c6';
}
