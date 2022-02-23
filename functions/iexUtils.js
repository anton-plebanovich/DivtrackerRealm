
// iexUtils.js

/** 
 * Computes and returns enabled in use symbols from companies and user transactions.
 * Returned symbols are shortened to `_id` and `s` fields.
 * @returns {Promise<[ShortSymbol]>} Array of short symbols.
*/
async function _getInUseShortSymbols() {
  // We combine transactions and companies distinct IDs. 
  // Idealy, we should be checking all tables but we assume that only two will be enough.
  // All symbols have company record so company DB contains all ever fetched symbols.
  // Meanwhile transactions may contain not yet fetched symbols or have less symbols than we already should be updating (transactions may be deleted).
  // So by combining we have all current + all future symbols. Idealy.
  const companiesCollection = db.collection("companies");
  const [companyIDs, distinctTransactionSymbolIDs] = await Promise.all([
    companiesCollection.distinct("_id", {}),
    _getDistinctTransactionSymbolIDs()
  ]);

  console.log(`Unique companies IDs (${companyIDs.length})`);
  console.logData(`Unique companies IDs (${companyIDs.length})`, companyIDs);

  // Compute distinct symbol IDs using both sources
  let symbolIDs = [...new Set(companyIDs.concat(distinctTransactionSymbolIDs))];

  const symbolsCollection = db.collection("symbols");
  const disabledSymbolIDs = await symbolsCollection.distinct("_id", { e: false });

  // Remove disabled symbols
  symbolIDs = symbolIDs.filter(x => !disabledSymbolIDs.includesObject(x));

  return await _getShortSymbols(symbolIDs);
};

getInUseShortSymbols = _getInUseShortSymbols;

/** 
 * Returns short symbols for symbol IDs
 * @param {[ObjectId]} symbolIDs
 * @returns {Promise<[ShortSymbol]>}
*/
async function _getShortSymbols(symbolIDs) {
  // Getting short symbols for IDs
  const symbolsCollection = db.collection("symbols");
  const shortSymbols = await symbolsCollection
    .find(
      { _id: { $in: symbolIDs } }, 
      { _id: 1, t: 1 }
    )
    .toArray();

  console.log(`Got short symbols (${shortSymbols.length}) for '${symbolIDs}'`);
  console.logData(`Got short symbols (${shortSymbols.length}) for '${symbolIDs}'`, shortSymbols);

  return shortSymbols;
};

getShortSymbols = _getShortSymbols;

/** 
 * @returns {Promise<[ObjectId]>} Array of unique transaction IDs
*/
async function _getDistinctTransactionSymbolIDs() {
  const transactionsCollection = db.collection("transactions");
  const distinctTransactionSymbolIDs = await transactionsCollection.distinct("s");
  distinctTransactionSymbolIDs.sort();

  console.log(`Distinct symbol IDs for transactions (${distinctTransactionSymbolIDs.length})`);
  console.logData(`Distinct symbol IDs for transactions (${distinctTransactionSymbolIDs.length})`, distinctTransactionSymbolIDs);

  return distinctTransactionSymbolIDs;
}

getDistinctTransactionSymbolIDs = _getDistinctTransactionSymbolIDs;

/** 
 * @returns {Promise<[ObjectId]>} Array of existing enabled symbol IDs, e.g. [ObjectId("61b102c0048b84e9c13e454d")]
*/
async function _getSupportedSymbolIDs() {
  const symbolsCollection = db.collection("symbols");
  const supportedSymbolIDs = await symbolsCollection.distinct("_id", { e: null });
  console.log(`Supported symbols (${supportedSymbolIDs.length})`);
  console.logData(`Supported symbols (${supportedSymbolIDs.length})`, supportedSymbolIDs);

  return supportedSymbolIDs;
};

getSupportedSymbolIDs = _getSupportedSymbolIDs;

///////////////////////////////////////////////////////////////////////////////// fetch.js

//////////////////////////////////// Tokens

// --- Sandbox (https://sandbox.iexapis.com/stable)
// Tpk_0a569a8bdc864334b284cb7112f579df - anton.plebanovich - Tsk_1d2c1478e285405984674a64db0f6073
// Tpk_ca8d3de2a6db4a58a61a93ac027e4725 - nohopenotop - Tsk_5e08d057b7044c8a8fec73f21083df12
// Tpk_c0f9be120e854f67b4e945d6eddad201 - nohopenotop+1 - Tsk_e1f2250214f64306bdd191d090bca196
// Tpk_9a73f18ccff14665881b21b54ca7da42 - nohopenotop+2 - Tsk_6c6f4cb4a8b145e782d17f344cdc4c42
// Tpk_d8f3a048a7a94866ad08c8b62042b16b - nohopenotop+3 - Tsk_c4dcd7a18ca14695a9c1697f54896ceb
// Tpk_581685f711114d9f9ab06d77506fdd49 - nohopenotop+4 - Tsk_732a341e427a42c99e14bb6bc3cd4eb8
// Tpk_b284e8658d5f4aa29d25ef84bf4d944e - nohopenotop+5 - Tsk_1104feb7c6de4d6c8236371142ce82b9

// --- Production (https://cloud.iexapis.com/stable)
// pk_9f1d7a2688f24e26bb24335710eae053 - anton.plebanovich - sk_9ca94a597d284cb7a8c5821dd8333fec
// pk_5d1af71fbfad462e9372b282c0db55e4 - nohopenotop - sk_1f3deae352b84e3a86584e8ab2a8d3c5
// pk_185c76a15c994a5c8447b07749203d94 - nohopenotop+1 - sk_60c63571a01b4a8ca155b024e9430e82
// pk_d6999cc3ea25413f92667cbbcd4aa9d2 - nohopenotop+2 - sk_46ac1f74991648c3a4ee170010b25784
// pk_069c02e17d9749a0b6a1284991879c77 - nohopenotop+3 - sk_4793d0f53a82435ba64fbb6a5d3ec334
// pk_462eaf9d7d94460a8751601cbc4c39e8 - nohopenotop+4 - sk_48a4856132604b47a77d96e64c1db39d
// pk_4e48fb2bb54f433ebbf42cf5bff3404f - nohopenotop+5 - sk_8a83efa05f144153a570966b4b4cea06

// --- Premium
// pk_01ef04dd60b5404b81d9cc47b2388176 - trackerdividend@gmail.com - sk_de6f102262874cfab3d9a83a6980e1db - 3ff51380e7f3a36ff4e0915e9d781878

/**
 * Default range to fetch.
 */
const defaultRange = '6y';

/**
 * Fetches companies in batch for short symbols.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @returns {[Company]} Array of requested objects.
 */
 fetchCompanies = async function fetchCompanies(shortSymbols) {
  throwIfUndefinedOrNull(shortSymbols, `fetchCompanies shortSymbols`);
  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);

  // https://cloud.iexapis.com/stable/stock/market/batch?token=pk_9f1d7a2688f24e26bb24335710eae053&types=company&symbols=AAPL,AAP
  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_581685f711114d9f9ab06d77506fdd49&types=company&symbols=AAPL,AAP
  return await _iexFetchBatchAndMapObjects('company', tickers, idByTicker, _fixCompany);
};

/**
 * Fetches dividends in batch for short symbols.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @param {boolean} isFuture Flag to fetch future or past dividends.
 * @param {string} range Range to fetch.
 * @returns {[Dividend]} Array of requested objects.
 */
fetchDividends = async function fetchDividends(shortSymbols, isFuture, range) {
  throwIfUndefinedOrNull(shortSymbols, `fetchDividends shortSymbols`);
  throwIfUndefinedOrNull(isFuture, `fetchDividends isFuture`);

  if (range == null) {
    range = defaultRange;
  }

  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);

  const parameters = { range: range };
  if (isFuture) {
    parameters.calendar = 'true';
  }
  
  // https://cloud.iexapis.com/stable/stock/market/batch?token=pk_9f1d7a2688f24e26bb24335710eae053&types=dividends&symbols=AAPL,AAP&range=6y&calendar=true
  // https://cloud.iexapis.com/stable/stock/market/batch?token=pk_9f1d7a2688f24e26bb24335710eae053&types=dividends&symbols=AAPL,AAP&range=6y
  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_581685f711114d9f9ab06d77506fdd49&types=dividends&symbols=AAPL,AAP&range=6y&calendar=true
  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_581685f711114d9f9ab06d77506fdd49&types=dividends&symbols=AAPL,AAP&range=6y
  return await _iexFetchBatchAndMapArray('dividends', tickers, idByTicker, _fixDividends, parameters);
};

/**
 * Fetches previous day prices in batch for short symbols.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @returns {[PreviousDayPrice]} Array of requested objects.
 */
 async function _fetchPreviousDayPrices(shortSymbols) {
  throwIfUndefinedOrNull(shortSymbols, `fetchPreviousDayPrices shortSymbols`);
  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);

  // https://cloud.iexapis.com/stable/stock/market/batch?token=pk_9f1d7a2688f24e26bb24335710eae053&types=previous&symbols=AAPL,AAP
  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_581685f711114d9f9ab06d77506fdd49&types=previous&symbols=AAPL,AAP
  return await _iexFetchBatchAndMapObjects('previous', tickers, idByTicker, _fixPreviousDayPrice);
};

fetchPreviousDayPrices = _fetchPreviousDayPrices;

/**
 * Fetches historical prices in batch for uniqueIDs.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @param {string} range Range to fetch.
 * @returns {[HistoricalPrice]} Array of requested objects.
 */
 fetchHistoricalPrices = async function fetchHistoricalPrices(shortSymbols, range) {
   throwIfUndefinedOrNull(shortSymbols, `fetchHistoricalPrices shortSymbols`);

  if (range == null) {
    range = defaultRange;
  }

  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);
  const parameters = { 
    range: range,
    chartCloseOnly: true, 
    chartInterval: 21 
  };

  // https://cloud.iexapis.com/stable/stock/market/batch?token=pk_9f1d7a2688f24e26bb24335710eae053&types=chart&symbols=AAPL,AAP&range=6y&chartCloseOnly=true&chartInterval=21
  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_581685f711114d9f9ab06d77506fdd49&types=chart&symbols=AAPL,AAP&range=6y&chartCloseOnly=true&chartInterval=21
  return await _iexFetchBatchAndMapArray('chart', tickers, idByTicker, _fixHistoricalPrices, parameters);
};

/**
 * Fetches quotes in batch for uniqueIDs.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @returns {[Quote]} Array of requested objects.
 */
 fetchQuotes = async function fetchQuotes(shortSymbols) {
  throwIfUndefinedOrNull(shortSymbols, `fetchQuotes shortSymbols`);
  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);

  // https://cloud.iexapis.com/stable/stock/market/batch?token=pk_9f1d7a2688f24e26bb24335710eae053&types=quote&symbols=AAPL,AAP
  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_581685f711114d9f9ab06d77506fdd49&types=quote&symbols=AAPL,AAP
  return await _iexFetchBatchAndMapObjects('quote', tickers, idByTicker, _fixQuote);
};

/**
 * Fetches splits in batch for uniqueIDs.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @param {string} range Range to fetch.
 * @returns {[Split]} Array of requested objects.
 */
 fetchSplits = async function fetchSplits(shortSymbols, range) {
  throwIfUndefinedOrNull(shortSymbols, `fetchSplits shortSymbols`);

  if (range == null) {
    range = defaultRange;
  }
  
  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);
  const parameters = { range: range };

  // https://cloud.iexapis.com/stable/stock/market/batch?types=splits&token=pk_9f1d7a2688f24e26bb24335710eae053&range=6y&symbols=AAPL,AAP
  // https://sandbox.iexapis.com/stable/stock/market/batch?types=splits&token=Tpk_581685f711114d9f9ab06d77506fdd49&range=6y&symbols=AAPL,AAP
  return await _iexFetchBatchAndMapArray('splits', tickers, idByTicker, _fixSplits, parameters);
};

// exports();
//
// iexFetchBatch('/dividends', ['AAP','AAPL','PBA'], { 'range': '90d' })

/**
 * Requests data from IEX cloud/sandbox for types and symbols by a batch.
 * Then, maps arrays data to our format in a flat array.
 * If symbols count exceed max allowed amount it splits it to several requests and returns composed result.
 * It switches between available tokens to evenly distribute the load.
 * @param {string} type Type to fetch, e.g. 'dividends'.
 * @param {[string]} tickers Ticker Symbols to fetch, e.g. ['AAP','AAPL','PBA'].
 * @param {[string]} idByTicker Dictionary of ticker symbol ID by ticker symbol.
 * @param {function} mapFunction Function to map data to our format.
 * @param {Object} queryParameters Additional query parameters.
 * @returns {Promise<[Object]>} Flat array of entities.
 */
async function _iexFetchBatchAndMapArray(type, tickers, idByTicker, mapFunction, queryParameters) {
  return _iexFetchBatchNew([type], tickers, queryParameters)
    .then(dataByTicker => {
      return tickers
        .map(ticker => {
          const tickerData = dataByTicker[ticker];
          if (tickerData != null && tickerData[type]) {
            return mapFunction(tickerData[type], idByTicker[ticker]);
          } else {
            return [];
          }
        })
        .flat()
      }
    );
}

/**
 * Requests data from IEX cloud/sandbox for types and symbols by a batch.
 * Then, maps objects data to our format in a array.
 * If symbols count exceed max allowed amount it splits it to several requests and returns composed result.
 * It switches between available tokens to evenly distribute the load.
 * @param {string} type Type to fetch, e.g. 'dividends'.
 * @param {[string]} tickers Ticker Symbols to fetch, e.g. ['AAP','AAPL','PBA'].
 * @param {[string]} idByTicker Dictionary of ticker symbol ID by ticker symbol.
 * @param {function} mapFunction Function to map data to our format.
 * @param {Object} queryParameters Additional query parameters.
 * @returns {Promise<[Object]>} Flat array of entities.
 */
async function _iexFetchBatchAndMapObjects(type, tickers, idByTicker, mapFunction, queryParameters) {
  return _iexFetchBatchNew([type], tickers, queryParameters)
    .then(dataByTicker =>
      tickers
        .compactMap(ticker => {
          const tickerData = dataByTicker[ticker];
          if (tickerData != null && tickerData[type]) {
            return mapFunction(tickerData[type], idByTicker[ticker]);
          } else {
            return null;
          }
        })
    );
}

/**
 * Requests data from IEX cloud/sandbox for types and symbols by a batch.
 * If symbols count exceed max allowed amount it splits it to several requests and returns composed result.
 * It switches between available tokens to evenly distribute the load.
 * @param {[string]} types Types to fetch, e.g. ['dividends'].
 * @param {[string]} tickers Ticker Symbols to fetch, e.g. ['AAP','AAPL','PBA'].
 * @param {Object} queryParameters Additional query parameters.
 * @returns {Promise<{string: {string: Object|[Object]}}>} Parsed EJSON object. Composed from several responses if max symbols count was exceeded. 
 * The first object keys are symbols. The next inner object keys are types. And the next inner object is an array of type objects.
 */
async function _iexFetchBatchNew(types, tickers, queryParameters) {
  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_d8f3a048a7a94866ad08c8b62042b16b&calendar=true&symbols=MSFT%2CAAPL&types=dividends&range=1y
  throwIfUndefinedOrNull(types, `types`);
  throwIfEmptyArray(tickers, `tickers`);

  if (queryParameters == null) {
    queryParameters = {};
  }

  const api = `/stock/market/batch`;
  const typesParameter = types.join(",");
  const maxSymbolsAmount = 100;
  const chunkedSymbolsArray = tickers.chunked(maxSymbolsAmount);
  var result = {};
  for (const chunkedSymbols of chunkedSymbolsArray) {
    const symbolsParameter = chunkedSymbols.join(",");
    const fullQueryParameters = Object.assign({}, queryParameters);
    fullQueryParameters.types = typesParameter;
    fullQueryParameters.symbols = symbolsParameter;

    console.log(`Fetching batch for symbols (${chunkedSymbols.length}) with query '${queryParameters.stringify()}': ${typesParameter} - ${symbolsParameter}`);
    
    let response
    try {
      response = await _iexFetch(api, fullQueryParameters);
    } catch(error) {
      if (error.statusCode == 404) {
        response = {};
      } else {
        throw error;
      }
    }

    result = Object.assign(result, response);
  }

  return result;
};

// exports();
//
// iexFetchBatchNew(['dividends'], ['AAP','AAPL','PBA'], { 'range': '1y', 'calendar': 'true' })

/**
 * Requests data from IEX cloud/sandbox depending on an environment. 
 * It switches between available tokens to evenly distribute the load.
 * @param {string} api API to call.
 * @param {Object} queryParameters Query parameters.
 * @returns Parsed EJSON object.
 */
async function _iexFetch(api, queryParameters) {

  if (queryParameters == null) {
    queryParameters = {}
  }
  
  // Use premium token if defined.
  const token = typeof premiumToken === 'undefined' ? tokens[counter % tokens.length] : premiumToken;
  counter++;
  queryParameters.token = token;

  const baseURL = context.values.get("base_url");

  return await fetch(baseURL, api, queryParameters);
}

iexFetch = _iexFetch;

// exports();
//
// iexFetch("/ref-data/symbols")
// iexFetch("/stock/JAZZ/company")
// iexFetch(`/stock/AAP/chart/1d`, { 
//     chartByDay: true,
//     chartCloseOnly: true,
//     exactDate: exactDateAPIString
//   })

///////////////////////////////////////////////////////////////////////////////// DATA FIX

/**
 * Fixes company object so it can be added to MongoDB.
 * @param {IEXCompany} iexCompany IEX Company object.
 * @param {ObjectId} symbolID Symbol object ID.
 * @returns {Company|null} Returns fixed object or `null` if fix wasn't possible.
 */
function _fixCompany(iexCompany, symbolID) {
  try {
    throwIfUndefinedOrNull(iexCompany, `fixCompany company`);
    throwIfUndefinedOrNull(symbolID, `fixCompany symbolID`);
  
    console.logVerbose(`Company data fix start`);
    const company = {};
    company._id = symbolID;
    company.i = iexCompany.industry;

    if  (iexCompany.issueType) {
      company.t = iexCompany.issueType.trim();
    }

    if  (iexCompany.companyName) {
      company.n = iexCompany.companyName.trim();
    }
  
    return company;
  } catch(error) {
    return null;
  }
};

fixCompany = _fixCompany;

/**
 * Fixes dividends object so it can be added to MongoDB.
 * @param {[IEXDividend]} iexDividends IEX dividends array.
 * @param {ObjectId} symbolID Symbol object ID.
 * @returns {[Dividend]} Returns fixed objects or an empty array if fix wasn't possible.
 */
function _fixDividends(iexDividends, symbolID) {
  try {
    throwIfUndefinedOrNull(iexDividends, `fixDividends iexDividends`);
    throwIfUndefinedOrNull(symbolID, `fixDividends uniqueID`);
    if (!iexDividends.length) { 
      console.logVerbose(`IEX dividends are empty for ${symbolID}. Nothing to fix.`);
      return []; 
    }
  
    console.logVerbose(`Fixing dividends for ${symbolID}`);
    return iexDividends
      .filterNull()
      .map(iexDividend => {
        const dividend = {};
        dividend.d = _getOpenDate(iexDividend.declaredDate);
        dividend.e = _getOpenDate(iexDividend.exDate);
        dividend.p = _getOpenDate(iexDividend.paymentDate);
        dividend.s = symbolID;

        if (iexDividend.amount != null) {
          dividend.a = BSON.Double(iexDividend.amount);
        }

        // We add only the first letter of a frequency
        if (iexDividend.frequency != null) {
          dividend.f = iexDividend.frequency.charAt(0);
        }
    
        // We do not add `USD` frequencies to the database.
        if (iexDividend.currency != null && iexDividend.currency !== "USD") {
          dividend.c = iexDividend.currency;
        }
    
        return dividend;
      });

  } catch(error) {
    return [];
  }
}

fixDividends = _fixDividends;

/**
 * Fixes previous day price object so it can be added to MongoDB.
 * @param {IEXPreviousDayPrice} iexPreviousDayPrice Previous day price object.
 * @param {ObjectId} symbolID Symbol object ID.
 * @returns {PreviousDayPrice|null} Returns fixed object or `null` if fix wasn't possible.
 */
function _fixPreviousDayPrice(iexPreviousDayPrice, symbolID) {
  try {
    throwIfUndefinedOrNull(iexPreviousDayPrice, `fixPreviousDayPrice iexPreviousDayPrice`);
    throwIfUndefinedOrNull(symbolID, `fixPreviousDayPrice symbolID`);
  
    console.logVerbose(`Previous day price data fix start`);
    const previousDayPrice = {};
    previousDayPrice._id = symbolID;

    if (iexPreviousDayPrice.close != null) {
      previousDayPrice.c = BSON.Double(iexPreviousDayPrice.close);
    }
  
    return previousDayPrice;

  } catch(error) {
    // {"AQNU":{"previous":null}}
    // {"AACOU":{"previous":null}}
    return null;
  }
};

fixPreviousDayPrice = _fixPreviousDayPrice;

/**
 * Fixes historical prices object so it can be added to MongoDB.
 * @param {[IEXHistoricalPrices]} iexHistoricalPrices Historical prices object.
 * @param {ObjectId} symbolID Symbol object ID.
 * @returns {[HistoricalPrices]} Returns fixed objects or an empty array if fix wasn't possible.
 */
function _fixHistoricalPrices(iexHistoricalPrices, symbolID) {
  try {
    throwIfUndefinedOrNull(iexHistoricalPrices, `fixHistoricalPrices iexHistoricalPrices`);
    throwIfUndefinedOrNull(symbolID, `fixHistoricalPrices uniqueID`);
    if (!iexHistoricalPrices.length) { 
      console.logVerbose(`Historical prices are empty for ${symbolID}. Nothing to fix.`);
      return []; 
    }
  
    console.logVerbose(`Fixing historical prices for ${symbolID}`);
    return iexHistoricalPrices
      .filterNull()
      .map(iexHistoricalPrice => {
        const historicalPrice = {};
        historicalPrice.d = _getCloseDate(iexHistoricalPrice.date);
        historicalPrice.s = symbolID;

        if (iexHistoricalPrice.close != null) {
          historicalPrice.c = BSON.Double(iexHistoricalPrice.close);
        }

        return historicalPrice;
      });

  } catch (error) {
    return [];
  }
};

fixHistoricalPrices = _fixHistoricalPrices;

/**
 * Fixes quote object so it can be added to MongoDB.
 * @param {IEXQuote} iexQuote Quote object.
 * @param {ObjectId} symbolID Symbol object ID.
 * @returns {Quote|null} Returns fixed object or `null` if fix wasn't possible.
 */
function _fixQuote(iexQuote, symbolID) {
  try {
    throwIfUndefinedOrNull(iexQuote, `fixQuote quote`);
    throwIfUndefinedOrNull(symbolID, `fixQuote symbolID`);
  
    console.logVerbose(`Previous day price data fix start`);
    const quote = {};
    quote._id = symbolID;
    quote.l = iexQuote.latestPrice;

    if (iexQuote.peRatio != null) {
      quote.p = BSON.Double(iexQuote.peRatio);
    }

    return quote;

  } catch(error) {
    return null;
  }
};

fixQuote = _fixQuote;

/**
 * Fixes splits object so it can be added to MongoDB.
 * @param {Object} iexSplits Splits object.
 * @param {ObjectId} symbolID Symbol object ID.
 * @returns {[Object]} Returns fixed objects or an empty array if fix wasn't possible.
 */
function _fixSplits(iexSplits, symbolID) {
  try {
    throwIfUndefinedOrNull(iexSplits, `fixSplits splits`);
    throwIfUndefinedOrNull(symbolID, `fixSplits symbolID`);
    if (!iexSplits.length) { 
      console.logVerbose(`Splits are empty for ${symbolID}. Nothing to fix.`);
      return []; 
    }
  
    console.logVerbose(`Fixing splits for ${symbolID}`);
    return iexSplits
      .filterNull()
      .map(iexSplit => {
        const split = {};
        split.e = _getOpenDate(iexSplit.exDate);
        split.s = symbolID;

        if (iexSplit.ratio != null) {
          split.r = BSON.Double(iexSplit.ratio);
        }

        return split;
      });

  } catch (error) {
    return [];
  }
};

fixSplits = _fixSplits;

/** 
 * First parameter: Date in the "yyyy-mm-dd" or timestamp or Date format, e.g. "2020-03-27" or '1633046400000' or Date.
 * Returns close 'Date' pointing to the U.S. stock market close time.
 *
 * Regular trading hours for the U.S. stock market, including 
 * the New York Stock Exchange (NYSE) and the Nasdaq Stock Market (Nasdaq),
 * are 9:30 a.m. to 4 p.m.
 */
 function _getCloseDate(closeDateValue) {
  if (closeDateValue == null) {
    return;
  }

  // Check if date is valid
  const date = new Date(closeDateValue);
  if (isNaN(date)) {
    console.logVerbose(`Invalid close date: ${closeDateValue}`);
    return;
  }

  // Eastern Standard Time (EST) time zone is 5 hours behind GMT during autumn/winter
  // https://en.wikipedia.org/wiki/Eastern_Time_Zone
  const closeDateStringWithTimeZone = `${date.dayString()}T16:00:00-0500`;
  const closeDate = new Date(closeDateStringWithTimeZone);
  if (isNaN(closeDate)) {
    console.logVerbose(`Invalid close date with time zone: ${closeDateStringWithTimeZone}`);
    return;
  } else {
    return closeDate;
  }
};

getCloseDate = _getCloseDate;

/** 
 * First parameter: Date in the "yyyy-mm-dd" or timestamp or Date format, e.g. "2020-03-27" or '1633046400000' or Date.
 * Returns open 'Date' pointing to the U.S. stock market open time.
 *
 * Regular trading hours for the U.S. stock market, including 
 * the New York Stock Exchange (NYSE) and the Nasdaq Stock Market (Nasdaq),
 * are 9:30 a.m. to 4 p.m.
 */
function _getOpenDate(openDateValue) {
  if (openDateValue == null) {
    return;
  }

  // Check if date is valid
  const date = new Date(openDateValue);
  if (isNaN(date)) {
    console.logVerbose(`Invalid open date: ${openDateValue}`);
    return;
  }

  // Eastern Standard Time (EST) time zone is 5 hours behind GMT during autumn/winter
  // https://en.wikipedia.org/wiki/Eastern_Time_Zone
  const openDateStringWithTimeZone = `${date.dayString()}T9:30:00-0500`;
  const openDate = new Date(openDateStringWithTimeZone);

  if (isNaN(openDate)) {
    console.logVerbose(`Invalid open date with time zone: ${openDateStringWithTimeZone}`);
    return;
  } else {
    return openDate;
  }
};

getOpenDate = _getOpenDate;

///////////////////////////////////////////////////////////////////////////////// INITIALIZATION

exports = function() {
  context.functions.execute("utils");

  if (typeof isIEXSandbox === 'undefined') {
    isIEXSandbox = context.values.get("base_url") === 'https://sandbox.iexapis.com/stable';
  }

  if (typeof isIEXProduction === 'undefined') {
    isIEXProduction = !isIEXSandbox;
  }

  if (typeof iex === 'undefined') {
    iex = atlas.db("iex");
  }

  /** Premium token. Will be used for all API calls if defined. */
  if (typeof premiumToken === 'undefined') {
    premiumToken = context.values.get("premium-token");
  }

  /** Tokens that are set deneding on an environment */
  if (typeof tokens === 'undefined') {
    tokens = context.values.get("tokens");
  }

  /** Initial token is chosen randomly and then it increase by 1 to diverse between tokens */
  if (typeof counter === 'undefined') {
    counter = Math.floor(Math.random() * tokens.length);
  }
  
  console.log("Imported IEX utils");
};
