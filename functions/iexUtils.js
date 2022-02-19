
// iexUtils.js

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
  _throwIfUndefinedOrNull(shortSymbols, `fetchCompanies shortSymbols`);
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
  _throwIfUndefinedOrNull(shortSymbols, `fetchDividends shortSymbols`);
  _throwIfUndefinedOrNull(isFuture, `fetchDividends isFuture`);

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
  _throwIfUndefinedOrNull(shortSymbols, `fetchPreviousDayPrices shortSymbols`);
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
   _throwIfUndefinedOrNull(shortSymbols, `fetchHistoricalPrices shortSymbols`);

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
  _throwIfUndefinedOrNull(shortSymbols, `fetchQuotes shortSymbols`);
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
  _throwIfUndefinedOrNull(shortSymbols, `fetchSplits shortSymbols`);

  if (range == null) {
    range = defaultRange;
  }
  
  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);
  const parameters = { range: range };

  // https://cloud.iexapis.com/stable/stock/market/batch?types=splits&token=pk_9f1d7a2688f24e26bb24335710eae053&range=6y&symbols=AAPL,AAP
  // https://sandbox.iexapis.com/stable/stock/market/batch?types=splits&token=Tpk_581685f711114d9f9ab06d77506fdd49&range=6y&symbols=AAPL,AAP
  return await _iexFetchBatchAndMapArray('splits', tickers, idByTicker, _fixSplits, parameters);
};

/**
 * Returns ticker symbols and ticker symbol IDs by symbol ticker name dictionary.
 * @param {[ShortSymbol]} shortSymbols Short symbol models.
 * @returns {[["AAPL"], {"AAPL":ObjectId}]} Returns array with ticker symbols as the first element and ticker symbol IDs by ticker symbol dictionary as the second element.
 */
 function getTickersAndIDByTicker(shortSymbols) {
  _throwIfUndefinedOrNull(shortSymbols, `getTickersAndIDByTicker shortSymbols`);
  const tickers = [];
  const idByTicker = {};
  for (const shortSymbol of shortSymbols) {
    const ticker = shortSymbol.t;
    tickers.push(ticker);
    idByTicker[ticker] = shortSymbol._id;
  }

  return [
    tickers,
    idByTicker
  ];
}

/**
 * Requests data from IEX cloud/sandbox depending on an environment by a batch.
 * If symbols count exceed max allowed amount it splits it to several requests and returns composed result.
 * It switches between available tokens to evenly distribute the load.
 * @param {string} api API to call.
 * @param {[string]} tickers Ticker symbols to fetch, e.g. ['AAP','AAPL','PBA'].
 * @param {Object} queryParameters Query parameters.
 * @returns Parsed EJSON object. Composed from several responses if max symbols count was exceeded.
 */
async function _iexFetchBatch(api, tickers, queryParameters) {
  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_ca8d3de2a6db4a58a61a93ac027e4725&types=company&symbols=UZF
  _throwIfUndefinedOrNull(api, `_api`);
  const fullAPI = `/stock/market${api}/batch`;
  _throwIfUndefinedOrNull(tickers, `tickers`);

  if (queryParameters == null) {
    queryParameters = {};
  }

  // TODO: We might fetch in parallel
  const maxSymbolsAmount = 100;
  const chunkedTickersArray = tickers.chunked(maxSymbolsAmount);
  var result = [];
  for (const chunkedTickers of chunkedTickersArray) {
    const tickerParameter = chunkedTickers.join(",");
    const fullQueryParameters = Object.assign({}, queryParameters);
    fullQueryParameters.symbols = tickerParameter;

    console.log(`Fetching '${api}' batch for symbols (${chunkedTickers.length}) with query '${queryParameters.stringify()}': ${tickerParameter}`);
    const response = await _iexFetch(fullAPI, fullQueryParameters);

    result = result.concat(response);
  }

  return result;
};

iexFetchBatch = _iexFetchBatch;

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
 * @returns {Promise<{string: {string: Object|[Object]}}>} Parsed EJSON object. Composed from several responses if max symbols count was exceeded. 
 * The first object keys are symbols. The next inner object keys are types. And the next inner object is an array of type objects.
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

iexFetchBatchAndMapArray = _iexFetchBatchAndMapArray;

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
 * @returns {Promise<{string: {string: Object|[Object]}}>} Parsed EJSON object. Composed from several responses if max symbols count was exceeded. 
 * The first object keys are symbols. The next inner object keys are types. And the next inner object is an array of type objects.
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

iexFetchBatchAndMapObjects = _iexFetchBatchAndMapObjects;

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
  _throwIfUndefinedOrNull(types, `types`);
  _throwIfUndefinedOrNull(tickers, `tickers`);

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

iexFetchBatchNew = _iexFetchBatchNew;

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
    _throwIfUndefinedOrNull(iexCompany, `fixCompany company`);
    _throwIfUndefinedOrNull(symbolID, `fixCompany symbolID`);
  
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
    _throwIfUndefinedOrNull(iexDividends, `fixDividends iexDividends`);
    _throwIfUndefinedOrNull(symbolID, `fixDividends uniqueID`);
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
    _throwIfUndefinedOrNull(iexPreviousDayPrice, `fixPreviousDayPrice iexPreviousDayPrice`);
    _throwIfUndefinedOrNull(symbolID, `fixPreviousDayPrice symbolID`);
  
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
    _throwIfUndefinedOrNull(iexHistoricalPrices, `fixHistoricalPrices iexHistoricalPrices`);
    _throwIfUndefinedOrNull(symbolID, `fixHistoricalPrices uniqueID`);
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
    _throwIfUndefinedOrNull(iexQuote, `fixQuote quote`);
    _throwIfUndefinedOrNull(symbolID, `fixQuote symbolID`);
  
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
    _throwIfUndefinedOrNull(iexSplits, `fixSplits splits`);
    _throwIfUndefinedOrNull(symbolID, `fixSplits symbolID`);
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
