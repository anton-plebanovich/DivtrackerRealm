
// fmpUtils.js

// 'Value is not an object: undefined' error is thrown when 'undefined' value is called as a function.

///////////////////////////////////////////////////////////////////////////////// EXTENSIONS

String.prototype.removeSensitiveData = function() {
  // We should always use 'strict' for primitive type extensions - https://stackoverflow.com/a/27736962/4124265
  'use strict';

  let safeString = this;
  
  const regexp = new RegExp(apikey, "g");
  safeString = safeString.replace(apikey, '<APIKEY>');

  return safeString;
};

///////////////////////////////////////////////////////////////////////////////// SYMBOLS

/** 
 * Computes and returns enabled short symbols.
 * Returned symbols are shortened to `_id` and `t` fields.
 * @returns {Promise<[ShortSymbol]>} Array of short symbols.
*/
async function _getShortSymbols() {
  // Getting short symbols for IDs
  const symbolsCollection = fmp.collection("symbols");
  const shortSymbols = await symbolsCollection
    .find(
      { e: null },
      { _id: 1, t: 1, c: 1 }
    )
    .toArray();

  console.log(`Got short symbols (${shortSymbols.length})`);
  console.logData(`Got short symbols (${shortSymbols.length})`, shortSymbols);

  return shortSymbols;
};

getShortSymbols = _getShortSymbols;

///////////////////////////////////////////////////////////////////////////////// fetch.js

//////////////////////////////////// Tokens

// 969387165d69a8607f9726e8bb52b901 - trackerdividend@gmail.com

//////////////////////////////////// Predefined Fetches

/// To prevent `414 Request-URI Too Large` error we need to split our requests by some value.
const defaultMaxFetchSize = 100;

/// To prevent `This request is limited to 5 symbols to prevent exceeding server response time.` error we need to limit our batch size.
const defaultMaxBatchSize = 5;

/// Each batch or chunked request will have concurrent fetches limited by this parameter.
const maxConcurrentFetchesPerRequest = 3;

// We do not need data below that date at the moment.
const minFetchDate = '2016-01-01';

/**
 * Fetches companies in batch for short symbols.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @returns {[Company]} Array of requested objects.
 */
fetchCompanies = async function fetchCompanies(shortSymbols, callback) {
  throwIfEmptyArray(shortSymbols, `fetchCompanies shortSymbols`);

  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);
  const api = `/v3/profile`;

  // https://financialmodelingprep.com/api/v3/profile/AAPL,AAP?apikey=969387165d69a8607f9726e8bb52b901
  return await _fmpFetchAndMapObjects(
    api,
    tickers,
    null,
    defaultMaxFetchSize,
    idByTicker,
    _fixFMPCompany, 
    callback
  );
};

/**
 * Fetches dividends in batch for short symbols.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @param {Int} Limit Per ticker limit.
 * @returns {[Dividend]} Array of requested objects.
 */
fetchDividends = async function fetchDividends(shortSymbols, limit, callback) {
  throwIfEmptyArray(shortSymbols, `fetchDividends shortSymbols`);

  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);
  const queryParameters = {};

  // FMP have dividends history from 1973 year for some companies and we do not need so much at the moment
  queryParameters.from = minFetchDate;

  // https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/AAPL?from=2016-01-01&apikey=969387165d69a8607f9726e8bb52b901
  // https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/AAPL,AAP?from=2016-01-01&apikey=969387165d69a8607f9726e8bb52b901
  return await _fmpFetchBatchAndMapArray(
    "/v3/historical-price-full/stock_dividend",
    tickers,
    queryParameters,
    defaultMaxBatchSize,
    limit,
    'historicalStockList',
    'historical',
    idByTicker,
    _fixFMPDividends,
    callback
  );
};

/**
 * Fetches dividends calendar for an year.
 * @returns {[Dividend]} Array of requested objects.
 */
fetchDividendsCalendar = async function fetchDividendsCalendar(shortSymbols) {
  throwIfEmptyArray(shortSymbols, `fetchDividends shortSymbols`);

  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);

  // https://financialmodelingprep.com/api/v3/stock_dividend_calendar?apikey=969387165d69a8607f9726e8bb52b901
  return await _fmpFetchAndMapFlatArray(
    '/v3/stock_dividend_calendar',
    tickers,
    null,
    idByTicker,
    _fixFMPDividends
  );
};

/**
 * Fetches historical prices in batch for uniqueIDs.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @returns {[HistoricalPrice]} Array of requested objects.
 */
fetchHistoricalPrices = async function fetchHistoricalPrices(shortSymbols, queryParameters, callback) {
  throwIfEmptyArray(shortSymbols, `fetchHistoricalPrices shortSymbols`);

  if (queryParameters == null) {
    queryParameters = {};
  }

  queryParameters.serietype = "line";

  // FMP have full historical prices history, e.g. from 1962 year for Coca-Cola and we do not need so much at the moment.
  // However, when we fetch with a batch and pass `from` parameter it will only fetch data for one year.
  // If we don't pass that parameter we will get data for 5 years instead.
  // queryParameters.from = minFetchDate;

  // We need to split tickers by exchanges or it won't work
  const results = [];
  const shortSymbolsByExchange = shortSymbols.toBuckets('c');
  const exchanges = Object.keys(shortSymbolsByExchange);
  console.log(`Fetching historical prices data for '${exchanges.length}' exchanges`);
  for (const [exchange, shortSymbols] of Object.entries(shortSymbolsByExchange)) {
    const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);
    console.log(`Fetching '${tickers.length}' tickers historical prices data for '${exchange}' exchange`);
  
    // https://financialmodelingprep.com/api/v3/historical-price-full/AAPL,AAP?serietype=line&from=2016-01-01&apikey=969387165d69a8607f9726e8bb52b901
    const result = await _fmpFetchBatchAndMapArray(
      "/v3/historical-price-full",
      tickers,
      queryParameters,
      defaultMaxBatchSize,
      null,
      'historicalStockList',
      'historical',
      idByTicker,
      _fixFMPHistoricalPrices,
      callback
    );

    console.log(`Fetched '${tickers.length}' tickers historical prices data for '${exchange}' exchange`);
    results.push(result);
  }
  console.log(`Fetched historical prices data for '${exchanges.length}' exchanges`);

  return results.flat();
};

/**
 * Fetches quotes in batch for uniqueIDs.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @returns {[Quote]} Array of requested objects.
 */
fetchQuotes = async function fetchQuotes(shortSymbols, callback) {
  throwIfEmptyArray(shortSymbols, `fetchQuotes shortSymbols`);

  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);
  const api = `/v3/quote`;

  // https://financialmodelingprep.com/api/v3/quote/GOOG,AAPL,FB?apikey=969387165d69a8607f9726e8bb52b901
  return await _fmpFetchAndMapObjects(
    api,
    tickers,
    null,
    defaultMaxFetchSize,
    idByTicker,
    _fixFMPQuote,
    callback
  );
};

/**
 * Fetches splits in batch for uniqueIDs.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @returns {[Split]} Array of requested objects.
 */
fetchSplits = async function fetchSplits(shortSymbols, callback) {
  throwIfEmptyArray(shortSymbols, `fetchSplits shortSymbols`);

  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);
  const queryParameters = {};

  // FMP have splits history from 1987 year for some companies and we do not need so much at the moment
  queryParameters.from = minFetchDate;

  // https://financialmodelingprep.com/api/v3/historical-price-full/stock_split/AAPL,AAP?from=2016-01-01&apikey=969387165d69a8607f9726e8bb52b901
  return await _fmpFetchBatchAndMapArray(
    "/v3/historical-price-full/stock_split",
    tickers,
    queryParameters,
    defaultMaxBatchSize,
    null,
    'historicalStockList',
    'historical',
    idByTicker,
    _fixFMPSplits,
    callback
  );
};

/**
 * Fetches symbols.
 * @returns {[Symbol]} Array of requested objects.
 */
fetchSymbols = async function fetchSymbols() {
  // https://financialmodelingprep.com/api/v3/stock/list?apikey=969387165d69a8607f9726e8bb52b901
  return await _fmpFetch("/v3/stock/list")
    .then(_fixFMPSymbols);
};

//////////////////////////////////// Generic Fetches

/**
 * @param {string} api API to call.
 * @param {Object} queryParameters Additional query parameters.
 * @param {[string]} idByTicker Dictionary of ticker symbol ID by ticker symbol.
 * @param {function} mapFunction Function to map data to our format.
 * @returns {Promise<[Object]>} Flat array of entities.
 */
async function _fmpFetchAndMapFlatArray(api, tickers, queryParameters, idByTicker, mapFunction) {
  return await _fmpFetch(api, queryParameters)
    .then(datas => {
      const datasByTicker = datas.toBuckets('symbol');
      return tickers
        .map(ticker => {
          const datas = datasByTicker[ticker];
          if (datas == null) {
            return []
          } else {
            return mapFunction(datas, idByTicker[ticker]);
          }
        })
        .flat()
      }
    );
}

/**
 * Requests data from FMP for tickers by a batch.
 * Then, maps arrays data to our format in a flat array.
 * If symbols count exceed max allowed amount it splits it to several requests and returns composed result.
 * @param {string} api API to call.
 * @param {Object} queryParameters Additional query parameters.
 * @param {Int} maxBatchSize Max allowed batch size.
 * @param {Int} Limit Per ticker limit.
 * @param {string} groupingKey Batch grouping key, e.g. 'historicalStockList'.
 * @param {string} dataKey Data key, e.g. 'historical'.
 * @param {[string]} idByTicker Dictionary of ticker symbol ID by ticker symbol.
 * @param {function} mapFunction Function to map data to our format.
 * @returns {Promise<[Object]>} Flat array of entities.
 */
async function _fmpFetchBatchAndMapArray(api, tickers, queryParameters, maxBatchSize, limit, groupingKey, dataKey, idByTicker, mapFunction, callback) {
  const _map = (datas) => {
    if (datas[groupingKey] != null) {
      const dataByTicker = datas[groupingKey].toDictionary('symbol');
      const existingTickers = Object.keys(dataByTicker).filter(x => tickers.includes(x));
      return existingTickers
        .map(ticker => {
          const data = dataByTicker[ticker];
          if (data != null && data[dataKey] != null) {
            let tickerData = data[dataKey];
            if (limit != null) {
              tickerData = tickerData.slice(0, limit);
            }

            return mapFunction(tickerData, idByTicker[ticker]);
          } else {
            return [];
          }
        })
        .flat();

    } else {
      throw `Unexpected response format for ${api}`
    }
  };

  let _mapAndCallback;
  if (callback != null) {
    _mapAndCallback = async (datas, tickers) => await callback(_map(datas), tickers.map(ticker => idByTicker[ticker]));
  } else {
    _mapAndCallback = undefined;
  }

  const response = await _fmpFetchBatch(api, tickers, queryParameters, maxBatchSize, groupingKey, _mapAndCallback);
  
  // Only return data if callback is missing. We should not perform double mapping.
  if (callback == null) {
    return _map(response);
  }
}

/**
 * Requests data from FMP for tickers by a batch.
 * Then, maps objects data to our format in a array.
 * If symbols count exceed max allowed amount it splits it to several requests and returns composed result.
 * @param {string} api API to call.
 * @param {Object} queryParameters Additional query parameters.
 * @param {[string]} idByTicker Dictionary of ticker symbol ID by ticker symbol.
 * @param {function} mapFunction Function to map data to our format.
 * @returns {Promise<[Object]>} Flat array of entities.
 */
 async function _fmpFetchAndMapObjects(api, tickers, queryParameters, maxFetchSize, idByTicker, mapFunction, callback) {
  const _map = (datas) => {
    const dataByTicker = datas.toDictionary('symbol');
    const existingTickers = Object.keys(dataByTicker).filter(x => tickers.includes(x));
    return existingTickers
      .compactMap(ticker => {
        const tickerData = dataByTicker[ticker];
        if (tickerData != null) {
          return mapFunction(tickerData, idByTicker[ticker]);
        } else {
          return null;
        }
      });
  };

  let _mapAndCallback;
  if (callback != null) {
    _mapAndCallback = async (datas, tickers) => await callback(_map(datas), tickers.map(ticker => idByTicker[ticker]));
  } else {
    _mapAndCallback = undefined;
  }
  
  const response = await _fmpFetchChunked(api, tickers, queryParameters, maxFetchSize, _mapAndCallback);

  // Only return data if callback is missing. We should not perform double mapping.
  if (callback == null) {
    return _map(response);
  }
}

//////////////////////////////////// Base Fetch

async function _fmpFetchChunked(api, tickers, queryParameters, maxFetchSize, callback) {
  throwIfUndefinedOrNull(api, `_fmpFetchBatch api`);
  throwIfEmptyArray(tickers, `_fmpFetchBatch tickers`);

  if (queryParameters == null) {
    queryParameters = {};
  } else {
    queryParameters = Object.assign({}, queryParameters);
  }

  let chunkedTickersArray;
  if (maxFetchSize == null) {
    chunkedTickersArray = [tickers];
  } else {
    chunkedTickersArray = tickers.chunked(maxFetchSize);
  }

  return await chunkedTickersArray
  .asyncMap(maxConcurrentFetchesPerRequest, async chunkedTickers => {
    const tickersString = chunkedTickers.join(",");
    const batchAPI = `${api}/${tickersString}`;
    console.log(`Fetching chunk for ${chunkedTickers.length} symbols with query '${queryParameters.stringify()}': ${tickersString}`);
    
    let response
    try {
      response = await _fmpFetch(batchAPI, queryParameters);
    } catch(error) {
      if (error.statusCode == 404) {
        response = [];
      } else {
        throw error;
      }
    }

    if (callback != null) {
      await callback(response, chunkedTickers);
    }
    
    return response;
  })
  .then(x => 
    x.filterNullAndUndefined()
    .flat()
  );
}

/**
 * @param {string} api API to call.
 * @param {[string]} tickers Ticker Symbols to fetch, e.g. ['AAP','AAPL','PBA'].
 * @param {Object} queryParameters Additional query parameters.
 * @param {Int} maxBatchSize Max allowed batch size.
 * @param {string} groupingKey Batch grouping key, e.g. 'historicalStockList'.
 * @param {function} callback TODO.
 * @returns {Promise<{string: {string: Object|[Object]}}>} Parsed EJSON object. Composed from several responses if max symbols count was exceeded. 
 * The first object keys are symbols. The next inner object keys are types. And the next inner object is an array of type objects.
 */
async function _fmpFetchBatch(api, tickers, queryParameters, maxBatchSize, groupingKey, callback) {
  throwIfUndefinedOrNull(api, `_fmpFetchBatch api`);
  throwIfEmptyArray(tickers, `_fmpFetchBatch tickers`);

  if (queryParameters == null) {
    queryParameters = {};
  } else {
    queryParameters = Object.assign({}, queryParameters);
  }

  let chunkedTickersArray;
  if (maxBatchSize == null) {
    chunkedTickersArray = [tickers];
  } else {
    chunkedTickersArray = tickers.chunked(maxBatchSize);
  }

  // Always map to the same format
  const emptyResponse = { [groupingKey]: [] };

  return await chunkedTickersArray
  .asyncMap(maxConcurrentFetchesPerRequest, async chunkedTickers => {
    const tickersString = chunkedTickers.join(",");
    const batchAPI = `${api}/${tickersString}`;
    console.log(`Fetching batch for ${chunkedTickers.length} symbols with query '${queryParameters.stringify()}': ${tickersString}`);
    
    let response
    try {
      response = await _fmpFetch(batchAPI, queryParameters);
    } catch(error) {
      if (error.statusCode == 404) {
        // Fix not found responses
        response = emptyResponse;
      } else {
        throw error;
      }
    }

    // Fix singular responses
    if (chunkedTickers.length == 1) {
      response = { [groupingKey]: [response] };
    }
    
    // Fix empty responses
    if (Object.entries(response).length == 0) {
      response = emptyResponse;
    }
    
    if (response[groupingKey] != null) {
      throwIfNotArray(response[groupingKey], `_fmpFetchBatch response[groupingKey]`);

      if (response[groupingKey].length == 0) {
        console.logVerbose(`No data for tickers: ${tickersString}`);
      }
    }

    if (callback != null) {
      await callback(response, chunkedTickers);
    }

    return response;
  })
  .then(results => {
    const datas = results
      .map(result => result[groupingKey])
      .filterNullAndUndefined()
      .flat();

    return { [groupingKey]: datas };
  })
};

/**
 * Requests data from FMP. 
 * @param {string} api API to call.
 * @param {Object} queryParameters Query parameters.
 * @returns Parsed EJSON object.
 */
async function _fmpFetch(api, queryParameters) {
  throwIfUndefinedOrNull(api, `_fmpFetch api`);
  if (queryParameters == null) {
    queryParameters = {}
  } else {
    queryParameters = Object.assign({}, queryParameters);
  }
  
  const baseURL = "https://financialmodelingprep.com/api";
  queryParameters.apikey = apikey;

  return await fetch(baseURL, api, queryParameters);
}

///////////////////////////////////////////////////////////////////////////////// DATA FIX

/**
 * Fixes company object so it can be added to MongoDB.
 * @param {FMPCompany} fmpCompany FMP Company object.
 * @param {ObjectId} symbolID Symbol object ID.
 * @returns {Company|null} Returns fixed object or `null` if fix wasn't possible.
 */
function _fixFMPCompany(fmpCompany, symbolID) {
  try {
    throwIfUndefinedOrNull(fmpCompany, `_fixFMPCompany company`);
    throwIfUndefinedOrNull(symbolID, `_fixFMPCompany symbolID`);
  
    console.logVerbose(`Company data fix start`);
    const company = {};
    company._id = symbolID;

    if (fmpCompany.currency) {
      company.setIfNotNullOrUndefined('c', fmpCompany.currency.toUpperCase());
    } else {
      console.error(`No currency '${symbolID}': ${fmpCompany.stringify()}`)
      company.c = "-";
    }

    if (fmpCompany.industry) {
      company.setIfNotNullOrUndefined('i', fmpCompany.industry);
    }

    if (fmpCompany.companyName) {
      company.setIfNotNullOrUndefined('n', fmpCompany.companyName);
    } else {
      console.error(`No company name '${symbolID}': ${fmpCompany.stringify()}`)
      company.n = "N/A";
    }

    // Refers to the common issue type of the stock.
    // ad - ADR
    // cs - Common Stock
    // cef - Closed End Fund
    // et - ETF
    // oef - Open Ended Fund
    // ps - Preferred Stock
    // rt - Right
    // struct - Structured Product
    // ut - Unit
    // wi - When Issued
    // wt - Warrant
    // empty - Other
    if (fmpCompany.isAdr == true) {
      company.t = "ad";
    } else if (fmpCompany.isEtf == true) {
      company.t = "et";
    } else if (fmpCompany.isFund == true) {
      company.t = "f";
    } else if (fmpCompany.exchangeShortName === 'MUTUAL_FUND') {
      company.t = "mf";
    } else if (fmpCompany.exchangeShortName === 'OTC') {
      company.t = "otc";
    } else {
      company.t = "s"; // Stock
    }

    return company;
  } catch(error) {
    console.error(`Unable to fix company ${fmpCompany.stringify()}: ${error}`);
    return null;
  }
};

/**
 * Fixes dividends object so it can be added to MongoDB.
 * @param {[FMPDividend]} fmpDividends FMP dividends array.
 * @param {ObjectId} symbolID Symbol object ID.
 * @returns {[Dividend]} Returns fixed objects or an empty array if fix wasn't possible.
 */
function _fixFMPDividends(fmpDividends, symbolID) {
  try {
    throwIfNotArray(fmpDividends, `_fixFMPDividends fmpDividends`);
    throwIfUndefinedOrNull(symbolID, `_fixFMPDividends uniqueID`);
    if (!fmpDividends.length) { 
      console.logVerbose(`FMP dividends are empty for ${symbolID}. Nothing to fix.`);
      return []; 
    }
  
    console.logVerbose(`Fixing dividends for ${symbolID}`);
    let dividends = fmpDividends
      .filterNullAndUndefined()
      // We may receive full timeline when one ticker is requested so we need to trim it
      .filter(fmpDividend => fmpDividend.date >= minFetchDate)
      .sorted((l, r) => l.date.localeCompare(r.date))
      .map(fmpDividend => {
        const dividend = {};
        dividend.setIfNotNullOrUndefined('e', _getOpenDate(fmpDividend.date));
        dividend.setIfNotNullOrUndefined('d', _getOpenDate(fmpDividend.declarationDate));
        dividend.setIfNotNullOrUndefined('p', _getOpenDate(fmpDividend.paymentDate));
        dividend.f = 'u'; // Will be updated later
        dividend.s = symbolID;

        const amount = _getFmpDividendAmount(fmpDividend);
        if (amount == null) {
          console.error(`No amount for dividend ${symbolID}: ${fmpDividend.stringify()}`)
          return null;

        } else {
          dividend.a = amount;
        }
    
        return dividend;
      })
      .filterNullAndUndefined()
    
    dividends = _removeDuplicatedDividends(dividends);
    dividends = _updateDividendsFrequency(dividends);

    return dividends;

  } catch(error) {
    console.error(`Unable to fix dividends ${fmpDividends.stringify()}: ${error}`);
    return [];
  }
}

fixFMPDividends = _fixFMPDividends;

function _getFmpDividendAmount(fmpDividend) {
  if (fmpDividend.dividend != null) {
    return BSON.Double(fmpDividend.dividend);

  } else if (fmpDividend.adjDividend != null) {
    // Workaround, some records doesn't have `dividend` and it looks like `adjDividend` may be used if there were no splits.
    // We can improve this later using splits info but just using `adjDividend` as is for now.
    return BSON.Double(fmpDividend.adjDividend);

  } else {
    return null
  }
}

// 7 days
const possiblyIrregularMinTimeInterval = 7 * 24 * 3600 * 1000;

// 13 days
const possiblyIrregularMaxTimeInterval = 13 * 24 * 3600 * 1000;

// TODO: Improve that logic to take into account whole context and so better series detection instead of just compare adjacent dividends.
function _updateDividendsFrequency(dividends) {
  let foundIrregular = false;

  const nonDeletedDividends = dividends.filter(x => x.x != true);

  // We do not try to determine frequency of series lower than 3.
  if (nonDeletedDividends.length < 3) {
    nonDeletedDividends.forEach(dividend => dividend.f = 'u');
    return dividends;
  }

  const mainFrequency = getMainFrequency(nonDeletedDividends);
  for (const [i, dividend] of nonDeletedDividends.entries()) {
    
    let iPrev = 1;
    let prevDividend;
    while (i - iPrev >= 0 && prevDividend == null) {
      prevDividend = nonDeletedDividends[i - iPrev];

      // Ignore irregular and unspecified dividends
      if (prevDividend.f === 'i' || prevDividend.f === 'u') {
        prevDividend = null;
      }

      iPrev++;
    }

    let iNext = 1;
    let nextDividend;
    while (i + iNext < nonDeletedDividends.length && nextDividend == null) {
      nextDividend = nonDeletedDividends[i + iNext];
      iNext++;
    }
    
    if (prevDividend != null && nextDividend != null) {
      const nextTimeInterval = Math.abs(nextDividend.e - dividend.e);
      const nextFrequency = getFrequencyForMillis(nextTimeInterval);
      
      const timeInterval = Math.abs(dividend.e - prevDividend.e);
      const frequency = getFrequencyForMillis(timeInterval);
      
      // We use shorter range when we have 'm' main frequency and longer when it's 'q' and above
      let isPossiblyIrregular;
      if (mainFrequency === 'm') {
        isPossiblyIrregular = nextTimeInterval <= possiblyIrregularMinTimeInterval || timeInterval <= possiblyIrregularMinTimeInterval;
      } else if (mainFrequency !== 'w') {
        isPossiblyIrregular = nextTimeInterval <= possiblyIrregularMaxTimeInterval || timeInterval <= possiblyIrregularMaxTimeInterval;
      } else {
        isPossiblyIrregular = false;
      }

      if (isPossiblyIrregular) {
        // Try to identify irregular dividends
        // TODO: if main frequency is 'm' we need to lower weekly range
        if (nextFrequency === 'w') {
          const thisDiff = math_bigger_times(dividend.a, prevDividend.a);
          const nextDiff = math_bigger_times(nextDividend.a, prevDividend.a);
          const isIrregular = thisDiff > nextDiff;
          if (isIrregular) {
            foundIrregular = true;
            dividend.f = 'i';
          } else {
            dividend.f = frequency;
          }
          continue;
        }

        if (frequency === 'w') {
          const thisDiff = math_bigger_times(dividend.a, nextDividend.a);
          const prevDiff = math_bigger_times(prevDividend.a, nextDividend.a);
          const isIrregular = thisDiff > prevDiff;
          if (isIrregular) {
            foundIrregular = true;
            dividend.f = 'i';
          } else {
            dividend.f = nextFrequency;
          }
          continue;
        }
      }

      if (frequency === prevDividend.f) {
        dividend.f = frequency;

      } else {
        let prevPrevDividend;
        while (i - iPrev >= 0 && prevPrevDividend == null) {
          prevPrevDividend = nonDeletedDividends[i - iPrev];
          
          // Ignore irregular and unspecified dividends
          if (prevPrevDividend.f === 'i' || prevPrevDividend.f === 'u') {
            prevPrevDividend = null;
          }
          
          iPrev++;
        }
        
        if (prevPrevDividend != null) {
          if (prevPrevDividend.f === prevDividend.f && prevDividend.f === nextFrequency) {
            // Missing dividends case. If two previous frequencies and one next is the same we just use it.
            dividend.f = nextFrequency;

          } else if (prevPrevDividend.f === prevDividend.f && getGradeDifference(frequency, prevDividend.f) === 1) {
            // 3-9 semi-annual or 1-5 quarterly case
            const frequency = getFrequencyForMillis((dividend.e - prevPrevDividend.e) / 2);
            dividend.f = frequency;

          } else {
            dividend.f = frequency;
          }
          
        } else {
          dividend.f = getFrequencyForMillis((nextDividend.e - prevDividend.e) / 2);
        }
      }
      
    } else if (prevDividend != null) {
      const frequency = getFrequencyForMillis(dividend.e - prevDividend.e);
      if (frequency === prevDividend.f) {
        dividend.f = frequency;

      } else {
        let prevPrevDividend;
        while (i - iPrev >= 0 && prevPrevDividend == null) {
          prevPrevDividend = nonDeletedDividends[i - iPrev];
    
          // Ignore irregular and unspecified dividends
          if (prevPrevDividend.f === 'i' || prevPrevDividend.f === 'u') {
            prevPrevDividend = null;
          }
    
          iPrev++;
        }

        if (prevPrevDividend != null && prevPrevDividend.f === prevDividend.f && getGradeDifference(frequency, prevDividend.f) === 1) {
          const frequency = getFrequencyForMillis((dividend.e - prevPrevDividend.e) / 2);
          dividend.f = frequency;

        } else {
          dividend.f = frequency;
        }
      }

    } else if (nextDividend != null) {
      const nextFrequency = getFrequencyForMillis(nextDividend.e - dividend.e);

      let nextNextDividend;
      while (i + iNext < nonDeletedDividends.length && nextNextDividend == null) {
        nextNextDividend = nonDeletedDividends[i + iNext];
        iNext++;
      }

      if (nextNextDividend != null) {
        const nextNextFrequency = getFrequencyForMillis(nextNextDividend.e - nextDividend.e);
        if (nextFrequency === nextNextFrequency) {
          dividend.f = nextNextFrequency;
        } else {
          dividend.f = getFrequencyForMillis((nextNextDividend.e - dividend.e) / 2);
        }

      } else {
        dividend.f = nextFrequency;
      }

    } else {
      dividend.f = 'u'; // The only record
    }
  }

  if (foundIrregular) {
    _updateDividendsFrequency(dividends.filter(x => x.f !== 'i'));
    return dividends;

  } else {
    return dividends;
  }
}

updateDividendsFrequency = _updateDividendsFrequency;

function getGradeDifference(lFrequency, rFrequency) {
  const lGrade = getGrade(lFrequency);
  const rGrade = getGrade(rFrequency);
  return Math.abs(rGrade - lGrade);
}

function getGrade(frequency) {
  switch (frequency) {
    case 'w': return 1;
    case 'm': return 2;
    case 'q': return 3;
    case 's': return 4;
    case 'a': return 5;
    default: return -1;
  }
}

function getFrequencyForMillis(millis) {
  const days = Math.abs(millis) / 86400000;
  if (days <= 11) {
    return 'w';
  } else if (days <= 45) {
    return 'm';
  } else if (days <= 135) {
    return 'q';
  } else if (days <= 270) {
    return 's';
  } else if (days <= 540) {
    return 'a';
  } else {
    return 'u';
  }
}

// TODO: Improve later by including more cases
function _removeDuplicatedDividends(dividends) {
  // Unable to determine duplicates when there are less than 3 records
  if (dividends.length < 3) {
    return dividends;
  }
  
  // We do not dedupe week frequency dividends atm
  const mainFrequency = getMainFrequency(dividends);
  if (mainFrequency === 'w') {
    return dividends;
  }

  const originalLength = dividends.length;
  const newDividends = dividends
    .filter((dividend, i, arr) => {
      if (i + 1 >= arr.length) {
        return true;
      }
      
      const nextDividend = arr[i + 1];

      // 80.2744 and 80.27435 for NNSB.ME
      const lhsAmount = dividend.a.valueOf();
      const rhsAmount = nextDividend.a.valueOf();
      const amountEqual = Math.abs(rhsAmount - lhsAmount) <= 0.0001
      const nextFrequency = getFrequencyForMillis(nextDividend.e - dividend.e);

      if (nextFrequency === 'w' && amountEqual) {
        console.error(`Duplicate dividend for ${dividend.s}: ${dividend.stringify()}`);
        return false;
      } else {
        return true;
      }
    });

  if (newDividends.length === originalLength) {
    return newDividends;
  } else {
    // We might have triplicates so several passes is required
    return _removeDuplicatedDividends(newDividends);
  }
}

removeDuplicatedDividends = _removeDuplicatedDividends;

  // Very raw but should be enough for now
function getMainFrequency(dividends) {
  dividends = dividends.filter(dividend => dividend.x != true && dividend.f !== 'i')
  if (dividends.length < 2) {
    return 'u';
  }

  const range = dividends[dividends.length - 1].e - dividends[0].e;
  const period = range / (dividends.length - 1);

  return getFrequencyForMillis(period);
}

/**
 * Fixes historical prices object so it can be added to MongoDB.
 * @param {[FMPHistoricalPrices]} fmpHistoricalPrices Historical prices object.
 * @param {ObjectId} symbolID Symbol object ID.
 * @returns {[HistoricalPrices]} Returns fixed objects or an empty array if fix wasn't possible.
 */
function _fixFMPHistoricalPrices(fmpHistoricalPrices, symbolID) {
  try {
    throwIfNotArray(fmpHistoricalPrices, `_fixFMPHistoricalPrices fmpHistoricalPrices`);
    throwIfUndefinedOrNull(symbolID, `_fixFMPHistoricalPrices uniqueID`);
    if (!fmpHistoricalPrices.length) { 
      console.logVerbose(`Historical prices are empty for ${symbolID}. Nothing to fix.`);
      return []; 
    }

    // Bucket prices by month using 15 day.
    const monthStart = Date.monthStart().dayString();
    const fmpHistoricalPricesByMonth = fmpHistoricalPrices
      // We may receive full timeline when one ticker is requested so we need to trim it
      .filter(fmpHistoricalPrice => fmpHistoricalPrice.date >= minFetchDate)
      // Do not add partial months
      .filter(x => x.date < monthStart)
      .toBuckets(fmpHistoricalPrice => {
        const components = fmpHistoricalPrice.date.split('-');
        components.pop();
        components.push("15");
        return components.join('-');
      })
    
    // Compute average price per month
    const averagePriceByMonth = {};
    Object.keys(fmpHistoricalPricesByMonth)
      .forEach(key => {
        const fmpHistoricalPrice = fmpHistoricalPricesByMonth[key];
        const sum = fmpHistoricalPrice.reduce((sum, fmpHistoricalPrice) => sum + fmpHistoricalPrice.close, 0);
        averagePriceByMonth[key] = sum / fmpHistoricalPrice.length;
      })
  
    console.logVerbose(`Fixing historical prices for ${symbolID}`);
    return Object.keys(averagePriceByMonth)
      .map(month => {
        const averagePrice = averagePriceByMonth[month]
        const historicalPrice = {};
        historicalPrice.setIfNotNullOrUndefined('c', BSON.Double(averagePrice));
        historicalPrice.setIfNotNullOrUndefined('d', _getCloseDate(month));
        historicalPrice.s = symbolID;

        return historicalPrice;
      });

  } catch (error) {
    console.error(`Unable to fix historical prices ${fmpHistoricalPrices.stringify()}: ${error}`);
    return [];
  }
};

/**
 * Fixes quote object so it can be added to MongoDB.
 * @param {FMPQuote} fmpQuote Quote object.
 * @param {ObjectId} symbolID Symbol object ID.
 * @returns {Quote|null} Returns fixed object or `null` if fix wasn't possible.
 */
function _fixFMPQuote(fmpQuote, symbolID) {
  try {
    throwIfUndefinedOrNull(fmpQuote, `_fixFMPQuote quote`);
    throwIfUndefinedOrNull(symbolID, `_fixFMPQuote symbolID`);
  
    console.logVerbose(`Previous day price data fix start`);
    const quote = {};
    quote._id = symbolID;

    if (fmpQuote.price != null) {
      quote.setIfNotNullOrUndefined('l', fmpQuote.price);
    } else {
      console.error(`No quote price for '${symbolID}': ${fmpQuote.stringify()}`)
      return null
    }

    if (fmpQuote.pe != null) {
      quote.setIfNotNullOrUndefined('p', BSON.Double(fmpQuote.pe));
    }

    return quote;

  } catch(error) {
    console.error(`Unable to fix quote ${fmpQuote.stringify()}: ${error}`);
    return null;
  }
};

/**
 * Fixes splits object so it can be added to MongoDB.
 * @param {[FMPSplit]} fmpSplits Splits object.
 * @param {ObjectId} symbolID Symbol object ID.
 * @returns {[Object]} Returns fixed objects or an empty array if fix wasn't possible.
 */
function _fixFMPSplits(fmpSplits, symbolID) {
  try {
    throwIfNotArray(fmpSplits, `_fixFMPSplits splits`);
    throwIfUndefinedOrNull(symbolID, `_fixFMPSplits symbolID`);
    if (!fmpSplits.length) { 
      console.logVerbose(`Splits are empty for ${symbolID}. Nothing to fix.`);
      return []; 
    }
  
    console.logVerbose(`Fixing splits for ${symbolID}`);
    return fmpSplits
      // We may receive full timeline when one ticker is requested so we need to trim it
      .filter(fmpSplit => fmpSplit.date >= minFetchDate)
      .filterNullAndUndefined()
      .map(fmpSplit => {
        const split = {};
        split.setIfNotNullOrUndefined('e', _getOpenDate(fmpSplit.date));
        split.s = symbolID;

        if (fmpSplit.denominator > 0 && fmpSplit.numerator > 0) {
          const ratio = BSON.Double(fmpSplit.denominator / fmpSplit.numerator);
          split.setIfNotNullOrUndefined('r', ratio);
        } else {
          console.error(`No ratio for split '${symbolID}': ${fmpSplit.stringify()}`)
          return null
        }

        return split;
      })
      .filterNullAndUndefined();

  } catch (error) {
    console.error(`Unable to fix splits ${fmpSplits.stringify()}: ${error}`);
    return [];
  }
};

/**
 * Fixes symbos object so it can be added to MongoDB.
 * @param {[FMPSymbol]} fmpSymbols Symbols object.
 * @returns {[Object]} Returns fixed objects or an empty array if fix wasn't possible.
 */
function _fixFMPSymbols(fmpSymbols) {
  try {
    throwIfNotArray(fmpSymbols, `_fixFMPSymbols fmpSymbols`);
    if (!fmpSymbols.length) { 
      console.logVerbose(`Symbols are empty. Nothing to fix.`);
      return []; 
    }
  
    console.logVerbose(`Fixing symbols`);
    return fmpSymbols
      .filterNullAndUndefined()
      // Limit to only supported types
      .filter(fmpSymbol => fmpSymbol.exchangeShortName === "MCX" || fmpSymbol.type === "fund")
      .map(fmpSymbol => {
        const symbol = {};
        symbol.setIfNotNullOrUndefined('c', fmpSymbol.exchangeShortName);
        symbol.setIfNotNullOrUndefined('n', fmpSymbol.name);
        symbol.setIfNotNullOrUndefined('t', fmpSymbol.symbol);

        return symbol;
      });

  } catch (error) {
    console.error(`Unable to fix symbols ${fmpSymbols.stringify()}: ${error}`);
    return [];
  }
};

// TODO: Fix FMP and IEX open and close date computations. They should instead of harcoded exchange work with passed exchange. Migrations.
// TODO: Also check if payment and declared date for dividends **MUST** be adjusted. Leave as is if possible.

/** 
 * First parameter: Date in the "yyyy-mm-dd" or timestamp or Date format, e.g. "2020-03-27" or '1633046400000' or Date.
 * Returns close 'Date' pointing to the U.S. stock market close time.
 *
 * The Moscow Exchange is open Monday through Friday from 9:50am to 6:39pm Moscow Standard Time (GMT+03:00).
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

  const closeDateStringWithTimeZone = `${date.dayString()}T18:39:00+0300`;
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
 * The Moscow Exchange is open Monday through Friday from 9:50am to 6:39pm Moscow Standard Time (GMT+03:00).
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

  const openDateStringWithTimeZone = `${date.dayString()}T9:50:00+0300`;
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

exports = function(database) {
  context.functions.execute("utils");

  if (typeof fmp === 'undefined') {
    database = _getFMPDatabaseName(database);
    fmp = atlas.db(database);
  }

  if (typeof apikey === 'undefined') {
    apikey = "969387165d69a8607f9726e8bb52b901";
    Object.freeze(apikey);
  }
  
  console.log("Imported FMP utils");
};

function _getFMPDatabaseName(database) {
  if (Object.prototype.toString.call(database) === '[object Object]') {
    // Trigger object, just erase
    database = null;
  }

  if (database != null && database !== 'Hello world!') {
    return database;
  } else {
    return "fmp";
  }
}

getFMPDatabaseName = _getFMPDatabaseName;
