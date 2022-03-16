
// fmpUtils.js

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
      { _id: 1, t: 1 }
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

/**
 * Fetches companies in batch for short symbols.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @returns {[Company]} Array of requested objects.
 */
fetchCompanies = async function fetchCompanies(shortSymbols) {
  throwIfEmptyArray(shortSymbols, `fetchCompanies shortSymbols`);

  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);
  const tickersString = tickers.join(",");
  const api = `/v3/profile/${tickersString}`;

  // https://financialmodelingprep.com/api/v3/profile/AAPL,AAP?apikey=969387165d69a8607f9726e8bb52b901
  return await _fmpFetchAndMapObjects(
    api,
    tickers,
    null,
    idByTicker,
    _fixFMPCompany
  );
};

/**
 * Fetches dividends in batch for short symbols.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @param {Int} Limit Per ticker limit.
 * @returns {[Dividend]} Array of requested objects.
 */
fetchDividends = async function fetchDividends(shortSymbols, limit) {
  throwIfEmptyArray(shortSymbols, `fetchDividends shortSymbols`);

  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);

  // https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/AAPL?apikey=969387165d69a8607f9726e8bb52b901
  // https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/AAPL,AAP?apikey=969387165d69a8607f9726e8bb52b901
  return await _fmpFetchBatchAndMapArray(
    "/v3/historical-price-full/stock_dividend",
    tickers,
    null,
    5,
    limit,
    'historicalStockList',
    'historical',
    idByTicker,
    _fixFMPDividends
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
fetchHistoricalPrices = async function fetchHistoricalPrices(shortSymbols, queryParameters) {
  throwIfEmptyArray(shortSymbols, `fetchHistoricalPrices shortSymbols`);

  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);

  if (queryParameters == null) {
    queryParameters = { serietype: "line" };
  } else {
    queryParameters.serietype = "line";
  }

  // https://financialmodelingprep.com/api/v3/historical-price-full/AAPL,AAP?serietype=line&apikey=969387165d69a8607f9726e8bb52b901
  return await _fmpFetchBatchAndMapArray(
    "/v3/historical-price-full",
    tickers,
    queryParameters,
    5,
    null,
    'historicalStockList',
    'historical',
    idByTicker,
    _fixFMPHistoricalPrices
  );
};

/**
 * Fetches quotes in batch for uniqueIDs.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @returns {[Quote]} Array of requested objects.
 */
fetchQuotes = async function fetchQuotes(shortSymbols) {
  throwIfEmptyArray(shortSymbols, `fetchQuotes shortSymbols`);

  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);
  const tickersString = tickers.join(",");
  const api = `/v3/quote/${tickersString}`;

  // https://financialmodelingprep.com/api/v3/quote/GOOG,AAPL,FB?apikey=969387165d69a8607f9726e8bb52b901
  return await _fmpFetchAndMapObjects(
    api,
    tickers,
    null,
    idByTicker,
    _fixFMPQuote
  );
};

/**
 * Fetches splits in batch for uniqueIDs.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @returns {[Split]} Array of requested objects.
 */
fetchSplits = async function fetchSplits(shortSymbols) {
  throwIfEmptyArray(shortSymbols, `fetchSplits shortSymbols`);

  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);

  // https://financialmodelingprep.com/api/v3/historical-price-full/stock_split/AAPL,AAP?apikey=969387165d69a8607f9726e8bb52b901
  return await _fmpFetchBatchAndMapArray(
    "/v3/historical-price-full/stock_split",
    tickers,
    null,
    5,
    null,
    'historicalStockList',
    'historical',
    idByTicker,
    _fixFMPSplits
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
async function _fmpFetchBatchAndMapArray(api, tickers, queryParameters, maxBatchSize, limit, groupingKey, dataKey, idByTicker, mapFunction) {
  return await _fmpFetchBatch(api, tickers, queryParameters, maxBatchSize, groupingKey)
    .then(async datas => {
      if (datas[groupingKey] != null) {
        const dataByTicker = datas[groupingKey].toDictionary('symbol');
        const fixedDataPromises = tickers
          .map(async ticker => {
            const data = dataByTicker[ticker];
            if (data != null && data[dataKey] != null) {
              let tickerData = data[dataKey];
              if (limit != null) {
                tickerData = tickerData.slice(0, limit);
              }

              return await mapFunction(tickerData, idByTicker[ticker]);
            } else {
              return [];
            }
          })
          
        return await Promise.allLmited(fixedDataPromises, 100)
          .then(x => x.flat())

      } else {
        throw `Unexpected response format for ${api}`
      }
    });
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
async function _fmpFetchAndMapObjects(api, tickers, queryParameters, idByTicker, mapFunction) {
  return await _fmpFetch(api, queryParameters)
    .then(datas => {
      const dataByTicker = datas.toDictionary('symbol');

      return tickers
        .compactMap(ticker => {
          const tickerData = dataByTicker[ticker];
          if (tickerData != null) {
            return mapFunction(tickerData, idByTicker[ticker]);
          } else {
            return null;
          }
        })
    });
}

//////////////////////////////////// Base Fetch

/**
 * @param {string} api API to call.
 * @param {[string]} tickers Ticker Symbols to fetch, e.g. ['AAP','AAPL','PBA'].
 * @param {Object} queryParameters Additional query parameters.
 * @param {Int} maxBatchSize Max allowed batch size.
 * @param {string} groupingKey Batch grouping key, e.g. 'historicalStockList'.
 * @returns {Promise<{string: {string: Object|[Object]}}>} Parsed EJSON object. Composed from several responses if max symbols count was exceeded. 
 * The first object keys are symbols. The next inner object keys are types. And the next inner object is an array of type objects.
 */
async function _fmpFetchBatch(api, tickers, queryParameters, maxBatchSize, groupingKey) {
  throwIfUndefinedOrNull(api, `_fmpFetchBatch api`);
  throwIfEmptyArray(tickers, `_fmpFetchBatch tickers`);

  if (queryParameters == null) {
    queryParameters = {};
  } else {
    queryParameters = Object.assign({}, queryParameters);
  }

  let chunkedSymbolsArray;
  if (maxBatchSize == null) {
    chunkedSymbolsArray = [tickers];
  } else {
    chunkedSymbolsArray = tickers.chunked(maxBatchSize);
  }

  var result = { [groupingKey]: [] };
  for (const chunkedSymbols of chunkedSymbolsArray) {
    const tickersString = chunkedSymbols.join(",");
    const batchAPI = `${api}/${tickersString}`;
    console.log(`Fetching batch for symbols (${chunkedSymbols.length}) with query '${queryParameters.stringify()}': ${tickersString}`);
    
    let response
    try {
      response = await _fmpFetch(batchAPI, queryParameters);
    } catch(error) {
      if (error.statusCode == 404) {
        response = {};
      } else {
        throw error;
      }
    }

    if (chunkedSymbols.length == 1) {
      if (Object.entries(response).length > 0) {
        result[groupingKey].push(response);
      } else {
        console.logVerbose(`No data for ticker: ${tickersString}`);
      }

    } else if (response[groupingKey] != null) {
      throwIfNotArray(response[groupingKey], `_fmpFetchBatch response[groupingKey]`);
      result[groupingKey] = result[groupingKey].concat(response[groupingKey]);

    } else {
      console.logVerbose(`No data for tickers: ${tickersString}`);
    }
  }

  return result;
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
    throwIfUndefinedOrNull(fmpCompany, `fixCompany company`);
    throwIfUndefinedOrNull(symbolID, `fixCompany symbolID`);
  
    console.logVerbose(`Company data fix start`);
    const company = {};
    company._id = symbolID;
    company.c = fmpCompany.currency;
    company.i = fmpCompany.industry;
    company.n = fmpCompany.companyName;

    if (fmpCompany.isAdr == true) {
      company.t = "ad";
    } else if (fmpCompany.isEtf == true) {
      company.t = "et";
    } else if (fmpCompany.isFund == true) {
      company.t = "f";
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
async function _fixFMPDividends(fmpDividends, symbolID) {
  try {
    throwIfNotArray(fmpDividends, `fixDividends fmpDividends`);
    throwIfUndefinedOrNull(symbolID, `fixDividends uniqueID`);
    if (!fmpDividends.length) { 
      console.logVerbose(`FMP dividends are empty for ${symbolID}. Nothing to fix.`);
      return []; 
    }

    // Get currency from company
    const currency = await fmp.collection("companies")
      .findOne(
        { _id: symbolID },
        { c: 1 }
      )
      .then(x => x.c);

    let previousFrequency = 'u';
    let nextDate = null;
  
    console.logVerbose(`Fixing dividends for ${symbolID}`);
    return fmpDividends
      .filterNull()
      .sort((l, r) => l.date.localeCompare(r.date))
      .map((fmpDividend, i, arr) => {
        let date;
        if (nextDate == null) {
          date = _getOpenDate(fmpDividend.date)
        } else {
          date = nextDate;
        }

        let nextDividend;
        if (i + 1 >= arr.length) {
          nextDividend = null;
          nextDate = null;
        } else {
          nextDividend = arr[i + 1];
          nextDate = _getOpenDate(nextDividend.date);
        }

        const dividend = {};
        dividend.c = currency;
        dividend.e = date;
        dividend.d = _getOpenDate(fmpDividend.declarationDate);
        dividend.p = _getOpenDate(fmpDividend.paymentDate);
        dividend.s = symbolID;

        const amount = _getFmpDividendAmount(fmpDividend);
        if (amount == null) {
          console.error(`No amount for dividend ${symbolID}: ${fmpDividend.stringify()}`)
          return null;

        } else {
          dividend.a = amount;
        }

        // Frequency computation
        // TODO: Improve later by including more cases
        if (nextDate != null) {
          const days = (nextDate - date) / 86400000;
          if (days <= 11) {
            dividend.f = 'w';
          } else if (days <= 45) {
            dividend.f = 'q';
          } else if (days <= 270) {
            dividend.f = 's';
          } else if (days <= 540) {
            dividend.f = 'a';
          } else {
            dividend.f = 'u';
          }
        } else {
          // Continue previous frequency when there is no next dividend
          dividend.f = previousFrequency;
        }

        previousFrequency = dividend.f;
    
        return dividend;
      })
      .filterNull()
      // Filter duplicate
      // TODO: Improve later by including more cases
      .filter((dividend, i, arr) => {
        if (i + 1 >= arr.length) {
          return true
        }
        
        const nextDividend = arr[i + 1];
        if (dividend.f !== nextDividend.f && dividend.f === 'w' && dividend.a === nextDividend.a) {
          console.error(`Duplicate dividend for ${dividend.s}: ${dividend.stringify()}`);
          return false;
        } else {
          console.log(`----`);
          console.log(`dividend: ${dividend}`);
          console.log(`nextDividend: ${nextDividend}`);
          console.log(`----`);
          return true;
        }
      });

  } catch(error) {
    console.error(`Unable to fix dividends ${fmpDividends.stringify()}: ${error}`);
    return [];
  }
}

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

/**
 * Fixes historical prices object so it can be added to MongoDB.
 * @param {[FMPHistoricalPrices]} fmpHistoricalPrices Historical prices object.
 * @param {ObjectId} symbolID Symbol object ID.
 * @returns {[HistoricalPrices]} Returns fixed objects or an empty array if fix wasn't possible.
 */
function _fixFMPHistoricalPrices(fmpHistoricalPrices, symbolID) {
  try {
    throwIfNotArray(fmpHistoricalPrices, `fixHistoricalPrices fmpHistoricalPrices`);
    throwIfUndefinedOrNull(symbolID, `fixHistoricalPrices uniqueID`);
    if (!fmpHistoricalPrices.length) { 
      console.logVerbose(`Historical prices are empty for ${symbolID}. Nothing to fix.`);
      return []; 
    }

    // Bucket prices by month using 15 day.
    const monthStart = Date.monthStart().dayString();
    const fmpHistoricalPricesByMonth = fmpHistoricalPrices
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
        historicalPrice.c = BSON.Double(averagePrice);
        historicalPrice.d = _getCloseDate(month);
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
    throwIfUndefinedOrNull(fmpQuote, `fixQuote quote`);
    throwIfUndefinedOrNull(symbolID, `fixQuote symbolID`);
  
    console.logVerbose(`Previous day price data fix start`);
    const quote = {};
    quote._id = symbolID;
    quote.l = fmpQuote.price;

    if (fmpQuote.pe != null) {
      quote.p = BSON.Double(fmpQuote.pe);
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
    throwIfNotArray(fmpSplits, `fixSplits splits`);
    throwIfUndefinedOrNull(symbolID, `fixSplits symbolID`);
    if (!fmpSplits.length) { 
      console.logVerbose(`Splits are empty for ${symbolID}. Nothing to fix.`);
      return []; 
    }
  
    console.logVerbose(`Fixing splits for ${symbolID}`);
    return fmpSplits
      .filterNull()
      .map(fmpSplit => {
        const split = {};
        split.e = _getOpenDate(fmpSplit.date);
        split.s = symbolID;

        if (fmpSplit.denominator > 0 && fmpSplit.numerator > 0) {
          split.r = BSON.Double(fmpSplit.denominator / fmpSplit.numerator);
        } else {
          console.error(`No ratio for split '${symbolID}': ${fmpSplit.stringify()}`)
          return null
        }

        return split;
      })
      .filterNull();

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
      .filterNull()
      // We only support 'MCX' at the moment
      .filter(fmpSymbol => fmpSymbol.exchangeShortName === "MCX")
      .map(fmpSymbol => {
        const symbol = {};
        symbol.n = fmpSymbol.name;
        symbol.t = fmpSymbol.symbol;

        return symbol;
      });

  } catch (error) {
    console.error(`Unable to fix symbols ${fmpSymbols.stringify()}: ${error}`);
    return [];
  }
};

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

exports = function() {
  context.functions.execute("utils");

  if (typeof fmp === 'undefined') {
    fmp = atlas.db("fmp");
  }

  if (typeof apikey === 'undefined') {
    apikey = "969387165d69a8607f9726e8bb52b901";
  }
  
  console.log("Imported FMP utils");
};
