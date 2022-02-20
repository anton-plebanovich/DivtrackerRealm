
// fmpUtils.js

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
 * @returns {[Dividend]} Array of requested objects.
 */
fetchDividends = async function fetchDividends(shortSymbols) {
  throwIfEmptyArray(shortSymbols, `fetchDividends shortSymbols`);

  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);
  const tickersString = tickers.join(",");
  const api = `/v3/historical-price-full/stock_dividend/${tickersString}`;

  // https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/AAPL?apikey=969387165d69a8607f9726e8bb52b901
  // https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/AAPL,AAP?apikey=969387165d69a8607f9726e8bb52b901
  return await _fmpFetchAndMapArray(
    api,
    tickers,
    null,
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
 * Fetches previous day prices in batch for short symbols.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @returns {[PreviousDayPrice]} Array of requested objects.
 */
fetchPreviousDayPrices = async function _fetchPreviousDayPrices(shortSymbols) {
  throwIfEmptyArray(shortSymbols, `fetchPreviousDayPrices shortSymbols`);

  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);
  const tickersString = tickers.join(",");
  const api = `/v3/historical-price-full/${tickersString}`;

  // https://financialmodelingprep.com/api/v3/historical-price-full/AAPL,AAP?timeseries=1&apikey=969387165d69a8607f9726e8bb52b901
  return await _fmpFetchAndMapArray(
    api,
    tickers,
    { timeseries: 1 },
    'historicalStockList',
    'historical',
    idByTicker,
    _fixFMPPreviousDayPrice
  );
};

/**
 * Fetches historical prices in batch for uniqueIDs.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @returns {[HistoricalPrice]} Array of requested objects.
 */
fetchHistoricalPrices = async function fetchHistoricalPrices(shortSymbols) {
  throwIfEmptyArray(shortSymbols, `fetchHistoricalPrices shortSymbols`);

  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);
  const tickersString = tickers.join(",");
  const api = `/v3/historical-price-full/${tickersString}`;

  // https://financialmodelingprep.com/api/v3/historical-price-full/AAPL,AAP?serietype=line&apikey=969387165d69a8607f9726e8bb52b901
  return await _fmpFetchAndMapArray(
    api,
    tickers,
    { serietype: "line" },
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
  const tickersString = tickers.join(",");
  const api = `/v3/historical-price-full/stock_split/${tickersString}`;

  // https://financialmodelingprep.com/api/v3/historical-price-full/stock_split/AAPL,AAP?apikey=969387165d69a8607f9726e8bb52b901
  return await _fmpFetchAndMapArray(
    api,
    tickers,
    null,
    'historicalStockList',
    'historical',
    idByTicker,
    _fixFMPSplits
  );
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
  return _fmpFetch(api, queryParameters)
    .then(datas => {
      const datasByTicker = datas.toBuckets('symbol');
      return tickers
        .map(ticker => {
          const datas = datasByTicker[ticker];
          return mapFunction(datas, idByTicker[ticker]);
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
 * @param {string} groupingKey Batch grouping key, e.g. 'historicalStockList'.
 * @param {string} dataKey Data key, e.g. 'historical'.
 * @param {[string]} idByTicker Dictionary of ticker symbol ID by ticker symbol.
 * @param {function} mapFunction Function to map data to our format.
 * @returns {Promise<[Object]>} Flat array of entities.
 */
async function _fmpFetchAndMapArray(api, tickers, queryParameters, groupingKey, dataKey, idByTicker, mapFunction) {
  return _fmpFetch(api, queryParameters)
    .then(datas => {
      if (tickers.length === 1) {
        const data = datas;
        const ticker = tickers[0];
        const tickerData = data[dataKey];
        if (tickerData != null) {
          return mapFunction(tickerData, idByTicker[ticker]);
        } else {
          return [];
        }

      } else if (datas[groupingKey] != null) {
        const dataByTicker = datas[groupingKey].toDictionary('symbol');
        return tickers
          .map(ticker => {
            const data = dataByTicker[ticker];
            const tickerData = data[dataKey];
            if (tickerData != null) {
              return mapFunction(tickerData, idByTicker[ticker]);
            } else {
              return [];
            }
          })
          .flat()

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
  return _fmpFetch(api, queryParameters)
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
 * Requests data from FMP. 
 * @param {string} api API to call.
 * @param {Object} queryParameters Query parameters.
 * @returns Parsed EJSON object.
 */
async function _fmpFetch(api, queryParameters) {
  throwIfUndefinedOrNull(api, `_fmpFetch api`);
  if (queryParameters == null) {
    queryParameters = {}
  }
  
  const baseURL = "https://financialmodelingprep.com/api";
  queryParameters.apikey = "969387165d69a8607f9726e8bb52b901";

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
    throwIfUndefinedOrNull(fmpDividends, `fixDividends fmpDividends`);
    throwIfUndefinedOrNull(symbolID, `fixDividends uniqueID`);
    if (!fmpDividends.length) { 
      console.logVerbose(`FMP dividends are empty for ${symbolID}. Nothing to fix.`);
      return []; 
    }
  
    console.logVerbose(`Fixing dividends for ${symbolID}`);
    return fmpDividends
      .filterNull()
      .map(fmpDividend => {
        const dividend = {};
        dividend.e = _getOpenDate(fmpDividend.date);
        dividend.d = _getOpenDate(fmpDividend.declarationDate);
        dividend.p = _getOpenDate(fmpDividend.paymentDate);
        dividend.s = symbolID;

        if (fmpDividend.dividend != null) {
          dividend.a = BSON.Double(fmpDividend.dividend);
        }
    
        return dividend;
      });

  } catch(error) {
    return [];
  }
}

/**
 * Fixes previous day price object so it can be added to MongoDB.
 * @param {FMPPreviousDayPrice} fmpPreviousDayPrice Previous day price object.
 * @param {ObjectId} symbolID Symbol object ID.
 * @returns {PreviousDayPrice|null} Returns fixed object or `null` if fix wasn't possible.
 */
function _fixFMPPreviousDayPrice(fmpPreviousDayPrice, symbolID) {
  try {
    throwIfUndefinedOrNull(fmpPreviousDayPrice, `fixPreviousDayPrice fmpPreviousDayPrice`);
    throwIfUndefinedOrNull(symbolID, `fixPreviousDayPrice symbolID`);
  
    console.logVerbose(`Previous day price data fix start`);
    const previousDayPrice = {};
    previousDayPrice._id = symbolID;

    if (fmpPreviousDayPrice.close != null) {
      previousDayPrice.c = BSON.Double(fmpPreviousDayPrice.close);
    }
  
    return previousDayPrice;

  } catch(error) {
    // {"AQNU":{"previous":null}}
    // {"AACOU":{"previous":null}}
    return null;
  }
};

/**
 * Fixes historical prices object so it can be added to MongoDB.
 * @param {[FMPHistoricalPrices]} fmpHistoricalPrices Historical prices object.
 * @param {ObjectId} symbolID Symbol object ID.
 * @returns {[HistoricalPrices]} Returns fixed objects or an empty array if fix wasn't possible.
 */
function _fixFMPHistoricalPrices(fmpHistoricalPrices, symbolID) {
  try {
    throwIfUndefinedOrNull(fmpHistoricalPrices, `fixHistoricalPrices fmpHistoricalPrices`);
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
    return null;
  }
};

/**
 * Fixes splits object so it can be added to MongoDB.
 * @param {Object} fmpSplits Splits object.
 * @param {ObjectId} symbolID Symbol object ID.
 * @returns {[Object]} Returns fixed objects or an empty array if fix wasn't possible.
 */
function _fixFMPSplits(fmpSplits, symbolID) {
  try {
    throwIfUndefinedOrNull(fmpSplits, `fixSplits splits`);
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

        if (fmpSplit.denominator != null && fmpSplit.numerator != null) {
          split.r = BSON.Double(fmpSplit.denominator / fmpSplit.numerator);
        } else {
          console.error(`No ratio for split: ${fmpSplit.stringify()}`)
        }

        return split;
      });

  } catch (error) {
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
  
  console.log("Imported FMP utils");
};
