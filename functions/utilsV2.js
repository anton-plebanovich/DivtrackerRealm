
// utilsV2.js

///////////////////////////////////////////////////////////////////////////////// EXTENSIONS

/**
 * Safely executes Bulk operation by catching 'no operations' error.
 */
Object.prototype.safeExecute = async function() {
  try {
    return await this.execute();
  } catch(error) {
    if (error.message !== 'Failed to execute bulk writes: no operations specified') {
      throw new _SystemError(error);
    }
  }
};

/**
 * Executes find by field and update or insert for a new object from an old object.
 * Uses `_id` field by default.
 */
Object.prototype.findAndUpdateOrInsertIfNeeded = function(newObject, oldObject, field) {
  if (newObject == null) {
    throw new _SystemError(`New object should not be null for insert or update`);
    
  } else if (oldObject == null) {
    // No old object means we should insert
    console.log(`Inserting: ${newObject.stringify()}`);
    return this.insert(newObject);

  } else {
    return this.findAndUpdateIfNeeded(newObject, oldObject, field);
  }
};

/**
 * Executes find by field and update for a new object from an old object if needed.
 * Uses `_id` field by default.
 */
Object.prototype.findAndUpdateIfNeeded = function(newObject, oldObject, field) {
  if (field == null) {
    field = '_id';
  }

  const value = newObject[field];

  if (newObject == null) {
    throw new SystemError(`New object should not be null for update`);
    
  } else if (oldObject == null) {
    throw new SystemError(`Old object should not be null for update`);

  } else if (newObject[field] == null) {
    throw new SystemError(`New object '${field}' field should not be null for update`);

  } else {
    const update = newObject.updateFrom(oldObject);
    if (update == null) {
      // Update is not needed
      return;

    } else {
      return this
        .find({ [field]: value })
        .updateOne(update);
    }
  }
};

/**
 * Computes update parameter for `updateOne` collection method.
 * Update direction is from `object` towards `this`.
 * Only non-equal fields are added to `$set` and missing fields
 * are added to `$unset`.
 */
Object.prototype.updateFrom = function(object) {
  if (this.isEqual(object)) {
    return null;
  }

  const set = Object.assign({}, this);
  const unset = {};

  // Delete `null` values from the `set`
  const newEntries = Object.entries(this);
  for (const [key, newValue] of newEntries) {
    if (newValue == null) {
      delete set[key];
    }
  }

  // Collect keys to unset
  let hasUnsets = false;
  const oldEntries = Object.entries(object);
  for (const [key, oldValue] of oldEntries) {
    const newValue = set[key];
    if (newValue == null) {
      unset[key] = "";
      hasUnsets = true;

    } else if (newValue.isEqual(oldValue)) {
      delete set[key];
    }
  }

  let update;
  if (hasUnsets) {
    update = { $set: set, $unset: unset };
  } else {
    update = { $set: set };
  }

  if (isSandbox) {
    console.logData(`Updating`, update);
  } else {
    console.log(`Updating: ${update.stringify()}`);
  }

  return update;
};

/**
 * @note Using 'distinct' instead of 'unique' to match MongoDB method and because values itself might not be _unique_.
 * @param {function|undefined|string} arg Check for equality if nothing is passed. 
 * Using comparison function if passed. 
 * Using string for key comparison if passed.
 * @returns {Array} Distinct array of elements
 */
Array.prototype.distinct = function(arg) {
  if (typeof arg === 'string' || arg instanceof String) {
    comparer = (a, b) => a[arg] === b[arg];
  } else if (!arg) {
    comparer = (a, b) => a === b;
  } else {
    comparer = arg;
  }

  const result = [];
  for (const item of this) {
    var hasItem = false;
    for (const distinctItem of result) {
      if (comparer(distinctItem, item)) {
        hasItem = true;
        break;
      }
    }

    if (!hasItem) {
      result.push(item);
    }
  }

  return result;
};

/**
 * Removes all occurencies of an object from the array.
 * @param {*} arg Object to remove.
 */
Array.prototype.remove = function(arg) {
  var i = 0;
  while (i < this.length) {
    if (this[i] === arg) {
      this.splice(i, 1);
    } else {
      ++i;
    }
  }
};

/**
 * Removes all occurencies of objets in the array.
 * @param {Array} arg Objects to remove.
 */
Array.prototype.removeContentsOf = function(arg) {
  for (const object of arg) {
    this.remove(object);
  }
};

/**
 * Splits array into array of chunked arrays.
 * @param {number} size Size of a chunk.
 * @returns {object[][]} Array of chunked arrays.
 */
Array.prototype.chunked = function(size) {
  var chunks = [];
  for (i=0,j=this.length; i<j; i+=size) {
      const chunk = this.slice(i,i+size);
      chunks.push(chunk);
  }

  return chunks;
};

/**
 * Filters `null` elements.
 * @param {*} callbackfn Mapping to perform. Null values are filtered.
 */
Array.prototype.filterNull = function() {
   return this.filter(x => x !== null);
};

/**
 * Maps array and filters `null` elements.
 * @param {*} callbackfn Mapping to perform. Null values are filtered.
 */
Array.prototype.compactMap = function(callbackfn) {
   return this.map(callbackfn).filterNull();
};

/**
 * Creates dictionary from objects using provided `key` as source for keys and object as value.
 * @param {*} callbackfn Mapping to perform. Null values are filtered.
 */
Array.prototype.toDictionary = function(key) {
  return this.reduce((dictionary, value) => 
    Object.assign(dictionary, {[value[key]]: value}
  ), {});
};

/**
 * @returns {Date} Yesterday day start date in UTC.
 */
Date.yesterday = function() {
  const yesterday = new Date();
  yesterday.setUTCDate(yesterday.getUTCDate() - 1);
  yesterday.setUTCHours(0);
  yesterday.setUTCMinutes(0);
  yesterday.setUTCSeconds(0);
  yesterday.setUTCMilliseconds(0);

  return yesterday;
};

/**
 * @param {Date} otherDate Other date.
 * @returns {number} absolute amount of days between source date and other date.
 */
Date.prototype.daysTo = function(otherDate) {
  // The number of milliseconds in all UTC days (no DST)
  const oneDay = 1000 * 60 * 60 * 24;

  // A day in UTC always lasts 24 hours (unlike in other time formats)
  const first = Date.UTC(otherDate.getUTCFullYear(), otherDate.getUTCMonth(), otherDate.getUTCDate());
  const second = Date.UTC(this.getUTCFullYear(), this.getUTCMonth(), this.getUTCDate());

  // so it's safe to divide by 24 hours
  return Math.abs(first - second) / oneDay;
};

/// `String` "2020-12-31"
Date.prototype.dayString = function() {
  return `${zeroPad(this.getUTCFullYear(), 2)}-${zeroPad(this.getUTCMonth() + 1, 2)}-${zeroPad(this.getUTCDate(), 2)}`;
};

/**
 * Returns `Date` in a format of API parameter. E.g. '20211002'.
 * @returns {string} date string.
 */
Date.prototype.apiParameter = function() {
  const year = zeroPad(this.getUTCFullYear(), 2);
  const month = zeroPad(this.getUTCMonth() + 1, 2);
  const day = zeroPad(this.getUTCDate(), 2);
  const dateString = `${year}${month}${day}`;
  
  return dateString;
};

/**
 * Stringifies object using JSON format.
 * @returns Stringified object in JSON format.
 */
Object.prototype.stringify = function() {
  return JSON.stringify(this);
};

/**
 * Checks simple plain objects equality.
 * @returns {boolean} Comparison result.
 */
Object.prototype.isEqual = function(rhs) {
  const lhsEntries = Object.entries(this);
  const rhsEntries = Object.entries(rhs);

  if (lhsEntries.length !== rhsEntries.length) {
    return false;

  } else if (!lhsEntries.length) {
    return this.toString() === rhs.toString();
  }

  for (const [key, lhsValue] of lhsEntries) {
    const rhsValue = rhs[key];
    if (!lhsValue.isEqual(rhsValue)) {
      return false;
    }
  }

  return true;
};

Number.prototype.isEqual = function(number) {
  return this === number;
};

String.prototype.isEqual = function(string) {
  return this === string;
};

///////////////////////////////////////////////////////////////////////////////// ERRORS PROCESSING

const errorType = {
	USER: "user",
	SYSTEM: "system",
	COMPOSITE: "composite",
};

class _UserError {
  constructor(message) {
    this.type = errorType.USER;
    
    if (typeof message === 'string') {
      this.message = message;
    } else {
      this.message = message.message;
    }
  }

  toString() {
    return this.stringify();
  }
}

UserError = _UserError;

class _SystemError {
  constructor(message) {
    this.type = errorType.SYSTEM;

    if (typeof message === 'string') {
      this.message = message;
    } else {
      this.message = message.message;
    }
  }

  toString() {
    return this.stringify();
  }
}

SystemError = _SystemError

class _CompositeError {
  constructor(errors) {
    if (Object.prototype.toString.call(errors) !== '[object Array]') {
      throw 'CompositeError should be initialized with errors array';
    }

    this.type = errorType.COMPOSITE;

    const system_errors = [];
    const user_errors = [];
    const composite_errors = [];
    const unknown_errors = [];
    errors.forEach(error => {
      if (error.type === errorType.SYSTEM) {
        system_errors.push(error);
      } else if (error.type === errorType.USER) {
        user_errors.push(error);
      } else if (error.type === errorType.COMPOSITE) {
        composite_errors.push(error);
      } else {
        unknown_errors.push(error);
      }
    });

    composite_errors.forEach(x => {
      if (typeof x.system_errors !== 'undefined' && x.system_errors.length) {
        system_errors.concat(x.system_errors);
      }

      if (typeof x.user_errors !== 'undefined' && x.user_errors.length) {
        user_errors.concat(x.user_errors);
      }
      
      if (typeof x.unknown_errors !== 'undefined' && x.unknown_errors.length) {
        unknown_errors.concat(x.unknown_errors);
      }
    });

    if (user_errors.length) {
      this.user_errors = user_errors;
    }
    
    if (system_errors.length) {
      this.system_errors = system_errors;
    }
    
    if (unknown_errors.length) {
      this.unknown_errors = unknown_errors;
    }
  }

  toString() {
    return this.stringify();
  }
}

CompositeError = _CompositeError;

var runtimeExtended = false;
function extendRuntime() {
  if (runtimeExtended) { return; }
  
  Promise.safeAllAndUnwrap = function(promises) {
    return Promise.safeAll(promises)
      .then(results => new Promise((resolve, reject) => {
          const datas = [];
          const errors = [];
          results.forEach(result => {
            const [data, error] = result;
            datas.push(data);
            if (typeof error !== 'undefined') {
              errors.push(error);
            }
          });

          if (errors.length) {
            reject(new CompositeError(errors));
          } else {
            resolve(datas);
          }
        })
      );
  };

  Promise.safeAll = function(promises) {
    return Promise.all(
      promises.map(promise => promise.safe(promise))
    );
  };

  Promise.prototype.safe = function() {
    return this.then(
      data => [data, undefined],
      error => [undefined, error]
    );
  };
  
  Promise.prototype.mapErrorToUser = function() {
    return new Promise((resolve, reject) =>
      this.then(
        data => resolve(data),
        error => reject(new _UserError(error))
      )
    );
  };
  
  Promise.prototype.mapErrorToSystem = function() {
    return new Promise((resolve, reject) =>
      this.then(
        data => resolve(data),
        error => reject(new _SystemError(error))
      )
    );
  };
  
  Promise.prototype.mapErrorToComposite = function() {
    return new Promise((resolve, reject) =>
      this.then(
        data => resolve(data),
        errors => reject(new _CompositeError(errors))
      )
    );
  };

  runtimeExtended = true;
}

///////////////////////////////////////////////////////////////////////////////// FUNCTIONS

function _logAndThrow(message) {
  _throwIfUndefinedOrNull(message, `logAndThrow message`);
  console.error(message);
  throw message;
}

logAndThrow = _logAndThrow;

/** Checks that we didn't exceed 90s timeout. */
checkExecutionTimeout = function checkExecutionTimeout() {
  if (typeof startDate === 'undefined') {
    startDate = new Date();
  }

  const endTime = new Date();
  const seconds = Math.round((endTime - startDate) / 1000);

  // One symbol full fetch takes 17.5s and we have only 90s function execution time so let's put some limit.
  const limit = 80;
  if (seconds > limit) {
    _logAndThrow('execution timeout');
  } else {
    console.logVerbose(`${limit - seconds} execution time left`);
  }
};

/**
 * Throws error with optional `message` is `object` is `undefined` or `null`.
 * @param {object} object Object to check.
 * @param {string} message Optional additional error message.
 * @returns {object} Passed object if it's defined and not `null`.
 */
function _throwIfUndefinedOrNull(object, message) {
  if (typeof object === 'undefined') {
    if (typeof message === 'undefined' && message.length) {
      _logAndThrow(`Object undefiled: ${message}`);
    } else {
      _logAndThrow(`Object undefiled`);
    }
    
  } else if (object === null) {
    if (typeof message !== 'undefined' && message.length) {
      _logAndThrow(`Object null: ${message}`);
    } else {
      _logAndThrow(`Object null`);
    }
    
  } else {
    return object;
  }
}

throwIfUndefinedOrNull = _throwIfUndefinedOrNull;

getValueAndQuitIfUndefined = function _getValueAndQuitIfUndefined(object, key) {
  if (!object) {
    _logAndThrow(`Object undefiled`);
    
  } else if (!key) {
    return object;

  } else if (!object[key]) {
    _logAndThrow(`Object's property undefiled. Key: ${key}. Object: ${object}.`);
    
  } else {
    return object[key];
  }
};

/** 
 * First parameter: Date in the "yyyy-mm-dd" or timestamp or Date format, e.g. "2020-03-27" or '1633046400000' or Date.
 * Returns close 'Date' pointing to the U.S. stock market close time.
 *
 * Regular trading hours for the U.S. stock market, including 
 * the New York Stock Exchange (NYSE) and the Nasdaq Stock Market (Nasdaq),
 * are 9:30 a.m. to 4 p.m.
 */
getCloseDate = function getCloseDate(closeDateValue) {
  _throwIfUndefinedOrNull(closeDateValue, `cloiseDateValue`);

  // Check if date is valid
  const date = new Date(closeDateValue);
  if (isNaN(date)) {
    console.error(`Invalid close date: ${closeDateValue}`);
    return null;
  }

  // Eastern Standard Time (EST) time zone is 5 hours behind GMT during autumn/winter
  // https://en.wikipedia.org/wiki/Eastern_Time_Zone
  const closeDateStringWithTimeZone = `${date.dayString()}T16:00:00-0500`;
  const closeDate = new Date(closeDateStringWithTimeZone);
  if (isNaN(closeDate)) {
    console.error(`Invalid close date with time zone: ${closeDateStringWithTimeZone}`);
    return null;
  } else {
    return closeDate;
  }
};

/** 
 * First parameter: Date in the "yyyy-mm-dd" or timestamp or Date format, e.g. "2020-03-27" or '1633046400000' or Date.
 * Returns open 'Date' pointing to the U.S. stock market open time.
 *
 * Regular trading hours for the U.S. stock market, including 
 * the New York Stock Exchange (NYSE) and the Nasdaq Stock Market (Nasdaq),
 * are 9:30 a.m. to 4 p.m.
 */
getOpenDate = function getOpenDate(openDateValue) {
  _throwIfUndefinedOrNull(openDateValue, `openDateValue`);

  // Check if date is valid
  const date = new Date(openDateValue);
  if (isNaN(date)) {
    console.error(`Invalid open date: ${openDateValue}`);
    return null;
  }

  // Eastern Standard Time (EST) time zone is 5 hours behind GMT during autumn/winter
  // https://en.wikipedia.org/wiki/Eastern_Time_Zone
  const openDateStringWithTimeZone = `${date.dayString()}T9:30:00-0500`;
  const openDate = new Date(openDateStringWithTimeZone);

  if (isNaN(openDate)) {
    console.error(`Invalid open date with time zone: ${openDateStringWithTimeZone}`);
    return null;
  } else {
    return openDate;
  }
};

// exports();
//
// checkExecutionTimeout()
// getCloseDate("2020-03-27");
// getOpenDate("2020-03-27");

/** 
 * Computes and returns sorted unique IDs from companies and user transactions.
 * @note Using 'unique' instead of 'distinct' here because values are actually _unique_.
 * @returns {Promise<["AAPL:NAS"]>} Array of unique IDs.
*/
getUniqueIDs = async function getUniqueIDs() {
  // We combine transactions and companies distinct IDs. 
  // Idealy, we should be checking all tables but we assume that only two will be enough.
  // All symbols have company record so company DB contains all ever fetched symbols.
  // Meanwhile transactions may contain not yet fetched symbols or have less symbols than we already should be updating (transactions may be deleted).
  // So by combining we have all current + all future symbols. Idealy.
  const companiesCollection = db.collection("companies");
  const symbolsCollection = db.collection("symbols");
  const [companiesUniqueIDs, uniqueTransactionIDs] = await Promise.all([
    companiesCollection.distinct("_id", {}),
    getUniqueTransactionIDs()
  ]); 

  console.log(`Unique companies IDs (${companiesUniqueIDs.length})`);
  console.logData(`Unique companies IDs (${companiesUniqueIDs.length})`, companiesUniqueIDs);

  // Compute unique IDs using both sources
  let uniqueIDs = companiesUniqueIDs
    .concat(uniqueTransactionIDs)
    .distinct();

  // Filter non-existing
  const allSymbols = await symbolsCollection.distinct("_id", { _id: { $in: uniqueIDs } });
  uniqueIDs = uniqueIDs.filter(id => allSymbols.includes(id));

  console.log(`Unique IDs (${uniqueIDs.length})`);
  console.logData(`Unique IDs (${uniqueIDs.length})`, uniqueIDs);

  return uniqueIDs;
};

/** 
 * @returns {Promise<["AAPL:NAS"]>} Array of unique transaction IDs, e.g. ["AAPL:NAS"]
*/
async function _getUniqueTransactionIDs() {
  // We project '_i' field first and then produce unique objects with only '_id' field
  const transactionsCollection = db.collection("transactions");
  const transactionsAggregation = [{$project: {
    _i: { $concat: [ "$s", ":", "$e" ] }
  }}, {$group: {
    _id: "$_i"
  }}];

  const uniqueTransactionIDs = await transactionsCollection
    .aggregate(transactionsAggregation)
    .toArray()
    // Extract IDs from [{ _id: "MSFT:NAS" }]
    .then(x => x.map(x => x._id));

  uniqueTransactionIDs.sort();

  console.log(`Unique transaction IDs (${uniqueTransactionIDs.length})`);
  console.logData(`Unique transaction IDs (${uniqueTransactionIDs.length})`, uniqueTransactionIDs);

  return uniqueTransactionIDs;
}

getUniqueTransactionIDs = _getUniqueTransactionIDs;

/** 
 * @returns {Promise<[["AAPL"], ["NAS"]]>} Array of unique symbols and exchanges IDs, e.g. [["AAPL"], ["NAS"]]
*/
getUniqueSymbolsAndExchanges = async function getUniqueSymbolsAndExchanges() {
  const symbolsCollection = db.collection("symbols");
  const uniqueIDs = await symbolsCollection.distinct("_id");
  const uniqueSymbols = [];
  const uniqueExchanges = [];
  uniqueIDs.forEach(uniqueID => {
      const [validSymbol, validExchange] = uniqueID.split(':');
      uniqueSymbols.push(validSymbol);

      if (!uniqueExchanges.includes(validExchange)) {
        uniqueExchanges.push(validExchange);
      }
    });

  console.log(`Unique symbols (${uniqueSymbols.length})`);
  console.logData(`Unique symbols (${uniqueSymbols.length})`, uniqueSymbols);
  console.log(`Unique exchanges (${uniqueExchanges.length}): ${uniqueExchanges}`);

  return [uniqueSymbols, uniqueExchanges];
};

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

//////////////////////////////////// Example calls

// Fetch the last historical price record for the given period for the AAPL symbol
// https://cloud.iexapis.com/stable/time-series/historical_prices/aapl?token=Tpk_0a569a8bdc864334b284cb7112f579df&from=2021-01-01&to=2021-02-01&last=1

/**
 * Requests data from IEX cloud/sandbox depending on an environment by a batch.
 * If symbols count exceed max allowed amount it splits it to several requests and returns composed result.
 * It switches between available tokens to evenly distribute the load.
 * @param {string} api API to call.
 * @param {[string]} symbols Symbols to fetch, e.g. ['AAP','AAPL','PBA'].
 * @param {Object} queryParameters Query parameters.
 * @returns Parsed EJSON object. Composed from several responses if max symbols count was exceeded.
 */
fetchBatch = async function fetchBatch(api, symbols, queryParameters) {
  // https://cloud.iexapis.com/stable/stock/STOR/dividends/10y?token=pk_9f1d7a2688f24e26bb24335710eae053&calendar=true
  _throwIfUndefinedOrNull(api, `_api`);
  const fullAPI = `/stock/market${api}/batch`;
  _throwIfUndefinedOrNull(symbols, `_symbols`);

  if (queryParameters == null) {
    queryParameters = {};
  }

  const maxSymbolsAmount = 100;
  const chunkedSymbolsArray = symbols.chunked(maxSymbolsAmount);
  var result = [];
  for (const chunkedSymbols of chunkedSymbolsArray) {
    const symbolsParameter = chunkedSymbols.join(",");
    const fullQueryParameters = Object.assign({}, queryParameters);
    fullQueryParameters.symbols = symbolsParameter;

    console.log(`Fetching batch for symbols (${chunkedSymbols.length}) with query '${queryParameters.stringify()}': ${symbolsParameter}`);
    const response = await _fetch(fullAPI, fullQueryParameters);

    result = result.concat(response);
  }

  return result;
};

// exports();
//
// fetchBatch('/dividends', ['AAP','AAPL','PBA'], { 'range': '90d' })

/**
 * Requests data from IEX cloud/sandbox for types and symbols by a batch.
 * If symbols count exceed max allowed amount it splits it to several requests and returns composed result.
 * It switches between available tokens to evenly distribute the load.
 * @param {[string]} types Types to fetch, e.g. ['dividends'].
 * @param {[string]} symbols Symbols to fetch, e.g. ['AAP','AAPL','PBA'].
 * @param {Object} queryParameters Additional query parameters, e.g..
 * @returns {{string: {string: [Object]}}} Parsed EJSON object. Composed from several responses if max symbols count was exceeded. 
 * The first object keys are symbols. The next inner object keys are types. And the next inner object is an array of type objects.
 */
fetchBatchNew = async function fetchBatchNew(types, symbols, queryParameters) {
  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_d8f3a048a7a94866ad08c8b62042b16b&calendar=true&symbols=MSFT%2CAAPL&types=dividends&range=1y
  _throwIfUndefinedOrNull(types, `types`);
  _throwIfUndefinedOrNull(symbols, `symbols`);

  if (queryParameters == null) {
    queryParameters = {};
  }

  const api = `/stock/market/batch`;
  const typesParameter = types.join(",");
  const maxSymbolsAmount = 100;
  const chunkedSymbolsArray = symbols.chunked(maxSymbolsAmount);
  var result = {};
  for (const chunkedSymbols of chunkedSymbolsArray) {
    const symbolsParameter = chunkedSymbols.join(",");
    const fullQueryParameters = Object.assign({}, queryParameters);
    fullQueryParameters.types = typesParameter;
    fullQueryParameters.symbols = symbolsParameter;

    console.log(`Fetching batch for symbols (${chunkedSymbols.length}) with query '${queryParameters.stringify()}': ${typesParameter} - ${symbolsParameter}`);
    const response = await _fetch(api, fullQueryParameters);

    result = Object.assign(result, response);
  }

  return result;
};

// exports();
//
// fetchBatchNew(['dividends'], ['AAP','AAPL','PBA'], { 'range': '1y', 'calendar': 'true' })

/**
 * Requests data from IEX cloud/sandbox depending on an environment. 
 * It switches between available tokens to evenly distribute the load.
 * @param {string} api API to call.
 * @param {Object} queryParameters Query parameters.
 * @returns Parsed EJSON object.
 */
async function _fetch(api, queryParameters) {
  _throwIfUndefinedOrNull(api, `api`);
  var query = "";
  if (queryParameters) {
    const querystring = require('querystring');
    query = `&${querystring.stringify(queryParameters)}`;
  }
  
  // Use premium token if defined.
  const token = typeof premiumToken === 'undefined' ? tokens[counter % tokens.length] : premiumToken;
  counter++;

  const baseURL = context.values.get("base_url");
  const url = `${baseURL}${api}?token=${token}${query}`;
  console.log(`Request with URL: ${url}`);
  var response = await context.http.get({ url: url });

  // Retry 5 times on retryable errors
  const delay = 100;
  for (let step = 0; step < 5 && (response.status === '403 Forbidden' || response.status === '502 Bad Gateway' || response.status === '429 Too Many Requests'); step++) {
    console.log(`Received '${response.status}' error with text '${response.body.text()}'. Trying to retry after a '${delay}' delay.`);
    await new Promise(r => setTimeout(r, delay));
    response = await context.http.get({ url: url });
  }
  
  if (response.status === '200 OK') {
    console.log(`Response for URL: ${url}`);
  } else {
    _logAndThrow(`Response status error '${response.status}' : '${response.body.text()}'`);
  }
  
  const ejsonBody = EJSON.parse(response.body.text());
  if (ejsonBody.length && ejsonBody.length > 1) {
    console.logVerbose(`Parse end. Objects count: ${ejsonBody.length}`);
  } else {
    console.logVerbose(`Parse end. Object: ${response.body.text()}`);
  }

  // We need some delay to prevent '429 Too Many Requests' error.
  // It looks like we can't have more than 8 requests per second so just inserting some safety delay.
  // await new Promise(r => setTimeout(r, 50));
  
  return ejsonBody;
}

fetch = _fetch;

// exports();
//
// fetch("/ref-data/symbols")
// fetch("/stock/JAZZ/company")
// fetch(`/stock/AAP/chart/1d`, { 
//     chartByDay: true,
//     chartCloseOnly: true,
//     exactDate: exactDateAPIString
//   })

/**
 * Default range to fetch.
 */
const defaultRange = '6y';

/**
 * Fetches companies in batch for uniqueIDs.
 * @param {[string]} symbolModels Unique IDs to fetch.
 * @returns {[Company]} Array of requested objects.
 */
 fetchCompanies = async function fetchCompanies(symbolModels) {
  const uniqueIDs = _throwIfUndefinedOrNull(symbolModels, `fetchCompanies arg1`);
  const [symbols, symbolsDictionary] = getSymbols(uniqueIDs);

  return await fetchBatch(`/company`, symbols)
    .then(companies => 
      companies.compactMap(company => 
        fixCompany(company, symbolsDictionary[company.symbol])
      )
    );
};

/**
 * Fetches dividends in batch for uniqueIDs.
 * @param {[string]} uniqueIDs Unique IDs to fetch.
 * @param {boolean} isFuture Flag to fetch future or past dividends.
 * @param {string} range Range to fetch.
 * @returns {[Dividend]} Array of requested objects.
 */
fetchDividends = async function fetchDividends(uniqueIDs, isFuture, range) {
  _throwIfUndefinedOrNull(uniqueIDs, `fetchDividends uniqueIDs`);
  _throwIfUndefinedOrNull(isFuture, `fetchDividends isFuture`);

  if (range == null) {
    range = defaultRange;
  }

  const [symbols, symbolsDictionary] = getSymbols(uniqueIDs);

  const parameters = { range: range };
  if (isFuture) {
    parameters.calendar = 'true';
  }

  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_581685f711114d9f9ab06d77506fdd49&types=dividends&symbols=AAPL,AAP&range=6y
  return await fetchBatchNew(['dividends'], symbols, parameters)
    .then(symbolsTypesDividends =>
      symbols
        .map(symbol => {
          const symbolsTypesDividend = symbolsTypesDividends[symbol];
          if (typeof symbolsTypesDividend != null && symbolsTypesDividend.dividends) {
            return _fixDividends(symbolsTypesDividend.dividends, symbolsDictionary[symbol]);
          } else {
            return [];
          }
        })
        .flat()
    );
};

/**
 * Fetches previous day prices in batch for uniqueIDs.
 * @param {[string]} uniqueIDs Unique IDs to fetch.
 * @returns {[PreviousDayPrice]} Array of requested objects.
 */
 fetchPreviousDayPrices = async function fetchPreviousDayPrices(uniqueIDs) {
  _throwIfUndefinedOrNull(uniqueIDs, `fetchPreviousDayPrices uniqueIDs`);
  const [symbols, symbolsDictionary] = getSymbols(uniqueIDs);

  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_581685f711114d9f9ab06d77506fdd49&types=previous&symbols=AAPL,AAP
  return await fetchBatchNew(['previous'], symbols)
    .then(symbolsTypesPDPs =>
      symbols
        .compactMap(symbol => {
          const symbolsTypesPDP = symbolsTypesPDPs[symbol];
          if (symbolsTypesPDP != null && symbolsTypesPDP.previous) {
            return fixPreviousDayPrice(symbolsTypesPDP.previous, symbolsDictionary[symbol]);
          } else {
            // {"AACOU":{"previous":null}}
            return null;
          }
        })
    );
};

/**
 * Fetches historical prices in batch for uniqueIDs.
 * @param {[string]} uniqueIDs Unique IDs to fetch.
 * @param {string} range Range to fetch.
 * @returns {[HistoricalPrice]} Array of requested objects.
 */
 fetchHistoricalPrices = async function fetchHistoricalPrices(uniqueIDs, range) {
   _throwIfUndefinedOrNull(uniqueIDs, `fetchHistoricalPrices uniqueIDs`);

  if (range == null) {
    range = defaultRange;
  }

  const [symbols, symbolsDictionary] = getSymbols(uniqueIDs);
  const parameters = { 
    range: range,
    chartCloseOnly: true, 
    chartInterval: 21 
  };

  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_581685f711114d9f9ab06d77506fdd49&types=chart&symbols=AAPL,AAP&range=6y&chartCloseOnly=true&chartInterval=21
  return await fetchBatchNew(['chart'], symbols, parameters)
    .then(symbolsTypesHPs =>
      symbols
        .map(symbol => {
          const symbolsTypesHP = symbolsTypesHPs[symbol];
          if (typeof symbolsTypesHP != null && symbolsTypesHP.chart) {
            return fixHistoricalPrices(symbolsTypesHP.chart, symbolsDictionary[symbol]);
          } else {
            return [];
          }
        })
        .flat()
    );
};

/**
 * Fetches quotes in batch for uniqueIDs.
 * @param {[string]} uniqueIDs Unique IDs to fetch.
 * @returns {[Quote]} Array of requested objects.
 */
 fetchQuotes = async function fetchQuotes(uniqueIDs) {
  _throwIfUndefinedOrNull(uniqueIDs, `fetchQuotes uniqueIDs`);
  const [symbols, symbolsDictionary] = getSymbols(uniqueIDs);

  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_581685f711114d9f9ab06d77506fdd49&types=quote&symbols=AAPL,AAP
  return await fetchBatchNew(['quote'], symbols)
    .then(symbolsTypesQuotes =>
      symbols
        .compactMap(symbol => {
          const symbolsTypesQuote = symbolsTypesQuotes[symbol];
          if (typeof symbolsTypesQuote != null && symbolsTypesQuote.quote) {
            return fixQuote(symbolsTypesQuote.quote, symbolsDictionary[symbol]);
          } else {
            return null;
          }
        })
    );
};

/**
 * Fetches splits in batch for uniqueIDs.
 * @param {[string]} uniqueIDs Unique IDs to fetch.
 * @param {string} range Range to fetch.
 * @returns {[Split]} Array of requested objects.
 */
 fetchSplits = async function fetchSplits(uniqueIDs, range) {
  _throwIfUndefinedOrNull(uniqueIDs, `fetchSplits uniqueIDs`);

  if (range == null) {
    range = defaultRange;
  }
  
  const [symbols, symbolsDictionary] = getSymbols(uniqueIDs);
  const parameters = { range: range };

  return await fetchBatch(`/splits`, symbols, parameters)
   .then(splitsArray => 
      splitsArray
        .map((splits, i) => 
          fixSplits(splits, symbolsDictionary[symbols[i]])
        )
        .flat()
   );
};

/**
 * Gets symbols and symbols dictionary from unique IDs.
 * @param {["AAPL:NAS"]} uniqueIDs Unique IDs.
 * @returns {[["AAPL"], {"AAPL":"AAPL:NAS"}]} Returns array with symbols as the first element and symbols dictionary as the second element.
 */
 function getSymbols(uniqueIDs) {
  _throwIfUndefinedOrNull(uniqueIDs, `getSymbols uniqueIDs`);
  const symbols = [];
  const symbolsDictionary = {};
  for (const uniqueID of uniqueIDs) {
    const symbol = uniqueID.split(':')[0];
    symbols.push(symbol);
    symbolsDictionary[symbol] = uniqueID;
  }

  return [
    symbols,
    symbolsDictionary
  ];
}

// EUR examples
// http://api.exchangeratesapi.io/v1/latest?access_key=3e5abac4ea1a6d6a306fee11759de63e
// http://data.fixer.io/api/latest?access_key=47e966a176f1449a35309057baac8f29

/**
 * Fetches to USD exchange rates.
 * @example https://openexchangerates.org/api/latest.json?app_id=b30ffad8d6b0439da92b3805191f7f40&base=USD
 * @returns {[ExchangeRate]} Array of requested objects.
 */
 fetchExchangeRates = async function fetchExchangeRates() {
  const baseURL = "https://openexchangerates.org/api";
  const api = "/latest.json";
  const appID = context.values.get("exchange-rates-app-id");
  const baseCurrency = "USD";
  const url = `${baseURL}${api}?app_id=${appID}&base=${baseCurrency}`;
  console.log(`Exchange rates request with URL: ${url}`);
  var response = await context.http.get({ url: url });

  // Retry 5 times on retryable errors
  const delay = 100;
  for (let step = 0; step < 5 && (response.status === '??????'); step++) {
    console.log(`Received '${response.status}' error with text '${response.body.text()}'. Trying to retry after a '${delay}' delay.`);
    await new Promise(r => setTimeout(r, delay));
    response = await context.http.get({ url: url });
  }
  
  if (response.status !== '200 OK') {
    _logAndThrow(`Response status error '${response.status}' : '${response.body.text()}'`);
  }
  
  const ejsonBody = EJSON.parse(response.body.text());
  if (ejsonBody.length && ejsonBody.length > 1) {
    console.logVerbose(`Parse end. Objects count: ${ejsonBody.length}`);
  } else {
    console.logVerbose(`Parse end. Object: ${response.body.text()}`);
  }

  // Fix data
  const entries = Object.entries(ejsonBody.rates);
  const exchangeRates = [];
  for (const [currency, rate] of entries) {
    const exchangeRate = { 
      _id: currency,
      _p: "P",
      r: BSON.Double(rate),
    };

    const currencySymbol = getCurrencySymbol(currency);
    if (currencySymbol) {
      exchangeRate.s = currencySymbol;
    }

    exchangeRates.push(exchangeRate);
  }

  return exchangeRates;
};

function getCurrencySymbol(currency) {
  // TODO: Add more later
  switch(currency) {
    case "CAD": return "$";
    case "CHF": return "¤";
    case "ILS": return "₪";
    case "NOK": return "kr";
    case "USD": return "$";
    default: return null;
  }
}

///////////////////////////////////////////////////////////////////////////////// DATA FIX

/**
 * Fixes company object so it can be added to MongoDB.
 * @param {Object} company Company object.
 * @param {Object} uniqueID Unique ID, e.g. 'AAPL:NAS'.
 * @returns {Object|null} Returns fixed object or `null` if fix wasn't possible.
 */
fixCompany = function fixCompany(company, arg2) {
  try {
    _throwIfUndefinedOrNull(company, `fixCompany company`);
    _throwIfUndefinedOrNull(uniqueID, `fixCompany uniqueID`);
  
    console.logVerbose(`Company data fix start`);
    const fixedCompany = {};
    fixedCompany._id = uniqueID;
    fixedCompany._p = "2";
    fixedCompany._ = "2";
    fixedCompany.n = company.companyName.trim();
    fixedCompany.s = company.industry;
    fixedCompany.t = company.issueType;
  
    return fixedCompany;
  } catch(error) {
    return null;
  }
};

/**
 * Fixes previous day price object so it can be added to MongoDB.
 * @param {Object} previousDayPrice Previous day price object.
 * @param {Object} uniqueID Unique ID, e.g. 'AAPL:NAS'.
 * @returns {Object|null} Returns fixed object or `null` if fix wasn't possible.
 */
fixPreviousDayPrice = function fixPreviousDayPrice(previousDayPrice, uniqueID) {
  try {
    _throwIfUndefinedOrNull(previousDayPrice, `fixPreviousDayPrice previousDayPrice`);
    _throwIfUndefinedOrNull(uniqueID, `fixPreviousDayPrice uniqueID`);
  
    console.logVerbose(`Previous day price data fix start`);
    const fixedPreviousDayPrice = {};
    fixedPreviousDayPrice._id = uniqueID;
    fixedPreviousDayPrice._p = "P";
    fixedPreviousDayPrice.c = BSON.Double(previousDayPrice.close);
  
    return fixedPreviousDayPrice;

  } catch(error) {
    return null;
  }
};

/**
 * Fixes quote object so it can be added to MongoDB.
 * @param {Object} quote Quote object.
 * @param {Object} uniqueID Unique ID, e.g. 'AAPL:NAS'.
 * @returns {Object|null} Returns fixed object or `null` if fix wasn't possible.
 */
fixQuote = function fixQuote(quote, uniqueID) {
  try {
    _throwIfUndefinedOrNull(quote, `quote arg1`);
    _throwIfUndefinedOrNull(uniqueID, `fixQuote uniqueID`);
  
    console.logVerbose(`Previous day price data fix start`);
    const fixedQuote = {};
    fixedQuote._id = uniqueID;
    fixedQuote._p = "P";
    fixedQuote.l = quote.latestPrice;
    fixedQuote.p = BSON.Double(quote.peRatio);

    if (quote.latestUpdate) {
      const date = new Date(quote.latestUpdate);
      if (date && !isNaN(date)) {
        fixedQuote.d = date;
      } else {
        fixedQuote.d = new Date();
      }
    } else {
      fixedQuote.d = new Date();
    }

    return fixedQuote;

  } catch(error) {
    return null;
  }
};

/**
 * Fixes dividends object so it can be added to MongoDB.
 * @param {Object} dividends Dividends object.
 * @param {Object} uniqueID Unique ID, e.g. 'AAPL:NAS'.
 * @returns {[Object]} Returns fixed objects or an empty array if fix wasn't possible.
 */
function _fixDividends(dividends, arg2) {
  try {
    _throwIfUndefinedOrNull(dividends, `fixDividends dividends`);
    _throwIfUndefinedOrNull(uniqueID, `fixDividends uniqueID`);
    if (!dividends.length) { 
      console.logVerbose(`Dividends are empty for ${uniqueID}. Nothing to fix.`);
      return []; 
    }
  
    console.logVerbose(`Fixing dividends for ${uniqueID}`);
    return dividends
      .filterNull()
      .map(dividend => {
        const fixedDividend = {};
        fixedDividend._p = "P";
        fixedDividend._i = uniqueID;
        fixedDividend.a = BSON.Double(dividend.amount);
        fixedDividend.d = getOpenDate(dividend.declaredDate);
        fixedDividend.e = getOpenDate(dividend.exDate);
        fixedDividend.p = getOpenDate(dividend.paymentDate);

        if (typeof dividend.frequency !== 'undefined') {
          fixedDividend.f = dividend.frequency.charAt(0);
        }
    
        if (dividend.currency !== "USD") {
          fixedDividend.c = dividend.currency;
        }
    
        return fixedDividend;
      });

  } catch(error) {
    return [];
  }
}

fixDividends = _fixDividends;

/**
 * Fixes splits object so it can be added to MongoDB.
 * @param {Object} splits Splits object.
 * @param {Object} uniqueID Unique ID, e.g. 'AAPL:NAS'.
 * @returns {[Object]} Returns fixed objects or an empty array if fix wasn't possible.
 */
fixSplits = function fixSplits(splits, uniqueID) {
  try {
    _throwIfUndefinedOrNull(arg1, `fixSplits splits`);
    _throwIfUndefinedOrNull(uniqueID, `fixSplits uniqueID`);
    if (!splits.length) { 
      console.logVerbose(`Splits are empty for ${uniqueID}. Nothing to fix.`);
      return []; 
    }
  
    console.logVerbose(`Fixing splits for ${uniqueID}`);
    return splits
      .filterNull()
      .map(split => {
        const fixedSplit = {};
        fixedSplit._p = "P";
        fixedSplit._i = uniqueID;
        fixedSplit.e = getOpenDate(split.exDate);
        fixedSplit.r = BSON.Double(split.ratio);

        return fixedSplit;
      });

  } catch (error) {
    return [];
  }
};

/**
 * Fixes historical prices object so it can be added to MongoDB.
 * @param {Object} historicalPrices Historical prices object.
 * @param {Object} uniqueID Unique ID, e.g. 'AAPL:NAS'.
 * @returns {[Object]} Returns fixed objects or an empty array if fix wasn't possible.
 */
fixHistoricalPrices = function fixHistoricalPrices(historicalPrices, uniqueID) {
  try {
    _throwIfUndefinedOrNull(historicalPrices, `fixHistoricalPrices historicalPrices`);
    _throwIfUndefinedOrNull(uniqueID, `fixHistoricalPrices uniqueID`);
    if (!historicalPrices.length) { 
      console.logVerbose(`Historical prices are empty for ${uniqueID}. Nothing to fix.`);
      return []; 
    }
  
    console.logVerbose(`Fixing historical prices for ${uniqueID}`);
    return historicalPrices
      .filterNull()
      .map(historicalPrice => {
        const fixedHistoricalPrice = {};
        fixedHistoricalPrice._p = "P";
        fixedHistoricalPrice._i = uniqueID;
        fixedHistoricalPrice.c = BSON.Double(historicalPrice.close);
        fixedHistoricalPrice.d = getCloseDate(historicalPrice.date);

        return fixedHistoricalPrice;
      });

  } catch (error) {
    return [];
  }
};

///////////////////////////////////////////////////////////////////////////////// INITIALIZATION

const zeroPad = (num, places) => String(num).padStart(places, '0');

getDateLogString = function getDateLogString() {
  const date = new Date();
  const month = zeroPad(date.getMonth() + 1, 2);
  const day = zeroPad(date.getDate(), 2);
  const hours = zeroPad(date.getHours(), 2);
  const minutes = zeroPad(date.getMinutes(), 2);
  const seconds = zeroPad(date.getSeconds(), 2);
  const milliseconds = zeroPad(date.getMilliseconds(), 3);
  const dateString = `${month}.${day} ${hours}:${minutes}:${seconds}.${milliseconds} |`;
  
  return dateString;
};

exports = function() {
  extendRuntime();

  if (typeof isSandbox === 'undefined') {
    isSandbox = context.values.get("base_url") === 'https://sandbox.iexapis.com/stable';
  }

  if (typeof isProduction === 'undefined') {
    isProduction = !isSandbox;
  }

  if (typeof atlas === 'undefined') {
    atlas = context.services.get("mongodb-atlas");
  }

  if (typeof db === 'undefined') {
    db = atlas.db("divtracker-v2");
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

  // Adjusting console log
  if (!console.logCopy) {
    console.logCopy = console.log.bind(console);
    console.log = function(message) {
      this.logCopy(getDateLogString(), message);
    };
  }
  
  if (!console.errorCopy) {
    console.errorCopy = console.error.bind(console);
    console.error = function(message) {
      const errorLogPrefix = `${getDateLogString()} [ ** ERRROR ** ]`;
      this.logCopy(errorLogPrefix, message);
    };
  }
  
  if (!console.logVerbose) {
    console.logVerbose = function(message) {
      // this.logCopy(getDateLogString(), message);
    };
  }
  
  if (!console.logData) {
    console.logData = function(message, data) {
      // this.logCopy(getDateLogString(), `${message}: ${data.stringify()}`);
    };
  }
  
  console.log("Adjusted console output format");
};
