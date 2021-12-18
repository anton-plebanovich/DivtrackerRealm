
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
 * Safely computes and executes update operation from old to new objects on a collection.
 */
Object.prototype.safeUpdateMany = async function(newObjects, oldObjects, field) {
  _throwIfEmptyArray(newObjects, `Please pass non-empty new objects array as the first argument.`);

  if (newObjects.length === 0) {
    console.error(`New objects are empty. Skipping update.`);
    return;
  }

  if (field == null) {
    field = "_id";
  }

  if (typeof oldObjects === 'undefined') {
    if (newObjects.length < 1000) {
      console.log(`Old objects are undefined. Fetching them by '${field}'.`);
      const fileds = newObjects.map(x => x[field]);
      oldObjects = await this.find({ [field]: { $in: fileds } }).toArray();

    } else {
      console.log(`Old objects are undefined. Fetching them by requesting all existing objects.`);
      oldObjects = await this.find().toArray();
    }
  }

  if (oldObjects == null || oldObjects === []) {
    console.log(`No old objects. Just inserting new objects.`);
    return await this.insertMany(newObjects);
  }

  const bulk = this.initializeUnorderedBulkOp();
  for (const newObject of newObjects) {
    const existingObject = oldObjects.find(x => x[field].isEqual(newObject[field]));
    bulk.findAndUpdateOrInsertIfNeeded(newObject, existingObject, field);
  }

  return await bulk.safeExecute();
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
    throw new _SystemError(`New object should not be null for update`);
    
  } else if (oldObject == null) {
    throw new _SystemError(`Old object should not be null for update`);

  } else if (newObject[field] == null) {
    throw new _SystemError(`New object '${field}' field should not be null for update`);

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
    comparer = (a, b) => a[arg].isEqual(b[arg]);
  } else if (arg == null) {
    comparer = (a, b) => a.isEqual(b);
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
 * Checks if object is included in the array using `isEqual`.
 */
Array.prototype.includesObject = function(object) {
  if (object == null) { return false; }
  return this.find(x => object.isEqual(x)) != null;
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
    if (lhsValue != null) { 
      if (!lhsValue.isEqual(rhsValue)) {
        return false;
      }

    } else if (rhsValue != null) { 
      if (!rhsValue.isEqual(lhsValue)) {
        return false;
      }
    }
  }

  return true;
};

Number.prototype.isEqual = function(number) {
  'use strict'; // https://stackoverflow.com/a/27736962/4124265
  return this === number;
};

String.prototype.isEqual = function(string) {
  'use strict'; // https://stackoverflow.com/a/27736962/4124265
  return this === string;
};

String.prototype.removeSensitiveData = function() {
  if (isSandbox) { return this; }

  let safeString = this;
  
  if (premiumToken != null) {
    safeString = safeString.replaceAll(premiumToken, 'sk_***')
  }

  if (tokens != null) {
    for (const token of tokens) {
      safeString = safeString.replaceAll(token, 'pk_***')
    }
  }

  return safeString;
};

///////////////////////////////////////////////////////////////////////////////// CLASSES

class _LazyString {
  constructor(closure) {
    const closureType = typeof closure;
    if (closureType !== 'function') {
      throw `LazyString accepts only function as an argument. Please use something like this: '() => "My string"'`;
    }

    this.closure = closure;
  }

  toString() {
    return this.closure().toString();
  }
}

LazyString = _LazyString;

///////////////////////////////////////////////////////////////////////////////// ERRORS PROCESSING

class _NetworkError {
  constructor(statusCode, message) {
    this.statusCode = statusCode
    this.message = message.removeSensitiveData()
  }

  toString() {
    return this.stringify();
  }
}

NetworkError = _NetworkError;

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
checkExecutionTimeout = function checkExecutionTimeout(limit) {
  if (typeof startDate === 'undefined') {
    startDate = new Date();
  }

  const endTime = new Date();
  const seconds = Math.round((endTime - startDate) / 1000);

  // One symbol full fetch takes 17.5s and we have only 120s function execution time so let's put some limit.
  if (limit == null) {
    limit = 110;
  }

  if (seconds > limit) {
    _logAndThrow('execution timeout');
  } else {
    console.logVerbose(`${limit - seconds} execution time left`);
  }
};

/**
 * Throws error with optional `message` if `object` is not an `Array`.
 * @param {object} object Object to check.
 * @param {string} message Optional error message to replace default one.
 * @param {Error} ErrorType Optional error type. `SystemError` by default.
 * @returns {object} Passed object if it's an `Array`.
 */
function _throwIfEmptyArray(object, message, ErrorType) {
  _throwIfNotArray(object, message, ErrorType)

  if (object.length) { return object; }
  if (ErrorType == null) { ErrorType = _SystemError; }
  if (message == null) { message = ""; }

  throw new ErrorType(`Array is empty. ${message}`);
}

throwIfEmptyArray = _throwIfEmptyArray;

/**
 * Throws error with optional `message` if `object` is not an `Array`.
 * @param {object} object Object to check.
 * @param {string} message Optional error message to replace default one.
 * @param {Error} ErrorType Optional error type. `SystemError` by default.
 * @returns {object} Passed object if it's an `Array`.
 */
function _throwIfNotArray(object, message, ErrorType) {
  _throwIfUndefinedOrNull(object, message, ErrorType)

  const type = Object.prototype.toString.call(object);
  if (type === '[object Array]') { return object; }
  if (ErrorType == null) { ErrorType = _SystemError; }
  if (message == null) { message = ""; }
  
  throw new ErrorType(`Argument should be of the Array type. Instead, received '${type}'. ${message}`);
}

throwIfNotArray = _throwIfNotArray;

/**
 * Throws error with optional `message` if `object` is `undefined` or `null`.
 * @param {object} object Object to check.
 * @param {string} message Optional additional error message.
 * @returns {object} Passed object if it's defined and not `null`.
 */
function _throwIfUndefinedOrNull(object, message) {
  if (typeof object === 'undefined') {
    if (message == null) { message = ""; }
    _logAndThrow(`Argument is undefined. ${message}`);
    
  } else if (object === null) {
    if (message == null) { message = ""; }
    _logAndThrow(`Argument is null. ${message}`);
    
  } else {
    return object;
  }
}

throwIfUndefinedOrNull = _throwIfUndefinedOrNull;

getValueAndQuitIfUndefined = function _getValueAndQuitIfUndefined(object, key) {
  if (!object) {
    _logAndThrow(`Object undefined`);
    
  } else if (!key) {
    return object;

  } else if (!object[key]) {
    _logAndThrow(`Object's property undefined. Key: ${key}. Object: ${object}.`);
    
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
function _getCloseDate(closeDateValue) {
  if (closeDateValue == null) {
    return;
  }

  // Check if date is valid
  const date = new Date(closeDateValue);
  if (isNaN(date)) {
    console.error(`Invalid close date: ${closeDateValue}`);
    return;
  }

  // Eastern Standard Time (EST) time zone is 5 hours behind GMT during autumn/winter
  // https://en.wikipedia.org/wiki/Eastern_Time_Zone
  const closeDateStringWithTimeZone = `${date.dayString()}T16:00:00-0500`;
  const closeDate = new Date(closeDateStringWithTimeZone);
  if (isNaN(closeDate)) {
    console.error(`Invalid close date with time zone: ${closeDateStringWithTimeZone}`);
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
    console.error(`Invalid open date: ${openDateValue}`);
    return;
  }

  // Eastern Standard Time (EST) time zone is 5 hours behind GMT during autumn/winter
  // https://en.wikipedia.org/wiki/Eastern_Time_Zone
  const openDateStringWithTimeZone = `${date.dayString()}T9:30:00-0500`;
  const openDate = new Date(openDateStringWithTimeZone);

  if (isNaN(openDate)) {
    console.error(`Invalid open date with time zone: ${openDateStringWithTimeZone}`);
    return;
  } else {
    return openDate;
  }
};

getOpenDate = _getOpenDate;

// exports();
//
// checkExecutionTimeout()
// getCloseDate("2020-03-27");
// getOpenDate("2020-03-27");

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

  // Compute symbol IDs using both sources
  let symbolIDs = companyIDs
    .concat(distinctTransactionSymbolIDs)
    .distinct();

  const symbolsCollection = db.collection("symbols");
  const disabledSymbolIDs = await symbolsCollection.distinct("_id", { e: false });

  // Remove disabled symbols
  symbolIDs = symbolIDs.filter(x => !disabledSymbolIDs.includesObject(x));

  return await getShortSymbols(symbolIDs);
};

getInUseShortSymbols = _getInUseShortSymbols;

/** 
 * Returns sorted short symbols for symbol IDs
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
    .sort({ t: 1 })
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

//////////////////////////////////// Example calls

// Fetch the last historical price record for the given period for the AAPL symbol
// https://cloud.iexapis.com/stable/time-series/historical_prices/aapl?token=Tpk_0a569a8bdc864334b284cb7112f579df&from=2021-01-01&to=2021-02-01&last=1

/**
 * Requests data from IEX cloud/sandbox depending on an environment by a batch.
 * If symbols count exceed max allowed amount it splits it to several requests and returns composed result.
 * It switches between available tokens to evenly distribute the load.
 * @param {string} api API to call.
 * @param {[string]} tickers Ticker symbols to fetch, e.g. ['AAP','AAPL','PBA'].
 * @param {Object} queryParameters Query parameters.
 * @returns Parsed EJSON object. Composed from several responses if max symbols count was exceeded.
 */
async function _fetchBatch(api, tickers, queryParameters) {
  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_ca8d3de2a6db4a58a61a93ac027e4725&types=company&symbols=UZF
  _throwIfUndefinedOrNull(api, `_api`);
  const fullAPI = `/stock/market${api}/batch`;
  _throwIfUndefinedOrNull(tickers, `tickers`);

  if (queryParameters == null) {
    queryParameters = {};
  }

  const maxSymbolsAmount = 100;
  const chunkedTickersArray = tickers.chunked(maxSymbolsAmount);
  var result = [];
  for (const chunkedTickers of chunkedTickersArray) {
    const tickerParameter = chunkedTickers.join(",");
    const fullQueryParameters = Object.assign({}, queryParameters);
    fullQueryParameters.symbols = tickerParameter;

    console.log(`Fetching '${api}' batch for symbols (${chunkedTickers.length}) with query '${queryParameters.stringify()}': ${tickerParameter}`);
    const response = await _fetch(fullAPI, fullQueryParameters);

    result = result.concat(response);
  }

  return result;
};

fetchBatch = _fetchBatch;

// exports();
//
// fetchBatch('/dividends', ['AAP','AAPL','PBA'], { 'range': '90d' })

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
async function _fetchBatchAndMapArray(type, tickers, idByTicker, mapFunction, queryParameters) {
  return _fetchBatchNew([type], tickers, queryParameters)
    .then(tickerDataDictionary => {
      return tickers
        .map(ticker => {
          const tickerData = tickerDataDictionary[ticker];
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

fetchBatchAndMapArray = _fetchBatchAndMapArray;

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
async function _fetchBatchAndMapObjects(type, tickers, idByTicker, mapFunction, queryParameters) {
  return _fetchBatchNew([type], tickers, queryParameters)
    .then(tickerDataDictionary =>
      tickers
        .compactMap(ticker => {
          const tickerData = tickerDataDictionary[ticker];
          if (tickerData != null && tickerData[type]) {
            return mapFunction(tickerData[type], idByTicker[ticker]);
          } else {
            // {"AACOU":{"previous":null}}
            return null;
          }
        })
    );
}

fetchBatchAndMapObjects = _fetchBatchAndMapObjects;

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
async function _fetchBatchNew(types, tickers, queryParameters) {
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
      response = await _fetch(api, fullQueryParameters);
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

fetchBatchNew = _fetchBatchNew;

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
    const statusCodeString = response.status.split(" ")[0];
    const statusCode = parseInt(statusCodeString, 10);
    const text = response.body.text();
    console.error(`Response status error '${response.status}' : '${text}'`);
    throw new _NetworkError(statusCode, text)
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
 * Fetches companies in batch for short symbols.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @returns {[Company]} Array of requested objects.
 */
 fetchCompanies = async function fetchCompanies(shortSymbols) {
  _throwIfUndefinedOrNull(shortSymbols, `fetchCompanies shortSymbols`);
  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);

  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_581685f711114d9f9ab06d77506fdd49&types=company&symbols=AAPL,AAP
  return await _fetchBatchAndMapObjects('company', tickers, idByTicker, _fixCompany);
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

  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_581685f711114d9f9ab06d77506fdd49&types=dividends&symbols=AAPL,AAP&range=6y
  return await _fetchBatchAndMapArray('dividends', tickers, idByTicker, _fixDividends, parameters);
};

/**
 * Fetches previous day prices in batch for short symbols.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @returns {[PreviousDayPrice]} Array of requested objects.
 */
 async function _fetchPreviousDayPrices(shortSymbols) {
  _throwIfUndefinedOrNull(shortSymbols, `fetchPreviousDayPrices shortSymbols`);
  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);

  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_581685f711114d9f9ab06d77506fdd49&types=previous&symbols=AAPL,AAP
  return await _fetchBatchAndMapObjects('previous', tickers, idByTicker, _fixPreviousDayPrice);
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

  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_581685f711114d9f9ab06d77506fdd49&types=chart&symbols=AAPL,AAP&range=6y&chartCloseOnly=true&chartInterval=21
  return await _fetchBatchAndMapArray('chart', tickers, idByTicker, _fixHistoricalPrices, parameters);
};

/**
 * Fetches quotes in batch for uniqueIDs.
 * @param {[ShortSymbol]} shortSymbols Short symbol models for which to fetch.
 * @returns {[Quote]} Array of requested objects.
 */
 fetchQuotes = async function fetchQuotes(shortSymbols) {
  _throwIfUndefinedOrNull(shortSymbols, `fetchQuotes shortSymbols`);
  const [tickers, idByTicker] = getTickersAndIDByTicker(shortSymbols);

  // https://sandbox.iexapis.com/stable/stock/market/batch?token=Tpk_581685f711114d9f9ab06d77506fdd49&types=quote&symbols=AAPL,AAP
  return await _fetchBatchAndMapObjects('quote', tickers, idByTicker, _fixQuote);
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

  // https://cloud.iexapis.com/stable/stock/market/batch?types=splits&token=sk_de6f102262874cfab3d9a83a6980e1db&range=6y&symbols=AAPL,AAP
  return await _fetchBatchAndMapArray('splits', tickers, idByTicker, _fixSplits, parameters);
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

// EUR examples
// http://api.exchangeratesapi.io/v1/latest?access_key=3e5abac4ea1a6d6a306fee11759de63e
// http://data.fixer.io/api/latest?access_key=47e966a176f1449a35309057baac8f29

/**
 * Fetches to USD exchange rates.
 * @example https://openexchangerates.org/api/latest.json?app_id=b30ffad8d6b0439da92b3805191f7f40&base=USD
 * @returns {[ExchangeRate]} Array of requested objects.
 */
 async function _fetchExchangeRates() {
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
    const exchangeRate = {};
    exchangeRate._id = currency;
    exchangeRate._p = "2";

    if (rate != null) {
      exchangeRate.r = BSON.Double(rate);
    }

    const currencySymbol = getCurrencySymbol(currency);
    if (currencySymbol) {
      exchangeRate.s = currencySymbol;
    }

    exchangeRates.push(exchangeRate);
  }

  return exchangeRates;
};

fetchExchangeRates = _fetchExchangeRates;

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
    company._p = "2";
    company.i = iexCompany.industry;
    company.t = iexCompany.issueType;

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
        dividend._p = "2";
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
    previousDayPrice._p = "2";

    if (iexPreviousDayPrice.close != null) {
      previousDayPrice.c = BSON.Double(iexPreviousDayPrice.close);
    }
  
    return previousDayPrice;

  } catch(error) {
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
        historicalPrice._p = "2";
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
    quote._p = "2";
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
        split._p = "2";
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
