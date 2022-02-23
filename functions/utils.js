
// utils.js

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

Object.prototype.safeUpsertMany = async function(newObjects, field) {
  _throwIfEmptyArray(newObjects, `Please pass non-empty new objects array as the first argument. safeUpsertMany`);

  if (newObjects.length === 0) {
    console.error(`New objects are empty. Skipping update.`);
    return;
  }

  if (field == null) {
    field = "_id";
  }

  const bulk = this.initializeUnorderedBulkOp();
  for (const newObject of newObjects) {
    bulk
      .find({ [field]: newObject[field] })
      .upsert()
      .updateOne({ $set: newObject });
  }

  return await bulk.safeExecute();
}

/**
 * Safely computes and executes update operation from old to new objects on a collection.
 */
Object.prototype.safeUpdateMany = async function(newObjects, oldObjects, field) {
  _throwIfEmptyArray(newObjects, `Please pass non-empty new objects array as the first argument. safeUpdateMany`);

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

  // Skip '_id' set
  delete set['_id'];
  
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
    // Just skip '_id'
    if (key === '_id') {
      continue;
    }

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

  console.logData(`Updating`, update);

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
 * Creates dictionary from objects using provided `key` or function as source for keys and object as value.
 * @param {function|string} arg Key map function or key.
 */
Array.prototype.toDictionary = function(arg) {
  let getKey
  if (typeof arg === 'string' || arg instanceof String) {
    getKey = (value) => value[arg];
  } else if (arg == null) {
    getKey = (value) => value;
  } else {
    getKey = arg;
  }

  return this.reduce((dictionary, value) => {
    const key = getKey(value);
    return Object.assign(dictionary, { [key]: value })
  }, {});
};

/**
 * Creates dictionary from objects using provided `key` or function as source for keys and objects with the same keys are collected to an array.
 * @param {function|string} arg Key map function or key.
 */
Array.prototype.toBuckets = function(arg) {
  let getKey
  if (typeof arg === 'string' || arg instanceof String) {
    getKey = (value) => value[arg];
  } else if (arg == null) {
    getKey = (value) => value;
  } else {
    getKey = arg;
  }

  return this.reduce((dictionary, value) => {
    const key = getKey(value);
    const dictionaryValue = dictionary[key];
    if (dictionaryValue == null) {
      dictionary[key] = [value];
    } else {
      dictionaryValue.push(value);
    }
    return dictionary
  }, {});
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
 * @returns {Date} Month day start date in UTC.
 */
Date.monthStart = function() {
  const yesterday = new Date();
  yesterday.setUTCDate(1);
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
 * Checks database objects for equality. 
 * Objects are considered equal if all non-null values are equal.
 * Object values are compared using `toString()` comparison.
 * @returns {boolean} Comparison result.
 */
Object.prototype.isEqual = function(rhs) {
  const lhsEntries = Object.entries(this).filter(([key, value]) => value != null);
  const rhsEntries = Object.entries(rhs).filter(([key, value]) => value != null);

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

Boolean.prototype.isEqual = function(boolean) {
  // We should always use 'strict' for primitive type extensions - https://stackoverflow.com/a/27736962/4124265
  'use strict';

  return this === boolean;
};

Number.prototype.isEqual = function(number) {
  // We should always use 'strict' for primitive type extensions - https://stackoverflow.com/a/27736962/4124265
  'use strict';

  return this === number;
};

String.prototype.isEqual = function(string) {
  // We should always use 'strict' for primitive type extensions - https://stackoverflow.com/a/27736962/4124265
  'use strict';

  return this === string;
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

class _NetworkResponse {
  constructor(url, response) {
    this.url = url;

    // https://docs.mongodb.com/realm/services/http-actions/http.get/#return-value
    // `statusCode` is a weird `number`. Something like `int32` instead of default `int64`.
    // This looks like Mongo customization. It leads to inability to properly use `include`.
    // Calling `.valueOf()` fixes the issue.
    this.statusCode = response.statusCode.valueOf();
    this.rawBody = response.body;

    let string;
    Object.defineProperty(this, "string", {
      get: function() {
        if (typeof string !== 'undefined') {
          return string;
        } else if (this.rawBody != null) {
          string = this.rawBody.text();
          return string;
        } else {
          string = null;
          return string;
        }
      }
    });

    let json;
    Object.defineProperty(this, "json", {
      get: function() {
        if (typeof json !== 'undefined') {
          return json;
        } else if (this.string != null) {
          json = EJSON.parse(this.string);
          return json;
        } else {
          json = null;
          return json;
        }
      }
    });

    let retryable;
    Object.defineProperty(this, "retryable", {
      get: function() {
        if (typeof retryable !== 'undefined') {
          return retryable;
        }

        const alwaysRetryableStatusCodes = [
          403, // Forbidden
          429, // Too Many Requests
          502, // Bad Gateway
        ];

        if (alwaysRetryableStatusCodes.includes(this.statusCode)) {
          retryable = true;
          return retryable;
        }

        // You have used all available credits for the month. Please upgrade or purchase additional packages to access more data.
        if (this.statusCode === 402) {
          // We can try and retry if there is no premium token and we are using ordinary tokens.
          // Some might not be expired yet.
          retryable = typeof premiumToken === 'undefined';

          return retryable;
        }

        retryable = false;
        return retryable;
      }
    });
  }

  toString() {
    return this.string;
  }

  toNetworkError() {
    return new _NetworkError(this.statusCode, this.string);
  }
}

class _NetworkError {
  constructor(statusCode, message) {
    this.statusCode = statusCode;
    this.message = message.removeSensitiveData();
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

  Promise.allLmited = async (promises, limit) => {
    let index = 0;
    const results = [];
  
    // Run a pseudo-thread
    const execThread = async () => {
      while (index < promises.length) {
        const curIndex = index++;
        // Use of `curIndex` is important because `index` may change after await is resolved
        results[curIndex] = await promises[curIndex];
      }
    };
  
    // Start threads
    const threads = [];
    for (let thread = 0; thread < limit; thread++) {
      threads.push(execThread());
    }
    await Promise.all(threads);
    return results;
  };
  
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

function _logAndReject(message, data) {
  _throwIfUndefinedOrNull(message, `_logAndReject message`);

  if (data != null) {
    console.error(`${message}: ${data}`);
  } else {
    console.error(message);
  }
  
  return Promise.reject(message);
}

logAndReject = _logAndReject;

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

  if (object.length > 0) { return object; }
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

getValueAndThrowfUndefined = function _getValueAndThrowfUndefined(object, key) {
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

///////////////////////////////////////////////////////////////////////////////// fetch.js

/**
 * Requests data from IEX cloud/sandbox depending on an environment. 
 * It switches between available tokens to evenly distribute the load.
 * @param {string} baseURL Base URL to use.
 * @param {string} api API to call.
 * @param {Object} queryParameters Query parameters.
 * @returns Parsed EJSON object.
 */
async function _fetch(baseURL, api, queryParameters) {
  _throwIfUndefinedOrNull(baseURL, `_fetch baseURL`);
  _throwIfUndefinedOrNull(api, `_fetch api`);

  let response = await _get(baseURL, api, queryParameters);

  // Retry 5 times on retryable errors
  for (let step = 0; step < 5 && response.retryable; step++) {
    const delay = (step + 1) * (500 + Math.random() * 1000);
    console.log(`Received '${response.statusCode}' error with text '${response.string}'. Trying to retry after a '${delay}' delay.`);
    await new Promise(r => setTimeout(r, delay));
    response = await _get(baseURL, api, queryParameters);
  }
  
  if (response.statusCode === 200) {
    console.log(`Response for URL: ${response.url}`);
  } else {
    console.error(`Response status error '${response.statusCode}' : '${response.string}'`);
    throw response.toNetworkError();
  }
  
  const json = response.json;
  if (!json.length && json.error != null) {
    console.error(`Response error '${response.statusCode}' : '${response.string}'`);
    throw response.toNetworkError();
  }

  if (json.length && json.length > 1) {
    console.logVerbose(`Parse end. Objects count: ${json.length}`);
  } else {
    console.logVerbose(`Parse end. Object: ${response.string}`);
  }
  
  return json;
}

fetch = _fetch;

async function _get(baseURL, api, queryParameters) {
  _throwIfUndefinedOrNull(baseURL, `_get baseURL`);
  _throwIfUndefinedOrNull(api, `_get api`);

  const url = _getURL(baseURL, api, queryParameters);
  console.log(`Request with URL: ${url}`);

  const response = await context.http.get({ url: url });

  return new _NetworkResponse(url, response);
}

function _getURL(baseURL, api, queryParameters) {
  _throwIfUndefinedOrNull(baseURL, `_getURL baseURL`);
  _throwIfUndefinedOrNull(api, `_getURL api`);

  let query;
  if (queryParameters) {
    const querystring = require('querystring');
    query = `${querystring.stringify(queryParameters)}`;
  } else {
    query = "";
  }

  const url = `${baseURL}${api}?${query}`;

  return url;
}

/**
 * Returns ticker symbols and ticker symbol IDs by symbol ticker name dictionary.
 * @param {[ShortSymbol]} shortSymbols Short symbol models.
 * @returns {[["AAPL"], {"AAPL":ObjectId}]} Returns array with ticker symbols as the first element and ticker symbol IDs by ticker symbol dictionary as the second element.
 */
 function _getTickersAndIDByTicker(shortSymbols) {
  _throwIfUndefinedOrNull(shortSymbols, `_getTickersAndIDByTicker shortSymbols`);
  const tickers = [];
  const idByTicker = {};
  for (const shortSymbol of shortSymbols) {
    const ticker = shortSymbol.t;
    _throwIfUndefinedOrNull(ticker, `_getTickersAndIDByTicker ticker`);
    tickers.push(ticker);
    idByTicker[ticker] = shortSymbol._id;
  }

  return [
    tickers,
    idByTicker
  ];
}

getTickersAndIDByTicker = _getTickersAndIDByTicker;

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
  for (let step = 0; step < 5 && (response.status === '??????'); step++) {
    const delay = (step + 1) * (500 + Math.random() * 1000);
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
    case "EUR": return "€";
    case "GBP": return "£";
    case "ILS": return "₪";
    case "NOK": return "kr";
    case "USD": return "$";
    default: return null;
  }
}

///////////////////////////////////////////////////////////////////////////////// UPDATE

async function _setUpdateDate(_id, date) {
  _throwIfUndefinedOrNull(_id, `_setUpdateDate _id`);
  if (date == null) {
    date = new Date();
  }

  const collection = db.collection("updates");
  return await collection.updateOne(
    { _id: _id }, 
    { $set: { d: date } }, 
    { "upsert": true }
  )
}

setUpdateDate = _setUpdateDate

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

  if (typeof atlas === 'undefined') {
    atlas = context.services.get("mongodb-atlas");
  }

  if (typeof db === 'undefined') {
    db = atlas.db("divtracker-v2");
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
  
  console.log("Imported utils");
};
