
// utils.js

// https://www.mongodb.com/docs/manual/reference/method/cursor.sort/
// 'uncaught promise rejection' - happens when there is async work executed without `await` that throw an error.

///////////////////////////////////////////////////////////////////////////////// MATH

math_bigger_times = function(l, r) {
  if (l > r) {
    return l / r;
  } else {
    return r / l;
  }
}

///////////////////////////////////////////////////////////////////////////////// EXTENSIONS

function normalizeFields(fields) {
  if (fields == null) {
    fields = ["_id"];
  } else if (Object.prototype.toString.call(fields) !== '[object Array]') {
    fields = [fields];
  }
  _throwIfEmptyArray(fields, `Please pass non-empty fields array. normalizeFields`);

  return fields;
}

function getFindOperation(objects, fields) {
  fields = normalizeFields(fields);
  
  const find = {};
  if (Object.prototype.toString.call(objects) === '[object Array]') {
    _throwIfEmptyArray(objects, `Please pass non-empty objects array or singular object. getFindOperation`);
    
    for (const field of fields) {
      const values = objects.map(x => x[field]);
      find[field] = { $in: values };
    }
  
  } else {
    _throwIfUndefinedOrNull(objects, `Please pass non-empty objects array or singular object. getFindOperation`);
    const object = objects;

    for (const field of fields) {
      find[field] = object[field];
    }
  }

  console.logVerbose(`Find operation: ${find.stringify()}`);

  return find;
}

/**
 * Safely executes Bulk operation by catching 'no operations' error.
 */
Object.prototype.safeExecute = async function() {
  try {
    // 100.000 operations max
    return await this.execute();
  } catch(error) {
    if (error.message === 'Failed to execute bulk writes: no operations specified') {
      console.log("No bulk operations to execute");
    } else {
      throw new _SystemError(error);
    }
  }
};

Object.prototype.safeInsertMissing = async function(newObjects, fields) {
  _throwIfNotArray(newObjects, `Please pass new objects array as the first argument. safeInsertMissing`);
  if (newObjects.length === 0) {
    console.log(`New objects array is empty. Nothing to insert.`);
    return;
  }

  fields = normalizeFields(fields);
  
  let bulk = this.initializeUnorderedBulkOp();
  let i = 0;
  for (const newObject of newObjects) {
    const find = fields.reduce((find, field) => {
      return Object.assign(find, { [field]: newObject[field] });
    }, {});

    bulk
      .find(find)
      .upsert()
      .updateOne({ $setOnInsert: newObject });

    i++;

    // Try to prevent 'pending promise returned that will never resolve/reject uncaught promise rejection: &{0xc1bac2aa90 0xc1bac2aa80}' error by splitting batch operations to chunks
    if (i === ENV.maxBulkSize) {
      console.log(`Too much bulk changes. Executing collected ${ENV.maxBulkSize}.`)
      i = 0
      await bulk.safeExecute();
      bulk = this.initializeUnorderedBulkOp();
    }
  }

  await bulk.safeExecute();
};

Object.prototype.safeUpsertMany = async function(newObjects, fields, setUpdateDate) {
  _throwIfNotArray(newObjects, `Please pass new objects array as the first argument. safeUpsertMany`);
  if (newObjects.length === 0) {
    console.log(`New objects array is empty. Nothing to upsert.`);
    return;
  }

  fields = normalizeFields(fields);

  if (setUpdateDate == null) {
    setUpdateDate = true;
  }

  let bulk = this.initializeUnorderedBulkOp();
  let i = 0;
  for (const newObject of newObjects) {
    const find = fields.reduce((find, field) => {
      return Object.assign(find, { [field]: newObject[field] });
    }, {});

    const update = { $set: newObject };
    if (setUpdateDate == true) {
      update.$currentDate = { u: true };
    }

    bulk
      .find(find)
      .upsert()
      .updateOne(update);

    i++;

    // Try to prevent 'pending promise returned that will never resolve/reject uncaught promise rejection: &{0xc1bac2aa90 0xc1bac2aa80}' error by splitting batch operations to chunks
    if (i === ENV.maxBulkSize) {
      console.log(`Too much bulk changes. Executing collected ${ENV.maxBulkSize}.`)
      i = 0
      await bulk.safeExecute();
      bulk = this.initializeUnorderedBulkOp();
    }
  }

  await bulk.safeExecute();
};

/**
 * Safely computes and executes update operation from old to new objects on a collection.
 * @note Marked as deleted records `x === true` can not be restored using this method.
 * @param {array|object|null} oldObjectsDictionary Old object array, already created old objects dictionary or just `null`.
 */
Object.prototype.safeUpdateMany = async function(newObjects, oldObjectsDictionary, fields, setUpdateDate, insertMissing) {
  _throwIfNotArray(newObjects, `Please pass new objects array as the first argument. safeUpdateMany`);
  if (newObjects.length === 0) {
    console.log(`New objects array is empty. Nothing to update.`);
    return;
  }

  fields = normalizeFields(fields);

  if (setUpdateDate == null) {
    setUpdateDate = true;
  }

  if (insertMissing == null) {
    insertMissing = true;
  }

  const invalidObjectsLength = newObjects.filter(newObject => 
    fields.contains((field) => newObject[field] == null)
  ).length;

  if (invalidObjectsLength !== 0) {
    _logAndThrow(`${invalidObjectsLength} of ${newObjects.length} new objects do not contain required '${fields}' fields`);
  }

  if (oldObjectsDictionary == null) {
    // Sort deleted to the start so they will be overridden in the dictionary
    const sort = { x: -1 };

    // Use IN search if objects count is less than 10% of existing
    const count = await this.count();
    if (newObjects.length < count * 0.1) {
      console.log(`Old objects are undefined. Fetching them by '${fields}'.`);
      const find = getFindOperation(newObjects, fields);
      oldObjectsDictionary = await this
        .find(find)
        .sort(sort)
        .toArray();

    } else {
      console.log(`Old objects are undefined. Fetching them by requesting all existing objects.`);
      oldObjectsDictionary = await this.fullFind();
      oldObjectsDictionary = oldObjectsDictionary.sortedDeletedToTheStart();
    }

    oldObjectsDictionary = oldObjectsDictionary.toDictionary(fields);

  } else if (Object.prototype.toString.call(oldObjectsDictionary) === '[object Array]') {
    oldObjectsDictionary = oldObjectsDictionary.sortedDeletedToTheStart();
    oldObjectsDictionary = oldObjectsDictionary.toDictionary(fields);

  } else {
    _throwIfNotObject(oldObjectsDictionary, `Old objects should be of an Array or Object type`);
  }

  if (!Object.keys(oldObjectsDictionary).length) {
    if (insertMissing) {
      console.log(`No old objects. Just inserting new objects.`);
      return await this.insertMany(newObjects);
    } else {
      console.log(`No old objects. Nothing to update.`);
      return;
    }
  }

  let bulk = this.initializeUnorderedBulkOp();
  let i = 0;
  for (const newObject of newObjects) {
    const existingObject = fields.reduce((dictionary, field) => {
      if (dictionary != null) {
        return dictionary[newObject[field]];
      } else {
        return null;
      }
    }, oldObjectsDictionary);

    const changed = bulk.findAndUpdateOrInsertIfNeeded(newObject, existingObject, fields, setUpdateDate, insertMissing);
    if (changed) {
      i++;
    }

    // Try to prevent 'pending promise returned that will never resolve/reject uncaught promise rejection: &{0xc1bac2aa90 0xc1bac2aa80}' error by splitting batch operations to chunks
    if (i === ENV.maxBulkSize) {
      console.log(`Too much bulk changes. Executing collected ${ENV.maxBulkSize}.`)
      i = 0
      await bulk.safeExecute();
      bulk = this.initializeUnorderedBulkOp();
    }
  }

  await bulk.safeExecute();
};

/**
 * Executes find by field and update or insert for a new object from an old object.
 * Uses `_id` field by default.
 */
Object.prototype.findAndUpdateOrInsertIfNeeded = function(newObject, oldObject, fields, setUpdateDate, insertMissing) {
  if (insertMissing == null) {
    insertMissing = true;
  }

  if (newObject == null) {
    throw new _SystemError(`New object should not be null for insert or update`);
    
  } else if (oldObject == null) {
    // No old object means we should insert
    if (insertMissing) {
      console.log(`Inserting: ${newObject.stringify()}`);
      this.insert(newObject);
      return true;

    } else {
      console.log(`Old object is missing. Skipping insert: ${newObject.stringify()}`);
      return false;
    }

  } else {
    return this.findAndUpdateIfNeeded(newObject, oldObject, fields, setUpdateDate);
  }
};

/**
 * Executes find by field and update for a new object from an old object if needed.
 * Uses `_id` field by default.
 */
Object.prototype.findAndUpdateIfNeeded = function(newObject, oldObject, fields, setUpdateDate) {
  fields = normalizeFields(fields);

  if (setUpdateDate == null) {
    setUpdateDate = true;
  }
  
  if (newObject == null) {
    throw new _SystemError(`New object should not be null for update`);
    
  } else if (oldObject == null) {
    throw new _SystemError(`Old object should not be null for update`);

  } else if (fields.contains(x => newObject[x] == null)) {
    throw new _SystemError(`New object '${newObject.stringify()}' '${fields}' fields should not be null for update`);

  } else {
    const update = newObject.updateFrom(oldObject, setUpdateDate);
    if (update == null) {
      // Update is not needed
      return false;

    } else { 
      if (oldObject._id == null) {
        throw `Unable to find old object. '_id' field is missing: ${oldObject.stringify()}`;
      } else {
        this
          .find({ _id: oldObject._id })
          .updateOne(update);

        return true;
      }
    }
  }
};

/**
 * Bypass find limit of 50000 objects by fetching all results successively  
 * @note We do not allow `sort` parameter to prevent ambiguity between fetches which may cause holey or intersecting result.
 */
Object.prototype.fullFind = async function(find, projection) {
  if (find == null) {
    find = {};
  }

  if (projection == null) {
    projection = {};
  } else if (projection._id === 0) {
    throw `'_id' field is required for 'fullFind' operation. Please update 'projection' object`;
  }

  let objectsPage;
  const objects = [];
  const pageSize = 50000;
  do {
    let compositeFind;
    if (objectsPage != null) {
      const pageFind = { _id: { $gt: objectsPage[objectsPage.length - 1]._id } };
      compositeFind = { $and: [find, pageFind] };
    } else {
      compositeFind = find;
    }

    objectsPage = await this.find(compositeFind, projection).sort({ _id: 1 }).limit(pageSize).toArray();
    objects.push(...objectsPage);
    console.logVerbose(`Full fetch objects length: ${objects.length}`);
  } while (objectsPage.length >= pageSize);

  return objects;
};

/**
 * Computes update parameter for `updateOne` collection method.
 * Update direction is from `object` towards `this`.
 * Only non-equal fields are added to `$set` and missing fields
 * are added to `$unset`.
 */
Object.prototype.updateFrom = function(_object, setUpdateDate) {
  if (_object == null) {
    _logAndThrow(`Unable to compute update from 'null'. This: ${this}`);
  }

  const object = Object.assign({}, _object);
  delete object._id;
  delete object.u;

  const set = Object.assign({}, this);
  delete set._id;
  delete set.u;

  if (set.isEqual(object)) {
    return null;
  }
  
  const unset = {};

  // Delete `null` values from the `set`
  const newEntries = Object.entries(this);
  for (const [key, newValue] of newEntries) {
    if (newValue == null) {
      delete set[key];
    }
  }

  // Collect keys to unset
  const oldEntries = Object.entries(object);
  for (const [key, oldValue] of oldEntries) {
    const newValue = set[key];
    if (newValue == null) {
      unset[key] = "";

    } else if (oldValue == null) {
      continue;
      
    } else if (newValue.isEqual(oldValue)) {
      delete set[key];
    }
  }

  // We should not unset deleted flag since it's manual fix operation and has higher priority from updates.
  if (unset.x != null) {
    delete unset.x;
  }

  const update = {};
  if (Object.keys(set).length) {
    update.$set = set;
  }
  if (Object.keys(unset).length) {
    update.$unset = unset;
  }
  if (Object.keys(update).length === 0) {
    return null;
  }

  if (setUpdateDate == true) {
    update.$currentDate = { u: true };
  }

  console.logData(`Updating`, { from: _object, to: this, update: update });

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
 * @param {array|string|null} arg Key(s) to use for comparison.
 */
Array.prototype.uniqueUnordered = function(key) {
  if (key != null) {
    const dictionary = this.toDictionary(key);
    let result = Object.values(dictionary);
    if (Object.prototype.toString.call(key) === '[object Array]') {
      for (i = 1; i < key.length; i++) {
        result = result
          .map(x => Object.values(x))
          .flat();
      }
    }

    return result

  } else {
    const dictionary = this.toDictionary(x => x.toString());
    return Object.values(dictionary);
  }
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
 * Splits array into array of chunked arrays of specific size.
 * @param {number} size Size of a chunk.
 * @returns {object[][]} Array of chunked arrays.
 */
Array.prototype.chunkedBySize = function(size) {
  var chunks = [];
  for (i=0,j=this.length; i<j; i+=size) {
      const chunk = this.slice(i,i+size);
      chunks.push(chunk);
  }

  return chunks;
};

/**
 * Splits array into array of chunked arrays of specific count.
 * @param {number} count Size of a chunk.
 * @returns {object[][]} Array of chunked arrays.
 */
Array.prototype.chunkedByCount = function(count) {
  if (count < 2) { return [a]; }
  const copy = [...this];
  let chunks = [];
  for (let i = count; i > 0; i--) {
    chunks.push(copy.splice(0, Math.ceil(copy.length / i)));
  }
  
  return chunks;
};

/**
 * Returns `count` random elements from an array.
 */
Array.prototype.getRandomElements = function(count) {
  const elements = [];
  const copy = [...this];
  for (var i = 0; i < count; i++) {
    if (copy.length === 0) {
      return elements;
    }

    let index = Math.floor(Math.random() * copy.length);
    const element = copy.splice(index, 1)[0];
    elements.push(element);
  }

  return elements;
}

/**
 * Assumes elements are an arrays and filters those that does not have length.
 */
Array.prototype.filterEmpty = function() {
  return this.filter(x => x?.length);
};

/**
 * Filters `null` and `undefined` elements.
 */
Array.prototype.filterNullAndUndefined = function() {
  return this.filter(x => x != null);
};

/**
 * Maps array and filters `null` elements.
 * @param {*} callbackfn Mapping to perform. Null values are filtered.
 */
Array.prototype.compactMap = function(callbackfn) {
  return this.map(callbackfn).filterNullAndUndefined();
};

/**
 * Creates dictionary from objects using provided `key` or function as source for keys and object as value.
 * If returned key is `null` or `undefined` then value is ignored for single-dimensional dictionary.
 * For multi-dimensional dictionary it is used as a key.
 * @param {function|array|string|null} arg Key map function or key.
 */
Array.prototype.toDictionary = function(arg) {
  let getKey;
  let isMultidimensional = false;
  if (arg == null) {
    getKey = (object) => object;
  } else if (typeof arg === 'string' || arg instanceof String) {
    getKey = (object) => object[arg];
  } else if (Object.prototype.toString.call(arg) === '[object Array]') {
    isMultidimensional = true;
    getKey = (object) => arg.map(key => object[key]);
  } else {
    getKey = arg;
  }

  if (isMultidimensional) {
    const dictionary = {};
    for (const element of this) {
      const keys = getKey(element);
      if (keys == null) { continue; }
      
      const lastIndex = keys.length - 1;
      let currentDictionary = dictionary;
      for (const [i, key] of keys.entries()) {
        if (i == lastIndex) {
          currentDictionary[key] = element;
        } else {
          if (currentDictionary[key] == null) {
            currentDictionary[key] = {};
          }
          currentDictionary = currentDictionary[key];
        }
      }
    }

    return dictionary;

  } else {
    return this.reduce((dictionary, element) => {
      const key = getKey(element);
      if (key != null) {
        return Object.assign(dictionary, { [key]: element });
      } else {
        return dictionary;
      }
    }, {});
  }
};

/**
 * Creates dictionary from objects using provided `key` or function as source for values and object as key.
 * @param {function|string} arg Key map function or key.
 */
Array.prototype.dictionaryMapValues = function(arg) {
  let getValue;
  if (typeof arg === 'string' || arg instanceof String) {
    getValue = (object) => object[arg];
  } else if (arg == null) {
    getValue = (object) => object;
  } else {
    getValue = arg;
  }

  return this.reduce((dictionary, key) => {
    const value = getValue(key);
    return Object.assign(dictionary, { [key]: value });
  }, {});
};

/**
 * Creates dictionary from objects using provided `key` or function as source for keys and objects with the same keys are collected to an array.
 * @param {function|string|array} arg Key map function or key.
 */
Array.prototype.toBuckets = function(arg) {
  let getKey;
  let isMultidimensional = false;
  if (typeof arg === 'string' || arg instanceof String) {
    getKey = (value) => value[arg];
  } else if (arg == null) {
    getKey = (value) => value;
  } else if (Object.prototype.toString.call(arg) === '[object Array]') {
    isMultidimensional = true;
    getKey = (object) => arg.map(key => object[key]);
  } else {
    getKey = arg;
  }

  if (isMultidimensional) {
    const dictionary = {};
    for (const element of this) {
      const keys = getKey(element);
      if (keys == null) { continue; }
      
      const lastIndex = keys.length - 1;
      let currentDictionary = dictionary;
      for (const [i, key] of keys.entries()) {
        if (i == lastIndex) {
          const bucket = currentDictionary[key];
          if (bucket == null) {
            currentDictionary[key] = [element];
          } else {
            bucket.push(element);
          }
          
        } else {
          if (currentDictionary[key] == null) {
            currentDictionary[key] = {};
          }
          currentDictionary = currentDictionary[key];
        }
      }
    }

    return dictionary;

  } else {
    return this.reduce((dictionary, value) => {
      const key = getKey(value);
      if (key == null) { return dictionary; }
  
      const bucket = dictionary[key];
      if (bucket == null) {
        dictionary[key] = [value];
      } else {
        bucket.push(value);
      }
      return dictionary;
    }, {});
  }
};

/**
 * Checks if object is included in the array using `isEqual`.
 */
Array.prototype.includesObject = function(object) {
  if (object == null) { return false; }
  return this.find(x => object.isEqual(x)) != null;
};

/**
 * Checks if element is contained in the array using passed function.
 */
Array.prototype.contains = function(func) {
  _throwIfNotFunction(func, 'Array.prototype.contains')
  return this.find(x => func(x)) != null;
};

/**
 * Sorts objects with `x: true` field to the start and returns resulted array.
 */
Array.prototype.sortedDeletedToTheStart = function() {
  return this.sorted((l, r) => {
    if (l.x === r.x) {
      return 0;
    } else if (l.x === true) {
      return -1;
    } else {
      return 1;
    }
  });
};

/**
 * Fixed sort. Sorts objects and return resulted array.
 */
Array.prototype.sorted = function(func) {
  // We need to copy because sort on array returned from the MongoDB collection breaks all contained ObjectIDs.
  const copy = [...this];
  return copy.sort(func);
};

/**
 * Performs map operator async with specified simultaneous concurrent queues.
 */
Array.prototype.asyncMap = async function(limit, callback) {
  let index = 0;
  const results = [];

  // Run a pseudo-thread
  const execThread = async () => {
    while (index < this.length) {
      const curIndex = index++;
      // Use of `curIndex` is important because `index` may change after await is resolved
      results[curIndex] = await callback(this[curIndex]);
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

/**
 * @returns {Date} Today day start date in UTC.
 */
Date.today = function() {
  const date = new Date();
  date.setUTCHours(0);
  date.setUTCMinutes(0);
  date.setUTCSeconds(0);
  date.setUTCMilliseconds(0);

  return date;
};

/**
 * @returns {Date} Yesterday day start date in UTC.
 */
Date.yesterday = function() {
  const date = new Date();
  date.setUTCDate(date.getUTCDate() - 1);
  date.setUTCHours(0);
  date.setUTCMinutes(0);
  date.setUTCSeconds(0);
  date.setUTCMilliseconds(0);

  return date;
};

/**
 * @returns {Date} Month day start date in UTC.
 */
Date.monthStart = function() {
  const date = new Date();
  date.setUTCDate(1);
  date.setUTCHours(0);
  date.setUTCMinutes(0);
  date.setUTCSeconds(0);
  date.setUTCMilliseconds(0);

  return date;
};

/**
 * @returns {Date} Previous month day start date in UTC.
 */
Date.previousMonthStart = function() {
  const date = new Date();
  date.setUTCMonth(date.getUTCMonth() - 1);
  date.setUTCDate(1);
  date.setUTCHours(0);
  date.setUTCMinutes(0);
  date.setUTCSeconds(0);
  date.setUTCMilliseconds(0);

  return date;
};

/**
 * @returns {Date} Previous month end date in UTC.
 */
Date.previousMonthEnd = function() {
  const date = new Date();
  date.setUTCDate(0);
  date.setUTCHours(23);
  date.setUTCMinutes(59);
  date.setUTCSeconds(59);
  date.setUTCMilliseconds(999);

  return date;
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
 * Returns `find` operator to search for new records after `this`.
 */
Date.prototype.getFindOperator = function() {
  return {
    $or: [
      { _id: { $gte: BSON.ObjectId.fromDate(this) } },
      { u: { $gte: this } }
    ]
  };
}

/**
 * Stringifies object using JSON format.
 * @returns Stringified object in JSON format.
 */
Object.prototype.stringify = function() {
  return JSON.stringify(this);
};

/**
 * Sets value for a key only if it is not null or undefined.
 */
Object.prototype.setIfNotNullOrUndefined = function(key, value) {
  if (value != null) {
    this[key] = value;
  }
}

/**
 * Checks database objects for equality. 
 * Objects are considered equal if all non-null values are equal.
 * Object values are compared using `toString()` comparison.
 * @returns {boolean} Comparison result.
 */
Object.prototype.isEqual = function(rhs) {
  if (rhs == null) {
    return false;
  }
  
  const lhsEntries = Object.entries(this).filter(([key, value]) => value != null);
  const rhsEntries = Object.entries(rhs).filter(([key, value]) => value != null);

  if (lhsEntries.length !== rhsEntries.length) {
    return false;

  } else if (lhsEntries.length === 0) {
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

///////////////////////////////////////////////////////////////////////////////// FUNCTIONS

/**
 * Returns `true` if dates are equal or both `null`.
 */
function _compareOptionalDates(left, right) {
  if (left == null && right == null) {
    return true;
  } else if (left == null && right != null) {
    return false;
  } else if (left != null && right == null) {
    return false;
  } else if (left != null && right != null) {
    return left.getTime() == right.getTime();
  }
}

compareOptionalDates = _compareOptionalDates;

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
        if (typeof string === 'undefined') {
          if (this.rawBody != null) {
            string = this.rawBody.text();
          } else {
            string = null;
          }
        }

        return string;
      }
    });

    let json;
    Object.defineProperty(this, "json", {
      get: function() {
        if (typeof json === 'undefined') {
          if (this.string != null) {
            try {
              const result = EJSON.parse(this.string);
              if (result != null) {
                json = result;
              } else {
                json = null;
              }

            } catch(error) {
              console.error(`Unable to map JSON from response: ${this.string}`);
              json = null;
            }
  
          } else {
            json = null;
          }
        }

        return json;
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
          500, // Internal Server Error
          502, // Bad Gateway
          504, // Gateway Time-out
        ];

        if (alwaysRetryableStatusCodes.includes(this.statusCode)) {
          retryable = true;
          return retryable;
        }

        retryable = false;
        return retryable;
      }
    });

    let retryDelay;
    Object.defineProperty(this, "retryDelay", {
      get: function() {
        if (typeof retryDelay === 'undefined') {
          if (this.statusCode === 429 && this.string != null) {
            try {
              const json = EJSON.parse(this.string);
              if (json != null) {
                retryDelay = this.json['X-Rate-Limit-Retry-After-Milliseconds'];
              } else {
                retryDelay = null;
              }
            } catch(error) {
              retryDelay = null;
            }
          } else {
            retryDelay = null;
          }
        }

        return retryDelay;
      }
    });
  }

  toString() {
    return this.string;
  }

  toNetworkError() {
    return new _NetworkError(this.statusCode, this.string);
  }

  possiblyRetryable(queryParameters) {
    // You have used all available credits for the month. Please upgrade or purchase additional packages to access more data.
    if (this.statusCode !== 402) {
      return false;
    }

    // Check if that's an IEX context
    if (typeof adjustTokenIfPossible !== 'function') {
      return false;
    }

    return adjustTokenIfPossible(queryParameters);
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

SystemError = _SystemError;

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
  
  // TODO: allSettled
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

  Promise.prototype.observeStatusAndCatch = function() {
    // Don't modify any promise that has been already modified.
    if (this.isFinished) return this;
  
    // Set initial state
    var isPending = true;
    var isRejected = false;
    var isFulfilled = false;
    var isFinished = false;
    var error;
  
    // Observe the promise, saving the fulfillment in a closure scope.
    var result = this.then(
        function(v) {
            isFinished = true;
            isFulfilled = true;
            isPending = false;
            return v; 
        }, 
        function(e) {
            isFinished = true;
            isRejected = true;
            isPending = false;
            error = e;
            return;
        }
    );
  
    result.isFinished = function() { return isFinished; };
    result.isFulfilled = function() { return isFulfilled; };
    result.isPending = function() { return isPending; };
    result.isRejected = function() { return isRejected; };
    result.throwIfRejected = function() { if (isRejected) { throw error; } };

    return result;
  };

  /**
   * This function returns an ObjectId embedded with a given datetime
   * Accepts both Date object and string input
   */
  BSON.ObjectId.fromDate = function(date, hex) {
    /* Convert string date to Date object (otherwise assume timestamp is a date) */
    if (typeof date === 'string') {
        date = new Date(date);
    }

    if (hex == null) {
      hex = "0000000000000000";
    }

    if (hex.length !== 16) {
      _logAndThrow(
        `Hex part of ObjectID should have 16 characters length, instead received '${hex}' with '${hex.length}' length`, 
        _SystemError
      );
    }

    const timestamp = date.getTime();
    if (timestamp < 0) {
      _logAndThrow(
        `Date should be above '1970-01-01', instead received '${date}' date with '${timestamp}' timestamp`, 
        _SystemError
      );
    }

    const seconds = timestamp / 1000;
    const hexSeconds = Math.floor(seconds)
      .toString(16)
      .padStart(8, "0");

    if (hexSeconds.length !== 8) {
      _logAndThrow(
        `Hex time part of ObjectID should have 8 characters length, instead received '${hexSeconds}' with '${hexSeconds.length}' length`, 
        _SystemError
      );
    }

    // 62649eca 6e72da17e710cd18
    // --time-- ------hex-------
    // ---8---- ------16--------
    return new BSON.ObjectId(hexSeconds + hex);
  };

  /**
   * Returns HEX part from `BSON.ObjectId`.
   */
  BSON.ObjectId.prototype.hex = function() {
    return this.toString().substring(8);
  }

  /**
   * Returns time part from `BSON.ObjectId` as `Date`.
   */
  BSON.ObjectId.prototype.date = function() {
    const timeHexString = this.toString().substring(0, 8);
    const timestamp = parseInt(timeHexString, 16) * 1000;
    return new Date(timestamp);
  }

  runtimeExtended = true;
}

///////////////////////////////////////////////////////////////////////////////// FUNCTIONS

function _logAndThrow(message, ErrorType) {
  if (ErrorType == null) { ErrorType = _SystemError; }
  _throwIfUndefinedOrNull(message, `logAndThrow message`, ErrorType);
  console.error(message);
  
  throw new ErrorType(message);
}

logAndThrow = _logAndThrow;

/**
 * @note Do not forget to use `return`!
 */
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

const defaultExecutionLimit = 110;
const defaultSystemExecutionLimit = 115;
executionTimeoutErrorMessage = 'execution time limit reached';

/** Checks that we didn't exceed timeout and throws an error if so. */
function _checkExecutionTimeoutAndThrow(limit) {
  const timeLeft = _getExecutionTimeLeft(limit);
  if (timeLeft <= 0) {
    throw new _SystemError(executionTimeoutErrorMessage);
  } else {
    return timeLeft;
  }
}

checkExecutionTimeoutAndThrow = _checkExecutionTimeoutAndThrow;

function _getExecutionTimeLeft(limit) {
  if (typeof startDate === 'undefined') {
    startDate = new Date();
  }

  const endTime = new Date();
  const seconds = Math.round((endTime - startDate) / 1000);

  // One symbol full fetch takes 17.5s and we have only 120s function execution time so let's put some limit.
  if (limit == null) {
    limit = defaultExecutionLimit;
  }

  const timeLeft = limit - seconds;
  if (timeLeft <= 0) {
    console.log(`${executionTimeoutErrorMessage}. Execution time: ${seconds} seconds`);
  } else {
    console.logVerbose(`${limit - seconds} execution time left`);
  }

  return timeLeft;
}

getExecutionTimeLeft = _getExecutionTimeLeft;

function _throwIfNotObject(object, message, ErrorType) {
  _throwIfUndefinedOrNull(object, message, ErrorType);

  const type = typeof object;
  const objectType = Object.prototype.toString.call(object);
  if (type === 'object' || objectType === '[object Object]') { return object; }
  if (ErrorType == null) { ErrorType = _SystemError; }
  if (message == null) { message = ""; }
  
  throw new ErrorType(`Argument should be of the 'object' type. Instead, received '${objectType} (${type})'. ${message}`);
}

throwIfNotObject = _throwIfNotObject;

function _throwIfNotFunction(func, message, ErrorType) {
  _throwIfUndefinedOrNull(func, message, ErrorType);

  const type = typeof func;
  if (type === 'function') { return func; }
  if (ErrorType == null) { ErrorType = _SystemError; }
  if (message == null) { message = ""; }
  
  throw new ErrorType(`Argument should be of the 'function' type. Instead, received '${type}'. ${message}`);
}

throwIfNotFunction = _throwIfNotFunction;

function _throwIfNotString(object, message, ErrorType) {
  _throwIfUndefinedOrNull(object, message, ErrorType);

  const type = Object.prototype.toString.call(object);
  if (type === 'string' || type === '[object String]') { return object; }
  if (ErrorType == null) { ErrorType = _SystemError; }
  if (message == null) { message = ""; }
  
  throw new ErrorType(`Argument should be of the 'string' type. Instead, received '${type}'. ${message}`);
}

throwIfNotString = _throwIfNotString;

/**
 * Throws error with optional `message` if `object` is not an `Array`.
 * @param {object} object Object to check.
 * @param {string} message Optional error message to replace default one.
 * @param {Error} ErrorType Optional error type. `SystemError` by default.
 * @returns {object} Passed object if it's an `Array`.
 */
function _throwIfEmptyArray(object, message, ErrorType) {
  _throwIfNotArray(object, message, ErrorType);

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
  _throwIfUndefinedOrNull(object, message, ErrorType);

  const type = Object.prototype.toString.call(object);
  if (type === '[object Array]') { return object; }
  if (ErrorType == null) { ErrorType = _SystemError; }
  if (message == null) { message = ""; }
  
  throw new ErrorType(`Argument should be of the 'Array' type. Instead, received '${type}'. ${message}`);
}

throwIfNotArray = _throwIfNotArray;

function _throwIfNotNumber(object, message, ErrorType) {
  _throwIfUndefinedOrNull(object, message, ErrorType);

  const type = typeof object;
  const objectType = Object.prototype.toString.call(object);
  if (type === 'number' || objectType === '[object Number]') { return object; }
  if (ErrorType == null) { ErrorType = _SystemError; }
  if (message == null) { message = ""; }
  
  throw new ErrorType(`Argument should be of the 'number' type. Instead, received '${objectType} (${type})'. ${message}`);
}

throwIfNotNumber = _throwIfNotNumber;

function _throwIfNotDate(object, message, ErrorType) {
  _throwIfUndefinedOrNull(object, message, ErrorType);

  const type = Object.prototype.toString.call(object);
  if (type === '[object Date]') { return object; }
  if (ErrorType == null) { ErrorType = _SystemError; }
  if (message == null) { message = ""; }
  
  throw new ErrorType(`Argument should be of the 'Date' type. Instead, received '${type}'. ${message}`);
}

throwIfNotDate = _throwIfNotDate;

function _throwIfNotObjectId(object, message, ErrorType) {
  _throwIfUndefinedOrNull(object, message, ErrorType);

  const type = Object.prototype.toString.call(object);
  if (type === '[object ObjectId]') { return object; }
  if (ErrorType == null) { ErrorType = _SystemError; }
  if (message == null) { message = ""; }
  
  throw new ErrorType(`Argument should be of the 'ObjectId' type. Instead, received '${type}'. ${message}`);
}

throwIfNotObjectId = _throwIfNotObjectId;

/**
 * Throws error with optional `message` if `object` is `undefined` or `null`.
 * @param {object} object Object to check.
 * @param {string} message Optional additional error message.
 * @param {Error} ErrorType Optional error type. `SystemError` by default.
 * @returns {object} Passed object if it's defined and not `null`.
 */
function _throwIfUndefinedOrNull(object, message, ErrorType) {
  if (ErrorType == null) { ErrorType = _SystemError; }
  if (typeof object === 'undefined') {
    if (message == null) { message = ""; }
    _logAndThrow(`Argument is undefined. ${message}`, ErrorType);
    
  } else if (object === null) {
    if (message == null) { message = ""; }
    _logAndThrow(`Argument is null. ${message}`, ErrorType);
    
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

  let response = await _httpGET(baseURL, api, queryParameters);

  // Retry several times on retryable errors
  for (let step = 0; step < 10 && (response.retryable || response.possiblyRetryable(queryParameters)); step++) {
    let delay;
    if (response.retryDelay != null) {
      delay = response.retryDelay;
    } else {
      delay = (step + 1) * (500 + Math.random() * 1000);
    }

    console.log(`Received '${response.statusCode}' error with text '${response.string}'. Trying to retry after a '${delay}' delay.`);
    _checkExecutionTimeoutAndThrow(defaultSystemExecutionLimit - delay / 1000);
    await new Promise(r => setTimeout(r, delay));
    response = await _httpGET(baseURL, api, queryParameters);
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
    console.logVerbose(`Parse end`);
  }
  console.logData(`Response`, json);
  
  return json;
}

fetch = _fetch;

async function _httpGET(baseURL, api, queryParameters) {
  _throwIfUndefinedOrNull(baseURL, `_httpGET baseURL`);
  _throwIfUndefinedOrNull(api, `_httpGET api`);

  const url = _getURL(baseURL, api, queryParameters);
  console.log(`Request with URL: ${url}`);

  _checkExecutionTimeoutAndThrow(defaultSystemExecutionLimit);

  try {
    const response = await context.http.get({ url: url });
    return new _NetworkResponse(url, response);
  } catch(error) {
    throw new _SystemError(error.message.removeSensitiveData());
  }
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

/** 
 * @returns {Promise<[ObjectId]>} Array of existing enabled symbol IDs from merged symbols database, e.g. [ObjectId("61b102c0048b84e9c13e454d")]
*/
async function _getExistingSymbolIDs() {
  const symbolsCollection = atlas.db("merged").collection("symbols");
  const supportedSymbolIDs = await symbolsCollection.distinct("_id", {});
  console.log(`Existing symbols (${supportedSymbolIDs.length})`);
  console.logData(`Existing symbols (${supportedSymbolIDs.length})`, supportedSymbolIDs);

  return supportedSymbolIDs;
};

getExistingSymbolIDs = _getExistingSymbolIDs;

/** 
 * @returns {Promise<[ObjectId]>} Array of existing enabled symbol IDs from merged symbols database, e.g. [ObjectId("61b102c0048b84e9c13e454d")]
*/
async function _getSupportedSymbolIDs() {
  const symbolsCollection = atlas.db("merged").collection("symbols");
  const supportedSymbolIDs = await symbolsCollection.distinct("_id", { "m.e": { $ne: false } });
  console.log(`Supported symbols (${supportedSymbolIDs.length})`);
  console.logData(`Supported symbols (${supportedSymbolIDs.length})`, supportedSymbolIDs);

  return supportedSymbolIDs;
}

getSupportedSymbolIDs = _getSupportedSymbolIDs;

///////////////////////////////////////////////////////////////////////////////// fetch.js

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

  // Retry several times on retryable errors
  for (let step = 0; step < 10 && (response.status === '??????'); step++) {
    const delay = (step + 1) * (500 + Math.random() * 1000);
    console.log(`Received '${response.status}' error with text '${response.body.text()}'. Trying to retry after a '${delay}' delay.`);
    _checkExecutionTimeoutAndThrow(defaultSystemExecutionLimit - delay / 1000);
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
  // https://github.com/mansourcodes/country-databases/blob/main/currency-details.json
  // https://en.wikipedia.org/wiki/Currency_sign_(typography)
  const additionalData = await _fetch(ENV.hostURL, '/currencies_info.json');
  const entries = Object.entries(ejsonBody.rates);
  const exchangeRates = [];
  for (const [currency, rate] of entries) {
    const exchangeRate = {};
    exchangeRate._id = currency.toUpperCase();

    if (rate != null && rate >= 0.0) {
      exchangeRate.r = BSON.Double(rate);
    } else {
      // Skip invalid exchange rates and presume previous valid.
      continue;
    }

    const currencySymbol = additionalData?.[exchangeRate._id]?.symbol_native;
    if (currencySymbol) {
      exchangeRate.s = currencySymbol;
    } else {
      // Fallback to generic currency symbol
      exchangeRate.s = "¤";
    }

    exchangeRates.push(exchangeRate);
  }

  return exchangeRates;
}

fetchExchangeRates = _fetchExchangeRates;

///////////////////////////////////////////////////////////////////////////////// UPDATE

async function _setUpdateDate(db, _id, date) {
  throwIfUndefinedOrNull(_id, `_setUpdateDate _id`);
  if (date == null) {
    date = new Date();
  }

  const collection = db.collection("updates");
  return await collection.updateOne(
    { _id: _id }, 
    { $set: { d: date }, $currentDate: { u: true } }, 
    { "upsert": true }
  )
};

setUpdateDate = _setUpdateDate;

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
  if (typeof ENV === 'undefined') {
    ENV = {
      environment: "tests",
      hostURL: 'https://divtrackertestsv2-tsyqw.mongodbstitch.com',
      maxBulkSize: 5000,
    };

    Object.freeze(ENV);
  }

  if (typeof startDate === 'undefined') {
    startDate = new Date();
  }

  if (typeof atlas === 'undefined') {
    atlas = context.services.get("mongodb-atlas");
  }

  if (typeof db === 'undefined') {
    db = atlas.db("divtracker-v2");
  }

  // Available sources
  if (typeof sources === 'undefined') {
    /**
     * Sources in descending priority order. Higher priority became the main source on conflicts.
     */
    sources = [
      { field: 'f', name: 'fmp', db: atlas.db("fmp") },
      { field: 'i', name: 'iex', db: atlas.db("divtracker-v2") },
    ];

    sourceByName = sources.toDictionary('name');
  }

  // Adjusting console log
  if (!console.logCopy) {
    console.logCopy = console.log.bind(console);

    logDefault = true;
    console.log = function(message) {
      if (logDefault) {
        this.logCopy(getDateLogString(), message);
      }
    };
  }
  
  if (!console.errorCopy) {
    console.errorCopy = console.error.bind(console);
    logError = true;
    console.error = function(message) {
      if (logError) {
        const timePrefix = getDateLogString();
        const errorLogPrefix = `${timePrefix} [ ** ERRROR ** ]`;
        this.logCopy(errorLogPrefix, message);
        this.errorCopy(message);
      }
    };
  }
  
  if (!console.logVerbose) {
    logVerbose = false;
    console.logVerbose = function(message) {
      if (logVerbose) {
        this.logCopy(getDateLogString(), message);
      }
    };
  }
  
  if (!console.logData) {
    logData = false;
    console.logData = function(message, data) {
      if (logData) {
        this.logCopy(getDateLogString(), `${message}: ${data.stringify()}`);
      }
    };
  }

  extendRuntime();
  
  console.log("Imported utils");
};
