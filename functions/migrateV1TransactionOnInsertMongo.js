
// migrateV1TransactionOnInsert.js

// https://docs.mongodb.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/

exports = async function(changeEvent) {
  const atlas = context.services.get("Cluster0");
  const db = atlas.db("divtracker-v2");
  const transactionsCollection = db.collection("transactions")

  // Delete V2 transaction on delete
  if (changeEvent.operationType === 'delete') {
    const id = changeEvent.documentKey._id;
    await transactionsCollection.deleteOne({ _id: new ObjectId(id) });
    return;
  }
  
  // Update/Insert V2 transaction
  const transaction = changeEvent.fullDocument;
  const symbolsCollection = db.collection("symbols");
  const symbol = await symbolsCollection.findOne({ t: transaction.s });
  const symbolID = symbol._id;

  const transactionV2 = {};
  transactionV2._id = transaction._id;
  transactionV2.a = transaction.a;
  transactionV2.c = transaction.c;
  transactionV2.d = transaction.d;
  transactionV2.p = transaction.p;
  transactionV2.s = symbolID;
  
  return await transactionsCollection.safeUpdateMany([transactionV2]);
};

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
      newObjects = await this.find({ [field]: { $in: fileds } }).toArray();

    } else {
      console.log(`Old objects are undefined. Fetching all.`);
      newObjects = await this.find().toArray();
    }
  }

  if (oldObjects === [] || oldObjects === null) {
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

  console.log(`Updating: ${update.stringify()}`);

  return update;
};

/**
 * Stringifies object using JSON format.
 * @returns Stringified object in JSON format.
 */
Object.prototype.stringify = function() {
  return JSON.stringify(this);
};

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

function _logAndThrow(message) {
  _throwIfUndefinedOrNull(message, `logAndThrow message`);
  console.error(message);
  throw message;
}
