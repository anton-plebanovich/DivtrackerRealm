
// mergedUpdateSymbols.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeOrderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.update/
// https://docs.mongodb.com/manual/reference/operator/query-logical/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write

/**
 * Available sources
 */
const sources = {
  iex: { field: 'i', database: 'divtracker-v2' },
  fmp: { field: 'f', database: 'fmp' },
};

/**
 * Fields in descending priority order. Higher priority became the main source on conflicts.
 */
const fields = [
  sources.fmp.field,
  sources.iex.field,
];

/**
 * Updates merged symbols collection.
 * @param {Date} date Before update date. Used to determine updated source symbols.
 * @param {String} sourceName Update source: 'iex' or 'fmp'. By default, updates from both sources.
 */
exports = async function(date, sourceName) {
  context.functions.execute("utils");

  let find;
  if (date == null || Object.prototype.toString.call(date) !== '[object Date]') {
    find = {};
  } else {
    find = date.getFindOperator();
  }

  const mergedSymbolsCollection = atlas.db("merged").collection("symbols");
  
  if (sourceName == null) {
    await update(mergedSymbolsCollection, find, sources.iex);
    await update(mergedSymbolsCollection, find, sources.fmp);
  } else {
    const source = sources[sourceName];
    await update(mergedSymbolsCollection, find, source);
  }
  
  await setUpdateDate(`merged-symbols`);
  
  console.log(`SUCCESS`);
};

async function update(mergedSymbolsCollection, find, source) {
  const sourceCollection = atlas.db(source.database).collection("symbols");
  const sourceField = source.field;
  const [sourceSymbols, mergedSymbols] = await Promise.all([
    sourceCollection.find(find, { u: false }).toArray(),
    mergedSymbolsCollection.find({}, { u: false }).toArray(),
  ]);

  const mergedSymbolByID = mergedSymbols.toDictionary(x => x[sourceField]?._id);
  const mergedSymbolByTicker = mergedSymbols.toDictionary(x => x.m.t);
  
  const operations = [];
  for (const sourceSymbol of sourceSymbols) {
    let operation;

    // First, we need to check if symbol already added
    operation = getUpdateMergedSymbolOperation(mergedSymbolByID, sourceField, sourceSymbol, '_id');
    if (addOperationIfNeeded(operation, operations)) {
      continue;
    }

    // Second, check if symbol is added from different source. Try to search by ticker as more robust.
    operation = getUpdateMergedSymbolOperation(mergedSymbolByTicker, sourceField, sourceSymbol, 't');
    if (addOperationIfNeeded(operation, operations)) {
      continue;
    }

    // Sadly, we can't merge using name because FMP may shorten symbol names:
    // JFR - Nuveen Floating Rate Income Fund
    // NFRIX - Nuveen Floating Rate Income Fund Class I
    // FMP => NFRIX - Nuveen Floating Rate Income Fund

    // Just insert new symbol
    const main = Object.assign({}, sourceSymbol);
    main.s = sourceField;

    const insertOne = {
      _id: sourceSymbol._id,
      m: main,
      [sourceField]: sourceSymbol,
    };
    operation = { insertOne: insertOne };
    operations.push(operation);
  }

  if (operations.length) {
    console.log(`Performing ${operations.length} symbols update operations for '${sourceField}' source field`);
    const options = { ordered: false };
    await mergedSymbolsCollection.bulkWrite(operations, options);
    console.log(`Performed ${operations.length} symbols update operations for '${sourceField}' source field`);
  } else {
    console.log(`No operations to perform for '${sourceField}' source field`);
  }
}

/**
 * Returns operation on success update
 */
function getUpdateMergedSymbolOperation(dictionary, sourceField, sourceSymbol, compareField) {
  const key = sourceSymbol[compareField];
  if (key == null) { return null; }

  const mergedSymbol = dictionary[key];
  if (mergedSymbol == null) {
    return null;
  } else {
    return getUpdateSymbolOperation(sourceField, sourceSymbol, mergedSymbol);
  }
}

/**
 * Returns operation on success update
 */
function getUpdateSymbolOperation(sourceField, sourceSymbol, mergedSymbol) {
  let newMainSource;
  for (const field of fields) {
    if (field === sourceField) {
      if (sourceSymbol.e != false) {
        newMainSource = Object.assign({}, sourceSymbol);
        newMainSource.s = sourceField;
        break;
      }

    } else {
      const sourceSymbol = mergedSymbol[field];
      if (sourceSymbol != null && sourceSymbol.e != false) {
        newMainSource = Object.assign({}, sourceSymbol);
        newMainSource.s = field;
        break;
      }
    }
  }

  // This may happen when there is only one disabled source.
  // Though, we still need to update data even if symbols is disabled.
  if (newMainSource == null) {
    newMainSource = Object.assign({}, sourceSymbol);
    newMainSource.s = sourceField;
  }

  console.logData(`New main source`, newMainSource);

  const newMainSourceIDString = newMainSource._id.toString();
  const mergedSymbolMainIDString = mergedSymbol.m._id.toString();
  const sourceSymbolIDString = sourceSymbol._id.toString();
  const isMainSourceChange = newMainSourceIDString !== mergedSymbolMainIDString;
  const isMainSourceUpdate = sourceSymbolIDString === newMainSourceIDString;
  
  // We can't detach main source
  const isSourceDetach = sourceSymbol.e == false && newMainSourceIDString !== sourceSymbolIDString;

  const set = {};

  if (!newMainSource.isEqual(mergedSymbol.m)) {
    set.m = newMainSource;
  }

  const unset = {};
  if (isSourceDetach) {
    unset[sourceField] = "";
  } else {
    const oldSourceSymbol = mergedSymbol[sourceField];
    if (oldSourceSymbol == null || !sourceSymbol.isEqual(oldSourceSymbol)) {
      set[sourceField] = sourceSymbol;
    }

    // Check if we can detach any other source. 
    // This is a case when we attach to a disabled merged symbol.
    const otherFields = fields.filter(field => field !== sourceField);
    const fieldToDetach = otherFields.find(field => mergedSymbol[field] != null && mergedSymbol[field].e == false);
    if (fieldToDetach != null) {
      unset[fieldToDetach] = "";
    }
  }

  const update = {};
  if (Object.keys(set).length !== 0) {
    update.$set = set;
  }

  if (Object.keys(unset).length !== 0) {
    update.$unset = unset;
  }

  if (Object.keys(update).length === 0) {
    return {};
  }

  if (isMainSourceChange) {
    update.$currentDate = { u: true, r: true };
  } else if (isMainSourceUpdate) {
    update.$currentDate = { u: true };
  } else {
    // Backup source updates do not change update date because only 'm' field is primary data and all other fields are here to simplify updates.
  }

  const filter = { _id: mergedSymbol._id };
  const updateOne = { filter: filter, update: update, upsert: false };
  const operation = { updateOne: updateOne };

  return operation;
}

/**
 * Returns `true` if there was an operation, even if it is empty.
 * Means update succeeded.
 */
function addOperationIfNeeded(operation, operations) {
  if (operation == null) {
    return false;
  } else if (Object.keys(operation).length === 0) {
    return true;
  } else {
    operations.push(operation);
    return true;
  }
}