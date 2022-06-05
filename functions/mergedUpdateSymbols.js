
// mergedUpdateSymbols.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeOrderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.update/
// https://docs.mongodb.com/manual/reference/operator/query-logical/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write

// Merged symbols collection fields.
// _id: ID. Inherited from source symbol ID on creation.
// f: FMP symbol ID or `null`
// i: IEX symbol ID or `null`
// r: Refetch date or `null`
// m: Main source symbol ID
// t: Main source ticker
// n: Main source company name
//
// Rules:
// First, we check IEX collection since it more robust. Then, FMP.
// When we have both FMP and IEX symbols we prioritize FMP as the main source.
// When the main source ID changes we update refetch date.

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
    sourceCollection.find(find).toArray(),
    mergedSymbolsCollection.find({}).toArray(),
  ]);

  const mergedSymbolByID = mergedSymbols.toDictionary(x => x[sourceField]?._id);
  const mergedSymbolByTicker = mergedSymbols.toDictionary(x => x.m.t);
  const mergedSymbolByName = mergedSymbols.toDictionary(x => x.m.n);
  
  const operations = [];
  for (const sourceSymbol of sourceSymbols) {
    let operation;

    // First, we need to check if symbol already added
    operation = getUpdateMergedSymbolOperation(mergedSymbolByID, sourceField, sourceSymbol, '_id');
    if (operation != null) {
      operations.push(operation);
      continue;
    }

    // Second, check if symbol is added from different source. Try to search by ticker as more robust.
    operation = getUpdateMergedSymbolOperation(mergedSymbolByTicker, sourceField, sourceSymbol, 't');
    if (operation != null) {
      operations.push(operation);
      continue;
    }

    // Sadly, we can't merge using name because FMP may shorten symbol names:
    // JFR - Nuveen Floating Rate Income Fund
    // NFRIX - Nuveen Floating Rate Income Fund Class I
    // FMP => NFRIX - Nuveen Floating Rate Income Fund

    // Just insert new symbol
    const insertOne = {
      _id: sourceSymbol._id,
      m: sourceSymbol,
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
 * Returns `true` on success update
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

function getUpdateSymbolOperation(sourceField, sourceSymbol, mergedSymbol) {
  let newMainSource = mergedSymbol.m;
  for (const field of fields) {
    if (field === sourceField) {
      if (sourceSymbol.e != false) {
        newMainSource = sourceSymbol;
        break;
      }

    } else {
      const sourceSymbol = mergedSymbol[field];
      if (sourceSymbol != null && sourceSymbol.e != false) {
        newMainSource = sourceSymbol;
        break;
      }
    }
  }

  console.logData(`New main source`, newMainSource);

  const newMainSourceIDString = newMainSource._id.toString();
  const mergedSymbolMainIDString = mergedSymbol.m._id.toString();
  const sourceSymbolIDString = sourceSymbol._id.toString();
  const isMainSourceChange = newMainSourceIDString !== mergedSymbolMainIDString;
  const isMainSourceUpdate = sourceSymbolIDString === newMainSourceIDString;
  const isSourceDetach = sourceSymbol.e == false && newMainSourceIDString !== sourceSymbolIDString;

  const set = {
    m: newMainSource,
  };

  const unset = {};
  if (isSourceDetach) {
    unset[sourceField] = "";
  } else {
    set[sourceField] = sourceSymbol;

    // Check if we can detach any other source. 
    // This is a case when we attach to a disabled merged symbol.
    const otherFields = fields.filter(field => field !== sourceField);
    const fieldToDetach = otherFields.find(field => mergedSymbol[field] != null && mergedSymbol[field].e == false);
    if (fieldToDetach != null) {
      unset[fieldToDetach] = "";
    }
  }

  let update;
  if (Object.keys(unset).length === 0) {
    update = { $set: set };
  } else {
    update = { $set: set, $unset: unset };
  }

  if (isMainSourceChange) {
    update.$currentDate = { u: true, r: true };
  } else if (isMainSourceUpdate) {
    update.$currentDate = { u: true };
  }

  const filter = { _id: mergedSymbol._id };
  const updateOne = { filter: filter, update: update, upsert: false };
  const operation = { updateOne: updateOne };

  return operation;
}
