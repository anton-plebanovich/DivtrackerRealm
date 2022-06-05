
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
  if (date == null) {
    find = {};
  } else {
    find = date.getFindOperator();
  }

  const mergedSymbolsCollection = atlas.db("merged").collection("symbols");
  
  if (sourceName == null) {
    update(mergedSymbolsCollection, find, sources.iex);
    update(mergedSymbolsCollection, find, sources.fmp);
  } else {
    const source = sources[sourceName];
    update(mergedSymbolsCollection, find, source);
  }
  
  await setUpdateDate(`merged-symbols`);
  
  console.log(`SUCCESS`);
};

async function update(mergedSymbolsCollection, find, source) {
  const sourceCollection = atlas.db(source.database).collection("symbols");
  const sourceSymbols = await sourceCollection.find(find).toArray();
  const sourceField = source.field;

  // TODO: We can do that in parallel
  // TODO?: Instead we can just get all symbols and use dictionaries to get data. Should be faster.
  for (const sourceSymbol of sourceSymbols) {
    // First, we need to check if symbol already added
    if (await updatemergedSymbolIfPossible(mergedSymbolsCollection, sourceField, sourceSymbol, '_id')) {
      continue;
    }

    // Second, check if symbol is added from different source. Try to search by ticker as more robust.
    if (await updatemergedSymbolIfPossible(mergedSymbolsCollection, sourceField, sourceSymbol, 't')) {
      continue;
    }

    // Third, try to search by name as least robust.
    if (await updatemergedSymbolIfPossible(mergedSymbolsCollection, sourceField, sourceSymbol, 'n')) {
      continue;
    }

    // Just insert new symbol
    await mergedSymbolsCollection.insertOne({
      _id: sourceSymbol._id,
      m: sourceSymbol,
      [sourceField]: sourceSymbol,
    });
  }
}

/**
 * Returns `true` on success update
 */
async function updatemergedSymbolIfPossible(mergedSymbolsCollection, sourceField, sourceSymbol, compareField) {
  const find = getFindOperator(sourceField, sourceSymbol, compareField);
  const mergedSymbol = await mergedSymbolsCollection.findOne(find);
  if (mergedSymbol != null) {
    await updateSymbol(mergedSymbolsCollection, sourceField, sourceSymbol, mergedSymbol);
    return true;
  } else {
    return false;
  }
}

function getFindOperator(sourceField, sourceSymbol, compareField) {
  const idField = `${sourceField}.${compareField}`;
  return { [idField]: sourceSymbol[compareField] };
}

async function updateSymbol(mergedSymbolsCollection, sourceField, sourceSymbol, mergedSymbol) {
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

  const newMainSourceIDString = newMainSource._id.toString();
  const mergedSymbolMainIDString = mergedSymbol.m._id.toString();
  const sourceSymbolIDString = sourceSymbol._id.toString();
  const isMainSourceChange = newMainSourceIDString !== mergedSymbolMainIDString;
  const isMainSourceUpdate = sourceSymbolIDString === newMainSourceIDString;
  const isSourceDetach = sourceSymbol.e == false && newMainSourceIDString !== sourceSymbolIDString;

  const find = { _id: mergedSymbol._id };
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
    const fieldToDetach = otherFields.first(field => mergedSymbol[field] != null && mergedSymbol[field].e == false);
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
    update.$currentDate.u = true;
    update.$currentDate.r = true;
  } else if (isMainSourceUpdate) {
    update.$currentDate.u = true;
  }

  await mergedSymbolsCollection.updateOne(find, update);
}
