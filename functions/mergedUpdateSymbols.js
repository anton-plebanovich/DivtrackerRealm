
// mergedUpdateSymbols.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeOrderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.update/
// https://docs.mongodb.com/manual/reference/operator/query-logical/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write

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
  
  const db = atlas.db("merged");
  const mergedSymbolsCollection = db.collection("symbols");
  
  if (sourceName == null) {
    // For backward compatibility with symbols in transactions we update IEX first
    await update(mergedSymbolsCollection, find, sourceByName.iex);
    await update(mergedSymbolsCollection, find, sourceByName.fmp);
  } else {
    const source = sourceByName[sourceName];
    await update(mergedSymbolsCollection, find, source);
  }
  
  await setUpdateDate(db, `merged-symbols`);
  
  console.log(`SUCCESS`);
};

async function update(mergedSymbolsCollection, find, source) {
  const sourceCollection = source.db.collection("symbols");
  const [sourceSymbols, mergedSymbols] = await Promise.all([
    sourceCollection.fullFind(find, { u: false }),
    mergedSymbolsCollection.fullFind({}, { u: false }),
  ]);

  const mergedSymbolByID = mergedSymbols.toDictionary(x => x[source.field]?._id);
  const mergedSymbolByTicker = mergedSymbols.toDictionary(x => x.m.t);
  
  const operations = [];
  for (const sourceSymbol of sourceSymbols) {
    let operation;

    // First, we need to check if symbol already added
    operation = getUpdateMergedSymbolOperation(mergedSymbolByID, source, sourceSymbol, '_id');
    if (addOperationIfNeeded(operation, operations)) {
      continue;
    }

    // TODO: IEX and FMP has different ticker format. Example: BF.B (IEX) and BF-B (FMP)
    // IEX special symbols: 
    // . | (0)
    // # | EHC# (1)
    // + | ZGN+
    // - | WFC-L
    // = | XPOA=
    // ^ | VHAQ^

    // FMP special symbols:
    // &. | GMRP&UI.NS
    // -
    // --. | ENRO-PREF-B.ST
    // -.
    // .
    // .. | BT.A.L (1)

    // Second, check if symbol is added from different source. Try to search by ticker as more robust.
    if (ENV.isIEXSandbox) {
      // On IEX sandbox exchanges are broken
      operation = getUpdateMergedSymbolOperation(mergedSymbolByTicker, source, sourceSymbol, 't');
    } else {
      operation = getUpdateMergedSymbolOperation(mergedSymbolByTicker, source, sourceSymbol, 't', 'c');
    }

    if (addOperationIfNeeded(operation, operations)) {
      continue;
    }

    // Do not add already disabled symbols
    if (sourceSymbol.e === false) {
      continue;
    }

    // Sadly, we can't merge using name because FMP may shorten symbol names:
    // JFR - Nuveen Floating Rate Income Fund
    // NFRIX - Nuveen Floating Rate Income Fund Class I
    // FMP => NFRIX - Nuveen Floating Rate Income Fund

    // Just insert new symbol
    const main = Object.assign({}, sourceSymbol);
    main.s = source.field;

    const insertOne = {
      _id: sourceSymbol._id,
      m: main,
      [source.field]: sourceSymbol,
    };
    operation = { insertOne: insertOne };
    operations.push(operation);
  }

  if (operations.length) {
    console.log(`Performing ${operations.length} symbols update operations for '${source.name}' source`);
    const options = { ordered: false };
    await mergedSymbolsCollection.bulkWrite(operations, options);
    console.log(`Performed ${operations.length} symbols update operations for '${source.name}' source`);
  } else {
    console.log(`No operations to perform for '${source.name}' source`);
  }
}

const notExchange = {
  '-': true,
  'ETF': true,
  'FGI': true,
  'MUTUAL_FUND': true,
  'OTC': true,
};

const countryByExchange = {
  ARCX: "USA",
  BATS: "USA",
  XASE: "USA",
  XNAS: "USA",
  XNCM: "USA",
  XNGS: "USA",
  XNMS: "USA",
  XNYS: "USA",
  XPOR: "USA",
}

/**
 * Returns operation on success update
 */
function getUpdateMergedSymbolOperation(mergedSymbolByKey, source, sourceSymbol, compareField, additionCompareField) {
  const key = sourceSymbol[compareField];
  if (key == null) { 
    return null; 
  }

  const mergedSymbol = mergedSymbolByKey[key];
  if (mergedSymbol == null) {
    return null;
  }

  if (additionCompareField != null) {
    const sourceAdditionValue = sourceSymbol[additionCompareField];
    const mergedAdditionValue = mergedSymbol.m?.[additionCompareField];
    if (sourceAdditionValue !== mergedAdditionValue) {
      if (additionCompareField === 'c' && (sourceAdditionValue == null || notExchange[sourceAdditionValue] != null || mergedAdditionValue == null || notExchange[mergedAdditionValue] != null)) {
        // We allow to merge symbols if at least one does not have an actual exchange
      } else if (additionCompareField === 'c' && countryByExchange[sourceAdditionValue] === countryByExchange[mergedAdditionValue]) {
        // We ignore case when symbol is on different american exchanges
      } else {
        if (sourceSymbol.e === false) {
          // Allow disabled symbol to pass
          console.log(`Conflicting symbol: ${sourceSymbol.stringify()}. Merged: ${mergedSymbol.stringify()}`);
          return null;
        } else {
          // We need to adjust tickers that are conflicting. Currently, we just disable them in one source.
          throw `Conflicting symbol: ${sourceSymbol.stringify()}. Merged: ${mergedSymbol.stringify()}`;
        }
      }
    }
  }
  
  return getUpdateSymbolOperation(source, sourceSymbol, mergedSymbol);
}

/**
 * Returns operation on success update
 */
function getUpdateSymbolOperation(source, sourceSymbol, mergedSymbol) {
  // If source is disabled and detached we do not need to do anything
  if (sourceSymbol.e == false && mergedSymbol[source.field] == null) {
    return {};
  }

  let newMainSymbol;
  for (const otherSource of sources) {
    const otherField = otherSource.field;
    if (otherField === source.field) {
      if (sourceSymbol.e != false) {
        newMainSymbol = Object.assign({}, sourceSymbol);
        newMainSymbol.s = source.field;
        break;
      }

    } else {
      const otherSourceSymbol = mergedSymbol[otherField];
      if (otherSourceSymbol != null && otherSourceSymbol.e != false) {
        newMainSymbol = Object.assign({}, otherSourceSymbol);
        newMainSymbol.s = otherField;
        break;
      }
    }
  }

  // This may happen when there is only one disabled source.
  // Though, we still need to update data even if symbols is disabled.
  if (newMainSymbol == null) {
    newMainSymbol = Object.assign({}, sourceSymbol);
    newMainSymbol.s = source.field;
  }

  // Main source and merged symbol IDs should be synchronized.
  newMainSymbol._id = mergedSymbol._id;

  console.logData(`New main source`, newMainSymbol);
  const isSourceChange = newMainSymbol.s !== mergedSymbol.m.s;
  const isMainSymbolUpdate = !newMainSymbol.isEqual(mergedSymbol.m);
  
  // We can't detach main source
  const isSourceDetach = sourceSymbol.e == false && newMainSymbol.s !== source.field;

  const set = {};

  if (isMainSymbolUpdate) {
    set.m = newMainSymbol;
  }
  
  const unset = {};
  if (isSourceDetach) {
    unset[source.field] = "";
  } else {
    const oldSourceSymbol = mergedSymbol[source.field];
    if (oldSourceSymbol == null || !sourceSymbol.isEqual(oldSourceSymbol)) {
      set[source.field] = sourceSymbol;
    }

    // Check if we can detach any other source. 
    // This is a case when we attach to a disabled merged symbol.
    const otherSources = sources.filter(otherSource => otherSource.field !== source.field);
    const sourceToDetach = otherSources.find(otherSource => mergedSymbol[otherSource.field]?.e == false);
    const fieldToDetach = sourceToDetach?.field;
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

  if (isSourceChange) {
    update.$currentDate = { u: true, r: true };
  } else if (isMainSymbolUpdate) {
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
