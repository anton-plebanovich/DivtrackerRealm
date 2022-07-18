
// getDataV2.js

/**
 * Returns data for clients.
 * @param {number} timestamp Last update timestamp in milliseconds. Used to compute changes from after it instead of returning everything.
 * @param {Array<String>} collectionNames Collection names for which data is requested. Assumes all collections if `null`.
 * @param {Array<ObjectId>} symbolIDs Symbol IDs for which data is requested. Everything is returned if `null`.
 * @param {Array<String>} fullFetchCollections Collections for which to perform full data fetch and ignore passed `symbolIDs`.
 * @returns {{ lastUpdateTimestamp: Int64, cleanups: [ObjectId], deletions: [String: [ObjectId]], updates: [String: [Object]] }} Response
 */
exports = async function(timestamp, collectionNames, symbolIDs, fullFetchCollections) {
  context.functions.execute("utils");

  const lastUpdateTimestamp = new Date().getTime();
  
  let previousUpdateDate = null;
  if (timestamp != null) {
    throwIfNotNumber(
      timestamp, 
      `Please pass timestamp in milliseconds as the first argument.`, 
      UserError
    );
    
    if (timestamp > lastUpdateTimestamp) {
      logAndThrow(
        `Invalid last update timestamp parameter. Passed timestamp '${timestamp}' is higher than the new update timestamp '${lastUpdateTimestamp}'`, 
        UserError
      );
    }

    if (timestamp < 0) {
      logAndThrow(
        `Invalid last update timestamp parameter. Timestamp '${timestamp}' should be greater than zero`, 
        UserError
      );
    }

    previousUpdateDate = new Date(timestamp);
  }

  if (collectionNames != null) {
    throwIfEmptyArray(
      collectionNames, 
      `Please pass collections array as the second argument. It may be null but must not be empty. Valid collections are: ${allCollections}.`, 
      UserError
    );

    throwIfNotString(
      collectionNames[0], 
      `Please pass collections array as the second argument. Valid collections are: ${allCollections}.`, 
      UserError
    );

    const excessiveCollections = collectionNames.filter(x => !allCollections.includes(x));
    if (excessiveCollections.length) {
      logAndThrow(
        `Invalid collections array as the second argument: ${excessiveCollections}. Valid collections are: ${allCollections}.`,
        UserError
        );
    }
    
  } else {
    collectionNames = allCollections;
  }
  
  if (symbolIDs != null) {
    throwIfEmptyArray(
      symbolIDs, 
      `Please pass symbol IDs array as the third argument. It may be null but must not be empty.`, 
      UserError
    );

    throwIfNotObjectId(
      symbolIDs[0], 
      `Please pass symbol IDs array as the third argument.`, 
      UserError
    );
  
    if (symbolIDs.length > maxSymbolsCount) {
      logAndThrow(
        `Max collections count '${maxSymbolsCount}' is exceeded. Please make sure there are less than 1000 unique symbols in the portfolio.`,
        UserError
      );
    }
  }

  if (fullFetchCollections != null) {
    throwIfNotArray(
      fullFetchCollections, 
      `Please pass full symbol collections array as the fourth argument. It may be null. Valid collections are: ${allCollections}.`, 
      UserError
    );

    if (fullFetchCollections.length) {
      throwIfNotString(
        fullFetchCollections[0], 
        `Please pass full symbol collections array as the fourth argument. Valid collections are: ${allCollections}.`, 
        UserError
      );
  
      // Check that there are no unexpected full fetch collections
      const excessiveFullFetchCollections = fullFetchCollections.filter(x => !allowedFullFetchCollections.includes(x));
      if (excessiveFullFetchCollections.length) {
        logAndThrow(
          `Invalid full symbol collections array as the fourth argument: ${excessiveFullFetchCollections}. Valid full symbol collections are: ${allowedFullFetchCollections}.`, 
          UserError
        );
      }
    }
  } else {
    fullFetchCollections = [];
  }

  // exchange-rates are always ignoring symbols when requested
  if (!fullFetchCollections.includes('exchange-rates')) {
    fullFetchCollections.push('exchange-rates');
  }

  // updates are always ignoring symbols when requested
  if (!fullFetchCollections.includes('updates')) {
    fullFetchCollections.push('updates');
  }

  // Check that we do not perform full fetch for collections that do not support it
  if (symbolIDs == null || symbolIDs.length === 0) {
    const excessiveCollections = collectionNames.filter(x => !allowedFullFetchCollections.includes(x));
    if (excessiveCollections.length > 0) {
      logAndThrow(
        `Invalid collections array as the second argument: ${collectionNames}. Full data is not supported for: ${excessiveCollections}. Full data fetch allowed collections: ${allowedFullFetchCollections}. Please provide concrete symbols to fetch or remove not supported collections.`,
        UserError
      );
    }
  }

  const mergedSymbolsCollection = atlas.db("merged").collection('symbols');

  // This goes to client and so contains merged symbol IDs
  let refetchMergedSymbolIDs = [];

  // This goes to specific collection query and so contains source symbol IDs
  let refetchSymbolIDsBySource = {};

  // This goes to specific collection query and so contains source symbol IDs
  let symbolIDsBySource = {};

  // Used to fix source object IDs if they don't match main symbol ID.
  let mainIDBySourceID = {}

  if (symbolIDs?.length) {
    const mergedSymbols = await mergedSymbolsCollection.find({ _id: { $in: symbolIDs } }).toArray();
    symbolIDsBySource = mergedSymbols.reduce((dictionary, mergedSymbol) => {
      const key = mergedSymbol.m.s;
      const value = mergedSymbol[key]._id;
      const bucket = dictionary[key];
      if (bucket == null) {
        dictionary[key] = [value];
      } else {
        bucket.push(value);
      }
      
      return dictionary;
    }, {});

    mainIDBySourceID = mergedSymbols.reduce((dictionary, mergedSymbol) => {
      const sourceField = mergedSymbol.m.s;
      const sourceID = mergedSymbol[sourceField]._id;
      const mainID = mergedSymbol.m._id;
      if (mainID.toString() !== sourceID.toString()) {
        dictionary[sourceID] = mainID;
      }

      return dictionary;
    }, {});

    // Set empty array if source is missing so we know there are nothing to fetch for the source
    sources.forEach(source => {
      if (symbolIDsBySource[source.field] == null) {
        symbolIDsBySource[source.field] = [];
      }
    });

    if (previousUpdateDate != null) {
      const refetchMergedSymbols = mergedSymbols.filter(x => x.r?.getTime() >= timestamp);
      refetchSymbolIDsBySource = refetchMergedSymbols.reduce((dictionary, mergedSymbol) => {
        const key = mergedSymbol.m.s;
        const value = mergedSymbol[key]._id;
        const bucket = dictionary[key];
        if (bucket == null) {
          dictionary[key] = [value];
        } else {
          bucket.push(value);
        }
        
        return dictionary;
      }, {});

      // Set empty array if source is missing so we know there are nothing to refetch for the source
      sources.forEach(source => {
        if (refetchSymbolIDsBySource[source.field] == null) {
          refetchSymbolIDsBySource[source.field] = [];
        }
      });
  
      refetchMergedSymbolIDs = refetchMergedSymbols.map(x => x._id);
    }
  }
  
  console.logData('refetchMergedSymbolIDs', refetchMergedSymbolIDs);
  console.logData('refetchSymbolIDsBySource', refetchSymbolIDsBySource);
  console.logData('symbolIDsBySource', symbolIDsBySource);
  console.logData('mainIDBySourceID', mainIDBySourceID);

  const operations = collectionNames.map(async collectionName => {
    if (collectionName === 'symbols') {
      const symbols = await getSymbolsData(mergedSymbolsCollection, previousUpdateDate, symbolIDs, fullFetchCollections);
      return { symbols: symbols };
    } else {
      const operations = sources.map(async source => {
        if (singularSourceCollections.includes(collectionName) && source.name !== 'iex') {
          return [];
        }

        const sourceSymbolIDs = symbolIDsBySource[source.field];
        const sourceRefetchSymbolIDs = refetchSymbolIDsBySource[source.field];
        return await getCollectionData(source, collectionName, previousUpdateDate, sourceSymbolIDs, sourceRefetchSymbolIDs, fullFetchCollections, mainIDBySourceID)
      });

      const arrays = await Promise.all(operations);
      const objects = arrays.flat();

      return { [collectionName]: objects };
    }
  });

  const operationResults = await Promise
    .all(operations)
    .mapErrorToSystem();

  const [deletions, updates] = operationResults.reduce((result, operationResult) => {
    const [deletions, updates] = result;
    const collectionName = Object.keys(operationResult)[0];
    const [deletedObjects, updatedObjects] = operationResult[collectionName].reduce((result, object) => {
      const [deletedObjects, updatedObjects] = result;
      if (object.x == true) {
        deletedObjects.push(object._id);
      } else {
        updatedObjects.push(object);
      }

      return [deletedObjects, updatedObjects];
    }, [[], []]);

    if (deletedObjects.length) {
      deletions[collectionName] = deletedObjects;
    }

    if (updatedObjects.length) {
      updates[collectionName] = updatedObjects;
    }
    
    return [deletions, updates];
  }, [{}, {}]);

  const response = {};
  response.lastUpdateTimestamp = lastUpdateTimestamp;

  if (refetchMergedSymbolIDs.length) {
    response.cleanups = refetchMergedSymbolIDs;
  }

  if (Object.keys(deletions).length) {
    response.deletions = deletions;
  }

  if (Object.keys(updates).length) {
    response.updates = updates;
  }

  return response;
};

//////////////////////////// HELPER FUNCTIONS

async function getSymbolsData(mergedSymbolsCollection, previousUpdateDate, symbolIDs, fullFetchCollections) {
  const find = {};

  const projection = { _id: 0, m: 1 };
  sources.forEach(source => projection[source.field] = 0);

  if (previousUpdateDate != null) {
    // TODO: We might need to use date - 5 mins to prevent race conditions. I am not 100% sure because I don't know if MongoDB handles it properly.
    Object.assign(find, previousUpdateDate.getFindOperator());
  }

  if (symbolIDs != null && !fullFetchCollections.includes('symbols')) {
    find._id = { $in: symbolIDs };
  }

  console.logData(`Performing 'symbols' find`, find);

  const mergedSymbols = await mergedSymbolsCollection.find(find, projection).toArray();
  const symbols = mergedSymbols.map(x => x.m);

  return symbols;
}

async function getCollectionData(source, collectionName, previousUpdateDate, symbolIDs, refetchSymbolIDs, fullFetchCollections, mainIDBySourceID) {
  const collection = source.db.collection(collectionName);
  const find = {};
  const projection = { u: 0 };

  // Remove system data from response
  const systemFields = systemFieldsByCollectionNameBySourceName[source.name]?.[collectionName];
  if (systemFields != null) {
    systemFields.forEach(x => projection[x] = 0);
  }

  if (symbolIDs != null && !fullFetchCollections.includes(collectionName)) {
    if (symbolIDs.length === 0) {
      console.logVerbose(`No symbols to fetch for '${source.name}-${collectionName}'. Skipping.`);
      return [];
    }

    if (singularSymbolCollections.includes(collectionName)) {
      find._id = { $in: symbolIDs };
    } else {
      find.s = { $in: symbolIDs };
    }
  }

  if (previousUpdateDate == null) {
    // We do not need to return deleted objects during full fetches for multiple objects collections
    if (!singularSymbolCollections.includes(collectionName)) {
      find.x = { $ne: true };
    }
    
  } else {
    // TODO: We might need to use date - 5 mins to prevent race conditions. I am not 100% sure because I don't know if MongoDB handles it properly.
    const dateFind = {};
    if (nonSearchableIDCollections.includes(collectionName)) {
      dateFind.u = { $gte: previousUpdateDate };
    } else {
      Object.assign(dateFind, previousUpdateDate.getFindOperator());
    }

    // Fetch everything for refetch symbols
    const fullFind = {};
    if (refetchSymbolIDs?.length) {
      if (singularSymbolCollections.includes(collectionName)) {
        fullFind._id = { $in: refetchSymbolIDs };
      } else {
        fullFind.s = { $in: refetchSymbolIDs };
      }
    }

    if (Object.keys(fullFind).length) {
      find.$or = [
        fullFind,
        dateFind,
      ];

    } else {
      Object.assign(find, dateFind);
    }
  }

  console.logData(`Performing '${source.name}-${collectionName}' find`, find);

  const objects = await collection.find(find, projection).toArray();
  fixData(objects, collectionName, mainIDBySourceID);

  return objects
}

function fixData(objects, collectionName, mainIDBySourceID) {
  if (nonSearchableIDCollections.includes(collectionName)) { return objects; }

  if (singularSymbolCollections.includes(collectionName)) {
    objects.forEach(object => {
      const mainID = mainIDBySourceID[object._id];
      if (mainID != null) {
        object._id = mainID;
      }
    });

  } else {
    objects.forEach(object => {
      const mainID = mainIDBySourceID[object.s];
      if (mainID != null) {
        object.s = mainID;
      }
    });
  }
}

//////////////////////////// CONSTANTS

const maxSymbolsCount = 1000;

const allCollections = [
  'companies',
  'dividends',
  'exchange-rates',
  'historical-prices',
  'quotes',
  'splits',
  'symbols',
  'updates',
];

const singularSymbolCollections = [
  'companies',
  'quotes',
  'symbols',
];

const allowedFullFetchCollections = [
  "exchange-rates", 
  "symbols", 
  "updates"
];

// Collections that have objects with non-searchable ID, e.g. 'USD' for 'exchange-rates'
const nonSearchableIDCollections = [
  'exchange-rates',
  'updates',
];

// Collections that has only one source - 'divtracker-v2'
const singularSourceCollections = [
  'exchange-rates',
  'updates',
];

const systemFieldsByCollectionNameBySourceName = {
  iex: {
    dividends: ['i'], // refid
    splits: ['i'], // refid
  }
};
