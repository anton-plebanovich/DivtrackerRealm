
// getDataV2.js

/**
 * Returns data for clients.
 * @param {number} timestamp Last update timestamp in milliseconds. Used to compute changes from after it instead of returning everything.
 * @param {Array<String>} collectionNames Collection names for which data is requested. Assumes all collections if `null`.
 * @param {Array<ObjectId>} symbolIDs Symbol IDs for which data is requested. Everything is returned if `null`.
 * @param {Array<String>} fullFetchCollections Collections for which to perform full data fetch and ignore passed `symbolIDs`.
 */
exports = async function(timestamp, collectionNames, symbolIDs, fullFetchCollections) {
  context.functions.execute("utils");

  const lastUpdateTimestamp = new Date().getTime();
  
  let previousUpdateDate = null;
  if (timestamp != null) {
    throw `timestamp: ${timestamp.getTime()}`;
    throwIfNotNumber(
      timestamp, 
      `Please pass timestamp in milliseconds as the first argument.`, 
      UserError
    );
    
    if (timestamp > lastUpdateTimestamp) {
      logAndThrow(`Invalid last update timestamp parameter. Passed timestamp '${timestamp}' is higher than the new update timestamp: ${lastUpdateTimestamp})`);
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
      logAndThrow(`Invalid collections array as the second argument: ${excessiveCollections}. Valid collections are: ${allCollections}.`);
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
      logAndThrow(`Max collections count '${maxSymbolsCount}' is exceeded. Please make sure there are less than 1000 unique symbols in the portfolio.`);
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
        logAndThrow(`Invalid full symbol collections array as the fourth argument: ${excessiveFullFetchCollections}. Valid full symbol collections are: ${allowedFullFetchCollections}.`);
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
      logAndThrow(`Invalid collections array as the second argument: ${collectionNames}. Full data is not supported for: ${excessiveCollections}. Full data fetch allowed collections: ${allowedFullFetchCollections}. Please provide concrete symbols to fetch or remove not supported collections.`);
    }
  }

  const mergedSymbolsCollection = atlas.db("merged").collection('symbols');

  // This goes to client and so contains merged symbol IDs
  let refetchMergedSymbolIDs = [];

  // This goes to specific collection query and so contains source symbol IDs
  let refetchSymbolIDsBySource = {};

  // This goes to specific collection query and so contains source symbol IDs
  let symbolIDsBySource = {};

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

    // Set empty array if source is missing so we know there are nothing to fetch for the source
    sources.forEach(source => {
      if (symbolIDsBySource[source.field] == null) {
        symbolIDsBySource[source.field] = [];
      }
    });

    if (previousUpdateDate != null) {
      const refetchMergedSymbols = mergedSymbols.filter(x => x.r?.getTime() >= lastUpdateTimestamp);
      refetchSymbolIDsBySource = refetchMergedSymbols.reduce((dictionary, refetchMergedSymbol) => {
        const key = refetchMergedSymbol.m.s;
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

  const operations = collectionNames.map(async collectionName => {
    if (collectionName === 'symbols') {
      const symbols = await getSymbolsData(mergedSymbolsCollection, previousUpdateDate, symbolIDs, fullFetchCollections);
      return { symbols: symbols };
    } else {
      const operations = sources.map(async source => {
        if (singularSourceCollections.includes(collectionName) && source.name !== 'iex') {
          return [];
        }

        const collection = atlas.db(source.databaseName).collection(collectionName);
        const sourceSymbolIDs = symbolIDsBySource[source.field];
        const sourceRefetchSymbolIDs = refetchSymbolIDsBySource[source.field];
        return await getCollectionData(collection, collectionName, previousUpdateDate, sourceSymbolIDs, sourceRefetchSymbolIDs, fullFetchCollections)
      });

      const arrays = await Promise.all(operations);

      return { [collectionName]: arrays.flat() };
    }
  });

  const operationResults = await Promise
    .all(operations)
    .mapErrorToSystem();

  const updates = operationResults.reduce((result, operationResult) => {
    return Object.assign(result, operationResult);
  }, {});

  const result = {};
  if (refetchMergedSymbolIDs.length) {
    result.cleanups = refetchMergedSymbolIDs;
  }
  result.updates = updates;
  result.lastUpdateTimestamp = lastUpdateTimestamp;

  return result;
};

//////////////////////////// HELPER FUNCTIONS

async function getSymbolsData(mergedSymbolsCollection, previousUpdateDate, symbolIDs, fullFetchCollections) {
  const find = {};
  if (previousUpdateDate != null) {
    // TODO: We might need to use date - 5 mins to prevent race conditions. I am not 100% sure because I don't know if MongoDB handles it properly.
    Object.assign(find, previousUpdateDate.getFindOperator());
  }

  if (symbolIDs != null && !fullFetchCollections.includes('symbols')) {
    find._id = { $in: symbolIDs };
  }

  console.log(`Performing 'symbols' find: ${find.stringify()}`);

  const projection = { m: true };
  const mergedSymbols = await mergedSymbolsCollection.find(find, projection).toArray();
  const symbols = mergedSymbols.map(x => x.m);

  return symbols;
}

async function getCollectionData(collection, collectionName, previousUpdateDate, symbolIDs, refetchSymbolIDs, fullFetchCollections) {
  if (symbolIDs?.length === 0) {
    return [];
  }

  const find = {};

  if (previousUpdateDate != null) {
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

  if (symbolIDs != null && !fullFetchCollections.includes(collectionName)) {
    if (singularSymbolCollections.includes(collectionName)) {
      find._id = { $in: symbolIDs };
    } else {
      find.s = { $in: symbolIDs };
    }
  }

  console.log(`Performing '${collectionName}' find: ${find.stringify()}`);

  const projection = { u: false };

  return await collection.find(find, projection).toArray();
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
