
// migrations.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.count/
// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/
// https://docs.mongodb.com/manual/reference/operator/update/unset/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.removeOne/
// https://docs.mongodb.com/manual/reference/method/Bulk.insert/

// V2 roll out
// - Manual: Run V1 -> V2 data migration

// V1 deprecation phase 1
// - V1 functions and triggers should be removed
// - V1 data should be erased in all collections
// - Check that V2 data is properly fetched, e.g. enough dividends and historical prices

// V1 deprecation phase 2
// - Manual: Disable sync
// - Manual: Drop `divtracker` database
// - Manual: Run partition key migration
// - Deploy: New schemes, function, and sync that are using optional _ partition key. V1 schemes remove.
exports = async function() {
  context.functions.execute("utilsV2");

  return await v2DatabaseFillMigration();
};

async function v2DatabaseFillMigration() {
  const v2SymbolsCollection = db.collection('symbols');

  // Fetch V2 exchange rates and symbols if needed
  if (await v2SymbolsCollection.count() <= 0) {
    await Promise.all([
      context.functions.execute("updateExchangeRatesV2"),
      context.functions.execute("updateSymbolsV2")
    ]);
  }

  const v2Symbols = await v2SymbolsCollection.find().toArray();
  const invalidEntitesFind = { $regex: ":(NAS|NYS|POR|USAMEX|USBATS|USPAC)" };

  return Promise.all([
    // fillV2CompanyCollectionMigration(v2Symbols, invalidEntitesFind),
    // fillV2DividendsCollectionMigration(v2Symbols, invalidEntitesFind),
    fillV2HistoricalPricesCollectionMigration(v2Symbols, invalidEntitesFind),
  ]);
}

async function fillV2CompanyCollectionMigration(v2Symbols, invalidEntitesFind) {
  const v1Collection = atlas.db("divtracker").collection('companies');

  // Check that data is valid
  const invalidEntities = await v1Collection.count({ _id: invalidEntitesFind });
  if (invalidEntities > 0) {
    throw `Found ${invalidEntities} invalid entities for V1 companies`;
  }
  
  const v1Companies = await v1Collection.find().toArray();

  /** V1
  {
    "_id": "AAPL:NAS",
    "_p": "P",
    "n": "Apple Inc",
    "s": "ap u MftrtEniiclorCrecmuc oaengtnu",
    "t": "cs"
  }
   */

  /** V2
  {
    "_id": {
      "$oid": "61b102c0048b84e9c13e4564"
    },
    "_p": "2",
    "i": "uric nrro uaetnMotuial etnCcgcmfpE",
    "n": "Apple Inc",
    "t": "cs"
  }
   */
  const v2Companies = v1Companies
    .map(v1Company => {
      // AAPL:XNAS -> AAPL
      const ticker = v1Company._id.split(':')[0];
      const symbolID = v2Symbols.find(x => x.t === ticker)._id;
      if (symbolID == null) {
        throw `Unknown V1 ticker '${ticker}' for company ${v1Company.stringify()}`;
      }

      const v2Company = {};
      v2Company._id = symbolID;
      v2Company._p = "2";
      v2Company.i = v1Company.s;
      v2Company.n = v1Company.n;
      v2Company.t = v1Company.t;

      return v2Company;
    });

  const v2Collection = db.collection('companies');

  // Delete existing if any to prevent duplication
  await v2Collection.deleteMany({});

  return await v2Collection.safeUpdateMany(v2Companies, []);
}

async function fillV2DividendsCollectionMigration(v2Symbols, invalidEntitesFind) {
  const v1Collection = atlas.db("divtracker").collection('dividends');

  // Check that data is valid
  const invalidEntities = await v1Collection.count({ _i: invalidEntitesFind });
  if (invalidEntities > 0) {
    throw `Found ${invalidEntities} invalid entities for V1 dividends`;
  }

  const v1Dividends = await v1Collection.find().toArray();
  /** V1
  {
    "_i": "ABBV:XNYS",
    "_id": {
      "$oid": "61b4ec78e9bd6c27860a5b47"
    },
    "_p": "P",
    "a": {
      "$numberDouble": "1.42"
    },
    "d": {
      "$date": {
        "$numberLong": "1635085800000"
      }
    },
    "e": {
      "$date": {
        "$numberLong": "1641825000000"
      }
    },
    "f": "q",
    "p": {
      "$date": {
        "$numberLong": "1644589800000"
      }
    }
  }
   */

  /** V2
  {
    "_id": {
      "$oid": "61b4ec78e9bd6c27860a5b47"
    },
    "_p": "2",
    "a": {
      "$numberDouble": "1.42"
    },
    "d": {
      "$date": {
        "$numberLong": "1635085800000"
      }
    },
    "e": {
      "$date": {
        "$numberLong": "1641825000000"
      }
    },
    "f": "q",
    "p": {
      "$date": {
        "$numberLong": "1644589800000"
      }
    },
    "s": {
      "$oid": "61b102c0048b84e9c13e456f"
    }
  }
   */
  const v2Dividends = v1Dividends
    .map(v1Dividend => {
      // AAPL:XNAS -> AAPL
      const ticker = v1Dividend._i.split(':')[0];
      const symbolID = v2Symbols.find(x => x.t === ticker)._id;
      if (symbolID == null) {
        throw `Unknown V1 ticker '${ticker}' for dividend ${v1Dividend.stringify()}`;
      }

      const v2Dividend = {};
      v2Dividend._id = v1Dividend._id;
      v2Dividend._p = "2";
      v2Dividend.a = v1Dividend.a;
      v2Dividend.c = v1Dividend.c;
      v2Dividend.d = v1Dividend.d;
      v2Dividend.e = v1Dividend.e;
      v2Dividend.f = v1Dividend.f;
      v2Dividend.p = v1Dividend.p;
      v2Dividend.s = symbolID;

      return v2Dividend;
    });

  const v2Collection = db.collection('dividends');

  // Delete existing if any to prevent duplication
  await v2Collection.deleteMany({});

  return await v2Collection.safeUpdateMany(v2Dividends, []);
}

async function fillV2HistoricalPricesCollectionMigration(v2Symbols, invalidEntitesFind) {
  const v1Collection = atlas.db("divtracker").collection('historical-prices');

  // Check that data is valid
  const invalidEntities = await v1Collection.count({ _i: invalidEntitesFind });
  if (invalidEntities > 0) {
    throw `Found ${invalidEntities} invalid entities for V1 historical prices`;
  }

  const v1HistoricalPrices = await v1Collection.find().toArray();
  /** V1
  {
    "_i": "AAPL:XNAS",
    "_id": {
      "$oid": "61b4ec77e9bd6c27860a4c2b"
    },
    "_p": "P",
    "c": {
      "$numberDouble": "28.516"
    },
    "d": {
      "$date": {
        "$numberLong": "1449867600000"
      }
    }
  }
   */

  /** V2
  {
    "_id": {
      "$oid": "61b4ec77e9bd6c27860a4c2b"
    },
    "_p": "2",
    "c": {
      "$numberDouble": "28.516"
    },
    "d": {
      "$date": {
        "$numberLong": "1449867600000"
      }
    },
    "s": {
      "$oid": "61b102c0048b84e9c13e4564"
    }
  }
   */
  const v2HistoricalPrices = v1HistoricalPrices
    .map(v1HistoricalPrice => {
      // AAPL:XNAS -> AAPL
      const ticker = v1HistoricalPrice._i.split(':')[0];
      const symbolID = v2Symbols.find(x => x.t === ticker)._id;
      if (symbolID == null) {
        throw `Unknown V1 ticker '${ticker}' for historical price ${v1HistoricalPrice.stringify()}`;
      }

      const v2HistoricalPrice = {};
      v2HistoricalPrice._id = v1HistoricalPrice._id;
      v2HistoricalPrice._p = "2";
      v2HistoricalPrice.c = v1HistoricalPrice.c;
      v2HistoricalPrice.d = v1HistoricalPrice.d;
      v2HistoricalPrice.s = symbolID;

      return v2HistoricalPrice;
    });

  const v2Collection = db.collection('historical-prices');

  // Delete existing if any to prevent duplication
  await v2Collection.deleteMany({});

  return await v2Collection.safeUpdateMany(v2HistoricalPrices, []);
}

////////////////////////////////////////////////////// Partition key migration

async function partitionKeyMigration() {
  const operations = [];

  // Deleting _p key in all data collections
  const dataCollections = [
    db.collection('companies'),
    db.collection('dividends'),
    db.collection('exchange-rates'),
    db.collection('historical-prices'),
    db.collection('previous-day-prices'),
    db.collection('quotes'),
    db.collection('splits'),
    db.collection('symbols'),
    db.collection('splits'),
  ];

  for (const collection of dataCollections) {
    const query = {};
    const update = { $unset: { "_p": "" } };
    const options = { "upsert": false };
    operations.push(
      collection.updateMany(query, update, options)
    );
  }

  // Renaming _p key to _ in all user collections
  const userCollections = [
    db.collection('settings'),
    db.collection('transactions'),
  ];

  for (const collection of userCollections) {
    const query = {};
    const update = { $rename: { _p: "_" } };
    const options = { "upsert": false };
    operations.push(
      collection.updateMany(query, update, options)
    );
  }

  return Promise.all(operations);
}
