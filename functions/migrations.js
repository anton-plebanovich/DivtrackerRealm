
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
  return Promise.all([
    fillV2CompanyCollectionMigration(v2Symbols),
  ]);
}

async function fillV2CompanyCollectionMigration(v2Symbols) {
  const v1Collection = atlas.db("divtracker").collection('companies');
  const v1Companies = await v1Collection.find().toArray();
  // V1: {"_id":"AAPL:NAS","_p":"P","n":"Apple Inc","s":"ap u MftrtEniiclorCrecmuc oaengtnu","t":"cs"}
  // V2: {"_id":{"$oid":"61b102c0048b84e9c13e4564"},"_p":"2","i":"uric nrro uaetnMotuial etnCcgcmfpE","n":"Apple Inc","t":"cs"}
  const v2Companies = v1Companies
    // AAPL:XNAS -> AAPL
    .map(v1Company => { 
      v1Company._id = v1Company._id.split(':')[0];
      return v1Company;
    })
    // Filter duplicates if any
    .distinct('_id')
    .map(v1Company => {
      const ticker = v1Company._id;
      const symbolID = v2Symbols.find(x => x.t === ticker)._id;
      if (symbolID == null) {
        throw `Unknown V1 ticker '${ticker}' for company ${v1Company.stringify()}`;
      }

      const v2Company = {};
      v2Company._id = symbolID;
      v2Company._p = "2";
      v2Company.n = v1Company.n;
      v2Company.i = v1Company.s;
      v2Company.t = v1Company.t;

      return v2Company;
    });

  const v2Collection = db.collection('companies');

  // Delete existing if any to prevent duplication
  await v2Collection.deleteMany({});

  return await v2Collection.insertMany(v2Companies);
}

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
