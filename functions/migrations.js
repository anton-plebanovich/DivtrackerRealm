
// migrations.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.count/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/
// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/
// https://docs.mongodb.com/manual/reference/operator/update/unset/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeOrderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.removeOne/
// https://docs.mongodb.com/manual/reference/method/Bulk.insert/

// V1 deprecation phase (maintenance) and V2 migration to the new partition key strategy
// - Deploy: Maintenance.
// - Wait 24 hours
// - Manual: Backup V1 and V2 data: `env=<ENV>; dtcheck backup --all --environment ${env} --database divtracker && dtcheck backup --all --environment ${env}  --database divtracker-v2`
// - Deploy: Maintenance. V1 functions and triggers (including migration) should be removed. V2 triggers are also temporary removed (disabling works as a pause).
// - Manual: Disable drafts
// - Manual: Disable sync
// - Manual: V1 -> V2 settings sync is broken for an array so we might want to check if data is not lost. Execute `checkV1AndV2Settings()` function in the playground.js for that.
// - Manual: Execute `loadMissingDataV2`
// - Manual: Check that V2 data is properly fetched, e.g. enough historical prices. Execute `checkV2Data()` function in the playground.js for that. If there are suspicious companies check their historical prices and then erase and refetch data if needed.
// - Manual: Drop `divtracker` database
// - Manual: Run partition key migration
// - Manual: Delete V1 schemes
// - Manual: Edit V2 schemes and make partition key optional while also renaming it to _. Recheck them once more.
// - Manual: Enable Automatic Deployment
// - Deploy: New schemes, functions, and sync that are using optional _ partition key. V1 schemes remove. Triggers restore.
// - Manual: Release new app (not deprecated) to stores
// - Deploy: Exchange maintenance with deprecation for old versions

exports = async function() {
  context.functions.execute("utilsV2");

  try {
    await partitionKeyMigration();
  } catch(error) {
    console.error(error);
  }
};

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
