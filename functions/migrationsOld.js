
// migrationsOld.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/
// https://docs.mongodb.com/manual/reference/operator/update/unset/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.removeOne/
// https://docs.mongodb.com/manual/reference/method/Bulk.insert/

exports = function() {};

////////////////////////////////// 03-12-2021

// IEX exchanges were renamed so we need to fix our data
async function exchangesFix_03122021() {
  await exchangesFix_id_Field_03122021('companies');
  await exchangesFix_i_Field_03122021('dividends');
  await exchangesFix_i_Field_03122021('historical-prices');
  await exchangesFix_id_Field_03122021('previous-day-prices');
  await exchangesFix_id_Field_03122021('quotes');
  await exchangesFixSettings_03122021();
  await exchangesFix_i_Field_03122021('splits');
  await exchangesFixTransactions_03122021();
}

async function exchangesFix_id_Field_03122021(collectionName) {
  const db = context.services.get("mongodb-atlas").db("divtracker");
  const collection = db.collection(collectionName);
  return await collection
    .find({})
    .toArray()
    .then(async entities => {
      const entitiesToMigrate = entities.filter(entity => {
        return isMigrationRequiredForID_03122021(entity._id);
      });

      if (!entitiesToMigrate.length) { return; }

      const bulk = collection.initializeUnorderedBulkOp();
      for (const entity of entitiesToMigrate) {
        const fixedID = fixExchangeNameInID_03122021(entity._id);
        const fixedEntity = Object.assign({}, entity);
        fixedEntity._id = fixedID;

        // Insert and replace new record if needed
        bulk.find({ _id: fixedID })
          .upsert()
          .replaceOne(fixedEntity);

        // Remove old
        bulk.find({ _id: entity._id })
          .removeOne();
      }
      return bulk.execute();
    });
}

async function exchangesFix_i_Field_03122021(collectionName) {
  const db = context.services.get("mongodb-atlas").db("divtracker");
  const collection = db.collection(collectionName);
  return await collection
    .find({}, { _id: 1, _i: 1 })
    .toArray()
    .then(async entities => {
      const entitiesToMigrate = entities.filter(entity => {
        return isMigrationRequiredForID_03122021(entity._i);
      });

      if (!entitiesToMigrate.length) { return; }

      if (entitiesToMigrate.length !== entities.length) {
        const migratedEntitiesIs = entities
          .filter(entity => {
            return !isMigrationRequiredForID_03122021(entity._i);
          })
          .map(x => x._i)
          .distinct();

        throw `Found conflict for '_i' entities for '${collectionName}' collection. Migrated entities: ${migratedEntitiesIs.stringify()}`
      }
      
      const bulk = collection.initializeUnorderedBulkOp();
      for (const entity of entitiesToMigrate) {
        const fixedID = fixExchangeNameInID_03122021(entity._i);
        bulk
          .find({ _id: entity._id })
          .updateOne({ $set: { _i: fixedID }});
      }
      return bulk.execute();
    });
}

async function exchangesFixSettings_03122021() {
  const db = context.services.get("mongodb-atlas").db("divtracker");
  const collection = db.collection('settings');
  return await collection
    .find({})
    .toArray()
    .then(async settings => {
      const settingsToMigrate = settings.filter(userSettings => {
        if (typeof userSettings.ts === 'undefined') {
          return false;
        } else if (typeof userSettings.ts[0] === 'undefined') {
          return false;
        } else {
          return isMigrationRequiredForID_03122021(userSettings.ts[0].i);
        }
      });

      if (!settingsToMigrate.length) { return; }
      
      const bulk = collection.initializeUnorderedBulkOp();
      for (const userSettings of settingsToMigrate) {
        for (const taxSettings of userSettings.ts) {
          const fixedExchange = fixExchangeNameInID_03122021(taxSettings.i);
          taxSettings.i = fixedExchange;
        }
        bulk
          .find({ _id: userSettings._id })
          .updateOne({ $set: userSettings });
      }
      return bulk.execute();
    });
}

async function exchangesFixTransactions_03122021() {
  const db = context.services.get("mongodb-atlas").db("divtracker");
  const collection = db.collection('transactions');
  return await collection
    .find({}, { _id: 1, e: 1 })
    .toArray()
    .then(async transactions => {
      const transactionsToMigrate = transactions.filter(transaction => {
        return isMigrationRequiredForExchange_03122021(transaction.e);
      });

      if (!transactionsToMigrate.length) { return; }
      
      const bulk = collection.initializeUnorderedBulkOp();
      for (const transaction of transactionsToMigrate) {
        const fixedExchange = fixExchangeName_03122021(transaction.e);
        bulk
          .find({ _id: transaction._id })
          .updateOne({ $set: { e: fixedExchange }});
      }
      return bulk.execute();
    });
}

function fixExchangeNameInID_03122021(_id) {
  const components = _id.split(':');
  const id = components[0];
  const exchange = components[1];
  const fixedExchange = fixExchangeName_03122021(exchange);
  return `${id}:${fixedExchange}`;
}

/**
 * NAS -> XNAS
 * NYS -> XNYS
 * POR -> XPOR
 * USAMEX -> XASE
 * USBATS -> BATS
 * USPAC -> ARCX
 */
function fixExchangeName_03122021(exchange) {
  if (exchange === 'NAS') {
    return 'XNAS';

  } else if (exchange === 'NYS') {
    return 'XNYS';

  } else if (exchange === 'POR') {
    return 'XPOR';

  } else if (exchange === 'USAMEX') {
    return 'XASE';

  } else if (exchange === 'USBATS') {
    return 'BATS';

  } else if (exchange === 'USPAC') {
    return 'ARCX';

  } else {
    console.error(`Already migrated exchange '${exchange}'?`);
    return exchange;
  }
}

function isMigrationRequiredForID_03122021(_id) {
  const exchange = _id.split(':')[1];
  return isMigrationRequiredForExchange_03122021(exchange);
}

function isMigrationRequiredForExchange_03122021(exchange) {
  if (exchange === 'NAS') {
    return true;

  } else if (exchange === 'NYS') {
    return true;

  } else if (exchange === 'POR') {
    return true;

  } else if (exchange === 'USAMEX') {
    return true;

  } else if (exchange === 'USBATS') {
    return true;

  } else if (exchange === 'USPAC') {
    return true;

  } else {
    return false;
  }
}

////////////////////////////////// 24-11-2021

// Remove `e` and `s` fields in the symbols collection
async function removeExcessiveFieldsInSymbols_24112021() {
  const db = context.services.get("mongodb-atlas").db("divtracker");
  const symbolsCollection = db.collection('symbols');

  const query = {};
  const update = { $unset: { e: "", s: "" } };
  const options = { "upsert": false };
  return symbolsCollection.updateMany(query, update, options);
}

// Truncates dividend frequency value to one letter
async function truncateDividendsFrequency_24112021() {
  const db = context.services.get("mongodb-atlas").db("divtracker");
  const dividendsCollection = db.collection('dividends');

  // Produce truncated dividends that will be used for bulk update
  // { _id: ObjectId("619d8187e3cabb8625968a99"), f: "q" }
  const project = { "f": { $substrBytes: [ "$f", 0, 1 ] } };
  const aggregation = [{ $project: project }];
  const dividends = await dividendsCollection
    .aggregate(aggregation)
    .toArray();

  // Find by `_id` and update `f`
  const operations = [];
  for (const dividend of dividends) {
    const filter = { _id: dividend._id };
    const update = { $set: { f: dividend.f } };
    const updateOne = { filter: filter, update: update, upsert: false };
    const operation = { updateOne: updateOne };
    operations.push(operation);
  }
  
  return dividendsCollection.bulkWrite(operations);
}

////////////////////////////////// 15-04-2021

// Add `exchange` field to all transactions
function executeTransactionsMigration_15042021() {
  const db = context.services.get("mongodb-atlas").db("divtracker");
  const transactionsCollection = db.collection('transactions');
  const symbolsCollection = db.collection('symbols');
  transactionsCollection
    .find({}, { symbol: 1 })
    .toArray()
    .then(async transactions => {
      const transactionsCount = transactions.length;
  
      if (transactionsCount) {
        const bulk = transactionsCollection.initializeUnorderedBulkOp();
        for (let i = 0; i < transactionsCount; i += 1) {
          const transaction = transactions[i];
          const symbol = await symbolsCollection
            .findOne({ symbol: transaction.symbol }, { exchange: 1 });
    
          if (!symbol) {
            console.error(`Skipping migration for '${transaction.symbol}' symbol`);
            continue;
          }
          
          const exchange = symbol.exchange;
          console.log(`Updating transaction '${transaction._id}' (${transaction.symbol}) with exchange: '${exchange}'`);
          bulk
            .find({ _id: transaction._id })
            .updateOne({ $set: { exchange: exchange }});
        }
        bulk.execute();
      }
    });
}
