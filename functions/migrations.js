
// migrations.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/
// https://docs.mongodb.com/manual/reference/operator/update/unset/

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

exports = async function() {
  context.functions.execute("utils");

  await removeExcessiveFieldsInSymbols_24112021();
  await truncateDividendsFrequency_24112021();
};
