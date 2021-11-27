
// updateExchangeRates.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/

/**
 * Fetches today's exchange rates and updates database.
 */
exports = async function() {
  context.functions.execute("utils");
  
  const exchangeRates = await fetchExchangeRates();
  const collection = db.collection("exchange-rates");
  
  // bulkWrite example
  const operations = [];
  for (const exchangeRate of exchangeRates) {
    const filter = { _id: exchangeRate._id };
    const update = { $set: exchangeRate };
    const updateOne = { filter: filter, update: update, upsert: true };
    const operation = { updateOne: updateOne };
    operations.push(operation);
  }
  
  return collection.bulkWrite(operations);

  console.log(`SUCCESS`);
};
