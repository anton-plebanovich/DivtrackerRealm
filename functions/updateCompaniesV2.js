
// updateCompaniesV2.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeUnorderedBulkOp/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write
// https://docs.mongodb.com/realm/mongodb/actions/collection.find/

/**
 * @note IEX update happens at 4am and 5am UTC every day
 */
exports = async function() {
  context.functions.execute("utilsV2");
  const shortSymbols = await getInUseShortSymbols();

  if (shortSymbols.length <= 0) {
    console.log(`No symbols. Skipping update.`);
    return;
  }

  const companiesCollection = db.collection("companies");
  const companies = await fetchCompanies(shortSymbols);
  const existingCompanies = await companiesCollection.find().toArray();
  await companiesCollection.safeUpdateMany(companies, existingCompanies)

  console.log(`SUCCESS`);
};
