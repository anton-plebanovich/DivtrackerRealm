
// companiesUpdate.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeUnorderedBulkOp/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write
// https://docs.mongodb.com/realm/mongodb/actions/collection.find/

exports = async function() {
  context.functions.execute("utils");
  const uniqueIDs = await getUniqueIDs();

  if (uniqueIDs.length <= 0) {
    console.log(`No uniqueIDs. Skipping update.`);
    return;
  }

  const companiesCollection = db.collection("companies");
  const companies = await fetchCompanies(uniqueIDs);
  if (companies.length) {
    console.log(`Updating changed`);

    const existingCompanies = await companiesCollection
      .find()
      .toArray();

    const bulk = companiesCollection.initializeUnorderedBulkOp();
    var hasChanges = false;

    const companiesCount = companies.length;
    for (let i = 0; i < companiesCount; i += 1) {
      const company = companies[i];
      const existingCompany = existingCompanies.find(x => x._id == company._id);
      if (existingCompany) {
        if (company.isEqual(existingCompany)) {
          console.logVerbose(`Skipping up to date company: ${company._id}`);
        } else {
          console.log(`Updating company: ${company._id}`);
          bulk.find({ _id: existingCompany._id }).replaceOne(company);
          hasChanges = true;
        }

      } else {
        // No existing company. Just insert.
        console.log(`Inserting company: ${company._id}`);
        bulk.insert(company);
        hasChanges = true;
      }
    }

    if (hasChanges) {
      bulk.execute();
    } else {
      console.log(`Nothing to update`);
    }

    console.log(`SUCCESS`);

  } else {
    console.error(`Companies are empty for IDs '${uniqueIDs}'`);
  }
};
