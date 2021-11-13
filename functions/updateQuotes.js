
// quotesUpdate.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeUnorderedBulkOp/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write
// https://docs.mongodb.com/realm/mongodb/actions/collection.find/

/**
 * @note IEX update happens at 4:30am-8pm ET Mon-Fri
 */
exports = async function() {
  context.functions.execute("utils");
  const uniqueIDs = await getUniqueIDs();

  if (uniqueIDs.length <= 0) {
    console.log(`No uniqueIDs. Skipping update.`);
    return;
  }

  const quotes = await fetchQuotes(uniqueIDs);
  if (quotes.length) {
    console.log(`Updating changed`);

    const quotesCollection = db.collection("quotes");
    const existingQuotes = await quotesCollection
      .find()
      .toArray();

    const bulk = quotesCollection.initializeUnorderedBulkOp();
    var hasChanges = false;

    const count = quotes.length;
    for (let i = 0; i < count; i += 1) {
      const quote = quotes[i];
      const existingQuote = existingQuotes.find(x => x._id == quote._id);
      if (existingQuote) {
        if (quote.isEqual(existingQuote)) {
          console.logVerbose(`Skipping up to date quote: ${quote._id}`);
        } else {
          console.logVerbose(`Updating quote: ${quote._id}`);
          bulk.find({ _id: existingQuote._id }).replaceOne(quote);
          hasChanges = true;
        }

      } else {
        // No existing quote. Just insert.
        console.log(`Inserting quote: ${quote._id}`);
        bulk.insert(quote);
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
    console.error(`Quotes are empty for symbols '${uniqueIDs}'`);
  }
};