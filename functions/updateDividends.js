
// updateDividends.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeUnorderedBulkOp/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write
// https://docs.mongodb.com/realm/mongodb/actions/collection.distinct/

function getLowerExDate(dividend) {
  const date = new Date(dividend.e);
  date.setDate(date.getDate() - 14);

  return date;
}

function getUpperExDate(dividend) {
  const date = new Date(dividend.e);
  date.setDate(date.getDate() + 14);

  return date;
}

/**
 * @note IEX update happens at 9am UTC
 */
exports = async function() {
  context.functions.execute("utils");
  const uniqueIDs = await getUniqueIDs();
  const collection = db.collection("dividends");

  if (uniqueIDs.length <= 0) {
    console.log(`No uniqueIDs. Skipping update.`);
    return;
  }

  // Future dividends update.
  // We remove symbols that already have future dividends and update only those that don't.
  const upToDateUniqueIDs = await collection.distinct("_i", { "e" : { $gte : new Date() }});
  const uniqueIDsToUpdate = [...uniqueIDs];
  uniqueIDsToUpdate.removeContentsOf(upToDateUniqueIDs);
  console.log(`Updating future dividends (${uniqueIDsToUpdate.length}) for '${uniqueIDsToUpdate}' IDs`);

  const futureDividends = await fetchDividends(uniqueIDsToUpdate, true);
  if (futureDividends.length) {
    const futureDividendUniqueIDs = futureDividends.map(x => x._i);
    console.log(`Inserting missed future dividends (${futureDividends.length}) for IDs '${futureDividendUniqueIDs}'`);
    const bulk = collection.initializeUnorderedBulkOp();

    const futureDividendsCount = futureDividends.length;
    for (let i = 0; i < futureDividendsCount; i += 1) {
      const futureDividend = futureDividends[i];
     
      const lowerExDate = getLowerExDate(futureDividend);
      const upperExDate = getUpperExDate(futureDividend);
      console.logVerbose(`Checking future dividend '${futureDividend._i}' for '${futureDividend.e}' ex date; lower - ${lowerExDate}; upper - ${upperExDate}`);

      const currency = typeof futureDividend.c === 'undefined' ? null : futureDividend.c;

      const lowerAmount = futureDividend.a * 0.9;
      const upperAmount = futureDividend.a * 1.1;

      // Amount might be 0 with empty string currency for declared dividends so we need to fix that
      bulk.find({ 
          _i: futureDividend._i,
          // Ex date might change a little bit.
          e: { $gte: lowerExDate, $lte: upperExDate }, 
          $and: [
            // We might have different frequency dividends on same or close date.
            { f: futureDividend.f },
            // Currency might be "" for future dividends.
            { $or: [{ c: currency }, { c: "" }] },
            // Amount might be 0.0 for future dividends.
            // GSK, TTE. Amount might change a little bit.
            { $or: [{ a: { $gte: lowerAmount, $lte: upperAmount } }, { a: 0.0 }] }
          ]
        })
        .upsert()
        .updateOne({ $set: futureDividend });
    }

    await bulk.execute();
    console.log(`Inserted missed future dividends for IDs '${futureDividendUniqueIDs}'`);

  } else {
    console.log(`Future dividends are empty for IDs '${uniqueIDsToUpdate}'`);
  }

  // Past dividends update. Just in case they weren't added for the future or were updated.
  const days = 3;
  console.log(`Fetching past dividends for the last ${days} days`);
  const daysParam = `${days}d`;
  const pastDividends = await fetchDividends(uniqueIDs, false, daysParam);
  if (pastDividends.length) {
    const pastDividendUniqueIDs = pastDividends.map(x => x._i);
    console.log(`Inserting missed past dividends (${pastDividends.length}) for IDs '${pastDividendUniqueIDs}'`);

    const bulk = collection.initializeUnorderedBulkOp();

    const pastDividendsCount = pastDividends.length;
    for (let i = 0; i < pastDividendsCount; i += 1) {
      const pastDividend = pastDividends[i];
      
      const lowerExDate = getLowerExDate(pastDividend);
      const upperExDate = getUpperExDate(pastDividend);
      console.logVerbose(`Checking past dividend '${pastDividend._i}' for '${pastDividend.e}' ex date; lower - ${lowerExDate}; upper - ${upperExDate}`);
      
      const currency = typeof pastDividend.c === 'undefined' ? null : pastDividend.c;

      const lowerAmount = pastDividend.a * 0.9;
      const upperAmount = pastDividend.a * 1.1;

      // Amount might be 0 with empty string currency for declared dividends so we need to fix that
      bulk.find({ 
          _i: pastDividend._i,
          // Ex date might change a little bit.
          e: { $gte: lowerExDate, $lte: upperExDate }, 
          $and: [
            // We might have different frequency dividends on same or close date.
            { f: pastDividend.f },
            // Currency might be "" for future dividends.
            { $or: [{ c: currency }, { c: "" }] },
            // Amount might be 0.0 for future dividends.
            // GSK, TTE. Amount might change a little bit.
            { $or: [{ a: { $gte: lowerAmount, $lte: upperAmount } }, { a: 0.0 }] }
          ]
        })
        .upsert()
        .updateOne({ $set: pastDividend });
    }

    await bulk.execute();
    console.log(`Inserted missed past dividends for IDs '${pastDividendUniqueIDs}'`);

    console.log(`SUCCESS`);

  } else {
    console.log(`Past dividends are empty for IDs '${uniqueIDs}'`);
  }
};
