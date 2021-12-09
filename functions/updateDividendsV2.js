
// updateDividendsV2.js

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
  context.functions.execute("utilsV2");
  const shortSymbols = await getInUseShortSymbols();
  const collection = db.collection("dividends");

  if (shortSymbols.length <= 0) {
    console.log(`No symbols. Skipping update.`);
    return;
  }

  // Future dividends update.
  // We remove symbols that already have future dividends and update only those that don't.
  const upToDateSymbolIDs = await collection.distinct("s", { "e" : { $gte : new Date() }});
  const shortSymbolsToUpdate = shortSymbols.filter(x => !upToDateSymbolIDs.includes(x.s));
  console.log(`Updating future dividends (${shortSymbolsToUpdate.length}) for '${shortSymbolsToUpdate}' IDs`);

  const futureDividends = await fetchDividends(shortSymbolsToUpdate, true);
  if (futureDividends.length) {
    console.log(`Inserting missed future dividends (${futureDividends.length})`);
    const bulk = collection.initializeUnorderedBulkOp();
    for (const futureDividend of futureDividends) {
      const lowerExDate = getLowerExDate(futureDividend);
      const upperExDate = getUpperExDate(futureDividend);
      console.logVerbose(`Checking future dividend '${futureDividend.s}' for '${futureDividend.e}' ex date; lower - ${lowerExDate}; upper - ${upperExDate}`);

      const currency = futureDividend.c == null ? null : futureDividend.c;
      let updateOne;
      if (currency == null) {
        // We might have empty string for future dividends and we need to unset it if 
        // the currency was changed to USD and so the field is absent.
        updateOne = { $set: futureDividend, $unset: { c: "" } };

      } else {
        updateOne = { $set: futureDividend };
      }

      const lowerAmount = futureDividend.a * 0.9;
      const upperAmount = futureDividend.a * 1.1;

      // Amount might be 0 with empty string currency for declared dividends so we need to fix that
      bulk.find({ 
          s: futureDividend.s,
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
        .updateOne(updateOne);
    }

    await bulk.execute();
    console.log(`Inserted missed future dividends`);

  } else {
    console.log(`Future dividends are empty for IDs '${shortSymbolsToUpdate}'`);
  }

  // Past dividends update. Just in case they weren't added for the future or were updated.
  const days = 3;
  console.log(`Fetching past dividends for the last ${days} days`);
  const daysParam = `${days}d`;
  const pastDividends = await fetchDividends(shortSymbols, false, daysParam);
  if (pastDividends.length) {
    console.log(`Inserting missed past dividends (${pastDividends.length})`);
    const bulk = collection.initializeUnorderedBulkOp();
    for (const pastDividend of pastDividends) {
      const lowerExDate = getLowerExDate(pastDividend);
      const upperExDate = getUpperExDate(pastDividend);
      console.logVerbose(`Checking past dividend '${pastDividend.s}' for '${pastDividend.e}' ex date; lower - ${lowerExDate}; upper - ${upperExDate}`);
      
      const currency = pastDividend.c == null ? null : pastDividend.c;
      let updateOne;
      if (currency == null) {
        // We might have empty string for future dividends and we need to unset it if 
        // the currency was changed to USD and so the field is absent.
        updateOne = { $set: pastDividend, $unset: { c: "" } };

      } else {
        updateOne = { $set: pastDividend };
      }

      const lowerAmount = pastDividend.a * 0.9;
      const upperAmount = pastDividend.a * 1.1;

      // Amount might be 0 with empty string currency for declared dividends so we need to fix that
      bulk.find({ 
          s: pastDividend.s,
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
        .updateOne(updateOne);
    }

    await bulk.execute();
    console.log(`Inserted missed past dividends`);

    console.log(`SUCCESS`);

  } else {
    console.log(`Past dividends are empty for IDs '${shortSymbols}'`);
  }
};
