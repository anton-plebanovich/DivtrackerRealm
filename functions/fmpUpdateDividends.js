
// fmpUpdateDividends.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeUnorderedBulkOp/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write
// https://docs.mongodb.com/realm/mongodb/actions/collection.distinct/

function getLowerExDate(dividend) {
  const date = new Date(dividend.e);
  date.setDate(date.getDate() - 6);

  return date;
}

function getUpperExDate(dividend) {
  const date = new Date(dividend.e);
  date.setDate(date.getDate() + 6);

  return date;
}

exports = async function() {
  context.functions.execute("fmpUtils");
  const shortSymbols = await getShortSymbols();
  const collection = fmp.collection("dividends");

  if (shortSymbols.length <= 0) {
    console.log(`No symbols. Skipping update.`);
    return;
  }

  const existingDividendsBySymbolID = await collection
    .aggregate([{
      $group: {
        _id: '$s',
        dividends: {
          $push: {
            a: '$a',
            c: '$c',
            d: "$d",
            e: '$e',
            f: "$f",
            p: "$p",
            s: "$s",
          }
        }
      }
    }])
    .toArray()
    .then(x => 
      x.reduce((dictionary, value) => {
        return Object.assign(dictionary, { [value._id.toString()]: value.dividends });
      }, {})
    );

  // Future dividends update.
  // We remove symbols that already have future dividends and update only those that don't.
  const upToDateSymbolIDs = await collection.distinct("s", { "e" : { $gte : new Date() }});
  const shortSymbolsToUpdate = shortSymbols.filter(x => !upToDateSymbolIDs.includes(x.s));
  const tickers = shortSymbolsToUpdate.map(x => x.t);
  console.log(`Updating future dividends (${tickers.length}) for '${tickers.stringify()}' IDs`);

  let futureDividends = await fetchDividendsCalendar(shortSymbolsToUpdate);
  if (futureDividends.length) {
    console.log(`Fixing future dividends (${futureDividends.length})`);
    futureDividends = fixDividends(futureDividends, existingDividendsBySymbolID);

    console.log(`Inserting missed future dividends (${futureDividends.length})`);
    const bulk = collection.initializeUnorderedBulkOp();
    for (const futureDividend of futureDividends) {
      const lowerExDate = getLowerExDate(futureDividend);
      const upperExDate = getUpperExDate(futureDividend);
      console.logVerbose(`Checking future dividend '${futureDividend.s}' for '${futureDividend.e}' ex date; lower - ${lowerExDate}; upper - ${upperExDate}`);

      const updateOne = { $set: futureDividend, $currentDate: { u: true } };
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
    console.log(`Future dividends are empty for IDs '${tickers.stringify()}'`);
  }

  // Past dividends update. Just in case they weren't added for the future or were updated.
  console.log(`Fetching 1 last dividend for each symbol`);
  let pastDividends = await fetchDividends(shortSymbols, 1);
  if (pastDividends.length) {
    console.log(`Fixing past dividends (${pastDividends.length})`);
    pastDividends = fixDividends(pastDividends, existingDividendsBySymbolID);

    console.log(`Inserting missed past dividends (${pastDividends.length})`);
    const bulk = collection.initializeUnorderedBulkOp();
    for (const pastDividend of pastDividends) {
      const lowerExDate = getLowerExDate(pastDividend);
      const upperExDate = getUpperExDate(pastDividend);
      console.logVerbose(`Checking past dividend '${pastDividend.s}' for '${pastDividend.e}' ex date; lower - ${lowerExDate}; upper - ${upperExDate}`);
      
      const updateOne = { $set: pastDividend, $currentDate: { u: true } };
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
    console.log(`Past dividends are empty for IDs '${shortSymbols.map(x => x.t)}'`);
  }

  await setUpdateDate("dividends");
};

function fixDividends(dividends, existingDividendsBySymbolID) {
  const dividendsBySymbolID = dividends.toBuckets(x => x.s.toString());
  const fixedDividends = [];

  for (const [symbolID, dividends] of Object.entries(dividendsBySymbolID)) {
    const existingDividends = existingDividendsBySymbolID[symbolID];

    // Check if there is nothing to fix
    if (existingDividends == null) {
      console.error(`Missing existing dividends for: ${symbolID}`);
      fixedDividends.concat(dividends);
      continue;
    }

    // We remove new dividends from existing to allow update in case something was changed
    const deduplicatedExistingDividends = existingDividends
      .filter(existingDividend => 
        dividends.find(dividend => 
          existingDividend.a == dividend.a && existingDividend.e == dividend.e
        ) == null
      );

    // Frequency fix using all known dividends
    let _fixedDividends = deduplicatedExistingDividends
      .concat(dividends)
      .sort((l, r) => l.e - r.e);
    
    _fixedDividends = updateDividendsFrequency(_fixedDividends);
    _fixedDividends = removeDuplicateDividends(_fixedDividends);

    _fixedDividends = _fixedDividends
      .filter(fixedDividend => 
        existingDividends.find(dividend => 
          fixedDividend.a == dividend.a && 
          fixedDividend.d == dividend.d && 
          fixedDividend.e == dividend.e && 
          fixedDividend.f == dividend.f && 
          fixedDividend.p == dividend.p
        ) == null
      );

    // Set update date
    for (const fixedDividends of _fixedDividends) {
      fixedDividends.$currentDate = { u: true };
    }

    // Push result to others
    fixedDividends.push(..._fixedDividends);
  }

  return fixedDividends;
}
