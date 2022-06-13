
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

exports = async function(database) {
  context.functions.execute("fmpUtils", database);
  database = getFMPDatabaseName(database);

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
            x: "$x",
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
      bulk.find({ 
          a: futureDividend.a,
          s: futureDividend.s,
          e: { $gte: lowerExDate, $lte: upperExDate }
        })
        .upsert()
        .updateOne(updateOne);
    }

    await bulk.safeExecute();
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
      bulk.find({ 
          a: pastDividend.a,
          s: pastDividend.s,
          e: { $gte: lowerExDate, $lte: upperExDate }
        })
        .upsert()
        .updateOne(updateOne);
    }

    await bulk.safeExecute();
    console.log(`Inserted missed past dividends`);

    console.log(`SUCCESS`);

  } else {
    console.log(`Past dividends are empty for IDs '${shortSymbols.map(x => x.t)}'`);
  }

  await setUpdateDate(`${database}-dividends`);
};

function fixDividends(dividends, existingDividendsBySymbolID) {
  const dividendsBySymbolID = dividends.toBuckets(x => x.s.toString());
  const fixedDividends = [];

  for (const [symbolID, dividends] of Object.entries(dividendsBySymbolID)) {
    const existingDividends = existingDividendsBySymbolID[symbolID];

    // Check if there is nothing to fix
    if (existingDividends == null) {
      // There were no dividends but we have them now. 
      // It's hard to say if that's the first record or the whole set was added so asking to fix manually.
      // dt data-status -e <ENV> -d fmp -c dividends --id <ID1> --id <ID2> && dt call-realm-function -e <ENV> -f fmpLoadMissingData --verbose
      console.error(`Missing existing dividends for: ${symbolID}. It's better to load missing dividends data for this.`);
      continue;
    }

    // We remove new dividends from existing to allow update in case something was changed.
    // We leave deleted dividends.
    const deduplicatedExistingDividends = [];
    for (const existingDividend of existingDividends) {
      const matchedDividendIndex = dividends.findIndex(dividend => 
        existingDividend.a == dividend.a && compareOptionalDates(existingDividend.e, dividend.e)
      );
      
      if (matchedDividendIndex === -1) {
        // No match, add existing if not deleted
        if (existingDividend.x != true) {
          deduplicatedExistingDividends.push(existingDividend);
        }

      } else if (existingDividend.x == true) {
        // Deleted dividend match, exclude from new
        dividends.splice(matchedDividendIndex, 1);

      } else {
        // Match, will be added later
      }
    }

    // Frequency fix using all known dividends
    let _fixedDividends = deduplicatedExistingDividends
      .concat(dividends)
      .sorted((l, r) => l.e - r.e);
    
    _fixedDividends = removeDuplicatedDividends(_fixedDividends);
    _fixedDividends = updateDividendsFrequency(_fixedDividends);

    _fixedDividends = _fixedDividends
      .filter(fixedDividend => 
        existingDividends.find(dividend => 
          fixedDividend.a == dividend.a && 
          fixedDividend.f == dividend.f && 
          compareOptionalDates(fixedDividend.d, dividend.d) && 
          compareOptionalDates(fixedDividend.e, dividend.e) && 
          compareOptionalDates(fixedDividend.p, dividend.p)
        ) == null
      );

    // Push result to others
    fixedDividends.push(..._fixedDividends);
  }

  return fixedDividends;
}