
// updateSymbols.js

// https://docs.mongodb.com/manual/reference/method/js-collection/
// https://docs.mongodb.com/manual/reference/method/js-bulk/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeUnorderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.update/
// https://docs.mongodb.com/realm/mongodb/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/#std-label-mongodb-service-collection-bulk-write

/**
 * @note IEX update happens at 8am, 9am, 12pm, 1pm UTC
 */
exports = async function() {
  context.functions.execute("utils");

  // https://sandbox.iexapis.com/stable/ref-data/symbols?token=Tpk_581685f711114d9f9ab06d77506fdd49
  const symbols = await fetch("/ref-data/symbols");
  
  console.log(`Data fix start`);
  symbols
    .filterNull()
    .filter(x => x.isEnabled)
    .forEach(function(symbol, index) {
      this[index] = {};
      this[index]._id = `${symbol.symbol}:${symbol.exchange}`;
      this[index]._p = "P";
      this[index].n = symbol.name;
    }, symbols);
  
  const collection = context.services.get("mongodb-atlas").db("divtracker").collection("symbols");

  const count = await collection.count({});
  if (count == 0) {
    console.log(`No data. Just inserting all records.`);
    await collection.insertMany(symbols);

  } else {
    const bulk = collection.initializeUnorderedBulkOp();

    const allIDs = symbols.map(x => x._id);

    // We do not delete old symbols but instead mark them as disabled
    console.log(`Disable excessive`);
    bulk.find({ "_id": { $nin: allIDs } })
      .update({ $set: { d: true } });
    
    console.log(`Inserting not inserted and update changed`);
    await collection.find({})
      .toArray()
      .then(existingSymbols => {
        if (!existingSymbols.length) { return; }

        const existingSymbolIDs = existingSymbols.map(x => x._id);
        const symbolsCount = symbols.length;
        for (let i = 0; i < symbolsCount; i += 1) {
          const symbol = symbols[i];
          if (existingSymbolIDs.includes(symbol._id)) {
            const index = existingSymbolIDs.indexOf(symbol._id);
            if (!existingSymbols[index].isEqual(symbol)) {
              console.log(`Updating: ${symbol._id}`);
              bulk.find({ _id: symbol._id })
                .updateOne({ 
                  $set: symbol, 
                  // If symbol appeared again we might need to remove `disabled` field
                  $unset: { d: "" } 
                });
            }

          } else {
            console.log(`Inserting: ${symbol._id}`);
            bulk.insert(symbol);
          }
        }
      });
    
    await bulk.execute();
  }

  const disabledSymbols = await collection
    .distinct('_id', { "d": true });

  if (disabledSymbols.length) {
    console.log(`Disabled symbols: ${disabledSymbols}`);
  }
  
  console.log(`SUCCESS`);
};
