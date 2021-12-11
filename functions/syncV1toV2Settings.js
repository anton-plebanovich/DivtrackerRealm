
// syncV1toV2Settings.js

// https://docs.mongodb.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/

exports = async function(changeEvent) {
  context.functions.execute("utilsV2");

  console.log(`Fixing settings for change event: ${changeEvent.stringify()}`)

  const v1Settings = changeEvent.fullDocument;
  const customTaxes = v1Settings.ts;
  const v2Settings = Object.assign({}, v1Settings);

  if (customTaxes != null && customTaxes.length) {
    v2Settings.ts = [];
    const symbolsCollection = db.collection("symbols");
    for (const customTax in customTaxes) {
      const symbol = await symbolsCollection.findOne({ t: customTax.s });
      const symbolID = symbol._id;
      v2Settings.ts.push({ s: symbolID, t: customTax.t });
    }
  }
  
  return await db.collection("settings").safeUpdateMany([v2Settings]);
};
