
// syncV1toV2Settings.js

// https://docs.mongodb.com/manual/reference/method/Bulk.find.upsert/
// https://docs.mongodb.com/realm/mongodb/actions/collection.bulkWrite/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/

exports = async function(changeEvent) {
  context.functions.execute("utilsV2");

  const settings = changeEvent.fullDocument;
  const customTaxes = settings.ts;
  if (customTaxes == null || !customTaxes.length) { return; }

  const settingsV2 = Object.assign({}, settings);
  settingsV2.ts = [];

  if (customTaxes != null) {
    const symbolsCollection = db.collection("symbols");
    for (const customTax in customTaxes) {
      const symbol = await symbolsCollection.findOne({ t: customTax.s });
      const symbolID = symbol._id;
      settingsV2.ts.push({ s: symbolID, t: customTax.t });
    }
  }
  
  return await db.collection("settings").safeUpdateMany([settings]);
};
