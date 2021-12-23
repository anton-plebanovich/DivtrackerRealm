
// playground.js

exports = async function() {
  context.functions.execute("utilsV2");

  await checkV1AndV2Settings();
  await checkV2Data();
  
  console.log(context.user.stringify());
};

/**
 * Checks that V2 settings has all V1 records and partialy compares them to make sure they are equal.
 */
async function checkV1AndV2Settings() {
  const v1SettingsCollection = atlas.db("divtracker").collection('settings');
  const v2SettingsCollection = db.collection('settings');
  const v1Settings = await v1SettingsCollection.find().toArray();
  for (const v1Setting of v1Settings) {
    const v2Setting = await v2SettingsCollection.findOne({ _id: v1Setting._id });

    if (v2Setting == null) {
      console.error(`V2 settings do not exist for user '${v1Setting._p}'`);
      continue;
    }

    const v1SettingShort = Object.assign({}, v1Setting);
    if (v1SettingShort.ts != null) {
      v1SettingShort.ts = v1SettingShort.ts.length;
    }

    const v2SettingShort = Object.assign({}, v2Setting);
    if (v2SettingShort.ts != null) {
      v2SettingShort.ts = v2SettingShort.ts.length;
    }

    if (v2SettingShort.isEqual(v1SettingShort)) {
      console.logVerbose(`V2 and V1 settings are equal for user '${v1Setting._p}'`);
    } else {
      console.error(`V2 and V1 settings are not equal for user '${v1Setting._p}'`);
      console.error(`V1: '${v1Setting.stringify()}'`);
      console.error(`V2: '${v2Setting.stringify()}'`);
    }
  }
}

/**
 * Checks that historical prices have more than 2 records. The first one might be inserted by the prices update.
 */
async function checkV2Data() {
  const historicalPricesCollection = db.collection('historical-prices');
  const aggregation = [{
    $group: {
      _id: '$s',
      count: {
        $sum: 1
      }
    }
  }, {
    $match: {
      count: {
        $lte: 2
      }
    }
  }, {
    $sort: {
      count: 1
    }
  }];
  const records = await historicalPricesCollection.aggregate(aggregation).toArray();

  const symbolsCollection = db.collection('symbols');
  if (records.length > 0) {
    const tickers = [];
    for (const record of records) {
      const symbolID = record._id;
      const symbol = await symbolsCollection.findOne({ _id: symbolID });
      tickers.push(`${symbol.t} - ${symbolID}`);
    }
    console.error(`Found suspicious tickers (${tickers.length}): ${tickers.stringify()}`);
  }
}
