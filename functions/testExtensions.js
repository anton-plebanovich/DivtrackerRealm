
// testExtensions.js

exports = async function() {
  setEnvironment();

  context.functions.execute("testUtils");
  const collection = db.collection("tmp");

  try {
    test_Object_prototype_updateFrom_preserve_deleted();
    test_Object_prototype_updateFrom_set_deleted();
    test_Array_prototype_sortedDeletedToTheStart();
    test_Array_prototype_toDictionary_keys_array();
    test_FMP_dividends_fix();
    await test(collection, test_singleKey_known_old_objects);
    await test(collection, test_singleKey_unknown_old_objects);
    await test(collection, test_singleKey_known_old_objects_without_update_date);
    await test(collection, test_multipleKeys_known_old_objects);
    await test(collection, test_multipleKeys_unknown_old_objects);
    await test(collection, test_multipleKeys_known_old_objects_without_update_date);
  } catch(error) {
    console.log(error);
    throw error;
  }
};

//////////////////////////// LOCAL

function test_Object_prototype_updateFrom_preserve_deleted() {
  const object = { a: 1 };
  const oldObject = { x: true, a: 2 };
  const update = object.updateFrom(oldObject, true);
  if (update.$currentDate.u != true) {
    throw `[test_Object_prototype_updateFrom] update date is not set`;
  }
  if (update.$set.a !== 1) {
    throw `[test_Object_prototype_updateFrom] 'a' is not updated`;
  }
  if (update.$unset != null) {
    throw `[test_Object_prototype_updateFrom] excessive unset operation`;
  }
}

function test_Object_prototype_updateFrom_set_deleted() {
  const object = { a: 2, x: true };
  const oldObject = { a: 2 };
  const update = object.updateFrom(oldObject, true);
  if (update.$currentDate.u != true) {
    throw `[test_Object_prototype_updateFrom] update date is not set`;
  }
  if (update.$set.x != true) {
    throw `[test_Object_prototype_updateFrom] 'x' is not updated`;
  }

  const setLength = Object.keys(update.$set).length;
  if (setLength !== 1) {
    throw `[test_Object_prototype_updateFrom] excessive set operations: ${setLength}`;
  }

  if (update.$unset != null) {
    throw `[test_Object_prototype_updateFrom] excessive unset operation`;
  }
}

function test_Array_prototype_sortedDeletedToTheStart() {
  const array = [{}, { x: true }, { x: false }, {}];
  const resultArray = array.sortedDeletedToTheStart();
  if (resultArray[0].x != true) {
    throw `[test_Array_Prototype_sortedDeletedToTheStart] deleted object was not sorted to the start`;
  }
}

function test_Array_prototype_toDictionary_keys_array() {
  const array = [
    {a: "a1", b: "b1"},
    {a: "a1", b: "b2"},
    {a: "a2", b: "b2"}
  ]
  
  const keys = ["a", "b"];
  const dic = array.toDictionary(["a", "b"]);
  if (dic.stringify() !== '{"a1":{"b1":{"a":"a1","b":"b1"},"b2":{"a":"a1","b":"b2"}},"a2":{"b2":{"a":"a2","b":"b2"}}}') {
    throw `[test_Array_prototype_toDictionary_keys_array] unexpected result`;
  }
}

function test_FMP_dividends_fix() {
  let dividends;
  let fixedDividends;

  // https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/WTCM.ME?apikey=969387165d69a8607f9726e8bb52b901
  /**
  Duplicates:
  {
    "date" : "2022-05-03",
    "dividend" : 0.562248,
  }, {
    "date" : "2022-04-29",
    "dividend" : 0.562248,
  }, {
    "date" : "2021-04-27",
    "dividend" : 0.562248,
  }

  Irregular:
  {
    "date" : "2018-05-03",
    "dividend" : 0.963855,
  }, {
    "date" : "2018-05-02",
    "adjDividend" : 0.481927,
  }
   */
  dividends = EJSON.parse('[{"date":"2022-05-03","label":"May 03, 22","adjDividend":0.562248,"dividend":0.562248,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2022-04-29","label":"April 29, 22","adjDividend":0.562248,"dividend":0.562248,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2021-04-27","label":"April 27, 21","adjDividend":0.562248,"dividend":0.562248,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2020-05-05","label":"May 05, 20","adjDividend":0.562248,"dividend":0.5622489959,"recordDate":"2020-05-06","paymentDate":"2020-06-01","declarationDate":"2020-03-19"},{"date":"2019-05-07","label":"May 07, 19","adjDividend":0.562248,"dividend":0.5622489959,"recordDate":"2019-05-08","paymentDate":"2019-07-01","declarationDate":"2019-03-25"},{"date":"2018-05-03","label":"May 03, 18","adjDividend":0.963855,"dividend":0.963855,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2018-05-02","label":"May 02, 18","adjDividend":0.481927,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2017-07-04","label":"July 04, 17","adjDividend":0.32658,"dividend":0.32658,"recordDate":"2017-07-05","paymentDate":"2017-09-01","declarationDate":"2017-05-25"},{"date":"2016-04-20","label":"April 20, 16","adjDividend":0.32658,"dividend":0.32658,"recordDate":"2016-04-21","paymentDate":"2016-07-01","declarationDate":"2016-03-18"}]');
  fixedDividends = fixFMPDividends(dividends, new BSON.ObjectId('624ca7e44fd65a51c3060213'));
  check_dividends_length('test_FMP_dividends_fix.WTCM.ME.dividends', fixedDividends, 8);
  check_dividend_frequency('test_FMP_dividends_fix.WTCM.ME.duplicate', fixedDividends[7], 'a');
  check_dividend_ex_date('test_FMP_dividends_fix.WTCM.ME.duplicate', fixedDividends[7], '2022-05-03');
  check_dividend_frequency('test_FMP_dividends_fix.WTCM.ME.before_duplicate', fixedDividends[6], 'a');
  check_dividend_ex_date('test_FMP_dividends_fix.WTCM.ME.before_duplicate', fixedDividends[6], '2021-04-27');
  check_dividend_frequency('test_FMP_dividends_fix.WTCM.ME.irregular', fixedDividends[3], 'i');
  check_dividend_ex_date('test_FMP_dividends_fix.WTCM.ME.irregular', fixedDividends[3], '2018-05-03');
  check_dividend_frequency('test_FMP_dividends_fix.WTCM.ME.before_irregular', fixedDividends[2], 'a');
  check_dividend_frequency('test_FMP_dividends_fix.WTCM.ME.after_irregular', fixedDividends[4], 'a');

  // https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/TDEYX?apikey=969387165d69a8607f9726e8bb52b901
  dividends = EJSON.parse('[{"date":"2021-12-09","label":"December 09, 21","adjDividend":0,"dividend":0,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2020-12-29","label":"December 29, 20","adjDividend":0.185,"dividend":0.185,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2020-10-02","label":"October 02, 20","adjDividend":0.001,"dividend":0.001,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2020-09-30","label":"September 30, 20","adjDividend":0.197,"dividend":0.197,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2019-12-27","label":"December 27, 19","adjDividend":0.076,"dividend":0.076,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2018-12-27","label":"December 27, 18","adjDividend":0.152,"dividend":0.152,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2017-12-27","label":"December 27, 17","adjDividend":0.303,"dividend":0.303,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2016-12-28","label":"December 28, 16","adjDividend":0.094,"dividend":0.094,"recordDate":"","paymentDate":"","declarationDate":""}]');
  fixedDividends = fixFMPDividends(dividends, new BSON.ObjectId('628915115422930228d3c3df'));
  check_dividend_frequency('test_FMP_dividends_fix.TDEYX.annual', fixedDividends[3], 'a');
  check_dividend_frequency('test_FMP_dividends_fix.TDEYX.semi_annual_1', fixedDividends[4], 's');
  check_dividend_frequency('test_FMP_dividends_fix.TDEYX.irregular', fixedDividends[5], 'i');
  check_dividend_frequency('test_FMP_dividends_fix.TDEYX.semi_annual_2', fixedDividends[6], 's');
  check_dividend_frequency('test_FMP_dividends_fix.TDEYX.unspecified', fixedDividends[7], 'u');

  // https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/OTT.L?apikey=969387165d69a8607f9726e8bb52b901
  dividends = EJSON.parse('[{"date":"2021-09-30","label":"September 30, 21","adjDividend":6,"dividend":6,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2017-06-29","label":"June 29, 17","adjDividend":4,"dividend":0.04,"recordDate":"2017-06-30","paymentDate":"2017-07-21","declarationDate":"2017-05-10"},{"date":"2016-04-28","label":"April 28, 16","adjDividend":0.15,"dividend":0.15,"recordDate":"2016-04-29","paymentDate":"2016-05-13","declarationDate":"2016-04-21"},{"date":"2016-01-21","label":"January 21, 16","adjDividend":0.07,"dividend":0.07,"recordDate":"2016-01-22","paymentDate":"2016-02-19","declarationDate":"2016-01-12"}]');
  fixedDividends = fixFMPDividends(dividends, new BSON.ObjectId('628915115422930228d3c414'));
  check_dividend_frequency('test_FMP_dividends_fix.OTT.L.0', fixedDividends[0], 'q');
  check_dividend_frequency('test_FMP_dividends_fix.OTT.L.1', fixedDividends[1], 'q');
  check_dividend_frequency('test_FMP_dividends_fix.OTT.L.2', fixedDividends[2], 'a');
  check_dividend_frequency('test_FMP_dividends_fix.OTT.L.3', fixedDividends[3], 'u');

  // https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/VGHAX?apikey=969387165d69a8607f9726e8bb52b901
  dividends = ('[{"date":"2022-03-28","label":"March 28, 22","adjDividend":0.063,"dividend":0.063,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2021-12-29","label":"December 29, 21","adjDividend":0.712,"dividend":0.712,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2021-03-26","label":"March 26, 21","adjDividend":1.406,"dividend":1.406,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2020-12-18","label":"December 18, 20","adjDividend":0.796,"dividend":5.189,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2020-03-27","label":"March 27, 20","adjDividend":0.053,"dividend":2.11,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2019-12-17","label":"December 17, 19","adjDividend":6.67,"dividend":6.67,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2019-03-29","label":"March 29, 19","adjDividend":3.75,"dividend":3.75,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2018-12-18","label":"December 18, 18","adjDividend":5.251,"dividend":5.251,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2018-03-29","label":"March 29, 18","adjDividend":2.171,"dividend":2.171,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2017-12-20","label":"December 20, 17","adjDividend":4.021,"dividend":4.021,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2017-03-28","label":"March 28, 17","adjDividend":2.363,"dividend":2.363,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2016-12-23","label":"December 23, 16","adjDividend":4.653,"dividend":4.653,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2016-03-16","label":"March 16, 16","adjDividend":2.049,"dividend":2.049,"recordDate":"","paymentDate":"","declarationDate":""}]');
  fixedDividends = fixFMPDividends(dividends, new BSON.ObjectId('628915115422930228d3c3ec'));
  fixedDividends.forEach((dividend, i) => check_dividend_frequency(`test_FMP_dividends_fix.VGHAX.${i}`, dividend, 's'));
}

function check_dividends_length(testName, dividends, length) {
  if (dividends.length !== length) {
    throw `[${testName}] Dividends length '${dividends.length}' expected to be equal to '${length}'`;
  }
}

function check_dividend_frequency(testName, dividend, frequency) {
  if (dividend.f !== frequency) {
    throw `[${testName}] Dividend frequency '${dividend.f}' expected to be equal to '${frequency}'`;
  }
}

function check_dividend_ex_date(testName, dividend, date) {
  if (dividend.e.getTime() !== new Date(`${date}T06:50:00.000+00:00`)) {
    throw `[${testName}] Dividend ex date '${dividend.e.dayString()}' expected to be equal to '${date}'`;
  }
}

//////////////////////////// ENVIRONMENT

function setEnvironment() {
  _object = { _id: BSON.ObjectId(), id1: "id1", id2: "id2", a: "a", b: "b" };
  _deletedObject1 = { _id: BSON.ObjectId(), id1: "id1", id2: "id2", a: "a", b: "b", x: true };
  _deletedObject2 = { _id: BSON.ObjectId(), id1: "id1", id2: "id2", a: "a", b: "b", x: true };
  _modifiedObject = { id1: "id1", id2: "id2", a: "A", c: "C" };
  _newObject = { _id: BSON.ObjectId(), id1: "id11", id2: "id22", a: "a1", b: "b1" };
}

//////////////////////////// TESTS

async function test(collection, testFunction) {
  await collection.deleteMany({});
  await testFunction(collection);
}

async function test_singleKey_known_old_objects(collection, keys) {
  await collection.insertOne(_deletedObject1);
  await collection.insertOne(_object);
  await collection.insertOne(_deletedObject2);
  await collection.safeUpdateMany([_modifiedObject, _newObject], [_object], 'id1', true);
  await checkObjects(collection, true);
}

async function test_singleKey_unknown_old_objects(collection) {
  await collection.insertOne(_object);
  await collection.safeUpdateMany([_modifiedObject, _newObject], undefined, 'id1', true);
  await checkObjects(collection, true);
}

async function test_singleKey_known_old_objects_without_update_date(collection) {
  await collection.insertOne(_object);
  await collection.safeUpdateMany([_modifiedObject, _newObject], undefined, 'id1', false);
  await checkObjects(collection, false);
}

async function test_multipleKeys_known_old_objects(collection) {
  await collection.insertOne(_deletedObject1);
  await collection.insertOne(_object);
  await collection.insertOne(_deletedObject2);
  await collection.safeUpdateMany([_modifiedObject, _newObject], [_object], ['id1', 'id2'], true);
  await checkObjects(collection, true);
}

async function test_multipleKeys_unknown_old_objects(collection) {
  await collection.insertOne(_object);
  await collection.safeUpdateMany([_modifiedObject, _newObject], undefined, ['id1', 'id2'], true);
  await checkObjects(collection, true);
}

async function test_multipleKeys_known_old_objects_without_update_date(collection) {
  await collection.insertOne(_object);
  await collection.safeUpdateMany([_modifiedObject, _newObject], undefined, ['id1', 'id2'], false);
  await checkObjects(collection, false);
}

//////////////////////////// Helpers

async function checkObjects(collection, setUpdateDate) {
  await checkObject(collection, _modifiedObject, setUpdateDate && true);
  await checkObject(collection, _newObject, setUpdateDate && false);
}

async function checkObject(collection, object, updated) {
  const modifiedObject = await collection.findOne({ id1: object.id1, id2: object.id2, x: { $ne: true } });

  if (updated) {
    if (modifiedObject.u == null) {
      throw `Modified object doesn't contain 'u' field`;
    } else {
      delete modifiedObject.u;
    }
  }

  if (!modifiedObject.isEqual(modifiedObject)) {
    throw `Modified object does match`;
  }
}
