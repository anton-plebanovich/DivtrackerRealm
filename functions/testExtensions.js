
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
    test_FMP_duplicate_dividends_remove();
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

function test_FMP_duplicate_dividends_remove() {
  // https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/WTCM.ME?apikey=969387165d69a8607f9726e8bb52b901
  const dividends = EJSON.parse('[{"date":"2022-05-03","label":"May 03, 22","adjDividend":0.562248,"dividend":0.562248,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2022-04-29","label":"April 29, 22","adjDividend":0.562248,"dividend":0.562248,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2021-04-27","label":"April 27, 21","adjDividend":0.562248,"dividend":0.562248,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2020-05-05","label":"May 05, 20","adjDividend":0.562248,"dividend":0.5622489959,"recordDate":"2020-05-06","paymentDate":"2020-06-01","declarationDate":"2020-03-19"},{"date":"2019-05-07","label":"May 07, 19","adjDividend":0.562248,"dividend":0.5622489959,"recordDate":"2019-05-08","paymentDate":"2019-07-01","declarationDate":"2019-03-25"},{"date":"2018-05-03","label":"May 03, 18","adjDividend":0.963855,"dividend":0.963855,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2018-05-02","label":"May 02, 18","adjDividend":0.481927,"recordDate":"","paymentDate":"","declarationDate":""},{"date":"2017-07-04","label":"July 04, 17","adjDividend":0.32658,"dividend":0.32658,"recordDate":"2017-07-05","paymentDate":"2017-09-01","declarationDate":"2017-05-25"},{"date":"2016-04-20","label":"April 20, 16","adjDividend":0.32658,"dividend":0.32658,"recordDate":"2016-04-21","paymentDate":"2016-07-01","declarationDate":"2016-03-18"}]');
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
   */
  const originalLength = dividends.length; // 9
  const fixedDividends = fixFMPDividends(dividends, new BSON.ObjectId('624ca7e44fd65a51c3060213'));
  const fixedLength = fixedDividends.length; // 8
  const duplicatesCount = originalLength - fixedLength;
  if (duplicatesCount !== 1) {
    throw `[test_FMP_duplicate_dividends_remove] Unexpected duplicated dividends count: ${duplicatesCount}`;
  }

  // Ascending sort
  const latestDividend = fixedDividends[fixedDividends.length - 1];
  if (latestDividend.f !== 'a') {
    throw `[test_FMP_duplicate_dividends_remove] Unexpected latest dividend frequency: ${latestDividend.f}`;
  }
  if (latestDividend.e.getTime() !== new Date('2022-05-03T06:50:00.000+00:00').getTime()) {
    throw `[test_FMP_duplicate_dividends_remove] Unexpected latest dividend ex date: ${latestDividend.e}`;
  }

  const beforeLatestDividend = fixedDividends[fixedDividends.length - 2];
  if (beforeLatestDividend.f !== 'a') {
    throw `[test_FMP_duplicate_dividends_remove] Unexpected before latest dividend frequency: ${beforeLatestDividend.f}`;
  }
  if (beforeLatestDividend.e.getTime() !== new Date('2021-04-27T06:50:00.000+00:00').getTime()) {
    throw `[test_FMP_duplicate_dividends_remove] Unexpected before latest dividend ex date: ${beforeLatestDividend.e}`;
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
