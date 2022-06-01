
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

function test_Object_prototype_updateFrom_preserve_deleted() {
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
