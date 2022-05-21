
// testExtensions.js

exports = async function() {
  setEnvironment();

  context.functions.execute("testUtils");
  const collection = db.collection("tmp");
  
  await test(collection, testKnownOldObjects);
  await test(collection, testUnknownOldObjects);
  await test(collection, testKnownOldObjectsWithoutUpdateDate);
};

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

async function testKnownOldObjects(collection) {
  await collection.insertOne(_deletedObject1);
  await collection.insertOne(_object);
  await collection.insertOne(_deletedObject2);
  await collection.safeUpdateMany([_modifiedObject, _newObject], [_object], ['id1', 'id2'], true);
  await checkObjects(collection, true);
}

async function testUnknownOldObjects(collection) {
  await collection.insertOne(_object);
  await collection.safeUpdateMany([_modifiedObject, _newObject], undefined, ['id1', 'id2'], true);
  await checkObjects(collection, true);
}

async function testKnownOldObjectsWithoutUpdateDate(collection) {
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
