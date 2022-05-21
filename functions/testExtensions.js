
// testExtensions.js

exports = async function() {
  context.functions.execute("testUtils");
  const collection = db.collection("tmp");
  
  await test(collection, testKnownOldObjects);
  await test(collection, testUnknownOldObjects);
  await test(collection, testKnownOldObjectsWithoutUpdateDate);
};

//////////////////////////// CONSTANTS

const _object = { id1: "id1", id2: "id2", a: "a", b: "b" };

const _modifiedObject = Object.assign({}, _object);
_modifiedObject.a = "A";
_modifiedObject.c = "C";

const _newObject = { id1: "id11", id2: "id22", a: "a1", b: "b1" };

//////////////////////////// TESTS

async function test(collection, testFunction) {
  await collection.drop();
  await testFunction(collection)
}

async function testKnownOldObjects(collection) {
  await collection.insertOne(_object);
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
  const modifiedObject = await collection.findOne({ id1: object.id1, id2: object.id2 });

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
