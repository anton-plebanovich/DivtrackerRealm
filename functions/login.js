
// login.js

exports = function(document) {
  context.functions.execute("utilsV2");
  
  console.log(`Login request for a user with document: ${document.stringify()}`);

  const id = getValueAndThrowfUndefined(document, "id");
  console.log(`Success login user with ID: ${id}`);

  return id;
};
