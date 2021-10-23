
// login.js

exports = function(document) {
  context.functions.execute("utils");
  
  console.log(`Login request for a user with document: ${document.stringify()}`);

  const id = getValueAndQuitIfUndefined(document, "id");
  console.log(`Success login user with ID: ${id}`);

  return id;
};
