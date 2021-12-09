
// playground.js

exports = async function() {
  context.functions.execute("utils");
  
  console.log(context.user.stringify());
};
