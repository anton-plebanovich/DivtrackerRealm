
// playground.js

exports = async function() {
  context.functions.execute("utilsV2");
  
  console.log(context.user.stringify());
};
