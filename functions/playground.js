
// playground.js

exports = async function() {
  context.functions.execute("fmpUtils");
  const collection = fmp.collection("tmp");
  
  console.log(context.user.stringify());
};
