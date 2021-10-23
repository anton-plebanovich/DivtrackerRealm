
exports = async function(){
  context.functions.execute("utils");
  
  console.log(context.user.stringify())
  // validateTransactions([{ _id: BSON.ObjectId("607afe635ed931675acfff1c"), _p: "614b283c15a0dc11514db030", s: "AAPL", e: "NAS", a: 1.0, d: new Date(), p: 1.0 }]);
};
