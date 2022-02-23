
// eraseData.js

exports = function() {
  context.functions.execute("utils");
  console.log(`Erasing data`);
  
  /// Delete ALL
  // db.collection('exchange-rates').deleteMany( { } );
  // db.collection('symbols').deleteMany( { } );
  
  // db.collection('companies').deleteMany( { } );
  // db.collection('dividends').deleteMany( { } );
  // db.collection('historical-prices').deleteMany( { } );
  // db.collection('previous-day-prices').deleteMany( { } );
  // db.collection('splits').deleteMany( { } );
  // db.collection('quotes').deleteMany( { } );
  
  // db.collection('settings').deleteMany( { } );
  // db.collection('transactions').deleteMany( { } );

  /// Delete for symbol
  // const symbol = new BSON.ObjectId("61c42676a2660ba02db3bb68");
  // db.collection('companies').deleteMany( { _id: symbol } );
  // db.collection('dividends').deleteMany( { s: symbol } );
  // db.collection('historical-prices').deleteMany( { s: symbol } );
  // db.collection('previous-day-prices').deleteMany( { _id: symbol } );
  // db.collection('quotes').deleteMany( { _id: symbol } );
  // db.collection('splits').deleteMany( { s: symbol } );
  
  /// Delete for symbols array
  // const symbols = [new BSON.ObjectId("61c42676a2660ba02db3bb68"), new BSON.ObjectId("61c42676a2660ba02db3ac55")];
  // db.collection('companies').deleteMany( { _id: { $in: symbols } } );
  // db.collection('dividends').deleteMany( { s: { $in: symbols } } );
  // db.collection('historical-prices').deleteMany( { s: { $in: symbols } } );
  // db.collection('previous-day-prices').deleteMany( { _id: { $in: symbols } } );
  // db.collection('quotes').deleteMany( { _id: { $in: symbols } } );
  // db.collection('splits').deleteMany( { s: { $in: symbols } } );
  
  /// Delete using date
  // db.collection('historical-prices').deleteMany( { d: { $gt: new Date("2021-10-02") } } );
  
  console.log(`SUCCESS`);
};
