
// eraseData.js

exports = function() {
  context.functions.execute("utilsV2");
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
  
  // db.collection('transactions').deleteMany( { } );
  
  /// Delete using date
  // db.collection('historical-prices').deleteMany( { d: { $gt: new Date("2021-10-02") } } );
  
  console.log(`SUCCESS`);
};
