
// eraseData.js

exports = function() {
  context.functions.execute("utils");
  console.log(`Erasing data`);
  
  /// Delete ALL
  // db.collection('symbols').deleteMany( { } );
  //
  // db.collection('companies').deleteMany( { } );
  // db.collection('dividends').deleteMany( { } );
  // db.collection('historical-prices').deleteMany( { } );
  // db.collection('previous-day-prices').deleteMany( { } );
  // db.collection('splits').deleteMany( { } );
  // db.collection('quotes').deleteMany( { } );
  //
  // db.collection('transactions').deleteMany( { } );
  
  /// Delete for symbol
  // const symbol = "AAP:NYS";
  // db.collection('companies').deleteMany( { _id: symbol } );
  // db.collection('dividends').deleteMany( { _i: symbol } );
  // db.collection('historical-prices').deleteMany( { _i: symbol } );
  // db.collection('previous-day-prices').deleteMany( { _id: symbol } );
  // db.collection('splits').deleteMany( { _i: symbol } );
  // db.collection('quotes').deleteMany( { _i: symbol } );
  
  /// Delete for symbols array
  // const symbols = ["ACRE:NYS","AMD:NAS","GIWWW:NAS","W:NYS","STOR:NYS","OZK:NAS","BABA:NYS","BMO:NYS","Z:NAS","IAA:NYS","CCI:NYS","BDX:NYS","RNWWW:NAS"];
  // db.collection('companies').deleteMany( { _id: { $in: symbols } } );
  // db.collection('dividends').deleteMany( { _i: { $in: symbols } } );
  // db.collection('historical-prices').deleteMany( { _i: { $in: symbols } } );
  // db.collection('previous-day-prices').deleteMany( { _id: { $in: symbols } } );
  // db.collection('splits').deleteMany( { _i: { $in: symbols } } );
  // db.collection('quotes').deleteMany( { _i: { $in: symbols } } );
  
  /// Delete using date
  // db.collection('historical-prices').deleteMany( { d: { $gt: new Date("2021-10-02") } } );

  /// Delete using regex
  // const regex = "^[A-Z]*$";
  // db.collection('companies').deleteMany( { _id: { $regex: regex } } );
  // db.collection('dividends').deleteMany( { _i: { $regex: regex } } );
  // db.collection('historical-prices').deleteMany( { _i: { $regex: regex } } );
  // db.collection('previous-day-prices').deleteMany( { _id: { $regex: regex } } );
  // db.collection('splits').deleteMany( { _i: { $regex: regex } } );
  // db.collection('quotes').deleteMany( { _id: { $regex: regex } } );
  
  console.log(`SUCCESS`);
};
