
// eraseData.js

exports = function() {
  context.functions.execute("utils");
  console.log(`Erasing data`);
  
  // db.collection('symbols').deleteMany( { } );
  
  // db.collection('companies').deleteMany( { } );
  // db.collection('dividends').deleteMany( { } );
  // db.collection('historical-prices').deleteMany( { } );
  // db.collection('previous-day-prices').deleteMany( { } );
  // db.collection('splits').deleteMany( { } );
  
  // db.collection('transactions').deleteMany( { } );
  
  // const symbol = "AAP:NYS";
  // db.collection('companies').deleteMany( { _id: symbol } );
  // db.collection('dividends').deleteMany( { _i: symbol } );
  // db.collection('historical-prices').deleteMany( { _i: symbol } );
  // db.collection('previous-day-prices').deleteMany( { _id: symbol } );
  // db.collection('splits').deleteMany( { _i: symbol } );
  
  // const symbols = ["ACRE:NYS","AMD:NAS","GIWWW:NAS","W:NYS","STOR:NYS","OZK:NAS","BABA:NYS","BMO:NYS","Z:NAS","IAA:NYS","CCI:NYS","BDX:NYS","RNWWW:NAS"];
  // db.collection('companies').deleteMany( { _id: { $in: symbols } } );
  // db.collection('dividends').deleteMany( { _i: { $in: symbols } } );
  // db.collection('historical-prices').deleteMany( { _i: { $in: symbols } } );
  // db.collection('previous-day-prices').deleteMany( { _id: { $in: symbols } } );
  // db.collection('splits').deleteMany( { _i: { $in: symbols } } );
  
  // db.collection('historical-prices').deleteMany( { d: { $gt: new Date("2021-10-02") } } );
  
  console.log(`SUCCESS`);
};
