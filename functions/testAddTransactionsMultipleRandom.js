
// testAddTransactionsMultipleRandom.js

function randomDate(start, end) {
  return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
}

/**
 * @example
   context.user.id = '61ae5154d9b3cb9ea55ec5c6';
   exports();
 */
 exports = async function() {
  context.functions.execute("testUtils");

  // Cleanup environment
  await cleanup();

  // Prepare async operations
  const symbols = await db.collection('symbols').find({ e: null }).toArray();
  const operations = [];
  let transactionsToCheck = [];
  for (let i = 0; i < defaultAsyncOperations; i++) {
    const transactions = await generateRandomTransactions(50, symbols);
    transactionsToCheck = transactionsToCheck.concat(transactions);
    operations.push(context.functions.execute("addTransactionsV2", transactions));
  }
  
  // Execute async operations
  await Promise.all(operations);

  // Check
  return await checkData(transactions);
};
