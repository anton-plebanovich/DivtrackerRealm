
// testAddTransactionsMultipleDuplicated.js

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

  try {
    await test();
  } catch(error) {
    console.log(error);
    throw error;
  }
};

async function test() {
  // Cleanup environment
  await cleanup();

  // Prepare async operations
  const operations = [];
  const transactions = await generateRandomTransactions(defaultAsyncTransactionsCount);
  let transactionsToCheck = [];
  for (let i = 0; i < defaultAsyncOperations; i++) {
    transactionsToCheck = transactionsToCheck.concat(transactions);
    operations.push(context.functions.execute("addTransactionsV2", transactions));
  }
  
  // Execute async operations
  await Promise.all(operations);

  // Check
  await checkData(transactionsToCheck);
}
