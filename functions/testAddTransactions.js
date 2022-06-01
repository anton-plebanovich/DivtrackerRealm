
// testAddTransactions.js

/**
 * @example exports();
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

  const transactions = await generateRandomTransactions();
  await context.functions.execute("addTransactionsV2", transactions);

  await checkData(transactions);
}
