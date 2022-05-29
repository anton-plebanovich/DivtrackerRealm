
// migrations.js

// https://docs.mongodb.com/realm/mongodb/actions/collection.count/
// https://docs.mongodb.com/realm/mongodb/actions/collection.insertMany/
// https://docs.mongodb.com/realm/mongodb/actions/collection.updateMany/
// https://docs.mongodb.com/manual/reference/operator/update/unset/
// https://docs.mongodb.com/manual/reference/method/db.collection.initializeOrderedBulkOp/
// https://docs.mongodb.com/manual/reference/method/Bulk.find.removeOne/
// https://docs.mongodb.com/manual/reference/method/Bulk.insert/

exports = async function() {
  context.functions.execute("utils");

  // Release new server with disabled FMP symbols update
  // dt backup --environment sandbox-anton --database fmp
  // dt restore --environment sandbox-anton --database fmp --to-database fmp-tmp
  // dt call-realm-function --environment sandbox-anton --function fmpUpdateSymbols --argument fmp-tmp --verbose
  // dt call-realm-function --environment sandbox-anton --function fmpLoadMissingData --argument fmp-tmp --retry-on-error 'execution time limit exceeded'
  // dt backup --environment sandbox-anton --database fmp-tmp
  // dt restore --environment local --backup-source-environment sandbox-anton --database fmp-tmp
  // Execute symbols migration
  // dt backup --environment local --database fmp-tmp
  // dt restore --environment sandbox-anton --backup-source-environment local --database fmp-tmp
  // Check data count
  // dt call-realm-function --environment sandbox-anton --function fmpLoadMissingData --argument fmp-tmp --verbose
  // dt call-realm-function --environment sandbox-anton --function fmpUpdateSymbols --argument fmp-tmp --verbose
  // dt call-realm-function --environment sandbox-anton --function fmpUpdateCompanies --argument fmp-tmp --verbose
  // dt call-realm-function --environment sandbox-anton --function fmpUpdateDividends --argument fmp-tmp --verbose
  // dt call-realm-function --environment sandbox-anton --function fmpUpdatePrices --argument fmp-tmp --verbose
  // dt call-realm-function --environment sandbox-anton --function fmpUpdateQuotes --argument fmp-tmp --verbose
  // dt call-realm-function --environment sandbox-anton --function fmpUpdateSplits --argument fmp-tmp --verbose
  // Check data count
  // dt backup --environment sandbox-anton --database fmp-tmp
  // dt restore --environment sandbox-anton --database fmp-tmp --to-database fmp
  // dt call-realm-function --environment sandbox-anton --function checkTransactionsV2 --verbose
  // Enable FMP symbols update
};
