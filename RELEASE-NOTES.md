
# 2022-06-XX | IEX calendar splits
 
- Release new server
- dt call-realm-function --environment sandbox-anton --function updateSplitsV2 --verbose
- dt check-splits --environment production
- Check future splits using sort operator: `{ e: -1 }`

# 2022-06-XX | FMP ETF support

- Release new server with disabled FMP symbols update
- dt backup --environment sandbox-anton --database fmp
- dt restore --environment sandbox-anton --database fmp --to-database fmp-tmp
- dt call-realm-function --environment sandbox-anton --function fmpUpdateSymbols --argument fmp-tmp --verbose
- dt call-realm-function --environment sandbox-anton --function fmpLoadMissingData --argument fmp-tmp --retry-on-error 'execution time limit exceeded'
- dt backup --environment sandbox-anton --database fmp-tmp
- dt restore --environment local --backup-source-environment sandbox-anton --database fmp-tmp
- Execute symbols migration
- dt backup --environment local --database fmp-tmp
- dt restore --environment sandbox-anton --backup-source-environment local --database fmp-tmp
- Check data count
- dt call-realm-function --environment sandbox-anton --function fmpLoadMissingData --argument fmp-tmp --verbose
- dt call-realm-function --environment sandbox-anton --function fmpUpdateSymbols --argument fmp-tmp --verbose
- dt call-realm-function --environment sandbox-anton --function fmpUpdateCompanies --argument fmp-tmp --verbose
- dt call-realm-function --environment sandbox-anton --function fmpUpdateDividends --argument fmp-tmp --verbose
- dt call-realm-function --environment sandbox-anton --function fmpUpdatePrices --argument fmp-tmp --verbose
- dt call-realm-function --environment sandbox-anton --function fmpUpdateQuotes --argument fmp-tmp --verbose
- dt call-realm-function --environment sandbox-anton --function fmpUpdateSplits --argument fmp-tmp --verbose
- Check data count
- dt backup --environment sandbox-anton --database fmp-tmp
- dt restore --environment sandbox-anton --database fmp-tmp --to-database fmp
- dt call-realm-function --environment sandbox-anton --function mergedUpdateSymbols --verbose
- dt call-realm-function --environment sandbox-anton --function checkTransactionsV2 --verbose
- Enable FMP symbols update
- Drop fmp-tmp database

# ######################################################################################################################

# 2022-06-11 | Merged symbols, getDataV2, IEX refid for splits and dedupe

- Check if there are conflicting transactions using `playground` function and not released code
- Release new server
- Execute `dt call-realm-function --environment sandbox-anton --function updateSymbolsV2 --verbose` and check using `dt check-symbols -e sandbox-anton`
- Execute `dt call-realm-function --environment sandbox-anton --function mergedUpdateSymbols --verbose` and check `merged.symbols` collection using `{ i: { $ne: null }, f: { $ne: null } }` and counts of other symbol collections `iexCount + fmpCount - doubleSourceCount = mergedCount`
- Execute `dt call-realm-function --environment sandbox-anton --function fmpUpdateSymbols --verbose` and check using find operator `{ c: null }`
- Execute `dt call-realm-function --environment sandbox-anton --function migrations --verbose` and check that all splits for enabled symbols are updated using find operator in the `divtracker-v2.splits` collection: `{ i: null }`
- Execute `dt call-realm-function --environment sandbox-anton --function fmpUpdateCompanies --verbose` and check that companies count is equal to symbols count in the `fmp` database
