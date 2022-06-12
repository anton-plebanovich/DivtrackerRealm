
# 2022-06-XX | Old format date, IEX calendar splits, and refid for dividends
 
- Release new server
- Execute `dt call-realm-function --environment sandbox-anton --function migrations --arg old_date_format_migration --verbose`
- Check `divtracker-v2.dividends` collection using sort operator: `{ e: 1 }`. There should be no `12` hour dates.
- Check `divtracker-v2.splits` collection using sort operator: `{ e: 1 }`. There should be no `12` hour dates.
- Execute `dt call-realm-function --environment sandbox-anton --function migrations --arg fetch_refid_for_IEX_splits --verbose`
- Check that all splits are updated using find operator in the `divtracker-v2.splits` collection: `{ i: null }`
- Execute `dt call-realm-function --environment sandbox-anton --function migrations --arg fetch_refid_for_IEX_dividends --verbose`
- Check that all future dividends for symbols are updated using find operator in the `divtracker-v2.dividends` collection: `{ i: null, e: { $gte: new Date() } }`, `{ i: null, s: { $in: [ObjectId('61c42676a2660ba02db39480'), ObjectId('61c42676a2660ba02db3afb2')] } }`
- Execute `dt call-realm-function --environment sandbox-anton --function updateSplitsV2 --verbose`
- Execute `dt call-realm-function --environment sandbox-anton --function updateDividendsFuture --verbose`
- Execute `dt call-realm-function --environment sandbox-anton --function updateDividendsPast --verbose`
- Execute `dt check-splits --environment sandbox-anton`
- Check future splits using sort operator: `{ e: -1 }`

# 2022-06-XX | FMP ETF support

- Disable all FMP triggers
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
- Enable all previously disabled triggers

# ######################################################################################################################

# 2022-06-11 | Merged symbols, getDataV2, IEX refid for splits and dedupe

- Check if there are conflicting transactions using `playground` function and not released code
- Release new server
- Execute `dt call-realm-function --environment sandbox-anton --function updateSymbolsV2 --verbose` and check using `dt check-symbols -e sandbox-anton`
- Execute `dt call-realm-function --environment sandbox-anton --function mergedUpdateSymbols --verbose` and check `merged.symbols` collection using `{ i: { $ne: null }, f: { $ne: null } }` and counts of other symbol collections `iexCount + fmpCount - doubleSourceCount = mergedCount`
- Execute `dt call-realm-function --environment sandbox-anton --function fmpUpdateSymbols --verbose` and check using find operator `{ c: null }`
- Execute `dt call-realm-function --environment sandbox-anton --function checkTransactionsV2 --verbose`
- Execute `dt call-realm-function --environment sandbox-anton --function migrations --verbose` and check that all splits are updated using find operator in the `divtracker-v2.splits` collection: `{ i: null }`
- Execute `dt call-realm-function --environment sandbox-anton --function fmpUpdateCompanies --verbose` and check that companies count is equal to symbols count in the `fmp` database
