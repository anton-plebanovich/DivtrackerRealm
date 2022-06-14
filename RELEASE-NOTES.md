
# 2022-06-XX | Old format date, IEX calendar splits, and refid for dividends
 
- Release new server
- Execute `dt call-realm-function --environment sandbox-anton --function migrations --argument old_data_delete_migration --verbose`
- Check `divtracker-v2.dividends`, `divtracker-v2.splits`, `fmp.dividends`, `fmp.splits` collections using find operator: `{ e: { $lt: ISODate('2016-01-01') }, x: { $ne: true } }`. There should be no records.
- Check `divtracker-v2.historical-prices` and `fmp.historical-prices` collections using find operator: `{ d: { $lt: ISODate('2016-01-01') }, x: { $ne: true } }`. There should be no records.
- Execute `dt call-realm-function --environment sandbox-anton --function migrations --argument old_date_format_splits_migration --verbose`
- Check `divtracker-v2.splits` collection using find operator: `{ $expr: { $eq: [{ $hour: "$e" }, 12] } }`. There should be no records.
- Execute `dt call-realm-function --environment sandbox-anton --function migrations --argument old_date_format_dividends_migration --verbose`
- Check `divtracker-v2.dividends` collection using find operator: `{ $expr: { $eq: [{ $hour: "$e" }, 12] } }`. There should be no records.
- Execute `dt call-realm-function --environment sandbox-anton --function migrations --argument fetch_refid_for_IEX_splits --verbose`
- Check that most splits are updated _except 4 known_ using find operator in the `divtracker-v2.splits` collection: `{ i: null }`
- Execute `dt call-realm-function --environment sandbox-anton --function migrations --argument fetch_refid_for_IEX_dividends --verbose`
- Check that all future dividends are updated using find operator in the `divtracker-v2.dividends` collection: `{ i: null, e: { $gte: new Date() } }`
- Check that 1 last past dividends are updated using find operator in the `divtracker-v2.dividends` collection: `{ i: { $ne: null }, e: { $lt: new Date() } }`
- Check that all specific tickers dividends are updated using find operator in the `divtracker-v2.dividends` collection: `{ i: null, s: { $in: [ObjectId('61c42676a2660ba02db39480'), ObjectId('61c42676a2660ba02db3afb2')] } }`
- Execute `dt call-realm-function --environment sandbox-anton --function migrations --argument delete_duplicated_FMP_dividends --verbose`
- Check that duplicated dividend is deleted and frequencies are fixed in the `fmp.dividends` collection using find operator: `{ s: ObjectId('624ca7e44fd65a51c3060213') }` and sort: `{ e: -1 }`
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
