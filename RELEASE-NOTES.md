
# 2022-06-XX | FMP ETF support

- Release new server with disabled FMP symbols update
- dt backup --environment sandbox --database fmp
- dt restore --environment sandbox --database fmp --to-database fmp-tmp
- dt call-realm-function --environment sandbox --function fmpUpdateSymbols --argument fmp-tmp --verbose
- dt call-realm-function --environment sandbox --function fmpLoadMissingData --argument fmp-tmp --retry-on-error 'execution time limit exceeded'
- dt backup --environment sandbox --database fmp-tmp
- dt restore --environment local --backup-source-environment sandbox-anton --database fmp-tmp
- Execute symbols migration
- dt backup --environment local --database fmp-tmp
- dt restore --environment sandbox --backup-source-environment local --database fmp-tmp
- Check data count
- dt call-realm-function --environment sandbox --function fmpLoadMissingData --argument fmp-tmp --verbose
- dt call-realm-function --environment sandbox --function fmpUpdateSymbols --argument fmp-tmp --verbose
- dt call-realm-function --environment sandbox --function fmpUpdateCompanies --argument fmp-tmp --verbose
- dt call-realm-function --environment sandbox --function fmpUpdateDividends --argument fmp-tmp --verbose
- dt call-realm-function --environment sandbox --function fmpUpdatePrices --argument fmp-tmp --verbose
- dt call-realm-function --environment sandbox --function fmpUpdateQuotes --argument fmp-tmp --verbose
- dt call-realm-function --environment sandbox --function fmpUpdateSplits --argument fmp-tmp --verbose
- Check data count
- dt backup --environment sandbox --database fmp-tmp
- dt restore --environment sandbox --database fmp-tmp --to-database fmp
- dt call-realm-function --environment sandbox --function mergedUpdateSymbols --verbose
- dt call-realm-function --environment sandbox --function checkTransactionsV2 --verbose
- Enable FMP symbols update

# 2022-06-XX | Merged symbols and getDataV2

- Check if there are conflicting transactions: `dt call-realm-function --environment sandbox --function playground --verbose`
- Release new server
- dt call-realm-function --environment sandbox --function mergedUpdateSymbols --verbose
- Push new app in the review and release
- Wait one week
- Deprecate old clients
- Wait one day
- Release more tickers

# 2022-06-XX | IEX calendar splits and dedupe

- Release new server
- dt check-splits --environment sandbox
- dt call-realm-function --environment sandbox --function migrations --verbose
- dt check-splits --environment sandbox
