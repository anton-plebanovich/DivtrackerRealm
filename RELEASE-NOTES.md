
# 2022-06-XX | FMP ETF support

- Release new server with disabled FMP symbols update
- dt backup --environment tests --database fmp
- dt restore --environment tests --database fmp --to-database fmp-tmp
- dt call-realm-function --environment tests --function fmpUpdateSymbols --argument fmp-tmp --verbose
- dt call-realm-function --environment tests --function fmpLoadMissingData --argument fmp-tmp --retry-on-error 'execution time limit exceeded'
- dt backup --environment tests --database fmp-tmp
- dt restore --environment local --backup-source-environment sandbox-anton --database fmp-tmp
- Execute symbols migration
- dt backup --environment local --database fmp-tmp
- dt restore --environment tests --backup-source-environment local --database fmp-tmp
- Check data count
- dt call-realm-function --environment tests --function fmpLoadMissingData --argument fmp-tmp --verbose
- dt call-realm-function --environment tests --function fmpUpdateSymbols --argument fmp-tmp --verbose
- dt call-realm-function --environment tests --function fmpUpdateCompanies --argument fmp-tmp --verbose
- dt call-realm-function --environment tests --function fmpUpdateDividends --argument fmp-tmp --verbose
- dt call-realm-function --environment tests --function fmpUpdatePrices --argument fmp-tmp --verbose
- dt call-realm-function --environment tests --function fmpUpdateQuotes --argument fmp-tmp --verbose
- dt call-realm-function --environment tests --function fmpUpdateSplits --argument fmp-tmp --verbose
- Check data count
- dt backup --environment tests --database fmp-tmp
- dt restore --environment tests --database fmp-tmp --to-database fmp
- dt call-realm-function --environment tests --function mergedUpdateSymbols --verbose
- dt call-realm-function --environment tests --function checkTransactionsV2 --verbose
- Enable FMP symbols update

# 2022-06-XX | getDataV2

- Release new server
- dt call-realm-function --environment tests --function mergedUpdateSymbols --verbose
- Release new client
- Deprecate old clients
- Release more tickers

# 2022-06-XX | IEX calendar splits and dedupe

- Release new server
- dt check-splits --environment tests
- dt call-realm-function --environment tests --function migrations --verbose
- dt check-splits --environment tests
