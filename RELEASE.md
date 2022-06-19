
# Normal release flow

- All development is happening in the `sandbox-anton` environment. If new functionality is added we should write new automatic tests to cover it.
- After a feature is done and tested in the `sandbox-anton` environment, it is then deployed to the `tests` environment. Then, we check that automatic tests are passing using `dt test --notify` command.
- If automatic tests succeeded we deploy to the `sandbox` environment for manual testing by the team.
- Just before release to production we should check error logs from all our environments to make sure we didn't introduce any new bugs.
- If logs look good we review all changes made once again by merging `sandbox` changes to `stage` branch without commiting.
- If changes look good we deploy to the `stage` environment by pushing changes to the `origin`.
- After deployment is finished we should check migrations if any using `production` environment data. To replace `stage` data with `production` data we follow steps below:
  - Go to `Deployment` -> `Configuration` and tap `Disable Drafts`, confirm by tapping `Disable Drafts` in the popup
  - Go to `Device Sync` and tap `Terminate Sync` confirm by typing `Terminate sync` in the field and tapping `Terminate Sync`
  - Execute command: `dt backup --environment production --verbose && dt erase-environment -e stage -d divtracker-v2 -c transactions && dt restore --environment stage --backup-source-environment production --data-collections --yes --verbose`
  - Enable sync back using default parameters. Use `{"%%partition":{"%in":["%%user.id",null]}}` for `Read Permissions` and `{"%%partition":"%%user.id"}` for `Write Permissions`
  - Go to `Deployment` -> `Configuration` and tap `Enable Automatic Deployment`
- If migrations looks good we also perform the last round of manual testing with the `release` app version here.
- If manual testing succeeded and we no longer need the `stage` environment we should erase data.
- We prepare to deploy to the `production` but we must stick to the 9:15-12:00 GMT time window on weekends.

# Hotfix release flow

All the same as for normal flow except we don't have the required time window and should do a fix as soon as possible but without skipping any steps.

# Environments

### Sandbox Anton

- Anton is the owner and we should not have any other developers working here.
- Data may be corrupted since various debug and testing activities are regularly performed.
- Data may be restored from other enviroments like `production` for test purposes.
- Erase command: `dt backup --environment production --verbose && dt erase-environment --environment sandbox-anton dt restore --environment sandbox-anton --backup-source-environment production --minimum --do-not-drop --yes --verbose`. We need to perform FMP migrations after `restore` if needed.

### Tests

- IEX data comes from the IEX Sandbox so it weakly correlate with real data.
- Data is erased during tests.
- We perform tests once daily starting from 0:00 GMT. The actual time may differ since the command is executed externally.
- We should not have 2 test commands running together or it will more likely fail.

### Sandbox

- We should not have any `testXXX` function deployed here so they should be removed during merge if needed.
- IEX data comes from the IEX Production so we should have the same data as in the `production` environment but in less volume.
- The team is working here so if something looks wrong it should be reported immediately and fixed.
- Erase command: `dt backup --environment sandbox-anton --verbose && dt erase-environment --environment sandbox && dt restore --environment sandbox --backup-source-environment sandbox-anton --minimum --do-not-drop --yes --verbose`

### Stage

- Migrations are tested here just before we are ready to deploy to the production. Otherwise, this environment is rarely used.
- Erase command: `dt backup --environment sandbox --verbose && dt erase-environment --environment stage && dt restore --environment stage --backup-source-environment sandbox --minimum --do-not-drop --yes --verbose`

### Production

- We have logs monitoring there once an hour so we should react immediately in case of misbehavior.
- All data adjust operations here should happen using `dt` command and only after double or even triple verification. There should be only one allowed person to perform such operations.
