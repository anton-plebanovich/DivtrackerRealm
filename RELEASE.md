
# Normal release flow

- All development is happening in the `sandbox-anton` environment. If new functionality is added we should write new automatic tests to cover it.
- After a feature is done and tested in the `sandbox-anton` environment, it is then deployed to the `tests` environment. Then, we check that automatic tests are passing using `dt test` command.
- If automatic tests succeeded we deploy to the `sandbox` environment for manual testing by the team.
- Just before release to production we should check error logs from all our environemnts to make sure we didn't introduce any new bugs.
- If logs look good we deploy to the `stage` environment to check migrations if any and also perform the last round of manual testing.
- If manual testing and migrations on the `stage` environment succeeded we prepare to deploy to the `production` but we must stick to the 9-12 GMT time window on weekends.

# Hotfix release flow

All the same as for normal flow except we don't have the required time window and should do a fix as soon as possible but without skipping any steps.

# Environments

### Sandbox Anton

- Anton is the owner and we should not have any other developers working here.
- Data may be corrupted since various debug and testing activities are regularly performed.
- Data may be restored from other enviroments like `production` for test purposes.
- IEX data comes from the IEX Sandbox so it weakly correlate with real data.

### Tests

- Data is erased during tests.
- We perform tests once daily starting from 0:00 GMT. The actual time may differ since the command is executed externally.
- We should not have 2 test commands running together or it will more likely fail.

### Sandbox

- When automated tests are passed in the `tests` environment we deploy to the `sandbox`.
- We should not have any `testXXX` function deployed here so they should be removed during merge if needed.
- IEX data comes from the IEX Production so we should have the same data as in the `production` environment but in less volume.
- Manual testing is happening here.
- The team is working here so if something looks wrong it should be reported immediately and fixed.

### Stage

- When we are ready to deploy to `production` we are deploying to the `stage` environment first. This allows us to review all code changes all at once.
- We then restore `production` data to the `stage` and perform required migrations to make sure they will pass flawlessly.
- Additionally, we should perform the most important manual testing to make sure affected and core areas are still working properly.

### Production

- If there are no issues with the `stage` deployment we deploy to the `production`
- We have logs monitoring there once an hour so we should react immediately in case of misbehavior.
- All data adjust operations here should happen using `dt` command and only after double or even triple verification. There should be only one allowed person to perform such operations.
