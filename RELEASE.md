
# Normal release flow

- All development is happening in the `sandbox-anton` environment
- After a feature is done and tested in the `sandbox-anton` environment, it is then deployed to the `tests` environment. If new functionality is added we should write new automatic tests to cover it. Then, we check that automatic tests are passing using `dt test` command.
- If automatic tests succeeded we deploy to the `sandbox` environment for manual testing by the team.
- If manual testing succeeded we deploy to the `production` but we must stick to the 9-12 GMT time window on weekends.

# Hotfix release flow

All the same as for normal flow except we don't have the required time window and should do a fix as soon as possible but without skipping any steps.
