# Solokit Test Command Update

We disabled the JavaScript and TypeScript test commands in Solokit because
`npm test -- --coverage` fails with `Missing script: "test"` in this repo.
Only the Python test command remains in `.session/config.json`.

Re-enable JS/TS tests when we add the needed npm scripts:
- Add a `test` script to `package.json` (and any required tooling).
- Restore the `javascript` and `typescript` commands in `.session/config.json`.
