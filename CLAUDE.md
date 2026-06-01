# perfidb

## Testing the binary

When running the `perfidb` binary to test changes, always pass `--file` pointing to a
temporary database file (e.g. `--file /tmp/perfidb-test.db`). Without `--file`, perfidb
loads the user's real database at `~/.perfidb/finance.db`, so testing against it would
mutate real data.
