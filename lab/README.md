# Physical-device sync lab

An isolated Day app, authenticated WebDAV fault server, and independent causal-history
oracle. Uses the working-tree `day-sync` and `day-sync-webdav`, actual SQLite databases,
and the native HTTP providers on physical Android and iOS devices. The app ID is
`dev.daybrite.syncdevicelab`; it does not open Fleet Reader's database or Fastmail account.

See the [2026-11-23 device results](RESULTS-2026-11-23.md) for the discovered cleanup race,
its regression tests, completed qualification, and remaining coverage.

The controller writes commands before sending them, records every acknowledged operation,
and checks candidate sets, resolved records, frontiers, pending uploads, waiting/held
objects, and poison flags. Matching devices alone is not sufficient: results must match
the original-intention oracle. A timeout fails the campaign and retains evidence.

## Run

Start a trusted HTTPS route to localhost port 18765. For example, Cloudflare's
[temporary testing tunnel](https://developers.cloudflare.com/tunnel/get-started/quick-tunnels/):

```sh
cloudflared tunnel --url http://127.0.0.1:18765 --no-autoupdate
python3 configure.py --url https://YOUR-TEMPORARY-HOST --state /tmp/sync-lab-run --team YOUR_APPLE_TEAM
python3 server.py --state /tmp/sync-lab-run
```

The server binds only localhost. Every route requires an ephemeral Basic credential and
random path prefix. Only synthetic data belongs in this fixture. Configuration, build
products, and signing settings are ignored by Git. `configure.py` refuses to overwrite
an existing run; it generates a new space and actors each time. Rebuild both devices
after configuring a new run or changing its HTTPS origin. TLS verification stays enabled.

From this directory, with the devices paired and developer mode enabled:

```sh
day launch --project app --day-src ../../day \
  -t android-material -t ios-uikit \
  --android-device PIXEL_SERIAL --ios-device IPHONE_UDID \
  --detach
python3 campaign.py --state /tmp/sync-lab-run --seed 20261123 --rounds 28
python3 recovery.py --campaign /tmp/sync-lab-run/campaigns/PRINTED-RUN-ID \
  --pixel PIXEL_SERIAL --iphone IPHONE_UDID --races 12
```

Adjust `--day-src` to the Day checkout, or omit it when using a consistent published
dependency graph. Keep both apps in the foreground for the transport campaign. The lab
holds a screen wake lock while its UI exists; background execution is still OS-controlled.
Physical kill tests use Android `am force-stop` and iOS `SIGKILL`, restricted to this app.

The first campaign covers concurrent offline edits/deletes, lost upload receipts and
peer compaction, HTTP 401/429/503/507, incomplete listings, malformed XML, persisted
reopen, large offline outboxes, snapshot publication, and bounded cleanup. The recovery
campaign adds native request timeouts, process death after acknowledged local writes,
process death after the server's durable PUT but before its response, overlapping
publication/compaction, forced peer deletion between LIST and GET on each native transport
through both sync and compaction, corrupt incoming bytes, lost snapshot parts, and fresh readers.

## Evidence and interpretation

`STATE/campaigns/RUN/` contains the command/result journal, original operations, summary
results, device process-control transcripts, and intentionally removed remote data.
`STATE/requests.jsonl` identifies the actual requests affected by each injected fault.
`STATE/dav/` is the persisted server state. App data contains the SQLite database,
durable last result, active command, and fresh-reader databases. Preserve source versions,
Cargo.lock, build logs, and device OS versions alongside these before comparing runs.

Commands are delivered at least once. A durable result prevents replay after a completed
command; death between a database commit and recording that result can repeat an edit.
The automated crash campaign kills during sync or after an acknowledged commit, not in
that controller-specific gap. It never counts an unacknowledged write as acknowledged.

This is an engine/transport qualification fixture. It does **not** exercise Fleet Reader's
feed adapter, UI state, credential storage, automatic retry scheduler, background-task
entitlements, or Fastmail-specific behavior. Its server implements the DAV operations
the engine uses; it is not a general DAV compliance test. Run the existing Apache/Nextcloud
interop suites and a separate disposable Fastmail space for provider qualification.
Neither a finite campaign nor a passing suite proves universal reliability.

## Validate the fixture

```sh
python3 -m unittest discover -s . -p 'test_*.py'
cargo fmt --manifest-path app/Cargo.toml --all -- --check
cargo test --manifest-path app/Cargo.toml
cargo clippy --manifest-path app/Cargo.toml --all-targets -- -D warnings
day lint --project app --strict --allow store-missing --allow locale-sync
```

Store-listing warnings are allowed because this test-only app is installed directly.
The oracle tests include matching-but-wrong replicas; fixture tests prove lost-ack writes
really persist, create-only writes cannot overwrite them, auth is required, and partial
listings are actually emitted. The portable `dayscript/demo.yaml` provides a separate
localized UI smoke check when the Day scripting connection is available. Protocol results
do not imply a UI smoke-test pass. Stop the app, server, and tunnel when finished; retain
run evidence.
