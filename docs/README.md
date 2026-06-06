# PyButt documentation

In-depth documentation for understanding and tuning PyButt. The top-level
[`README`](../README.md) covers installation and command/option reference; these
documents explain the *why* and *how* behind the tuning knobs so you can make
informed choices.

## Reading order

1. **[Concepts](concepts.md)** — the mental model. How data streams through an
   export and an import, where memory is actually consumed, and how rows flow
   into Parquet rowgroups and SQL Server columnstore rowgroups. Start here.
2. **[Tuning](tuning.md)** — every tuning knob (`--fetch-size`,
   `--rowgroup-size`, `--batch-size`, `--worker-count`, `--file-count`,
   `--transaction-mode`, `--retries`): what it does, what it does *not* do, and
   how the knobs interact.
3. **[Engines](engines.md)** — how the `duckdb`, `pyodbc`, and `mssql-python`
   engines differ for export and import, including the full transaction-mode ×
   engine behaviour matrix.
4. **[Defaults](defaults.md)** — the current defaults, and the (evolving)
   rationale for engine-specific defaults. This is the living record we update as
   new tuning options and per-engine defaults are introduced.

## Scope of these docs

- The CLI/API help text is intentionally terse (one line per option). When an
  option's behaviour is subtle, the help text points here.
- These docs are the source of truth for behavioural detail and rationale. If you
  change tuning behaviour or defaults in code, update the relevant document in
  the same change.
