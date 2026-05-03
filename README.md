# AutoIndex for PostgreSQL

A self-tuning index advisor built directly into PostgreSQL 19devel. AutoIndex observes every query that passes through the executor, accumulates evidence using the **Ski Rental online algorithm**, and automatically creates B-tree indexes at exactly the point where building the index becomes cheaper than continuing to pay the sequential-scan cost. Indexes that stop being used are automatically dropped.

---

## How It Works

### The Ski Rental Decision

The central question in automatic indexing is: *when have you seen enough slow queries to justify building an index?* AutoIndex frames this as the classic Ski Rental problem:

- **C_rent** — the planner's estimated cost each time a query runs without the index (extracted from `PlannedStmt->planTree->total_cost`).
- **C_buy** — the estimated one-time cost to build the index, calculated from `pg_class` statistics:

  ```
  C_buy = relpages × seq_page_cost + reltuples × cpu_operator_cost × ln(reltuples + 1)
  ```

- **Trigger** — as soon as `Σ C_rent ≥ C_buy`, the advisor issues `CREATE INDEX`. This gives a competitive ratio of 2 against the offline optimum.

### Architecture

```
Every query (all backends)                BGW (single process)
─────────────────────────────             ──────────────────────────────
ExecutorEnd hook                          every analysis_interval seconds:
  ├─ walk plan tree                         drain_ring_buffer()
  │    extract (relid, attnum) from           add plan_cost to cumulative_rent
  │    predicates / sorts / joins             for each pred/sort/join column
  └─ CAS push → WorkloadRingBuffer        
                    ↓ shared memory         every advisor_interval seconds:
                    ↑                         evict stale entries
                  head/tail                   for each entry where
                  (atomic u64)                  Σ C_rent ≥ C_buy:
                                              CREATE INDEX
                                              advisor_hash_remove()

                                          every lifecycle_interval hours:
                                            DROP INDEX for autoindex_% with
                                            idx_scan = 0 for N days
```

### Components

| File | Purpose |
|------|---------|
| `autoindex.c` | BGW entry point, shared memory setup, GUC registration, scheduler loop |
| `workload_capture.c` | `ExecutorEnd` hook — extracts columns from plan tree, lock-free ring buffer push |
| `index_advisor.c` | Ski Rental hash table, `estimate_index_build_cost()`, `CREATE INDEX` DDL |
| `index_lifecycle.c` | Drops idle `autoindex_%` indexes via `pg_stat_user_indexes` |
| `autoindex.h` | `WorkloadEntry`, `WorkloadRingBuffer`, `AutoIndexShmemHeader`, GUC externs |

---

## Prerequisites

| Dependency | Version | Install |
|------------|---------|---------|
| macOS (Apple Silicon) | 15+ | — |
| Meson | ≥ 1.10 | `brew install meson` |
| Ninja | ≥ 1.13 | `brew install ninja` |
| OpenSSL 3 | 3.x | `brew install openssl@3` |
| Python 3 | 3.11+ | `brew install python@3.11` |
| ICU, readline, zstd, lz4 | any | `brew install icu4c readline zstd lz4` |

---

## Build & Install

```bash
# 1. Set variables (adjust PROJECT if needed)
export PROJECT=/Users/rajatdas/Academic/DBIS_Project
export PGSRC=$PROJECT/postgres
export PGINSTALL=$PROJECT/pginstall
OSSL_VER=$(ls /opt/homebrew/Cellar/openssl@3/ | head -1)
export PKG_CONFIG_PATH="/opt/homebrew/Cellar/openssl@3/${OSSL_VER}/lib/pkgconfig"

# 2. Configure
cd $PGSRC
PKG_CONFIG_PATH="$PKG_CONFIG_PATH" meson setup --wipe builddir --prefix="$PGINSTALL"

# 3. Compile (server binary only, ~2 min first build)
ninja -C builddir src/backend/postgres

# 4. Install
ninja -C builddir install
export PATH=$PGINSTALL/bin:$PATH

# 5. Verify
postgres --version   # PostgreSQL 19devel
```

---

## Quick Start

```bash
export PGDATA=$PROJECT/pgdata_bench
export PGPORT=5434

# Initialise a fresh cluster
initdb -D $PGDATA --no-locale --encoding=UTF8

# Enable autoindex in postgresql.conf
cat >> $PGDATA/postgresql.conf << 'EOF'
autoindex.enabled           = on
autoindex.analysis_interval = 5
autoindex.advisor_interval  = 10
autoindex.min_exec_freq     = 5
autoindex.min_score         = 1.0
shared_buffers              = 128MB
EOF

# Start
pg_ctl -D $PGDATA -l $PGDATA/server.log -o "-p $PGPORT" start

# Watch it work
tail -f $PGDATA/server.log | grep --line-buffered -E "autoindex|ERROR|FATAL"
```

Run a workload — once `Σ C_rent ≥ C_buy` for any column, you'll see log lines like:

```
LOG:  autoindex: created index autoindex_orders_status on orders(status) ...
```

Stop the server:

```bash
pg_ctl -D $PGDATA stop -m fast
```

---

## GUC Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `autoindex.enabled` | `on` | Master on/off switch |
| `autoindex.analysis_interval` | `60` s | How often to drain the ring buffer |
| `autoindex.advisor_interval` | `300` s | How often to evaluate Ski Rental thresholds |
| `autoindex.lifecycle_interval` | `6` h | How often to drop unused indexes |
| `autoindex.min_exec_freq` | `100` | Minimum executions before an index is considered |
| `autoindex.min_score` | `1.0` | Minimum `cumulative_rent / c_buy` ratio to create |
| `autoindex.max_indexes_per_table` | `5` | Index cap per table |
| `autoindex.drop_after_idle_days` | `7` | Days of zero scans before a lifecycle drop |
| `autoindex.workload_window_days` | `3` | Eviction window for stale hash entries |

All parameters are settable at runtime via `ALTER SYSTEM SET … ; SELECT pg_reload_conf();`.

---

## Benchmark

The `run_benchmark.py` script measures 8 representative queries before and after automatic index creation — 25 runs each, writing raw timing and summary statistics to `benchmark_results.csv`.

```bash
# Load schema and data (50k customers, 200k orders) first — see benchmark_runbook.md
python3 run_benchmark.py
```

### Results (Apple M-series, shared_buffers = 128 MB)

| Query | Before (ms) | After (ms) | Speedup | Scan change |
|-------|------------|-----------|---------|-------------|
| Q1 city equality | 3.795 | 0.455 | **8.3×** | Seq → Index Only |
| Q2 status equality | 9.862 | 3.602 | **2.7×** | Seq → Bitmap Heap |
| Q3 customer_id point | 7.967 | 0.089 | **89.5×** | Seq → Index Only |
| Q4 amount range | 10.252 | 1.798 | **5.7×** | Seq → Index Only |
| Q5 country equality | 3.795 | 0.799 | **4.7×** | Seq → Index Only |
| Q6 age range | 3.499 | 0.732 | **4.8×** | Seq → Index Only |
| Q7 join + predicate | 17.669 | 7.970 | **2.2×** | Hash Join → Nested Loop |
| Q8 ORDER BY LIMIT | 11.603 | 0.121 | **95.9×** | Sort → Index Only |

Full raw data and per-run statistics are in `benchmark_results.csv`. For complete reproduction steps see [`benchmark_runbook.md`](benchmark_runbook.md).

---

## Project Files

| File | Description |
|------|-------------|
| `postgres/` | PostgreSQL 19devel source with AutoIndex integrated |
| `run_benchmark.py` | 3-phase benchmark script (before / trigger / after) |
| `benchmark_results.csv` | Raw measurements + summary (400 + 16 rows) |
| `benchmark_runbook.md` | Step-by-step guide to reproduce the benchmark from scratch |
| `project_details.md` | Deep-dive technical reference (architecture, design decisions, bugs fixed) |
| `demo_guide.md` | 12-step live demo guide with expected outputs and reviewer Q&A |
| `performance_evaluation.md` | Rigorous analysis of speedups, scan transitions, and write overhead |
| `report.tex` / `report.pdf` | 5-page project report (LaTeX source + compiled PDF) |

---

## Design Notes

**Why BGW-local state instead of shared memory?**  
The Ski Rental hash table only needs to be read and written by the single advisor BGW. Module-level C statics survive across `WaitLatch` sleep cycles within the same process, eliminating the need for LWLocks or DSM allocations on the hot path.

**Why `optimizer/optimizer.h` instead of `optimizer/cost.h`?**  
PostgreSQL 19devel reorganised the optimizer headers. `seq_page_cost` and `cpu_operator_cost` are declared in `optimizer/optimizer.h`; using them keeps `C_buy` in exactly the same abstract cost units as the planner's `total_cost`.

**Why does Q7 show more buffer hits after indexing?**  
The planner switches from Hash Join (both tables scanned once) to Nested Loop (5 000 customers × index probe on orders). More index pages are loaded, so raw buffer hits rise — but wall-clock time still falls 2.2× because random index reads are faster than the hash-build cost at this data size.

---

## License

Inherits PostgreSQL's BSD-style licence. AutoIndex-specific files (`src/backend/autoindex/`, `src/include/autoindex/`) are released under the same terms.
