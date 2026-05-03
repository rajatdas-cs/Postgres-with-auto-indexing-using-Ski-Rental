#ifndef AUTOINDEX_H
#define AUTOINDEX_H

#include "postgres.h"
#include "access/transam.h"
#include "port/atomics.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"

struct QueryDesc;					/* forward declaration */

/* ----------------------------------------------------------------
 * Tunable compile-time limits
 * ----------------------------------------------------------------
 */
#define AUTOINDEX_MAX_RELS		8	/* max tables per captured query */
#define AUTOINDEX_MAX_COLS		16	/* max predicate/sort/join cols */
#define AUTOINDEX_RING_SIZE		4096 /* ring buffer slots (power of 2) */
#define AUTOINDEX_RING_MASK		(AUTOINDEX_RING_SIZE - 1)

/* ----------------------------------------------------------------
 * Workload capture -- one entry per finished query
 * ----------------------------------------------------------------
 */
typedef struct WorkloadEntry
{
	uint64		query_hash;		/* normalized query fingerprint */
	Oid			relids[AUTOINDEX_MAX_RELS];
	int16		pred_attnums[AUTOINDEX_MAX_COLS];
	Oid			pred_relids[AUTOINDEX_MAX_COLS];
	int16		sort_attnums[AUTOINDEX_MAX_COLS];
	Oid			sort_relids[AUTOINDEX_MAX_COLS];
	int16		join_attnums[AUTOINDEX_MAX_COLS];
	Oid			join_relids[AUTOINDEX_MAX_COLS];
	int			n_rels;
	int			n_pred;
	int			n_sort;
	int			n_join;
	double		plan_cost;		/* planner estimated total cost */
	double		actual_ms;		/* actual wall-clock ms (0 if unavail) */
	TimestampTz ts;				/* when the query finished */
} WorkloadEntry;

/* ----------------------------------------------------------------
 * Ring buffer in shared memory
 * ----------------------------------------------------------------
 */
typedef struct WorkloadRingBuffer
{
	pg_atomic_uint64 head;		/* consumer (BGW) position */
	pg_atomic_uint64 tail;		/* producer (backends) position */
	WorkloadEntry entries[AUTOINDEX_RING_SIZE];
} WorkloadRingBuffer;

/* ----------------------------------------------------------------
 * Shared-memory header
 * ----------------------------------------------------------------
 */
typedef struct AutoIndexShmemHeader
{
	int			version;		/* protocol version */
	pid_t		bgw_pid;		/* BGW process id, 0 if not running */
	pg_atomic_uint64 total_written; /* total entries ever written */
	pg_atomic_uint64 total_dropped; /* entries dropped (buffer full) */
	WorkloadRingBuffer ring;
} AutoIndexShmemHeader;

/* ----------------------------------------------------------------
 * GUC variables (defined in autoindex.c)
 * ----------------------------------------------------------------
 */
extern bool autoindex_enabled;
extern int	autoindex_analysis_interval;	/* seconds */
extern int	autoindex_advisor_interval;		/* seconds */
extern int	autoindex_lifecycle_interval;	/* hours */
extern int	autoindex_min_exec_freq;
extern double autoindex_min_score;
extern int	autoindex_max_indexes_per_table;
extern int	autoindex_drop_after_idle_days;
extern int	autoindex_workload_window_days;

/* ----------------------------------------------------------------
 * Shared-memory callbacks (for subsystemlist.h)
 * ----------------------------------------------------------------
 */
extern const ShmemCallbacks AutoIndexShmemCallbacks;

/* ----------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------
 */

/* autoindex.c -- BGW entry point & registration */
extern void AutoIndexRegister(void);
extern void AutoIndexMain(Datum main_arg);

/* workload_capture.c -- executor hook */
extern void autoindex_install_hooks(void);
extern void autoindex_workload_capture_hook(struct QueryDesc *queryDesc);

/* index_advisor.c -- scoring engine */
extern void autoindex_run_advisor(void);

/* index_lifecycle.c -- create / drop logic */
extern void autoindex_run_lifecycle(void);

/* shared memory access */
extern AutoIndexShmemHeader *AutoIndexShmem;

#endif							/* AUTOINDEX_H */
