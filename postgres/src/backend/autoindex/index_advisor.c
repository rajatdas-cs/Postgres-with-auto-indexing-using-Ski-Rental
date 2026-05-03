/*-------------------------------------------------------------------------
 *
 * index_advisor.c
 *	  Online index advisor based on the Ski Rental Problem.
 *
 * Instead of a static score threshold, each candidate (relid, attnum)
 * accumulates "rent" -- the plan cost paid on every query execution that
 * would have benefited from the index.  When the cumulative rent reaches
 * C_buy (the estimated one-time cost to build the index), the advisor
 * issues CREATE INDEX.
 *
 * Ski Rental mapping:
 *   C_rent  = plan_cost of each captured query (PlannedStmt->planTree->total_cost)
 *   C_buy   = estimated index build cost from pg_class.reltuples / relpages
 *   Trigger = SUM(C_rent) >= C_buy  =>  CREATE INDEX
 *
 * State persists across advisor passes (no reset between calls).
 * An eviction pass removes entries not seen within workload_window_days
 * to prevent unbounded memory growth.
 */
#include "postgres.h"

#include "autoindex/autoindex.h"

#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_statistic.h"
#include "executor/spi.h"
#include "optimizer/optimizer.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/* ----------------------------------------------------------------
 * Persistent aggregation hash table (BGW-local, survives across passes)
 * ----------------------------------------------------------------
 */

typedef enum ColUsageType
{
	USAGE_PREDICATE = 'p',
	USAGE_SORT = 's',
	USAGE_JOIN = 'j'
} ColUsageType;

typedef struct ColStatsKey
{
	Oid			relid;
	int16		attnum;
	ColUsageType usage_type;
} ColStatsKey;

typedef struct ColStatsEntry
{
	ColStatsKey key;
	int64		exec_count;		/* total executions seen */
	double		cumulative_rent; /* SUM(C_rent): total plan cost accumulated */
	TimestampTz last_seen;		/* for eviction policy */
} ColStatsEntry;

#define ADVISOR_HASH_SIZE	2048
#define ADVISOR_HASH_MASK	(ADVISOR_HASH_SIZE - 1)

/*
 * These statics are BGW-process-local and survive across WaitLatch cycles,
 * giving us persistent state across advisor passes without shared memory.
 */
static ColStatsEntry advisor_hash[ADVISOR_HASH_SIZE];
static bool advisor_hash_used[ADVISOR_HASH_SIZE];

static uint32
advisor_hash_fn(Oid relid, int16 attnum, ColUsageType utype)
{
	uint32		h = (uint32) relid;

	h = h * 31 + (uint32) attnum;
	h = h * 31 + (uint32) utype;
	return h & ADVISOR_HASH_MASK;
}

static ColStatsEntry *
advisor_hash_find_or_create(Oid relid, int16 attnum, ColUsageType utype)
{
	uint32		idx = advisor_hash_fn(relid, attnum, utype);
	uint32		start = idx;

	for (;;)
	{
		if (!advisor_hash_used[idx])
		{
			advisor_hash_used[idx] = true;
			memset(&advisor_hash[idx], 0, sizeof(ColStatsEntry));
			advisor_hash[idx].key.relid = relid;
			advisor_hash[idx].key.attnum = attnum;
			advisor_hash[idx].key.usage_type = utype;
			return &advisor_hash[idx];
		}
		if (advisor_hash[idx].key.relid == relid &&
			advisor_hash[idx].key.attnum == attnum &&
			advisor_hash[idx].key.usage_type == utype)
		{
			return &advisor_hash[idx];
		}
		idx = (idx + 1) & ADVISOR_HASH_MASK;
		if (idx == start)
			return NULL;		/* table full */
	}
}

/*
 * Evict entries not seen within workload_window_days.
 * Called at the start of each advisor pass to bound memory usage.
 */
static void
advisor_hash_evict_stale(void)
{
	TimestampTz now = GetCurrentTimestamp();
	int64		max_age = (int64) autoindex_workload_window_days * USECS_PER_DAY;
	int			i;
	int			evicted = 0;

	for (i = 0; i < ADVISOR_HASH_SIZE; i++)
	{
		if (!advisor_hash_used[i])
			continue;
		if (advisor_hash[i].last_seen != 0 &&
			(now - advisor_hash[i].last_seen) > max_age)
		{
			advisor_hash_used[i] = false;
			memset(&advisor_hash[i], 0, sizeof(ColStatsEntry));
			evicted++;
		}
	}

	if (evicted > 0)
		ereport(DEBUG1,
				(errmsg("autoindex advisor: evicted %d stale entries (window=%d days)",
						evicted, autoindex_workload_window_days)));
}

/*
 * Remove a single entry from the hash after its index has been created,
 * so it cannot re-trigger.
 */
static void
advisor_hash_remove(Oid relid, int16 attnum, ColUsageType utype)
{
	uint32		idx = advisor_hash_fn(relid, attnum, utype);
	uint32		start = idx;

	for (;;)
	{
		if (!advisor_hash_used[idx])
			return;
		if (advisor_hash[idx].key.relid == relid &&
			advisor_hash[idx].key.attnum == attnum &&
			advisor_hash[idx].key.usage_type == utype)
		{
			advisor_hash_used[idx] = false;
			memset(&advisor_hash[idx], 0, sizeof(ColStatsEntry));
			return;
		}
		idx = (idx + 1) & ADVISOR_HASH_MASK;
		if (idx == start)
			return;
	}
}

/* ----------------------------------------------------------------
 * Drain ring buffer -- accumulates C_rent into persistent hash
 * ----------------------------------------------------------------
 */
static void
drain_ring_buffer(void)
{
	WorkloadRingBuffer *ring;
	uint64		head;
	uint64		tail;
	int			i;

	if (AutoIndexShmem == NULL)
		return;

	ring = &AutoIndexShmem->ring;
	head = pg_atomic_read_u64(&ring->head);
	tail = pg_atomic_read_u64(&ring->tail);

	while (head != tail)
	{
		WorkloadEntry *e = &ring->entries[head & AUTOINDEX_RING_MASK];

		/* predicate columns: primary candidates for indexing */
		for (i = 0; i < e->n_pred; i++)
		{
			ColStatsEntry *cs = advisor_hash_find_or_create(
				e->pred_relids[i], e->pred_attnums[i], USAGE_PREDICATE);
			if (cs)
			{
				cs->exec_count++;
				cs->cumulative_rent += e->plan_cost;
				cs->last_seen = e->ts;
			}
		}

		/* join columns */
		for (i = 0; i < e->n_join; i++)
		{
			ColStatsEntry *cs = advisor_hash_find_or_create(
				e->join_relids[i], e->join_attnums[i], USAGE_JOIN);
			if (cs)
			{
				cs->exec_count++;
				cs->cumulative_rent += e->plan_cost;
				cs->last_seen = e->ts;
			}
		}

		/* sort columns */
		for (i = 0; i < e->n_sort; i++)
		{
			ColStatsEntry *cs = advisor_hash_find_or_create(
				e->sort_relids[i], e->sort_attnums[i], USAGE_SORT);
			if (cs)
			{
				cs->exec_count++;
				cs->cumulative_rent += e->plan_cost;
				cs->last_seen = e->ts;
			}
		}

		head++;
	}

	pg_atomic_write_u64(&ring->head, head);
}

/* ----------------------------------------------------------------
 * C_buy estimation via SPI
 * ----------------------------------------------------------------
 */

/*
 * Estimate the one-time cost to build a single-column btree index.
 *
 * Formula approximates the planner cost model for CREATE INDEX:
 *   sequential scan of the heap  +  per-tuple CPU work for sorting
 *
 *   C_buy = relpages * seq_page_cost
 *           + reltuples * cpu_operator_cost * ln(reltuples + 1)
 *
 * Both terms use the same abstract cost units as the planner, making
 * the comparison SUM(C_rent) >= C_buy dimensionally consistent.
 */
static double
estimate_index_build_cost(Oid relid)
{
	int			ret;
	double		reltuples = 1.0;
	double		relpages = 1.0;
	double		c_buy;
	StringInfoData query;

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT COALESCE(reltuples, 1), COALESCE(relpages, 1) "
					 "FROM pg_class WHERE oid = %u",
					 relid);

	ret = SPI_execute(query.data, true, 1);
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		char	   *v1 = SPI_getvalue(SPI_tuptable->vals[0],
									  SPI_tuptable->tupdesc, 1);
		char	   *v2 = SPI_getvalue(SPI_tuptable->vals[0],
									  SPI_tuptable->tupdesc, 2);

		if (v1)
		{
			reltuples = strtod(v1, NULL);
			pfree(v1);
		}
		if (v2)
		{
			relpages = strtod(v2, NULL);
			pfree(v2);
		}
	}
	pfree(query.data);

	if (reltuples < 1.0)
		reltuples = 1.0;
	if (relpages < 1.0)
		relpages = 1.0;

	/*
	 * seq_page_cost and cpu_operator_cost come from optimizer/cost.h.
	 * Defaults: seq_page_cost=1.0, cpu_operator_cost=0.0025.
	 * The log factor captures the sort cost for index building.
	 */
	c_buy = (relpages * seq_page_cost) +
		(reltuples * cpu_operator_cost * log(reltuples + 1.0));

	/* Floor so tiny tables still require a meaningful number of queries. */
	if (c_buy < 100.0)
		c_buy = 100.0;

	return c_buy;
}

/* ----------------------------------------------------------------
 * Safety-check helpers (unchanged from original)
 * ----------------------------------------------------------------
 */

static bool
index_already_exists(Oid relid, int16 attnum)
{
	int			ret;
	bool		exists = false;
	StringInfoData query;

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT 1 FROM pg_index i "
					 "JOIN pg_attribute a ON a.attrelid = i.indexrelid "
					 "WHERE i.indrelid = %u "
					 "AND a.attnum = 1 "
					 "AND EXISTS ("
					 "  SELECT 1 FROM pg_attribute a2 "
					 "  WHERE a2.attrelid = i.indrelid "
					 "  AND a2.attnum = %d "
					 "  AND a2.attname = a.attname"
					 ") LIMIT 1",
					 relid, attnum);

	ret = SPI_execute(query.data, true, 1);
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
		exists = true;

	pfree(query.data);
	return exists;
}

static int
count_auto_indexes(Oid relid)
{
	int			ret;
	int			count = 0;
	StringInfoData query;

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT count(*) FROM pg_class c "
					 "JOIN pg_index i ON c.oid = i.indexrelid "
					 "WHERE i.indrelid = %u "
					 "AND c.relname LIKE 'autoindex_%%'",
					 relid);

	ret = SPI_execute(query.data, true, 1);
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		char	   *val = SPI_getvalue(SPI_tuptable->vals[0],
									   SPI_tuptable->tupdesc, 1);

		if (val != NULL)
		{
			count = atoi(val);
			pfree(val);
		}
	}
	pfree(query.data);
	return count;
}

static char *
get_column_name(Oid relid, int16 attnum)
{
	return get_attname(relid, attnum, false);
}

static char *
get_table_name(Oid relid)
{
	return get_rel_name(relid);
}

static void
create_index_for_candidate(Oid relid, int16 attnum)
{
	StringInfoData cmd;
	char	   *tblname;
	char	   *colname;
	int			ret;

	tblname = get_table_name(relid);
	colname = get_column_name(relid, attnum);

	if (tblname == NULL || colname == NULL)
		return;

	initStringInfo(&cmd);
	appendStringInfo(&cmd,
					 "CREATE INDEX IF NOT EXISTS autoindex_%s_%s "
					 "ON %s (%s)",
					 tblname, colname,
					 quote_identifier(tblname),
					 quote_identifier(colname));

	ereport(LOG,
			(errmsg("autoindex: creating index on %s(%s)",
					tblname, colname)));

	ret = SPI_execute(cmd.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(WARNING,
				(errmsg("autoindex: failed to create index on %s(%s): SPI returned %d",
						tblname, colname, ret)));
	else
		ereport(LOG,
				(errmsg("autoindex: successfully created index on %s(%s)",
						tblname, colname)));

	pfree(cmd.data);
}

/* ----------------------------------------------------------------
 * Candidate struct for sorting before cap enforcement
 * ----------------------------------------------------------------
 */
typedef struct IndexCandidate
{
	Oid			relid;
	int16		attnum;
	ColUsageType usage_type;
	int64		exec_count;
	double		cumulative_rent;
	double		c_buy;
} IndexCandidate;

/* ----------------------------------------------------------------
 * Main advisor entry point
 * ----------------------------------------------------------------
 */
void
autoindex_run_advisor(void)
{
	IndexCandidate candidates[ADVISOR_HASH_SIZE];
	int			n_candidates = 0;
	int			i;

	/*
	 * Evict stale entries before draining so old costs don't accumulate
	 * indefinitely for queries that have stopped running.
	 */
	advisor_hash_evict_stale();

	/*
	 * Drain the ring buffer, adding each entry's plan_cost to the
	 * cumulative_rent of its predicate/join/sort columns.
	 * State persists across advisor passes -- no reset here.
	 */
	drain_ring_buffer();

	/* log ring buffer stats */
	if (AutoIndexShmem)
	{
		uint64		written = pg_atomic_read_u64(&AutoIndexShmem->total_written);
		uint64		dropped = pg_atomic_read_u64(&AutoIndexShmem->total_dropped);
		int			n_tracked = 0;

		for (i = 0; i < ADVISOR_HASH_SIZE; i++)
			if (advisor_hash_used[i])
				n_tracked++;

		ereport(DEBUG1,
				(errmsg("autoindex advisor: ring written=%llu dropped=%llu tracked=%d",
						(unsigned long long) written,
						(unsigned long long) dropped,
						n_tracked)));
	}

	/* check if there's anything to evaluate */
	{
		bool		any = false;

		for (i = 0; i < ADVISOR_HASH_SIZE; i++)
			if (advisor_hash_used[i])
			{
				any = true;
				break;
			}
		if (!any)
			return;
	}

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		ereport(WARNING, (errmsg("autoindex advisor: SPI_connect failed")));
		return;
	}

	/*
	 * Ski Rental evaluation loop.
	 *
	 * For each tracked column, compute C_buy (the one-time index build
	 * cost).  If cumulative_rent >= C_buy and the minimum frequency guard
	 * is satisfied, this column is a candidate for index creation.
	 */
	for (i = 0; i < ADVISOR_HASH_SIZE; i++)
	{
		ColStatsEntry *cs;
		double		c_buy;

		if (!advisor_hash_used[i])
			continue;

		cs = &advisor_hash[i];

		/* only predicate and join columns benefit from btree indexes */
		if (cs->key.usage_type != USAGE_PREDICATE &&
			cs->key.usage_type != USAGE_JOIN)
			continue;

		/* skip invalid attnums (whole-row references) */
		if (cs->key.attnum <= 0)
			continue;

		/* minimum frequency guard: avoid acting on outlier single queries */
		if (cs->exec_count < autoindex_min_exec_freq)
		{
			ereport(DEBUG1,
					(errmsg("autoindex advisor: relid=%u attnum=%d exec_count=%lld < min_exec_freq=%d, skipping",
							cs->key.relid, cs->key.attnum,
							(long long) cs->exec_count,
							autoindex_min_exec_freq)));
			continue;
		}

		c_buy = estimate_index_build_cost(cs->key.relid);

		ereport(DEBUG1,
				(errmsg("autoindex advisor: ski-rental relid=%u attnum=%d "
						"exec_count=%lld cumulative_rent=%.2f c_buy=%.2f (%s)",
						cs->key.relid, cs->key.attnum,
						(long long) cs->exec_count,
						cs->cumulative_rent, c_buy,
						cs->cumulative_rent >= c_buy ? "TRIGGER" : "renting")));

		if (cs->cumulative_rent >= c_buy)
		{
			candidates[n_candidates].relid = cs->key.relid;
			candidates[n_candidates].attnum = cs->key.attnum;
			candidates[n_candidates].usage_type = cs->key.usage_type;
			candidates[n_candidates].exec_count = cs->exec_count;
			candidates[n_candidates].cumulative_rent = cs->cumulative_rent;
			candidates[n_candidates].c_buy = c_buy;
			n_candidates++;
		}
	}

	/* sort by cumulative_rent descending so the highest-value indexes are
	 * created first when per-table caps are tight */
	for (i = 0; i < n_candidates - 1; i++)
	{
		int			j;

		for (j = i + 1; j < n_candidates; j++)
		{
			if (candidates[j].cumulative_rent > candidates[i].cumulative_rent)
			{
				IndexCandidate tmp = candidates[i];

				candidates[i] = candidates[j];
				candidates[j] = tmp;
			}
		}
	}

	/* create indexes and remove triggered entries from the tracking table */
	for (i = 0; i < n_candidates; i++)
	{
		IndexCandidate *c = &candidates[i];

		ereport(LOG,
				(errmsg("autoindex advisor: ski-rental triggered relid=%u attnum=%d "
						"cumulative_rent=%.2f >= c_buy=%.2f (exec_count=%lld)",
						c->relid, c->attnum,
						c->cumulative_rent, c->c_buy,
						(long long) c->exec_count)));

		if (count_auto_indexes(c->relid) >= autoindex_max_indexes_per_table)
		{
			ereport(DEBUG1,
					(errmsg("autoindex: skipping relid=%u attnum=%d -- per-table cap reached",
							c->relid, c->attnum)));
			continue;
		}

		if (index_already_exists(c->relid, c->attnum))
		{
			ereport(DEBUG1,
					(errmsg("autoindex: skipping relid=%u attnum=%d -- already exists",
							c->relid, c->attnum)));
			/* still remove from tracking so we don't re-evaluate */
			advisor_hash_remove(c->relid, c->attnum, c->usage_type);
			continue;
		}

		create_index_for_candidate(c->relid, c->attnum);

		/*
		 * Remove from the tracking table after creation so this entry does
		 * not trigger again.  If the index is later dropped by the lifecycle
		 * manager, the column will re-accumulate rent from scratch.
		 */
		advisor_hash_remove(c->relid, c->attnum, c->usage_type);
	}

	SPI_finish();
}
