/*-------------------------------------------------------------------------
 *
 * workload_capture.c
 *	  Executor hook that captures predicate, sort and join columns from
 *	  every query and writes them into a lock-free ring buffer in shared
 *	  memory.
 *
 * The hook is installed on ExecutorEnd (not ExecutorFinish) so that
 * instrumentation data is available.  Each backend appends a
 * WorkloadEntry via Compare-And-Swap on the tail index -- no LWLock
 * required on the write path.
 * */
#include "postgres.h"

#include "autoindex/autoindex.h"

#include "catalog/pg_class.h"
#include "executor/executor.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/optimizer.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

/* saved previous hook value */
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

/* ----------------------------------------------------------------
 * Plan-tree walker helpers
 * ----------------------------------------------------------------
 */

/*
 * Extract Var nodes from an expression tree, collecting (relid, attnum)
 * pairs into the WorkloadEntry.
 */
static void
extract_vars_from_expr(Node *node, WorkloadEntry *entry,
					   PlannedStmt *pstmt,
					   int16 *attnums, Oid *relids, int *count, int max_count)
{
	List	   *vars;
	ListCell   *lc;

	if (node == NULL)
		return;

	vars = pull_var_clause(node, PVC_RECURSE_AGGREGATES |
						   PVC_RECURSE_PLACEHOLDERS);

	foreach(lc, vars)
	{
		Var		   *var = (Var *) lfirst(lc);
		Index		varno;
		RangeTblEntry *rte;

		if (!IsA(var, Var))
			continue;

		varno = var->varno;
		if (varno <= 0 || varno > list_length(pstmt->rtable))
			continue;

		rte = (RangeTblEntry *) list_nth(pstmt->rtable, varno - 1);
		if (rte->rtekind != RTE_RELATION)
			continue;

		/* skip system catalogs */
		if (rte->relid < FirstNormalObjectId)
			continue;

		if (*count < max_count)
		{
			attnums[*count] = var->varattno;
			relids[*count] = rte->relid;
			(*count)++;
		}
	}

	list_free(vars);
}

/*
 * Recursively walk the plan tree to find predicate, sort, and join columns.
 */
static void
walk_plan_tree(Plan *plan, WorkloadEntry *entry, PlannedStmt *pstmt)
{
	if (plan == NULL)
		return;

	/* Predicate columns: WHERE quals on scan nodes */
	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_TidRangeScan:
			if (plan->qual)
				extract_vars_from_expr((Node *) plan->qual, entry, pstmt,
									   entry->pred_attnums,
									   entry->pred_relids,
									   &entry->n_pred,
									   AUTOINDEX_MAX_COLS);
			break;

		/* Join columns */
		case T_HashJoin:
		case T_MergeJoin:
		case T_NestLoop:
			{
				Join	   *join = (Join *) plan;

				if (join->joinqual)
					extract_vars_from_expr((Node *) join->joinqual, entry,
										   pstmt,
										   entry->join_attnums,
										   entry->join_relids,
										   &entry->n_join,
										   AUTOINDEX_MAX_COLS);
			}
			break;

		/* Sort columns */
		case T_Sort:
		case T_IncrementalSort:
			{
				Sort	   *sort = (Sort *) plan;
				int			i;

				for (i = 0; i < sort->numCols && entry->n_sort < AUTOINDEX_MAX_COLS; i++)
				{
					AttrNumber	attno = sort->sortColIdx[i];

					/*
					 * The sort column refers to the output tlist. Walk the
					 * target list to find the underlying Var.
					 */
					if (attno > 0 && attno <= list_length(plan->targetlist))
					{
						TargetEntry *tle = (TargetEntry *)
							list_nth(plan->targetlist, attno - 1);

						if (tle && IsA(tle->expr, Var))
						{
							Var		   *var = (Var *) tle->expr;
							Index		varno = var->varno;

							if (varno > 0 && varno <= list_length(pstmt->rtable))
							{
								RangeTblEntry *rte = (RangeTblEntry *)
									list_nth(pstmt->rtable, varno - 1);

								if (rte->rtekind == RTE_RELATION &&
									rte->relid >= FirstNormalObjectId)
								{
									entry->sort_attnums[entry->n_sort] = var->varattno;
									entry->sort_relids[entry->n_sort] = rte->relid;
									entry->n_sort++;
								}
							}
						}
					}
				}
			}
			break;

		default:
			/* check quals on any node type */
			if (plan->qual)
				extract_vars_from_expr((Node *) plan->qual, entry, pstmt,
									   entry->pred_attnums,
									   entry->pred_relids,
									   &entry->n_pred,
									   AUTOINDEX_MAX_COLS);
			break;
	}

	/* Collect table OIDs referenced */
	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_TidRangeScan:
			{
				Scan	   *scan = (Scan *) plan;
				Index		scanrelid = scan->scanrelid;

				if (scanrelid > 0 && scanrelid <= list_length(pstmt->rtable))
				{
					RangeTblEntry *rte = (RangeTblEntry *)
						list_nth(pstmt->rtable, scanrelid - 1);

					if (rte->rtekind == RTE_RELATION &&
						rte->relid >= FirstNormalObjectId &&
						entry->n_rels < AUTOINDEX_MAX_RELS)
					{
						/* avoid duplicates */
						bool		dup = false;
						int			j;

						for (j = 0; j < entry->n_rels; j++)
						{
							if (entry->relids[j] == rte->relid)
							{
								dup = true;
								break;
							}
						}
						if (!dup)
							entry->relids[entry->n_rels++] = rte->relid;
					}
				}
			}
			break;

		default:
			break;
	}

	/* recurse into child plans */
	walk_plan_tree(plan->lefttree, entry, pstmt);
	walk_plan_tree(plan->righttree, entry, pstmt);
}

/* ----------------------------------------------------------------
 * Ring buffer write (lock-free CAS)
 * ----------------------------------------------------------------
 */
static void
ring_buffer_push(WorkloadEntry *entry)
{
	WorkloadRingBuffer *ring;
	uint64		tail;
	uint64		head;
	uint64		next;

	if (AutoIndexShmem == NULL)
		return;

	ring = &AutoIndexShmem->ring;

	for (;;)
	{
		tail = pg_atomic_read_u64(&ring->tail);
		head = pg_atomic_read_u64(&ring->head);
		next = tail + 1;

		/* Check if buffer is full */
		if ((next & AUTOINDEX_RING_MASK) == (head & AUTOINDEX_RING_MASK))
		{
			/* buffer full -- drop entry */
			pg_atomic_fetch_add_u64(&AutoIndexShmem->total_dropped, 1);
			return;
		}

		/* Try to claim the slot */
		if (pg_atomic_compare_exchange_u64(&ring->tail, &tail, next))
		{
			/* We own slot at tail */
			memcpy(&ring->entries[tail & AUTOINDEX_RING_MASK],
				   entry, sizeof(WorkloadEntry));
			pg_atomic_fetch_add_u64(&AutoIndexShmem->total_written, 1);
			return;
		}
		/* CAS failed, another backend beat us -- retry */
	}
}

/* ----------------------------------------------------------------
 * ExecutorEnd hook
 * ----------------------------------------------------------------
 */
void
autoindex_workload_capture_hook(QueryDesc *queryDesc)
{
	PlannedStmt *pstmt;
	WorkloadEntry entry;
	bool		captured = false;

	/*
	 * Extract workload data BEFORE calling standard_ExecutorEnd, because
	 * that function may free the plan tree and related structures.
	 */
	if (autoindex_enabled && AutoIndexShmem != NULL &&
		queryDesc != NULL && queryDesc->plannedstmt != NULL)
	{
		pstmt = queryDesc->plannedstmt;

		if (pstmt->commandType != CMD_UTILITY && pstmt->planTree != NULL)
		{
			memset(&entry, 0, sizeof(WorkloadEntry));
			entry.plan_cost = pstmt->planTree->total_cost;
			entry.ts = GetCurrentTimestamp();
			entry.query_hash = pstmt->queryId;

			/* walk the plan tree to extract columns */
			walk_plan_tree(pstmt->planTree, &entry, pstmt);

			if (entry.n_pred > 0 || entry.n_sort > 0 || entry.n_join > 0)
				captured = true;
		}
	}

	/* now call the previous hook or standard implementation */
	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

	/* push captured data into the ring buffer */
	if (captured)
		ring_buffer_push(&entry);
}

/* ----------------------------------------------------------------
 * Hook installation
 * ----------------------------------------------------------------
 */
void
autoindex_install_hooks(void)
{
	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = autoindex_workload_capture_hook;
}
