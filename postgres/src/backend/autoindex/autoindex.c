/*-------------------------------------------------------------------------
 *
 * autoindex.c
 *	  Background worker entry point, shared memory setup, GUC registration,
 *	  and the main scheduler loop for the automatic indexing subsystem.
 *
 * The BGW wakes on a configurable interval, drains the workload ring
 * buffer, and periodically invokes the index advisor and lifecycle
 * manager.
 */
#include "postgres.h"

#include "autoindex/autoindex.h"

#include "access/xact.h"
#include "miscadmin.h"
#include "utils/snapmgr.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "tcop/utility.h"

/* ----------------------------------------------------------------
 * GUC variables
 * ----------------------------------------------------------------
 */
bool		autoindex_enabled = true;
int			autoindex_analysis_interval = 60;	/* seconds */
int			autoindex_advisor_interval = 300;	/* seconds */
int			autoindex_lifecycle_interval = 6;	/* hours */
int			autoindex_min_exec_freq = 100;
double		autoindex_min_score = 1.0;
int			autoindex_max_indexes_per_table = 5;
int			autoindex_drop_after_idle_days = 7;
int			autoindex_workload_window_days = 3;

/* pointer to shared memory segment */
AutoIndexShmemHeader *AutoIndexShmem = NULL;

/* ----------------------------------------------------------------
 * Shared-memory callbacks
 * ----------------------------------------------------------------
 */
static void
autoindex_shmem_request(void *arg)
{
	ShmemRequestStruct(.name = "AutoIndex",
					   .size = sizeof(AutoIndexShmemHeader),
					   .ptr = (void **) &AutoIndexShmem,
		);
}

static void
autoindex_shmem_init(void *arg)
{
	/* zero-initialize the whole structure */
	memset(AutoIndexShmem, 0, sizeof(AutoIndexShmemHeader));
	AutoIndexShmem->version = 1;
	AutoIndexShmem->bgw_pid = 0;
	pg_atomic_init_u64(&AutoIndexShmem->total_written, 0);
	pg_atomic_init_u64(&AutoIndexShmem->total_dropped, 0);
	pg_atomic_init_u64(&AutoIndexShmem->ring.head, 0);
	pg_atomic_init_u64(&AutoIndexShmem->ring.tail, 0);

	/*
	 * Install executor hooks in the postmaster process.  Forked backends
	 * will inherit the hook pointers.
	 */
	autoindex_install_hooks();
}

const ShmemCallbacks AutoIndexShmemCallbacks = {
	.request_fn = autoindex_shmem_request,
	.init_fn = autoindex_shmem_init,
};

/* ----------------------------------------------------------------
 * GUC registration (called from AutoIndexRegister)
 * ----------------------------------------------------------------
 */
static void
autoindex_register_gucs(void)
{
	DefineCustomBoolVariable("autoindex.enabled",
							"Enable the automatic indexing subsystem.",
							NULL,
							&autoindex_enabled,
							true,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("autoindex.analysis_interval",
							"How often the BGW drains the ring buffer (seconds).",
							NULL,
							&autoindex_analysis_interval,
							60,
							1, 3600,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	DefineCustomIntVariable("autoindex.advisor_interval",
							"How often the index advisor scoring pass runs (seconds).",
							NULL,
							&autoindex_advisor_interval,
							300,
							10, 86400,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	DefineCustomIntVariable("autoindex.lifecycle_interval",
							"How often the lifecycle (drop) pass runs (hours).",
							NULL,
							&autoindex_lifecycle_interval,
							6,
							1, 168,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("autoindex.min_exec_freq",
							"Minimum executions before a column is eligible for indexing.",
							NULL,
							&autoindex_min_exec_freq,
							100,
							1, INT_MAX,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomRealVariable("autoindex.min_score",
							 "Minimum advisor score for an index to be created.",
							 NULL,
							 &autoindex_min_score,
							 1.0,
							 0.0, 1e12,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("autoindex.max_indexes_per_table",
							"Hard cap on auto-created indexes per table.",
							NULL,
							&autoindex_max_indexes_per_table,
							5,
							1, 100,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("autoindex.drop_after_idle_days",
							"Days of zero usage before an auto-index is removed.",
							NULL,
							&autoindex_drop_after_idle_days,
							7,
							1, 365,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("autoindex.workload_window_days",
							"How far back the workload history is considered.",
							NULL,
							&autoindex_workload_window_days,
							3,
							1, 30,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("autoindex");
}

/* ----------------------------------------------------------------
 * Background worker registration
 * ----------------------------------------------------------------
 */
void
AutoIndexRegister(void)
{
	BackgroundWorker bgw;

	if (IsBinaryUpgrade)
		return;

	/* Register GUC parameters */
	autoindex_register_gucs();

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_restart_time = 10;	/* restart after 10s on crash */
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "AutoIndexMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "autoindex worker");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "autoindex worker");
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}

/* ----------------------------------------------------------------
 * Drain the ring buffer (called from main loop)
 *
 * Reads entries from the ring buffer and aggregates them into the
 * advisor's in-memory hash table via autoindex_run_advisor().
 * For now we simply advance the head pointer -- the advisor reads
 * directly from the ring on its own pass.
 * ----------------------------------------------------------------
 */
static void
autoindex_drain_workload_buffer(void)
{
	/* nothing to drain explicitly -- advisor reads ring directly */
}

/* ----------------------------------------------------------------
 * BGW main loop
 * ----------------------------------------------------------------
 */
void
AutoIndexMain(Datum main_arg)
{
	TimestampTz last_advisor_run;
	TimestampTz last_lifecycle_run;

	/* establish signal handlers */
	pqsignal(SIGTERM, die);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	BackgroundWorkerUnblockSignals();

	/* connect to the "postgres" database */
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	/* record our pid */
	if (AutoIndexShmem)
		AutoIndexShmem->bgw_pid = MyProcPid;

	ereport(LOG, (errmsg("autoindex worker started")));

	last_advisor_run = GetCurrentTimestamp();
	last_lifecycle_run = GetCurrentTimestamp();

	while (!ShutdownRequestPending)
	{
		TimestampTz now;
		int			rc;

		/* Wait for latch or timeout */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   (long) autoindex_analysis_interval * 1000L,
					   PG_WAIT_EXTENSION);

		ResetLatch(MyLatch);

		/* reload config if signaled */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (!autoindex_enabled)
			continue;

		now = GetCurrentTimestamp();

		/* Fast loop: drain ring buffer */
		autoindex_drain_workload_buffer();

		/* Slow loop: run advisor */
		if (TimestampDifferenceExceeds(last_advisor_run, now,
									   (long) autoindex_advisor_interval * 1000L))
		{
			SetCurrentStatementStartTimestamp();
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());
			PG_TRY();
			{
				autoindex_run_advisor();
			}
			PG_CATCH();
			{
				EmitErrorReport();
				FlushErrorState();
				if (ActiveSnapshotSet())
					PopActiveSnapshot();
				AbortCurrentTransaction();
				ereport(LOG,
						(errmsg("autoindex: advisor pass failed, will retry")));
			}
			PG_END_TRY();
			if (ActiveSnapshotSet())
				PopActiveSnapshot();
			if (IsTransactionState())
				CommitTransactionCommand();
			last_advisor_run = now;
		}

		/* Even slower loop: run lifecycle manager */
		if (TimestampDifferenceExceeds(last_lifecycle_run, now,
									   (long) autoindex_lifecycle_interval * 3600L * 1000L))
		{
			SetCurrentStatementStartTimestamp();
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());
			PG_TRY();
			{
				autoindex_run_lifecycle();
			}
			PG_CATCH();
			{
				EmitErrorReport();
				FlushErrorState();
				if (ActiveSnapshotSet())
					PopActiveSnapshot();
				AbortCurrentTransaction();
				ereport(LOG,
						(errmsg("autoindex: lifecycle pass failed, will retry")));
			}
			PG_END_TRY();
			if (ActiveSnapshotSet())
				PopActiveSnapshot();
			if (IsTransactionState())
				CommitTransactionCommand();
			last_lifecycle_run = now;
		}
	}

	if (AutoIndexShmem)
		AutoIndexShmem->bgw_pid = 0;

	ereport(LOG, (errmsg("autoindex worker shutting down")));

	proc_exit(0);
}
