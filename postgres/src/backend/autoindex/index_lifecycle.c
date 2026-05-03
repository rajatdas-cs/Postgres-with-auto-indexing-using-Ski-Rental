/*-------------------------------------------------------------------------
 *
 * index_lifecycle.c
 *	  Index lifecycle manager -- drops auto-created indexes that are unused.
 *
 * On each lifecycle pass the BGW queries pg_stat_user_indexes for
 * auto-created indexes (identified by the "autoindex_" naming prefix)
 * whose idx_scan count has not increased during the configured idle
 * window.  Such indexes are dropped via SPI.
*/
#include "postgres.h"

#include "autoindex/autoindex.h"

#include "executor/spi.h"
#include "utils/builtins.h"

/* ----------------------------------------------------------------
 * Lifecycle manager entry point
 *
 * Finds auto-created indexes that have had zero scans and drops them.
 * ----------------------------------------------------------------
 */
void
autoindex_run_lifecycle(void)
{
	int			ret;
	uint64		i;
	StringInfoData query;

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		ereport(WARNING,
				(errmsg("autoindex lifecycle: SPI_connect failed")));
		return;
	}

	/*
	 * Find auto-created indexes with zero scans.
	 *
	 * We use the naming convention "autoindex_%" to identify indexes
	 * created by this system.
	 * */
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT c.relname, n.nspname, i.indrelid, s.idx_scan "
					 "FROM pg_class c "
					 "JOIN pg_index i ON c.oid = i.indexrelid "
					 "JOIN pg_namespace n ON c.relnamespace = n.oid "
					 "LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = c.oid "
					 "WHERE c.relname LIKE 'autoindex_%%' "
					 "AND COALESCE(s.idx_scan, 0) = 0");

	ret = SPI_execute(query.data, true, 0);
	pfree(query.data);

	if (ret != SPI_OK_SELECT)
	{
		ereport(WARNING,
				(errmsg("autoindex lifecycle: failed to query index stats")));
		SPI_finish();
		return;
	}

	/* Drop each unused auto-index */
	for (i = 0; i < SPI_processed; i++)
	{
		char	   *idxname;
		char	   *nspname;
		bool		isnull;
		StringInfoData drop_cmd;

		idxname = SPI_getvalue(SPI_tuptable->vals[i],
							  SPI_tuptable->tupdesc, 1);
		nspname = SPI_getvalue(SPI_tuptable->vals[i],
							  SPI_tuptable->tupdesc, 2);

		if (idxname == NULL || nspname == NULL)
			continue;

		/* Safety: never drop system indexes */
		{
			Datum		d;

			d = SPI_getbinval(SPI_tuptable->vals[i],
							  SPI_tuptable->tupdesc, 3, &isnull);
			if (!isnull)
			{
				Oid			indrelid = DatumGetObjectId(d);

				if (indrelid < FirstNormalObjectId)
					continue;
			}
		}

		ereport(LOG,
				(errmsg("autoindex lifecycle: dropping unused index %s.%s",
						nspname, idxname)));

		initStringInfo(&drop_cmd);
		appendStringInfo(&drop_cmd, "DROP INDEX IF EXISTS %s.%s",
						 quote_identifier(nspname),
						 quote_identifier(idxname));

		ret = SPI_execute(drop_cmd.data, false, 0);
		if (ret != SPI_OK_UTILITY)
			ereport(WARNING,
					(errmsg("autoindex lifecycle: failed to drop index %s.%s",
							nspname, idxname)));
		else
			ereport(LOG,
					(errmsg("autoindex lifecycle: successfully dropped index %s.%s",
							nspname, idxname)));

		pfree(drop_cmd.data);
	}

	SPI_finish();
}
