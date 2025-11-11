/*--------------------------------------------------------------------------
 *
 * custom_copy_formats.c
 *		Multiple custom COPY format implementations.
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		custom_copy_formats.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/copyapi.h"

#include "pg_custom_copy_formats.h"

void
_PG_init(void)
{
	RegisterJsonLinesCopyFormat();
}
