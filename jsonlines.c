/*--------------------------------------------------------------------------
 *
 * jsonlines.c
 *		JSON Lines text format support for COPY command.
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		jsonlines.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/copyapi.h"
#include "commands/copystate.h"
#include "commands/defrem.h"
#include "common/compression.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"

#ifdef HAVE_LIBZ
#include "zlib.h"
#endif

#include "pg_custom_copy_formats.h"

PG_MODULE_MAGIC;

#ifdef HAVE_LIBZ
#define GZIP_CHUNK_SIZE	(256 * 1024)
#endif

/*
 * Struct for COPY options for jsonlines format.
 */
typedef struct JsonLinesOptions
{
	pg_compress_algorithm compression;
	pg_compress_specification compression_specification;

	char	*compression_detail_str;
} JsonLinesOptions;

typedef struct CopyToStateJsonLines
{
	CopyToStateData base;

	JsonLinesOptions options;

#ifdef HAVE_LIBZ
	z_stream	strm;
	StringInfoData	inbuf;
	unsigned char	outbuf[GZIP_CHUNK_SIZE];
#endif
} CopyToStateJsonLines;

typedef struct CopyFromStateJsonLines
{
	CopyFromStateData base;

	pg_compress_algorithm compression;

#ifdef HAVE_LIBZ
	z_stream	strm;
	StringInfoData	inbuf;
	unsigned char	outbuf[GZIP_CHUNK_SIZE];

#define RAW_BUF_SIZE 65536      /* we palloc RAW_BUF_SIZE+1 bytes */
    char       *raw_buf;
    int         raw_buf_index;  /* next byte to process */
    int         raw_buf_len;    /* total # of bytes stored */
    /* Shorthand for number of unconsumed bytes available in raw_buf */
#define RAW_BUF_BYTES(cstate) ((cstate)->raw_buf_len - (cstate)->raw_buf_index)
#endif

	/*
	 * XXX All following fields are borrowed from CopyFromStateBuiltins, which
	 * are for builtin formats such as text and CSV since reading text-based
	 * data is the common routine also for this jsonlines format.
	 */

    StringInfoData line_buf;

#define INPUT_BUF_SIZE 65536    /* we palloc INPUT_BUF_SIZE+1 bytes */
    char       *input_buf;
    int         input_buf_index;    /* next byte to process */
    int         input_buf_len;  /* total # of bytes stored */
    bool        input_reached_eof;  /* true if we reached EOF */
    bool        input_reached_error;    /* true if a conversion error happened */
    /* Shorthand for number of unconsumed bytes available in input_buf */
#define INPUT_BUF_BYTES(cstate) ((cstate)->input_buf_len - (cstate)->input_buf_index)
} CopyFromStateJsonLines;

static void JsonLinesCopyFromInFunc(CopyFromState cstate, Oid atttypid, FmgrInfo *finfo,
									Oid *typioparam);
static void JsonLinesCopyFromStart(CopyFromState cstate, TupleDesc tupDesc);
static bool JsonLinesCopyFromOneRow(CopyFromState ccstate, ExprContext *econtext, Datum *values,
									bool *nulls, CopyFromRowInfo *rowinfo);
static void JsonLinesCopyFromEnd(CopyFromState cstate);
static void JsonLinesCopyToOutFunc(CopyToState cstate, Oid atttypid, FmgrInfo *finfo);
static void JsonLinesCopyToStart(CopyToState ccstate, TupleDesc tupDesc);
static void JsonLinesCopyToOneRow(CopyToState ccstate, TupleTableSlot *slot);
static void JsonLinesCopyToEnd(CopyToState ccstate);

/*
 * GZIP support
 */

static void
initialize_deflate_gzip(z_stream *strm, pg_compress_specification *spec)
{
#ifndef HAVE_LIBZ
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("gzip compression is not supported by this build")));
#else

	MemSet(strm, 0, sizeof(z_stream));
	if (deflateInit2(strm, spec->level, Z_DEFLATED, 15 + 16, 8,
					 Z_DEFAULT_STRATEGY) != Z_OK)
		ereport(ERROR,
				errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("could not initialize compression library"));
#endif
}

static void
initialize_inflate_gzip(z_stream *strm)
{
#ifndef HAVE_LIBZ
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("gzip compression is not supported by this build")));
#else

	MemSet(strm, 0, sizeof(z_stream));
	if (inflateInit2(strm,15 + 32) != Z_OK)
		ereport(ERROR,
				errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("could not initialize compression library"));
#endif
}

#ifdef HAVE_LIBZ
static void
write_gzip(CopyToStateJsonLines *cstate, char *rowdata, int flush_flag)
{
	Size	row_len = strlen(rowdata);

	cstate->strm.next_in = (unsigned char *) rowdata;
	cstate->strm.avail_in = row_len;

	do
	{
		Size	written;

		cstate->strm.next_out = cstate->outbuf;
		cstate->strm.avail_out = GZIP_CHUNK_SIZE;

		if (deflate(&cstate->strm, flush_flag) == Z_STREAM_ERROR)
			elog(ERROR, "could not compress data: %s", cstate->strm.msg);

		written = GZIP_CHUNK_SIZE - cstate->strm.avail_out;

		if (written > 0)
		{
			appendBinaryStringInfo(cstate->base.fe_msgbuf, cstate->outbuf, written);
			CopyToFlushData((CopyToState) cstate);
		}
	}
	while (cstate->strm.avail_out == 0);
}

static void
read_gzip(CopyFromStateJsonLines *cstate)
{
	Size	written;
	Size	inbytes;

	/* Read compressed data to refill the raw_buf if it's empty */
	if (RAW_BUF_BYTES(cstate) == 0)
	{
		cstate->raw_buf_len = CopyFromGetData((CopyFromState) cstate, cstate->raw_buf, 1, RAW_BUF_SIZE);
		cstate->raw_buf_index = 0;
		cstate->base.bytes_processed += cstate->raw_buf_len;
	}

	/*
	 * When decompressing the data, the output buffer could be full before reaching
	 * the end of raw_buf. Therefore, we keep track of raw_buf_index that points the
	 * index at which we've fed to the decompression stream.
	 */
	inbytes = RAW_BUF_BYTES(cstate);
	cstate->strm.next_in = (unsigned char *) (cstate->raw_buf + cstate->raw_buf_index);
	cstate->strm.avail_in = inbytes;

	/*
	 * We can always use the whole input_buf as the output buffer of decompression
	 * since this function is called when the input_buf is empty.
	 */
	cstate->strm.next_out = (unsigned char *) cstate->input_buf;
	cstate->strm.avail_out = INPUT_BUF_SIZE;

	if (inflate(&cstate->strm, Z_NO_FLUSH) < 0)
	{
		inflateEnd(&cstate->strm);
		elog(ERROR, "could not decompress data: %s", cstate->strm.msg);
	}

	written = INPUT_BUF_SIZE - cstate->strm.avail_out;

	/* advance raw_buf_index */
	cstate->raw_buf_index += (inbytes - cstate->strm.avail_in);

	/* update input_buf fields */
	cstate->input_buf[written] = '\0';
	cstate->input_buf_len = written;
	cstate->input_buf_index = 0;
}

static void
end_deflate_gzip(CopyToStateJsonLines *cstate)
{
	write_gzip(cstate, "", Z_FINISH);
	deflateEnd(&cstate->strm);
}

static void
end_inflate_gzip(CopyFromStateJsonLines *cstate)
{
	inflateEnd(&cstate->strm);
}
#endif

/*
 * Read one line from the source.
 *
 * This function uses raw_buf and line_buf, but not input_buf. raw_buf is used to
 * load the raw data from the source, and transferred data into line_buf until
 * it finds a new line character, which is the data separator of JSON Lines format.
 *
 * XXX: support only '\n' new line.
 */
static bool
JsonLineReadLine(CopyFromStateJsonLines *cstate)
{
	int			nbytes;
	bool		result = false;

	resetStringInfo(&cstate->line_buf);

	for (;;)
	{
		char		*ptr;

		/* Load more data if needed */
		if (INPUT_BUF_BYTES(cstate) <= 0)
		{
			int		inbytes;

			if (cstate->compression == PG_COMPRESSION_NONE)
			{
				inbytes = CopyFromGetData((CopyFromState) cstate, cstate->input_buf, 1, INPUT_BUF_SIZE);
				cstate->input_buf[inbytes] = '\0';
				cstate->input_buf_len = inbytes;
				cstate->input_buf_index = 0;
				cstate->base.bytes_processed += inbytes;
			}
			else if (cstate->compression == PG_COMPRESSION_GZIP)
			{
				read_gzip(cstate);
			}

			if (INPUT_BUF_BYTES(cstate) <= 0)
			{
				result = true;
				break;
			}
		}

		ptr = strchr(cstate->input_buf + cstate->input_buf_index, '\n');

		if (ptr == NULL)
		{
			appendBinaryStringInfo(&cstate->line_buf,
								   cstate->input_buf + cstate->input_buf_index,
								   cstate->input_buf_len - cstate->input_buf_index);
			cstate->input_buf_index = cstate->input_buf_len;
			continue;
		}

		nbytes = (ptr - cstate->input_buf) - cstate->input_buf_index;
		appendBinaryStringInfo(&cstate->line_buf,
							   cstate->input_buf + cstate->input_buf_index,
							   nbytes);
		cstate->input_buf_index += nbytes;

		/* consume '\n' */
		cstate->input_buf_index++;
		break;
	}

	return result;
}

/*
 * Assign the input function data to the given *flinfo.
 */
static void
JsonLinesCopyFromInFunc(CopyFromState cstate, Oid atttypid, FmgrInfo *finfo, Oid *typioparam)
{
	Oid			func_oid;

	getTypeInputInfo(atttypid, &func_oid, typioparam);
	fmgr_info(func_oid, finfo);
}

static void
JsonLinesCopyFromStart(CopyFromState ccstate, TupleDesc tupDesc)
{
	CopyFromStateJsonLines *cstate = (CopyFromStateJsonLines *) ccstate;
	const char *extension = strrchr(cstate->base.filename, '.');

	if (strcmp(extension, ".gz") == 0)
	{
		cstate->compression = PG_COMPRESSION_GZIP;
		initialize_inflate_gzip(&cstate->strm);

		cstate->raw_buf = palloc(RAW_BUF_SIZE + 1);
		cstate->raw_buf_index = cstate->raw_buf_len = 0;
	}
	else
		cstate->compression = PG_COMPRESSION_NONE;

	/*
	 * Allocate buffers for the input pipeline.
	 *
	 * attribute_buf and raw_buf are used in both text and binary modes, but
	 * input_buf and line_buf only in text mode.
	 */

	cstate->input_buf = palloc(INPUT_BUF_SIZE + 1);
	cstate->input_buf_index = cstate->input_buf_len = 0;
	cstate->input_reached_eof = false;
	cstate->input_reached_error = false;

	initStringInfo(&cstate->line_buf);
	cstate->base.line_buf = &cstate->line_buf;
}

/*
 * Write a C-string representation of the given JsonbValue to 'str'.
 */
static void
GetJsonbValueAsCString(JsonbValue *v, StringInfo str)
{
	switch (v->type)
	{
		case jbvNull:
			/* must be handled by the caller */
			break;

		case jbvBool:
			appendStringInfoString(str, v->val.boolean ? "true" : "false");
			break;

		case jbvString:
			appendBinaryStringInfo(str, v->val.string.val, v->val.string.len);
			break;
		case jbvNumeric:
			{
				Datum		cstr;

				cstr = DirectFunctionCall1(numeric_out,
										   PointerGetDatum(v->val.numeric));

				appendStringInfoString(str, DatumGetCString(cstr));
				break;
			}

		case jbvBinary:
			(void) JsonbToCString(str, v->val.binary.data, v->val.binary.len);
			break;

		default:
			elog(ERROR, "unrecognized jsonb type: %d", (int) v->type);
	}

	return;
}

static bool
JsonLinesCopyFromOneRow(CopyFromState ccstate, ExprContext *econtext, Datum *values,
						bool *nulls, CopyFromRowInfo *rowinfo)
{
	CopyFromStateJsonLines *cstate = (CopyFromStateJsonLines *) ccstate;
	TupleDesc tupdesc = RelationGetDescr(cstate->base.rel);
	Jsonb	*jb;
	Datum	jsonb_data;
	ListCell	*lc;
	StringInfoData buf;
	bool	ret;

	if (JsonLineReadLine(cstate))
		return false;

	/* Convert the raw input line to a jsonb value */
	ret = DirectInputFunctionCallSafe(jsonb_in, cstate->line_buf.data,
									  JSONBOID, -1,
									  (Node *) cstate->base.escontext,
									  &jsonb_data);

	if (!ret)
		elog(ERROR, "invalid data for jsonb value");

	jb = DatumGetJsonbP(jsonb_data);

	initStringInfo(&buf);
	foreach(lc, cstate->base.attnumlist)
	{
		int	attnum = lfirst_int(lc);
		JsonbValue	*v;
		JsonbValue vbuf;
		Form_pg_attribute att = TupleDescAttr(tupdesc, attnum - 1);
		char	*attname = NameStr(att->attname);

		/* The jsonb value for the key with the column name */
		v = getKeyJsonValueFromContainer(&jb->root,
										 attname, strlen(attname), &vbuf);

		/*
		 * Fill with NULL if either not found or the value represent NULL.
		 */
		if (v == NULL || v->type == jbvNull)
		{
			nulls[attnum - 1] = true;
			continue;
		}

		nulls[attnum - 1] = false;

		/* Convert the jsonb value to cstring */
		GetJsonbValueAsCString(v, &buf);

		/* Convert the cstring data into the column */
		ret = InputFunctionCallSafe(&(cstate->base.in_functions[attnum - 1]),
									buf.data,
									cstate->base.typioparams[attnum - 1],
									att->atttypmod,
									(Node *) cstate->base.escontext,
									&values[attnum - 1]);

		if (!ret)
			elog(ERROR, "could not convert jsonb value \"%s\" to data for column \"%s\"",
				 buf.data, attname);

		resetStringInfo(&buf);
	}

	/* Set output parameters */
	if (rowinfo)
	{
		rowinfo->lineno = cstate->base.cur_lineno;
		rowinfo->tuplen = cstate->line_buf.len;
	}

	return true;
}

static void
JsonLinesCopyFromEnd(CopyFromState ccstate)
{
	CopyFromStateJsonLines *cstate = (CopyFromStateJsonLines *) ccstate;

	if (cstate->compression == PG_COMPRESSION_GZIP)
		end_inflate_gzip(cstate);
}

static void
JsonLinesCopyToOutFunc(CopyToState cstate, Oid atttypid, FmgrInfo *finfo)
{
	/* Nothing to do */
}

static void
JsonLinesCopyToStart(CopyToState ccstate, TupleDesc tupDesc)
{
	CopyToStateJsonLines *cstate = (CopyToStateJsonLines *) ccstate;
	char       *error_detail;

	parse_compress_specification(cstate->options.compression,
								 cstate->options.compression_detail_str,
								 &cstate->options.compression_specification);
	error_detail =
		validate_compress_specification(&cstate->options.compression_specification);
	if (error_detail != NULL)
		ereport(ERROR,
				errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("invalid compression specification: %s",
					   error_detail));

	switch (cstate->options.compression)
	{
		case PG_COMPRESSION_NONE:
			break;
		case PG_COMPRESSION_GZIP:
			initialize_deflate_gzip(&cstate->strm,
									&cstate->options.compression_specification);

			initStringInfo(&cstate->inbuf);
			break;
		case PG_COMPRESSION_LZ4:
			break;
		case PG_COMPRESSION_ZSTD:
			break;
	}
}

static void
JsonLinesCopyToOneRow(CopyToState ccstate, TupleTableSlot *slot)
{
	CopyToStateJsonLines *cstate = (CopyToStateJsonLines *) ccstate;
	Datum	json_text;
	char	*str;

	/*
	 * Convert the whole row to json value using row_to_json() function.
	 */
	json_text = DirectFunctionCall1(row_to_json, ExecFetchSlotHeapTupleDatum(slot));

	str = text_to_cstring(DatumGetTextP(json_text));

	if (cstate->options.compression == PG_COMPRESSION_NONE)
	{
		appendBinaryStringInfo(cstate->base.fe_msgbuf, str, strlen(str));
		appendStringInfoCharMacro(cstate->base.fe_msgbuf, '\n');
		/* End of row */
		CopyToFlushData((CopyToState) cstate);
	}
	else if (cstate->options.compression == PG_COMPRESSION_GZIP)
	{
		resetStringInfo(&cstate->inbuf);
		appendStringInfoString(&cstate->inbuf, str);
		appendStringInfoCharMacro(&cstate->inbuf, '\n');
		write_gzip(cstate, cstate->inbuf.data, Z_NO_FLUSH);
	}
}
static void
JsonLinesCopyToEnd(CopyToState ccstate)
{
	CopyToStateJsonLines *cstate = (CopyToStateJsonLines *) ccstate;

	if (cstate->options.compression == PG_COMPRESSION_GZIP)
		end_deflate_gzip(cstate);
}

static Size
JsonLinesCopyToEsimateSpace(void)
{
	return sizeof(CopyToStateJsonLines);
}

static Size
JsonLinesCopyFromEsimateSpace(void)
{
	return sizeof(CopyFromStateJsonLines);
}

static bool
JsonLinesCopyToProcessOneOption(CopyToState ccstate, DefElem *option)
{
	CopyToStateJsonLines *cstate = (CopyToStateJsonLines *) ccstate;

	if (strcmp(option->defname, "compression") == 0)
	{
		char       *optval = defGetString(option);

		if (!parse_compress_algorithm(optval, &cstate->options.compression))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized compression algorithm: \"%s\"",
							optval)));

		/* XXX TODO */
		if (cstate->options.compression == PG_COMPRESSION_LZ4)
			ereport(ERROR,
					errmsg("LZ4 compression is not supported"));
		else if (cstate->options.compression == PG_COMPRESSION_ZSTD)
			ereport(ERROR,
					errmsg("Zstd compression is not supported"));

		return true;
	}
	else if (strcmp(option->defname, "compression_detail") == 0)
	{
		cstate->options.compression_detail_str = defGetString(option);

		return true;
	}

	return false;
}

/*
static bool
JsonLinesCopyFromProcessOneOption(CopyFromState ccstate, DefElem *option)
{
}
*/


static const CopyToRoutine JsonLinesCopyToRoutine = {
	.CopyToEstimateStateSpace = JsonLinesCopyToEsimateSpace,
	.CopyToProcessOneOption = JsonLinesCopyToProcessOneOption,
	.CopyToOutFunc = JsonLinesCopyToOutFunc,
	.CopyToStart = JsonLinesCopyToStart,
	.CopyToOneRow = JsonLinesCopyToOneRow,
	.CopyToEnd = JsonLinesCopyToEnd,
};

static const CopyFromRoutine JsonLinesCopyFromRoutine = {
	.CopyFromEstimateStateSpace = JsonLinesCopyFromEsimateSpace,
	//.CopyFromProcessOneOption = JsonLinesCopyFromProcessOneOption,
	.CopyFromProcessOneOption = NULL,
	.CopyFromInFunc = JsonLinesCopyFromInFunc,
	.CopyFromStart = JsonLinesCopyFromStart,
	.CopyFromOneRow = JsonLinesCopyFromOneRow,
	.CopyFromEnd = JsonLinesCopyFromEnd,
};

void
RegisterJsonLinesCopyFormat(void)
{
	RegisterCopyCustomFormat("jsonlines",
							 &JsonLinesCopyFromRoutine,
							 &JsonLinesCopyToRoutine);
}

