/*-------------------------------------------------------------------------
 *
 * bson.h
 *	  Declarations for BSON data type support.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/bson.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef BSON_H
#define BSON_H

#include "fmgr.h"
#include "lib/stringinfo.h"

/* functions in bson.c */
extern Datum bson_in(PG_FUNCTION_ARGS);
extern Datum bson_out(PG_FUNCTION_ARGS);
extern Datum bson_recv(PG_FUNCTION_ARGS);
extern Datum bson_send(PG_FUNCTION_ARGS);
extern Datum array_to_bson(PG_FUNCTION_ARGS);
extern Datum array_to_bson_pretty(PG_FUNCTION_ARGS);
extern Datum row_to_bson(PG_FUNCTION_ARGS);
extern Datum row_to_bson_pretty(PG_FUNCTION_ARGS);
extern Datum to_bson(PG_FUNCTION_ARGS);

extern Datum bson_agg_transfn(PG_FUNCTION_ARGS);
extern Datum bson_agg_finalfn(PG_FUNCTION_ARGS);

extern void escape_bson(StringInfo buf, const char *str);

/* functions in bsonfuncs.c */
extern Datum bson_object_field(PG_FUNCTION_ARGS);
extern Datum bson_object_field_text(PG_FUNCTION_ARGS);
extern Datum bson_array_element(PG_FUNCTION_ARGS);
extern Datum bson_array_element_text(PG_FUNCTION_ARGS);
extern Datum bson_extract_path(PG_FUNCTION_ARGS);
extern Datum bson_extract_path_text(PG_FUNCTION_ARGS);
extern Datum bson_object_keys(PG_FUNCTION_ARGS);
extern Datum bson_array_length(PG_FUNCTION_ARGS);
extern Datum bson_each(PG_FUNCTION_ARGS);
extern Datum bson_each_text(PG_FUNCTION_ARGS);
extern Datum bson_array_elements(PG_FUNCTION_ARGS);
extern Datum bson_populate_record(PG_FUNCTION_ARGS);
extern Datum bson_populate_recordset(PG_FUNCTION_ARGS);

#endif   /* BSON_H */
