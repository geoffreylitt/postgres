/*-------------------------------------------------------------------------
 *
 * bsonapi.h
 *	  Declarations for BSON API support.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/bsonapi.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef BSONAPI_H
#define BSONAPI_H

#include "lib/stringinfo.h"

typedef enum
{
	BSON_TOKEN_INVALID,
	BSON_TOKEN_STRING,
	BSON_TOKEN_NUMBER,
	BSON_TOKEN_OBJECT_START,
	BSON_TOKEN_OBJECT_END,
	BSON_TOKEN_ARRAY_START,
	BSON_TOKEN_ARRAY_END,
	BSON_TOKEN_COMMA,
	BSON_TOKEN_COLON,
	BSON_TOKEN_TRUE,
	BSON_TOKEN_FALSE,
	BSON_TOKEN_NULL,
	BSON_TOKEN_END,
}	BsonTokenType;


/*
 * All the fields in this structure should be treated as read-only.
 *
 * If strval is not null, then it should contain the de-escaped value
 * of the lexeme if it's a string. Otherwise most of these field names
 * should be self-explanatory.
 *
 * line_number and line_start are principally for use by the parser's
 * error reporting routines.
 * token_terminator and prev_token_terminator point to the character
 * AFTER the end of the token, i.e. where there would be a nul byte
 * if we were using nul-terminated strings.
 */
typedef struct BsonLexContext
{
	char	   *input;
	int			input_length;
	char	   *token_start;
	char	   *token_terminator;
	char	   *prev_token_terminator;
	BsonTokenType token_type;
	int			lex_level;
	int			line_number;
	char	   *line_start;
	StringInfo	strval;
} BsonLexContext;

typedef void (*bson_struct_action) (void *state);
typedef void (*bson_ofield_action) (void *state, char *fname, bool isnull);
typedef void (*bson_aelem_action) (void *state, bool isnull);
typedef void (*bson_scalar_action) (void *state, char *token, BsonTokenType tokentype);


/*
 * Semantic Action structure for use in parsing bson.
 * Any of these actions can be NULL, in which case nothing is done at that
 * point, Likewise, semstate can be NULL. Using an all-NULL structure amounts
 * to doing a pure parse with no side-effects, and is therefore exactly
 * what the bson input routines do.
 */
typedef struct bsonSemAction
{
	void	   *semstate;
	bson_struct_action object_start;
	bson_struct_action object_end;
	bson_struct_action array_start;
	bson_struct_action array_end;
	bson_ofield_action object_field_start;
	bson_ofield_action object_field_end;
	bson_aelem_action array_element_start;
	bson_aelem_action array_element_end;
	bson_scalar_action scalar;
}	bsonSemAction, *BsonSemAction;

/*
 * parse_bson will parse the string in the lex calling the
 * action functions in sem at the appropriate points. It is
 * up to them to keep what state they need	in semstate. If they
 * need access to the state of the lexer, then its pointer
 * should be passed to them as a member of whatever semstate
 * points to. If the action pointers are NULL the parser
 * does nothing and just continues.
 */
extern void pg_parse_bson(BsonLexContext *lex, BsonSemAction sem);

/*
 * constructor for BsonLexContext, with or without strval element.
 * If supplied, the strval element will contain a de-escaped version of
 * the lexeme. However, doing this imposes a performance penalty, so
 * it should be avoided if the de-escaped lexeme is not required.
 */
extern BsonLexContext *makeBsonLexContext(text *bson, bool need_escapes);

#endif   /* BSONAPI_H */
