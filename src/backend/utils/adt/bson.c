/*-------------------------------------------------------------------------
 *
 * bson`.c
 *		BSON data type support.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/bson.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/transam.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "parser/parse_coerce.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/bson.h"
#include "utils/bsonapi.h"
#include "utils/typcache.h"
#include "utils/syscache.h"

/*
 * The context of the parser is maintained by the recursive descent
 * mechanism, but is passed explicitly to the error reporting routine
 * for better diagnostics.
 */
typedef enum					/* contexts of BSON parser */
{
	BSON_PARSE_VALUE,			/* expecting a value */
	BSON_PARSE_STRING,			/* expecting a string (for a field name) */
	BSON_PARSE_ARRAY_START,		/* saw '[', expecting value or ']' */
	BSON_PARSE_ARRAY_NEXT,		/* saw array element, expecting ',' or ']' */
	BSON_PARSE_OBJECT_START,	/* saw '{', expecting label or '}' */
	BSON_PARSE_OBJECT_LABEL,	/* saw object label, expecting ':' */
	BSON_PARSE_OBJECT_NEXT,		/* saw object value, expecting ',' or '}' */
	BSON_PARSE_OBJECT_COMMA,	/* saw object ',', expecting next label */
	BSON_PARSE_END				/* saw the end of a document, expect nothing */
}	BsonParseContext;

static inline void bson_lex(BsonLexContext *lex);
static inline void bson_lex_string(BsonLexContext *lex);
static inline void bson_lex_number(BsonLexContext *lex, char *s);
static inline void parse_scalar(BsonLexContext *lex, BsonSemAction sem);
static void parse_object_field(BsonLexContext *lex, BsonSemAction sem);
static void parse_object(BsonLexContext *lex, BsonSemAction sem);
static void parse_array_element(BsonLexContext *lex, BsonSemAction sem);
static void parse_array(BsonLexContext *lex, BsonSemAction sem);
static void report_parse_error(BsonParseContext ctx, BsonLexContext *lex);
static void report_invalid_token(BsonLexContext *lex);
static int	report_bson_context(BsonLexContext *lex);
static char *extract_mb_char(char *s);
static void composite_to_bson(Datum composite, StringInfo result,
				  bool use_line_feeds);
static void array_dim_to_bson(StringInfo result, int dim, int ndims, int *dims,
				  Datum *vals, bool *nulls, int *valcount,
				  TYPCATEGORY tcategory, Oid typoutputfunc,
				  bool use_line_feeds);
static void array_to_bson_internal(Datum array, StringInfo result,
					   bool use_line_feeds);

/* the null action object used for pure validation */
static bsonSemAction nullSemAction =
{
	NULL, NULL, NULL, NULL, NULL,
	NULL, NULL, NULL, NULL, NULL
};
static BsonSemAction NullSemAction = &nullSemAction;

/* Recursive Descent parser support routines */

/*
 * lex_peek
 *
 * what is the current look_ahead token?
*/
static inline BsonTokenType
lex_peek(BsonLexContext *lex)
{
	return lex->token_type;
}

/*
 * lex_accept
 *
 * accept the look_ahead token and move the lexer to the next token if the
 * look_ahead token matches the token parameter. In that case, and if required,
 * also hand back the de-escaped lexeme.
 *
 * returns true if the token matched, false otherwise.
 */
static inline bool
lex_accept(BsonLexContext *lex, BsonTokenType token, char **lexeme)
{
	if (lex->token_type == token)
	{
		if (lexeme != NULL)
		{
			if (lex->token_type == BSON_TOKEN_STRING)
			{
				if (lex->strval != NULL)
					*lexeme = pstrdup(lex->strval->data);
			}
			else
			{
				int			len = (lex->token_terminator - lex->token_start);
				char	   *tokstr = palloc(len + 1);

				memcpy(tokstr, lex->token_start, len);
				tokstr[len] = '\0';
				*lexeme = tokstr;
			}
		}
		bson_lex(lex);
		return true;
	}
	return false;
}

/*
 * lex_accept
 *
 * move the lexer to the next token if the current look_ahead token matches
 * the parameter token. Otherwise, report an error.
 */
static inline void
lex_expect(BsonParseContext ctx, BsonLexContext *lex, BsonTokenType token)
{
	if (!lex_accept(lex, token, NULL))
		report_parse_error(ctx, lex);;
}

/*
 * All the defined	type categories are upper case , so use lower case here
 * so we avoid any possible clash.
 */
/* fake type category for BSON so we can distinguish it in datum_to_bson */
#define TYPCATEGORY_BSON 'j'
/* fake category for types that have a cast to bson */
#define TYPCATEGORY_BSON_CAST 'c'
/* letters appearing in numeric output that aren't valid in a BSON number */
#define NON_NUMERIC_LETTER "NnAaIiFfTtYy"
/* chars to consider as part of an alphanumeric token */
#define BSON_ALPHANUMERIC_CHAR(c)  \
	(((c) >= 'a' && (c) <= 'z') || \
	 ((c) >= 'A' && (c) <= 'Z') || \
	 ((c) >= '0' && (c) <= '9') || \
	 (c) == '_' || \
	 IS_HIGHBIT_SET(c))

/*
 * Input.
 */
Datum
bson_in(PG_FUNCTION_ARGS)
{
	char	   *bson = PG_GETARG_CSTRING(0);
	text	   *result = cstring_to_text(bson);
	BsonLexContext *lex;

	/* validate it */
	lex = makeBsonLexContext(result, false);
	pg_parse_bson(lex, NullSemAction);

	/* Internal representation is the same as text, for now */
	PG_RETURN_TEXT_P(result);
}

/*
 * Output.
 */
Datum
bson_out(PG_FUNCTION_ARGS)
{
	/* we needn't detoast because text_to_cstring will handle that */
	Datum		txt = PG_GETARG_DATUM(0);

	PG_RETURN_CSTRING(TextDatumGetCString(txt));
}

/*
 * Binary send.
 */
Datum
bson_send(PG_FUNCTION_ARGS)
{
	text	   *t = PG_GETARG_TEXT_PP(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendtext(&buf, VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * Binary receive.
 */
Datum
bson_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	text	   *result;
	char	   *str;
	int			nbytes;
	BsonLexContext *lex;

	str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);

	result = palloc(nbytes + VARHDRSZ);
	SET_VARSIZE(result, nbytes + VARHDRSZ);
	memcpy(VARDATA(result), str, nbytes);

	/* Validate it. */
	lex = makeBsonLexContext(result, false);
	pg_parse_bson(lex, NullSemAction);

	PG_RETURN_TEXT_P(result);
}

/*
 * makeBsonLexContext
 *
 * lex constructor, with or without StringInfo object
 * for de-escaped lexemes.
 *
 * Without is better as it makes the processing faster, so only make one
 * if really required.
 */
BsonLexContext *
makeBsonLexContext(text *bson, bool need_escapes)
{
	BsonLexContext *lex = palloc0(sizeof(BsonLexContext));

	lex->input = lex->token_terminator = lex->line_start = VARDATA(bson);
	lex->line_number = 1;
	lex->input_length = VARSIZE(bson) - VARHDRSZ;
	if (need_escapes)
		lex->strval = makeStringInfo();
	return lex;
}

/*
 * pg_parse_bson
 *
 * Publicly visible entry point for the BSON parser.
 *
 * lex is a lexing context, set up for the bson to be processed by calling
 * makeBsonLexContext(). sem is a strucure of function pointers to semantic
 * action routines to be called at appropriate spots during parsing, and a
 * pointer to a state object to be passed to those routines.
 */
void
pg_parse_bson(BsonLexContext *lex, BsonSemAction sem)
{
	BsonTokenType tok;

	/* get the initial token */
	bson_lex(lex);

	tok = lex_peek(lex);

	/* parse by recursive descent */
	switch (tok)
	{
		case BSON_TOKEN_OBJECT_START:
			parse_object(lex, sem);
			break;
		case BSON_TOKEN_ARRAY_START:
			parse_array(lex, sem);
			break;
		default:
			parse_scalar(lex, sem);		/* bson can be a bare scalar */
	}

	lex_expect(BSON_PARSE_END, lex, BSON_TOKEN_END);

}

/*
 *	Recursive Descent parse routines. There is one for each structural
 *	element in a bson document:
 *	  - scalar (string, number, true, false, null)
 *	  - array  ( [ ] )
 *	  - array element
 *	  - object ( { } )
 *	  - object field
 */
static inline void
parse_scalar(BsonLexContext *lex, BsonSemAction sem)
{
	char	   *val = NULL;
	bson_scalar_action sfunc = sem->scalar;
	char	  **valaddr;
	BsonTokenType tok = lex_peek(lex);

	valaddr = sfunc == NULL ? NULL : &val;

	/* a scalar must be a string, a number, true, false, or null */
	switch (tok)
	{
		case BSON_TOKEN_TRUE:
			lex_accept(lex, BSON_TOKEN_TRUE, valaddr);
			break;
		case BSON_TOKEN_FALSE:
			lex_accept(lex, BSON_TOKEN_FALSE, valaddr);
			break;
		case BSON_TOKEN_NULL:
			lex_accept(lex, BSON_TOKEN_NULL, valaddr);
			break;
		case BSON_TOKEN_NUMBER:
			lex_accept(lex, BSON_TOKEN_NUMBER, valaddr);
			break;
		case BSON_TOKEN_STRING:
			lex_accept(lex, BSON_TOKEN_STRING, valaddr);
			break;
		default:
			report_parse_error(BSON_PARSE_VALUE, lex);
	}

	if (sfunc != NULL)
		(*sfunc) (sem->semstate, val, tok);
}

static void
parse_object_field(BsonLexContext *lex, BsonSemAction sem)
{
	/*
	 * an object field is "fieldname" : value where value can be a scalar,
	 * object or array
	 */

	char	   *fname = NULL;	/* keep compiler quiet */
	bson_ofield_action ostart = sem->object_field_start;
	bson_ofield_action oend = sem->object_field_end;
	bool		isnull;
	char	  **fnameaddr = NULL;
	BsonTokenType tok;

	if (ostart != NULL || oend != NULL)
		fnameaddr = &fname;

	if (!lex_accept(lex, BSON_TOKEN_STRING, fnameaddr))
		report_parse_error(BSON_PARSE_STRING, lex);

	lex_expect(BSON_PARSE_OBJECT_LABEL, lex, BSON_TOKEN_COLON);

	tok = lex_peek(lex);
	isnull = tok == BSON_TOKEN_NULL;

	if (ostart != NULL)
		(*ostart) (sem->semstate, fname, isnull);

	switch (tok)
	{
		case BSON_TOKEN_OBJECT_START:
			parse_object(lex, sem);
			break;
		case BSON_TOKEN_ARRAY_START:
			parse_array(lex, sem);
			break;
		default:
			parse_scalar(lex, sem);
	}

	if (oend != NULL)
		(*oend) (sem->semstate, fname, isnull);

	if (fname != NULL)
		pfree(fname);
}

static void
parse_object(BsonLexContext *lex, BsonSemAction sem)
{
	/*
	 * an object is a possibly empty sequence of object fields, separated by
	 * commas and surrounde by curly braces.
	 */
	bson_struct_action ostart = sem->object_start;
	bson_struct_action oend = sem->object_end;
	BsonTokenType tok;

	if (ostart != NULL)
		(*ostart) (sem->semstate);

	/*
	 * Data inside an object at at a higher nesting level than the object
	 * itself. Note that we increment this after we call the semantic routine
	 * for the object start and restore it before we call the routine for the
	 * object end.
	 */
	lex->lex_level++;

	/* we know this will succeeed, just clearing the token */
	lex_expect(BSON_PARSE_OBJECT_START, lex, BSON_TOKEN_OBJECT_START);

	tok = lex_peek(lex);
	switch (tok)
	{
		case BSON_TOKEN_STRING:
			parse_object_field(lex, sem);
			while (lex_accept(lex, BSON_TOKEN_COMMA, NULL))
				parse_object_field(lex, sem);
			break;
		case BSON_TOKEN_OBJECT_END:
			break;
		default:
			/* case of an invalid initial token inside the object */
			report_parse_error(BSON_PARSE_OBJECT_START, lex);
	}

	lex_expect(BSON_PARSE_OBJECT_NEXT, lex, BSON_TOKEN_OBJECT_END);

	lex->lex_level--;

	if (oend != NULL)
		(*oend) (sem->semstate);
}

static void
parse_array_element(BsonLexContext *lex, BsonSemAction sem)
{
	bson_aelem_action astart = sem->array_element_start;
	bson_aelem_action aend = sem->array_element_end;
	BsonTokenType tok = lex_peek(lex);

	bool		isnull;

	isnull = tok == BSON_TOKEN_NULL;

	if (astart != NULL)
		(*astart) (sem->semstate, isnull);

	/* an array element is any object, array or scalar */
	switch (tok)
	{
		case BSON_TOKEN_OBJECT_START:
			parse_object(lex, sem);
			break;
		case BSON_TOKEN_ARRAY_START:
			parse_array(lex, sem);
			break;
		default:
			parse_scalar(lex, sem);
	}

	if (aend != NULL)
		(*aend) (sem->semstate, isnull);
}

static void
parse_array(BsonLexContext *lex, BsonSemAction sem)
{
	/*
	 * an array is a possibly empty sequence of array elements, separated by
	 * commas and surrounded by square brackets.
	 */
	bson_struct_action astart = sem->array_start;
	bson_struct_action aend = sem->array_end;

	if (astart != NULL)
		(*astart) (sem->semstate);

	/*
	 * Data inside an array at at a higher nesting level than the array
	 * itself. Note that we increment this after we call the semantic routine
	 * for the array start and restore it before we call the routine for the
	 * array end.
	 */
	lex->lex_level++;

	lex_expect(BSON_PARSE_ARRAY_START, lex, BSON_TOKEN_ARRAY_START);
	if (lex_peek(lex) != BSON_TOKEN_ARRAY_END)
	{

		parse_array_element(lex, sem);

		while (lex_accept(lex, BSON_TOKEN_COMMA, NULL))
			parse_array_element(lex, sem);
	}

	lex_expect(BSON_PARSE_ARRAY_NEXT, lex, BSON_TOKEN_ARRAY_END);

	lex->lex_level--;

	if (aend != NULL)
		(*aend) (sem->semstate);
}

/*
 * Lex one token from the input stream.
 */
static inline void
bson_lex(BsonLexContext *lex)
{
	char	   *s;
	int			len;

	/* Skip leading whitespace. */
	s = lex->token_terminator;
	len = s - lex->input;
	while (len < lex->input_length &&
		   (*s == ' ' || *s == '\t' || *s == '\n' || *s == '\r'))
	{
		if (*s == '\n')
			++lex->line_number;
		++s;
		++len;
	}
	lex->token_start = s;

	/* Determine token type. */
	if (len >= lex->input_length)
	{
		lex->token_start = NULL;
		lex->prev_token_terminator = lex->token_terminator;
		lex->token_terminator = s;
		lex->token_type = BSON_TOKEN_END;
	}
	else
		switch (*s)
		{
				/* Single-character token, some kind of punctuation mark. */
			case '{':
				lex->prev_token_terminator = lex->token_terminator;
				lex->token_terminator = s + 1;
				lex->token_type = BSON_TOKEN_OBJECT_START;
				break;
			case '}':
				lex->prev_token_terminator = lex->token_terminator;
				lex->token_terminator = s + 1;
				lex->token_type = BSON_TOKEN_OBJECT_END;
				break;
			case '[':
				lex->prev_token_terminator = lex->token_terminator;
				lex->token_terminator = s + 1;
				lex->token_type = BSON_TOKEN_ARRAY_START;
				break;
			case ']':
				lex->prev_token_terminator = lex->token_terminator;
				lex->token_terminator = s + 1;
				lex->token_type = BSON_TOKEN_ARRAY_END;
				break;
			case ',':
				lex->prev_token_terminator = lex->token_terminator;
				lex->token_terminator = s + 1;
				lex->token_type = BSON_TOKEN_COMMA;
				break;
			case ':':
				lex->prev_token_terminator = lex->token_terminator;
				lex->token_terminator = s + 1;
				lex->token_type = BSON_TOKEN_COLON;
				break;
			case '"':
				/* string */
				bson_lex_string(lex);
				lex->token_type = BSON_TOKEN_STRING;
				break;
			case '-':
				/* Negative number. */
				bson_lex_number(lex, s + 1);
				lex->token_type = BSON_TOKEN_NUMBER;
				break;
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
				/* Positive number. */
				bson_lex_number(lex, s);
				lex->token_type = BSON_TOKEN_NUMBER;
				break;
			default:
				{
					char	   *p;

					/*
					 * We're not dealing with a string, number, legal
					 * punctuation mark, or end of string.	The only legal
					 * tokens we might find here are true, false, and null,
					 * but for error reporting purposes we scan until we see a
					 * non-alphanumeric character.	That way, we can report
					 * the whole word as an unexpected token, rather than just
					 * some unintuitive prefix thereof.
					 */
					for (p = s; BSON_ALPHANUMERIC_CHAR(*p) && p - s < lex->input_length - len; p++)
						 /* skip */ ;

					/*
					 * We got some sort of unexpected punctuation or an
					 * otherwise unexpected character, so just complain about
					 * that one character.
					 */
					if (p == s)
					{
						lex->prev_token_terminator = lex->token_terminator;
						lex->token_terminator = s + 1;
						report_invalid_token(lex);
					}

					/*
					 * We've got a real alphanumeric token here.  If it
					 * happens to be true, false, or null, all is well.  If
					 * not, error out.
					 */
					lex->prev_token_terminator = lex->token_terminator;
					lex->token_terminator = p;
					if (p - s == 4)
					{
						if (memcmp(s, "true", 4) == 0)
							lex->token_type = BSON_TOKEN_TRUE;
						else if (memcmp(s, "null", 4) == 0)
							lex->token_type = BSON_TOKEN_NULL;
						else
							report_invalid_token(lex);
					}
					else if (p - s == 5 && memcmp(s, "false", 5) == 0)
						lex->token_type = BSON_TOKEN_FALSE;
					else
						report_invalid_token(lex);

				}
		}						/* end of switch */
}

/*
 * The next token in the input stream is known to be a string; lex it.
 */
static inline void
bson_lex_string(BsonLexContext *lex)
{
	char	   *s;
	int			len;

	if (lex->strval != NULL)
		resetStringInfo(lex->strval);

	len = lex->token_start - lex->input;
	len++;
	for (s = lex->token_start + 1; *s != '"'; s++, len++)
	{
		/* Premature end of the string. */
		if (len >= lex->input_length)
		{
			lex->token_terminator = s;
			report_invalid_token(lex);
		}
		else if ((unsigned char) *s < 32)
		{
			/* Per RFC4627, these characters MUST be escaped. */
			/* Since *s isn't printable, exclude it from the context string */
			lex->token_terminator = s;
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type bson"),
					 errdetail("Character with value 0x%02x must be escaped.",
							   (unsigned char) *s),
					 report_bson_context(lex)));
		}
		else if (*s == '\\')
		{
			/* OK, we have an escape character. */
			s++;
			len++;
			if (len >= lex->input_length)
			{
				lex->token_terminator = s;
				report_invalid_token(lex);
			}
			else if (*s == 'u')
			{
				int			i;
				int			ch = 0;

				for (i = 1; i <= 4; i++)
				{
					s++;
					len++;
					if (len >= lex->input_length)
					{
						lex->token_terminator = s;
						report_invalid_token(lex);
					}
					else if (*s >= '0' && *s <= '9')
						ch = (ch * 16) + (*s - '0');
					else if (*s >= 'a' && *s <= 'f')
						ch = (ch * 16) + (*s - 'a') + 10;
					else if (*s >= 'A' && *s <= 'F')
						ch = (ch * 16) + (*s - 'A') + 10;
					else
					{
						lex->token_terminator = s + pg_mblen(s);
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
								 errmsg("invalid input syntax for type bson"),
								 errdetail("\"\\u\" must be followed by four hexadecimal digits."),
								 report_bson_context(lex)));
					}
				}
				if (lex->strval != NULL)
				{
					char		utf8str[5];
					int			utf8len;
					char	   *converted;

					unicode_to_utf8(ch, (unsigned char *) utf8str);
					utf8len = pg_utf_mblen((unsigned char *) utf8str);
					utf8str[utf8len] = '\0';
					converted = pg_any_to_server(utf8str, 1, PG_UTF8);
					appendStringInfoString(lex->strval, converted);
					if (converted != utf8str)
						pfree(converted);

				}
			}
			else if (lex->strval != NULL)
			{
				switch (*s)
				{
					case '"':
					case '\\':
					case '/':
						appendStringInfoChar(lex->strval, *s);
						break;
					case 'b':
						appendStringInfoChar(lex->strval, '\b');
						break;
					case 'f':
						appendStringInfoChar(lex->strval, '\f');
						break;
					case 'n':
						appendStringInfoChar(lex->strval, '\n');
						break;
					case 'r':
						appendStringInfoChar(lex->strval, '\r');
						break;
					case 't':
						appendStringInfoChar(lex->strval, '\t');
						break;
					default:
						/* Not a valid string escape, so error out. */
						lex->token_terminator = s + pg_mblen(s);
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
								 errmsg("invalid input syntax for type bson"),
							errdetail("Escape sequence \"\\%s\" is invalid.",
									  extract_mb_char(s)),
								 report_bson_context(lex)));
				}
			}
			else if (strchr("\"\\/bfnrt", *s) == NULL)
			{
				/*
				 * Simpler processing if we're not bothered about de-escaping
				 *
				 * It's very tempting to remove the strchr() call here and
				 * replace it with a switch statement, but testing so far has
				 * shown it's not a performance win.
				 */
				lex->token_terminator = s + pg_mblen(s);
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type bson"),
						 errdetail("Escape sequence \"\\%s\" is invalid.",
								   extract_mb_char(s)),
						 report_bson_context(lex)));
			}

		}
		else if (lex->strval != NULL)
		{
			appendStringInfoChar(lex->strval, *s);
		}

	}

	/* Hooray, we found the end of the string! */
	lex->prev_token_terminator = lex->token_terminator;
	lex->token_terminator = s + 1;
}

/*-------------------------------------------------------------------------
 * The next token in the input stream is known to be a number; lex it.
 *
 * In BSON, a number consists of four parts:
 *
 * (1) An optional minus sign ('-').
 *
 * (2) Either a single '0', or a string of one or more digits that does not
 *	   begin with a '0'.
 *
 * (3) An optional decimal part, consisting of a period ('.') followed by
 *	   one or more digits.	(Note: While this part can be omitted
 *	   completely, it's not OK to have only the decimal point without
 *	   any digits afterwards.)
 *
 * (4) An optional exponent part, consisting of 'e' or 'E', optionally
 *	   followed by '+' or '-', followed by one or more digits.	(Note:
 *	   As with the decimal part, if 'e' or 'E' is present, it must be
 *	   followed by at least one digit.)
 *
 * The 's' argument to this function points to the ostensible beginning
 * of part 2 - i.e. the character after any optional minus sign, and the
 * first character of the string if there is none.
 *
 *-------------------------------------------------------------------------
 */
static inline void
bson_lex_number(BsonLexContext *lex, char *s)
{
	bool		error = false;
	char	   *p;
	int			len;

	len = s - lex->input;
	/* Part (1): leading sign indicator. */
	/* Caller already did this for us; so do nothing. */

	/* Part (2): parse main digit string. */
	if (*s == '0')
	{
		s++;
		len++;
	}
	else if (*s >= '1' && *s <= '9')
	{
		do
		{
			s++;
			len++;
		} while (*s >= '0' && *s <= '9' && len < lex->input_length);
	}
	else
		error = true;

	/* Part (3): parse optional decimal portion. */
	if (len < lex->input_length && *s == '.')
	{
		s++;
		len++;
		if (len == lex->input_length || *s < '0' || *s > '9')
			error = true;
		else
		{
			do
			{
				s++;
				len++;
			} while (*s >= '0' && *s <= '9' && len < lex->input_length);
		}
	}

	/* Part (4): parse optional exponent. */
	if (len < lex->input_length && (*s == 'e' || *s == 'E'))
	{
		s++;
		len++;
		if (len < lex->input_length && (*s == '+' || *s == '-'))
		{
			s++;
			len++;
		}
		if (len == lex->input_length || *s < '0' || *s > '9')
			error = true;
		else
		{
			do
			{
				s++;
				len++;
			} while (len < lex->input_length && *s >= '0' && *s <= '9');
		}
	}

	/*
	 * Check for trailing garbage.	As in bson_lex(), any alphanumeric stuff
	 * here should be considered part of the token for error-reporting
	 * purposes.
	 */
	for (p = s; BSON_ALPHANUMERIC_CHAR(*p) && len < lex->input_length; p++, len++)
		error = true;
	lex->prev_token_terminator = lex->token_terminator;
	lex->token_terminator = p;
	if (error)
		report_invalid_token(lex);
}

/*
 * Report a parse error.
 *
 * lex->token_start and lex->token_terminator must identify the current token.
 */
static void
report_parse_error(BsonParseContext ctx, BsonLexContext *lex)
{
	char	   *token;
	int			toklen;

	/* Handle case where the input ended prematurely. */
	if (lex->token_start == NULL || lex->token_type == BSON_TOKEN_END)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type bson"),
				 errdetail("The input string ended unexpectedly."),
				 report_bson_context(lex)));

	/* Separate out the current token. */
	toklen = lex->token_terminator - lex->token_start;
	token = palloc(toklen + 1);
	memcpy(token, lex->token_start, toklen);
	token[toklen] = '\0';

	/* Complain, with the appropriate detail message. */
	if (ctx == BSON_PARSE_END)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type bson"),
				 errdetail("Expected end of input, but found \"%s\".",
						   token),
				 report_bson_context(lex)));
	else
	{
		switch (ctx)
		{
			case BSON_PARSE_VALUE:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type bson"),
						 errdetail("Expected BSON value, but found \"%s\".",
								   token),
						 report_bson_context(lex)));
				break;
			case BSON_PARSE_STRING:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type bson"),
						 errdetail("Expected string, but found \"%s\".",
								   token),
						 report_bson_context(lex)));
				break;
			case BSON_PARSE_ARRAY_START:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type bson"),
						 errdetail("Expected array element or \"]\", but found \"%s\".",
								   token),
						 report_bson_context(lex)));
				break;
			case BSON_PARSE_ARRAY_NEXT:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type bson"),
					  errdetail("Expected \",\" or \"]\", but found \"%s\".",
								token),
						 report_bson_context(lex)));
				break;
			case BSON_PARSE_OBJECT_START:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type bson"),
					 errdetail("Expected string or \"}\", but found \"%s\".",
							   token),
						 report_bson_context(lex)));
				break;
			case BSON_PARSE_OBJECT_LABEL:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type bson"),
						 errdetail("Expected \":\", but found \"%s\".",
								   token),
						 report_bson_context(lex)));
				break;
			case BSON_PARSE_OBJECT_NEXT:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type bson"),
					  errdetail("Expected \",\" or \"}\", but found \"%s\".",
								token),
						 report_bson_context(lex)));
				break;
			case BSON_PARSE_OBJECT_COMMA:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type bson"),
						 errdetail("Expected string, but found \"%s\".",
								   token),
						 report_bson_context(lex)));
				break;
			default:
				elog(ERROR, "unexpected bson parse state: %d", ctx);
		}
	}
}

/*
 * Report an invalid input token.
 *
 * lex->token_start and lex->token_terminator must identify the token.
 */
static void
report_invalid_token(BsonLexContext *lex)
{
	char	   *token;
	int			toklen;

	/* Separate out the offending token. */
	toklen = lex->token_terminator - lex->token_start;
	token = palloc(toklen + 1);
	memcpy(token, lex->token_start, toklen);
	token[toklen] = '\0';

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type bson"),
			 errdetail("Token \"%s\" is invalid.", token),
			 report_bson_context(lex)));
}

/*
 * Report a CONTEXT line for bogus BSON input.
 *
 * lex->token_terminator must be set to identify the spot where we detected
 * the error.  Note that lex->token_start might be NULL, in case we recognized
 * error at EOF.
 *
 * The return value isn't meaningful, but we make it non-void so that this
 * can be invoked inside ereport().
 */
static int
report_bson_context(BsonLexContext *lex)
{
	const char *context_start;
	const char *context_end;
	const char *line_start;
	int			line_number;
	char	   *ctxt;
	int			ctxtlen;
	const char *prefix;
	const char *suffix;

	/* Choose boundaries for the part of the input we will display */
	context_start = lex->input;
	context_end = lex->token_terminator;
	line_start = context_start;
	line_number = 1;
	for (;;)
	{
		/* Always advance over newlines (context_end test is just paranoia) */
		if (*context_start == '\n' && context_start < context_end)
		{
			context_start++;
			line_start = context_start;
			line_number++;
			continue;
		}
		/* Otherwise, done as soon as we are close enough to context_end */
		if (context_end - context_start < 50)
			break;
		/* Advance to next multibyte character */
		if (IS_HIGHBIT_SET(*context_start))
			context_start += pg_mblen(context_start);
		else
			context_start++;
	}

	/*
	 * We add "..." to indicate that the excerpt doesn't start at the
	 * beginning of the line ... but if we're within 3 characters of the
	 * beginning of the line, we might as well just show the whole line.
	 */
	if (context_start - line_start <= 3)
		context_start = line_start;

	/* Get a null-terminated copy of the data to present */
	ctxtlen = context_end - context_start;
	ctxt = palloc(ctxtlen + 1);
	memcpy(ctxt, context_start, ctxtlen);
	ctxt[ctxtlen] = '\0';

	/*
	 * Show the context, prefixing "..." if not starting at start of line, and
	 * suffixing "..." if not ending at end of line.
	 */
	prefix = (context_start > line_start) ? "..." : "";
	suffix = (lex->token_type != BSON_TOKEN_END && context_end - lex->input < lex->input_length && *context_end != '\n' && *context_end != '\r') ? "..." : "";

	return errcontext("BSON data, line %d: %s%s%s",
					  line_number, prefix, ctxt, suffix);
}

/*
 * Extract a single, possibly multi-byte char from the input string.
 */
static char *
extract_mb_char(char *s)
{
	char	   *res;
	int			len;

	len = pg_mblen(s);
	res = palloc(len + 1);
	memcpy(res, s, len);
	res[len] = '\0';

	return res;
}

/*
 * Turn a scalar Datum into BSON, appending the string to "result".
 *
 * Hand off a non-scalar datum to composite_to_bson or array_to_bson_internal
 * as appropriate.
 */
static void
datum_to_bson(Datum val, bool is_null, StringInfo result,
			  TYPCATEGORY tcategory, Oid typoutputfunc)
{
	char	   *outputstr;
	text	   *bsontext;

	if (is_null)
	{
		appendStringInfoString(result, "null");
		return;
	}

	switch (tcategory)
	{
		case TYPCATEGORY_ARRAY:
			array_to_bson_internal(val, result, false);
			break;
		case TYPCATEGORY_COMPOSITE:
			composite_to_bson(val, result, false);
			break;
		case TYPCATEGORY_BOOLEAN:
			if (DatumGetBool(val))
				appendStringInfoString(result, "true");
			else
				appendStringInfoString(result, "false");
			break;
		case TYPCATEGORY_NUMERIC:
			outputstr = OidOutputFunctionCall(typoutputfunc, val);

			/*
			 * Don't call escape_bson here if it's a valid BSON number.
			 * Numeric output should usually be a valid BSON number and BSON
			 * numbers shouldn't be quoted. Quote cases like "Nan" and
			 * "Infinity", however.
			 */
			if (strpbrk(outputstr, NON_NUMERIC_LETTER) == NULL)
				appendStringInfoString(result, outputstr);
			else
				escape_bson(result, outputstr);
			pfree(outputstr);
			break;
		case TYPCATEGORY_BSON:
			/* BSON will already be escaped */
			outputstr = OidOutputFunctionCall(typoutputfunc, val);
			appendStringInfoString(result, outputstr);
			pfree(outputstr);
			break;
		case TYPCATEGORY_BSON_CAST:
			bsontext = DatumGetTextP(OidFunctionCall1(typoutputfunc, val));
			outputstr = text_to_cstring(bsontext);
			appendStringInfoString(result, outputstr);
			pfree(outputstr);
			pfree(bsontext);
			break;
		default:
			outputstr = OidOutputFunctionCall(typoutputfunc, val);
			escape_bson(result, outputstr);
			pfree(outputstr);
			break;
	}
}

/*
 * Process a single dimension of an array.
 * If it's the innermost dimension, output the values, otherwise call
 * ourselves recursively to process the next dimension.
 */
static void
array_dim_to_bson(StringInfo result, int dim, int ndims, int *dims, Datum *vals,
				  bool *nulls, int *valcount, TYPCATEGORY tcategory,
				  Oid typoutputfunc, bool use_line_feeds)
{
	int			i;
	const char *sep;

	Assert(dim < ndims);

	sep = use_line_feeds ? ",\n " : ",";

	appendStringInfoChar(result, '[');

	for (i = 1; i <= dims[dim]; i++)
	{
		if (i > 1)
			appendStringInfoString(result, sep);

		if (dim + 1 == ndims)
		{
			datum_to_bson(vals[*valcount], nulls[*valcount], result, tcategory,
						  typoutputfunc);
			(*valcount)++;
		}
		else
		{
			/*
			 * Do we want line feeds on inner dimensions of arrays? For now
			 * we'll say no.
			 */
			array_dim_to_bson(result, dim + 1, ndims, dims, vals, nulls,
							  valcount, tcategory, typoutputfunc, false);
		}
	}

	appendStringInfoChar(result, ']');
}

/*
 * Turn an array into BSON.
 */
static void
array_to_bson_internal(Datum array, StringInfo result, bool use_line_feeds)
{
	ArrayType  *v = DatumGetArrayTypeP(array);
	Oid			element_type = ARR_ELEMTYPE(v);
	int		   *dim;
	int			ndim;
	int			nitems;
	int			count = 0;
	Datum	   *elements;
	bool	   *nulls;
	int16		typlen;
	bool		typbyval;
	char		typalign,
				typdelim;
	Oid			typioparam;
	Oid			typoutputfunc;
	TYPCATEGORY tcategory;
	Oid			castfunc = InvalidOid;

	ndim = ARR_NDIM(v);
	dim = ARR_DIMS(v);
	nitems = ArrayGetNItems(ndim, dim);

	if (nitems <= 0)
	{
		appendStringInfoString(result, "[]");
		return;
	}

	get_type_io_data(element_type, IOFunc_output,
					 &typlen, &typbyval, &typalign,
					 &typdelim, &typioparam, &typoutputfunc);

	if (element_type > FirstNormalObjectId)
	{
		HeapTuple	tuple;
		Form_pg_cast castForm;

		tuple = SearchSysCache2(CASTSOURCETARGET,
								ObjectIdGetDatum(element_type),
								ObjectIdGetDatum(BSONOID));
		if (HeapTupleIsValid(tuple))
		{
			castForm = (Form_pg_cast) GETSTRUCT(tuple);

			if (castForm->castmethod == COERCION_METHOD_FUNCTION)
				castfunc = typoutputfunc = castForm->castfunc;

			ReleaseSysCache(tuple);
		}
	}

	deconstruct_array(v, element_type, typlen, typbyval,
					  typalign, &elements, &nulls,
					  &nitems);

	if (castfunc != InvalidOid)
		tcategory = TYPCATEGORY_BSON_CAST;
	else if (element_type == RECORDOID)
		tcategory = TYPCATEGORY_COMPOSITE;
	else if (element_type == BSONOID)
		tcategory = TYPCATEGORY_BSON;
	else
		tcategory = TypeCategory(element_type);

	array_dim_to_bson(result, 0, ndim, dim, elements, nulls, &count, tcategory,
					  typoutputfunc, use_line_feeds);

	pfree(elements);
	pfree(nulls);
}

/*
 * Turn a composite / record into BSON.
 */
static void
composite_to_bson(Datum composite, StringInfo result, bool use_line_feeds)
{
	HeapTupleHeader td;
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupdesc;
	HeapTupleData tmptup,
			   *tuple;
	int			i;
	bool		needsep = false;
	const char *sep;

	sep = use_line_feeds ? ",\n " : ",";

	td = DatumGetHeapTupleHeader(composite);

	/* Extract rowtype info and find a tupdesc */
	tupType = HeapTupleHeaderGetTypeId(td);
	tupTypmod = HeapTupleHeaderGetTypMod(td);
	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

	/* Build a temporary HeapTuple control structure */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
	tmptup.t_data = td;
	tuple = &tmptup;

	appendStringInfoChar(result, '{');

	for (i = 0; i < tupdesc->natts; i++)
	{
		Datum		val,
					origval;
		bool		isnull;
		char	   *attname;
		TYPCATEGORY tcategory;
		Oid			typoutput;
		bool		typisvarlena;
		Oid			castfunc = InvalidOid;

		if (tupdesc->attrs[i]->attisdropped)
			continue;

		if (needsep)
			appendStringInfoString(result, sep);
		needsep = true;

		attname = NameStr(tupdesc->attrs[i]->attname);
		escape_bson(result, attname);
		appendStringInfoChar(result, ':');

		origval = heap_getattr(tuple, i + 1, tupdesc, &isnull);

		getTypeOutputInfo(tupdesc->attrs[i]->atttypid,
						  &typoutput, &typisvarlena);

		if (tupdesc->attrs[i]->atttypid > FirstNormalObjectId)
		{
			HeapTuple	cast_tuple;
			Form_pg_cast castForm;

			cast_tuple = SearchSysCache2(CASTSOURCETARGET,
							   ObjectIdGetDatum(tupdesc->attrs[i]->atttypid),
										 ObjectIdGetDatum(BSONOID));
			if (HeapTupleIsValid(cast_tuple))
			{
				castForm = (Form_pg_cast) GETSTRUCT(cast_tuple);

				if (castForm->castmethod == COERCION_METHOD_FUNCTION)
					castfunc = typoutput = castForm->castfunc;

				ReleaseSysCache(cast_tuple);
			}
		}

		if (castfunc != InvalidOid)
			tcategory = TYPCATEGORY_BSON_CAST;
		else if (tupdesc->attrs[i]->atttypid == RECORDARRAYOID)
			tcategory = TYPCATEGORY_ARRAY;
		else if (tupdesc->attrs[i]->atttypid == RECORDOID)
			tcategory = TYPCATEGORY_COMPOSITE;
		else if (tupdesc->attrs[i]->atttypid == BSONOID)
			tcategory = TYPCATEGORY_BSON;
		else
			tcategory = TypeCategory(tupdesc->attrs[i]->atttypid);

		/*
		 * If we have a toasted datum, forcibly detoast it here to avoid
		 * memory leakage inside the type's output routine.
		 */
		if (typisvarlena && !isnull)
			val = PointerGetDatum(PG_DETOAST_DATUM(origval));
		else
			val = origval;

		datum_to_bson(val, isnull, result, tcategory, typoutput);

		/* Clean up detoasted copy, if any */
		if (val != origval)
			pfree(DatumGetPointer(val));
	}

	appendStringInfoChar(result, '}');
	ReleaseTupleDesc(tupdesc);
}

/*
 * SQL function array_to_bson(row)
 */
extern Datum
array_to_bson(PG_FUNCTION_ARGS)
{
	Datum		array = PG_GETARG_DATUM(0);
	StringInfo	result;

	result = makeStringInfo();

	array_to_bson_internal(array, result, false);

	PG_RETURN_TEXT_P(cstring_to_text(result->data));
}

/*
 * SQL function array_to_bson(row, prettybool)
 */
extern Datum
array_to_bson_pretty(PG_FUNCTION_ARGS)
{
	Datum		array = PG_GETARG_DATUM(0);
	bool		use_line_feeds = PG_GETARG_BOOL(1);
	StringInfo	result;

	result = makeStringInfo();

	array_to_bson_internal(array, result, use_line_feeds);

	PG_RETURN_TEXT_P(cstring_to_text(result->data));
}

/*
 * SQL function row_to_bson(row)
 */
extern Datum
row_to_bson(PG_FUNCTION_ARGS)
{
	Datum		array = PG_GETARG_DATUM(0);
	StringInfo	result;

	result = makeStringInfo();

	composite_to_bson(array, result, false);

	PG_RETURN_TEXT_P(cstring_to_text(result->data));
}

/*
 * SQL function row_to_bson(row, prettybool)
 */
extern Datum
row_to_bson_pretty(PG_FUNCTION_ARGS)
{
	Datum		array = PG_GETARG_DATUM(0);
	bool		use_line_feeds = PG_GETARG_BOOL(1);
	StringInfo	result;

	result = makeStringInfo();

	composite_to_bson(array, result, use_line_feeds);

	PG_RETURN_TEXT_P(cstring_to_text(result->data));
}

/*
 * SQL function to_bson(anyvalue)
 */
Datum
to_bson(PG_FUNCTION_ARGS)
{
	Oid			val_type = get_fn_expr_argtype(fcinfo->flinfo, 0);
	StringInfo	result;
	Datum		orig_val,
				val;
	TYPCATEGORY tcategory;
	Oid			typoutput;
	bool		typisvarlena;
	Oid			castfunc = InvalidOid;

	if (val_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));


	result = makeStringInfo();

	orig_val = PG_ARGISNULL(0) ? (Datum) 0 : PG_GETARG_DATUM(0);

	getTypeOutputInfo(val_type, &typoutput, &typisvarlena);

	if (val_type > FirstNormalObjectId)
	{
		HeapTuple	tuple;
		Form_pg_cast castForm;

		tuple = SearchSysCache2(CASTSOURCETARGET,
								ObjectIdGetDatum(val_type),
								ObjectIdGetDatum(BSONOID));
		if (HeapTupleIsValid(tuple))
		{
			castForm = (Form_pg_cast) GETSTRUCT(tuple);

			if (castForm->castmethod == COERCION_METHOD_FUNCTION)
				castfunc = typoutput = castForm->castfunc;

			ReleaseSysCache(tuple);
		}
	}

	if (castfunc != InvalidOid)
		tcategory = TYPCATEGORY_BSON_CAST;
	else if (val_type == RECORDARRAYOID)
		tcategory = TYPCATEGORY_ARRAY;
	else if (val_type == RECORDOID)
		tcategory = TYPCATEGORY_COMPOSITE;
	else if (val_type == BSONOID)
		tcategory = TYPCATEGORY_BSON;
	else
		tcategory = TypeCategory(val_type);

	/*
	 * If we have a toasted datum, forcibly detoast it here to avoid memory
	 * leakage inside the type's output routine.
	 */
	if (typisvarlena && orig_val != (Datum) 0)
		val = PointerGetDatum(PG_DETOAST_DATUM(orig_val));
	else
		val = orig_val;

	datum_to_bson(val, false, result, tcategory, typoutput);

	/* Clean up detoasted copy, if any */
	if (val != orig_val)
		pfree(DatumGetPointer(val));

	PG_RETURN_TEXT_P(cstring_to_text(result->data));
}

/*
 * bson_agg transition function
 */
Datum
bson_agg_transfn(PG_FUNCTION_ARGS)
{
	Oid			val_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	MemoryContext aggcontext,
				oldcontext;
	StringInfo	state;
	Datum		orig_val,
				val;
	TYPCATEGORY tcategory;
	Oid			typoutput;
	bool		typisvarlena;
	Oid			castfunc = InvalidOid;

	if (val_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "bson_agg_transfn called in non-aggregate context");
	}

	if (PG_ARGISNULL(0))
	{
		/*
		 * Make this StringInfo in a context where it will persist for the
		 * duration off the aggregate call. It's only needed for this initial
		 * piece, as the StringInfo routines make sure they use the right
		 * context to enlarge the object if necessary.
		 */
		oldcontext = MemoryContextSwitchTo(aggcontext);
		state = makeStringInfo();
		MemoryContextSwitchTo(oldcontext);

		appendStringInfoChar(state, '[');
	}
	else
	{
		state = (StringInfo) PG_GETARG_POINTER(0);
		appendStringInfoString(state, ", ");
	}

	/* fast path for NULLs */
	if (PG_ARGISNULL(1))
	{
		orig_val = (Datum) 0;
		datum_to_bson(orig_val, true, state, 0, InvalidOid);
		PG_RETURN_POINTER(state);
	}


	orig_val = PG_GETARG_DATUM(1);

	getTypeOutputInfo(val_type, &typoutput, &typisvarlena);

	if (val_type > FirstNormalObjectId)
	{
		HeapTuple	tuple;
		Form_pg_cast castForm;

		tuple = SearchSysCache2(CASTSOURCETARGET,
								ObjectIdGetDatum(val_type),
								ObjectIdGetDatum(BSONOID));
		if (HeapTupleIsValid(tuple))
		{
			castForm = (Form_pg_cast) GETSTRUCT(tuple);

			if (castForm->castmethod == COERCION_METHOD_FUNCTION)
				castfunc = typoutput = castForm->castfunc;

			ReleaseSysCache(tuple);
		}
	}

	if (castfunc != InvalidOid)
		tcategory = TYPCATEGORY_BSON_CAST;
	else if (val_type == RECORDARRAYOID)
		tcategory = TYPCATEGORY_ARRAY;
	else if (val_type == RECORDOID)
		tcategory = TYPCATEGORY_COMPOSITE;
	else if (val_type == BSONOID)
		tcategory = TYPCATEGORY_BSON;
	else
		tcategory = TypeCategory(val_type);

	/*
	 * If we have a toasted datum, forcibly detoast it here to avoid memory
	 * leakage inside the type's output routine.
	 */
	if (typisvarlena)
		val = PointerGetDatum(PG_DETOAST_DATUM(orig_val));
	else
		val = orig_val;

	if (!PG_ARGISNULL(0) &&
	  (tcategory == TYPCATEGORY_ARRAY || tcategory == TYPCATEGORY_COMPOSITE))
	{
		appendStringInfoString(state, "\n ");
	}

	datum_to_bson(val, false, state, tcategory, typoutput);

	/* Clean up detoasted copy, if any */
	if (val != orig_val)
		pfree(DatumGetPointer(val));

	/*
	 * The transition type for array_agg() is declared to be "internal", which
	 * is a pass-by-value type the same size as a pointer.	So we can safely
	 * pass the ArrayBuildState pointer through nodeAgg.c's machinations.
	 */
	PG_RETURN_POINTER(state);
}

/*
 * bson_agg final function
 */
Datum
bson_agg_finalfn(PG_FUNCTION_ARGS)
{
	StringInfo	state;

	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));

	state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);

	if (state == NULL)
		PG_RETURN_NULL();

	appendStringInfoChar(state, ']');

	PG_RETURN_TEXT_P(cstring_to_text(state->data));
}

/*
 * Produce a BSON string literal, properly escaping characters in the text.
 */
void
escape_bson(StringInfo buf, const char *str)
{
	const char *p;

	appendStringInfoCharMacro(buf, '\"');
	for (p = str; *p; p++)
	{
		switch (*p)
		{
			case '\b':
				appendStringInfoString(buf, "\\b");
				break;
			case '\f':
				appendStringInfoString(buf, "\\f");
				break;
			case '\n':
				appendStringInfoString(buf, "\\n");
				break;
			case '\r':
				appendStringInfoString(buf, "\\r");
				break;
			case '\t':
				appendStringInfoString(buf, "\\t");
				break;
			case '"':
				appendStringInfoString(buf, "\\\"");
				break;
			case '\\':
				appendStringInfoString(buf, "\\\\");
				break;
			default:
				if ((unsigned char) *p < ' ')
					appendStringInfo(buf, "\\u%04x", (int) *p);
				else
					appendStringInfoCharMacro(buf, *p);
				break;
		}
	}
	appendStringInfoCharMacro(buf, '\"');
}
