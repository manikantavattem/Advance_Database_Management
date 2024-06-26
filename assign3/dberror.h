#ifndef DBERROR_H
#define DBERROR_H

#include "stdio.h"

/* module wide constants */
#define PAGE_SIZE 4096

/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4
#define RC_CREATE_FILE_FAIL 5 
#define RC_GET_NUMBER_OF_BYTES_FAILED 6 
#define RC_SHUTDOWN_POOL_FAILED 7 
#define RC_STRATEGY_NOT_FOUND 8 
#define RC_PAGE_NOT_FOUND 9

#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205

#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 303
#define RC_ENOUGH_NUMB_PAGES 304
#define RC_INSERT_BUFPOOL_FAILED 305
#define RC_BUFFER_POOL_CREATION_FAILED 306
#define RC_FILE_HANDLE_CREATION_FAILED 307
#define RC_FILE_OPEN_FAILED 308
#define RC_PAGE_INFO_CREATION_FAILED 309
#define RC_FRAME_INITIALIZATION_FAILED 310
#define RC_BUFFER_POOL_INSERTION_FAILED 311
#define RC_BUFFER_POOL_NOT_FOUND 312
#define RC_BUFFER_POOL_NOT_SHUTDOWN 313
#define RC_BUFFER_POOL_DELETION_FAILED 314
#define RC_REPLACEMENT_PAGE_NOT_FOUND 315

#define RC_MARK_DIRTY_FAILED 400
#define RC_FORCE_PAGE_ERROR 401
#define RC_UNPIN_FAILED 402
#define RC_OUT_OF_BOUNDS 403
#define RC_MEMORY_ALLOCATION_FAIL 404
#define RC_READ_FAILED 405
#define RC_FIFO_FAILED 406
#define RC_LRU_FAILED 407
#define RC_LFU_FAILED 408
#define RC_PIN_FAILED 409
#define RC_ERR 410


/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(rc,message) \
		do {			  \
			RC_message=message;	  \
			return rc;		  \
		} while (0)		  \

// check the return code and exit if it is an error
#define CHECK(code)							\
		do {									\
			int rc_internal = (code);						\
			if (rc_internal != RC_OK)						\
			{									\
				char *message = errorMessage(rc_internal);			\
				printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, message); \
				free(message);							\
				exit(1);							\
			}									\
		} while(0);


#endif