/*
 * Wrapper around libs3 calls to make library less painful to use
 * for s3fs project.
 *
 * Uses code from libs3 source, by Bryan Ischo (from s3.c).
 * 
 * @author jsommers@colgate.edu
 * COSC 301, Fall 2012
 */
#ifndef __LIBS3_WRAPPER_H__
#define __LIBS3_WRAPPER_H__ 

#include "libs3.h"
#include <sys/types.h>
#include <stdint.h>

/* 
 * Initialize credentials.  This function looks for two shell environment
 * variables: "S3_ACCESS_KEY_ID" and "S3_SECRET_ACCESS_KEY".  If they
 * exist, the function returns 0.  Otherwise it returns -1.
 * This function must be called before any other library functions
 * are called.
 */
int s3fs_init_credentials();

/*
 * Given a bucket name, test whether we can access the bucket on s3.  This
 * function returns 0 on success and -1 on error.  There is also a reason
 * message printed to stderr to help debug access problems.
 */
int s3fs_test_bucket(const char *bucket);

/* 
 * Clear *all* objects out of a bucket.  Totally destructive, so be
 * careful.
 * Returns 0 on success and -1 on failure.
 */
int s3fs_clear_bucket(const char *bucket);  

/*
 * Get/read an object from s3 in a given bucket, identified by the given key.
 *
 * buf is allocated (malloc'ed) and returned by the function to hold the *full* 
 * object.  It is the responsibility of the calling function to free the object
 * at the appropriate time.
 *
 * start_byte is the starting byte to read from, byte_count is the number of
 * bytes to read.  If both values are 0, the *entire* object is retrieved.
 * 
 * Returns the number of bytes read, or -1 on error.  If the object contains
 * 0 bytes, *buf will point to NULL, and the return value will be 0.  Thus
 * a return value of 0 or greater means *success*.
 */
ssize_t s3fs_get_object(const char *bucket, const char *key, uint8_t **buf, 
                        ssize_t start_byte, ssize_t byte_count);

/* 
 * Write a full object to s3.  The object is written to the given bucket,
 * with the given key.  Only writing of complete files/objects is
 * supported by the library.
 *
 * Exactly byte_count bytes from buf are written to the object.  It is valid
 * to write a zero-lengthed object.  In that case, an "empty" object is
 * constructed on s3.
 *
 * This function returns the number of bytes written, of -1 on error.
 */
ssize_t s3fs_put_object(const char *bucket, const char *key, 
                        const uint8_t *buf, ssize_t byte_count); 

/* 
 * Remove a given object from the given bucket.
 *
 * This function returns 0 on success and -1 on failure.
 */ 
int s3fs_remove_object(const char *bucket, const char *key);

#endif // __LIBS3_WRAPPER_H__
