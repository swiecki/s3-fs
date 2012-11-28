/*
 * Simple tests for libs3 using the wrapper functions for use in the s3fs
 * project.
 *
 * You can use these tests to ensure that you have proper connectivity to
 * your s3bucket, and also as examples for how to use the libs3_wrapper
 * functions.  
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "libs3_wrapper.h"
#include "s3fs.h" // for environment strings to look for

int main(int argc, char **argv) {

    /*
     * Basic tests:
     *  - Test the bucket (ensure basic connectivity)
     *  - Clear the bucket
     *  - Create an object
     *  - Get the object and verify it
     *  - Remove the object
     *  - Try to get the object again, it should fail.
     *  - Done.
     */

     // initial setup

    char *s3key = getenv(S3ACCESSKEY);
    if (!s3key) {
        fprintf(stderr, "%s environment variable must be defined\n", S3ACCESSKEY);
    }
    char *s3secret = getenv(S3SECRETKEY);
    if (!s3secret) {
        fprintf(stderr, "%s environment variable must be defined\n", S3SECRETKEY);
    }
    char *s3bucket = getenv(S3BUCKET);
    if (!s3bucket) {
        fprintf(stderr, "%s environment variable must be defined\n", S3BUCKET);
    }

    printf("Using bucket: %s\n", s3bucket);
    
    if (s3fs_init_credentials() < 0) {
        printf("Failed to initialize S3 credentials.\n");
        return -1;
    }
 
    if (s3fs_test_bucket(s3bucket) < 0) {
        printf("Failed to connect to bucket (s3fs_test_bucket)\n");
    } else {
        printf("Successfully connected to bucket (s3fs_test_bucket)\n");
    }

    if (s3fs_clear_bucket(s3bucket) < 0) {
        printf("Failed to clear bucket (s3fs_clear_bucket)\n");
    } else {
        printf("Successfully cleared the bucket (removed all objects)\n");
    }

    const char *test_key = "thekey";
    const char *test_object = "This is a test, this is only a test.  Don't expect much.";
    ssize_t object_length = strlen(test_object) + 1;

    ssize_t rv = s3fs_put_object(s3bucket, "thekey", (uint8_t*)test_object, object_length);
    if (rv < 0) {
        printf("Failure in s3fs_put_object\n");
    } else if (rv < object_length) {
        printf("Failed to upload full test object (s3fs_put_object %d)\n", rv);
    } else {
        printf("Successfully put test object in s3 (s3fs_put_object)\n");
    }

    uint8_t *retrieved_object = NULL;
    // zeroes as last two args means that we want to retrieve entire object
    rv = s3fs_get_object(s3bucket, test_key, &retrieved_object, 0, 0);
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
    } else if (rv < object_length) {
        printf("Failed to retrieve entire object (s3fs_get_object %d)\n", rv);
    } else {
        printf("Successfully retrieved test object from s3 (s3fs_get_object)\n");
        if (strcmp((const char *)retrieved_object, test_object) == 0) {
            printf("Retrieved object looks right.\n");
        } else {
            printf("Retrieved object doesn't match what we sent?!\n");
        }
    }

    // s3fs_get_object does an implicit malloc.  we gotta free that
    // memory.  no leakage!
    if (retrieved_object) {
        free (retrieved_object);
    }

    if (s3fs_remove_object(s3bucket, test_key) < 0) {
        printf("Failure to remove test object (s3fs_remove_object)\n");
    } else {
        printf("Success in removing test object (s3fs_remove_object)\n");
    }

    rv = s3fs_get_object(s3bucket, test_key, &retrieved_object, 0, 0);
    if (rv == -1) {
        printf("Got expected failure in trying to retrieve test object after removing it\n");
    } else {
        printf("Unexpected return value in trying to retrieve an already-removed object: %d\n", rv);
    }

    printf("Done with s3fs tests.  Share and enjoy.\n");
    return 0;
}
