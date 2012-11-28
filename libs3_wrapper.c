/*
 * libs3_wrapper.c, to make life a little bit easier when using
 * libs3.  Developed for use in s3fs project, COSC 301, Fall 2012.
 * jsommers@colgate.edu
 *
 * Based on s3.c from libs3 source (copyinfo below).  Much/most
 * of the original s3.c code is included; unused code is #ifdef'ed 
 * out.
 */

/** **************************************************************************
 * s3.c
 * 
 * Copyright 2008 Bryan Ischo <bryan@ischo.com>
 * 
 * This file is part of libs3.
 * 
 * libs3 is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, version 3 of the License.
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link the code of this library and its programs with the
 * OpenSSL library, and distribute linked combinations including the two.
 *
 * libs3 is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License version 3
 * along with libs3, in a file named COPYING.  If not, see
 * <http://www.gnu.org/licenses/>.
 *
 ************************************************************************** **/

/**
 * This is a 'driver' program that simply converts command-line input into
 * calls to libs3 functions, and prints the results.
 **/

#include <ctype.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include "libs3.h"

// include forward declarations
#include "libs3_wrapper.h"


// Some Unix stuff (to work around Windows issues)
#ifndef SLEEP_UNITS_PER_SECOND
#define SLEEP_UNITS_PER_SECOND 1
#endif

// prototype declarations
int __s3fs_test_bucket(const char *bucketName);
int __s3fs_clear_bucket(const char *bucketName);
int __s3fs_remove_object(const char *bucketName, const char *key);
ssize_t __s3fs_get_object(const char *bucketName, const char *key, uint8_t **buf, ssize_t start_byte, ssize_t byte_count);
ssize_t __s3fs_put_object(const char *bucketName, const char *key, const uint8_t *buf, ssize_t contentLength); 


// Command-line options, saved as globals ------------------------------------

// static int forceG = 0;
static int showResponsePropertiesG = 0;
static S3Protocol protocolG = S3ProtocolHTTPS;
static S3UriStyle uriStyleG = S3UriStylePath;
static int retriesG = 5;


// Environment variables, saved as globals ----------------------------------

static const char *accessKeyIdG = 0;
static const char *secretAccessKeyG = 0;


// Request results, saved as globals -----------------------------------------

static int statusG = 0;
static char errorDetailsG[4096] = { 0 };



// Option prefixes -----------------------------------------------------------

// s3fs Global lock

static pthread_mutex_t global_lock = PTHREAD_MUTEX_INITIALIZER;

void s3fs_lock() {
    pthread_mutex_lock(&global_lock);
}

void s3fs_unlock() {
    pthread_mutex_unlock(&global_lock);
}

// --------------------------------------------------------


#define LOCATION_PREFIX "location="
#define LOCATION_PREFIX_LEN (sizeof(LOCATION_PREFIX) - 1)
#define CANNED_ACL_PREFIX "cannedAcl="
#define CANNED_ACL_PREFIX_LEN (sizeof(CANNED_ACL_PREFIX) - 1)
#define PREFIX_PREFIX "prefix="
#define PREFIX_PREFIX_LEN (sizeof(PREFIX_PREFIX) - 1)
#define MARKER_PREFIX "marker="
#define MARKER_PREFIX_LEN (sizeof(MARKER_PREFIX) - 1)
#define DELIMITER_PREFIX "delimiter="
#define DELIMITER_PREFIX_LEN (sizeof(DELIMITER_PREFIX) - 1)
#define MAXKEYS_PREFIX "maxkeys="
#define MAXKEYS_PREFIX_LEN (sizeof(MAXKEYS_PREFIX) - 1)
#define FILENAME_PREFIX "filename="
#define FILENAME_PREFIX_LEN (sizeof(FILENAME_PREFIX) - 1)
#define CONTENT_LENGTH_PREFIX "contentLength="
#define CONTENT_LENGTH_PREFIX_LEN (sizeof(CONTENT_LENGTH_PREFIX) - 1)
#define CACHE_CONTROL_PREFIX "cacheControl="
#define CACHE_CONTROL_PREFIX_LEN (sizeof(CACHE_CONTROL_PREFIX) - 1)
#define CONTENT_TYPE_PREFIX "contentType="
#define CONTENT_TYPE_PREFIX_LEN (sizeof(CONTENT_TYPE_PREFIX) - 1)
#define MD5_PREFIX "md5="
#define MD5_PREFIX_LEN (sizeof(MD5_PREFIX) - 1)
#define CONTENT_DISPOSITION_FILENAME_PREFIX "contentDispositionFilename="
#define CONTENT_DISPOSITION_FILENAME_PREFIX_LEN \
    (sizeof(CONTENT_DISPOSITION_FILENAME_PREFIX) - 1)
#define CONTENT_ENCODING_PREFIX "contentEncoding="
#define CONTENT_ENCODING_PREFIX_LEN (sizeof(CONTENT_ENCODING_PREFIX) - 1)
#define EXPIRES_PREFIX "expires="
#define EXPIRES_PREFIX_LEN (sizeof(EXPIRES_PREFIX) - 1)
#define X_AMZ_META_PREFIX "x-amz-meta-"
#define X_AMZ_META_PREFIX_LEN (sizeof(X_AMZ_META_PREFIX) - 1)
#define IF_MODIFIED_SINCE_PREFIX "ifModifiedSince="
#define IF_MODIFIED_SINCE_PREFIX_LEN (sizeof(IF_MODIFIED_SINCE_PREFIX) - 1)
#define IF_NOT_MODIFIED_SINCE_PREFIX "ifNotmodifiedSince="
#define IF_NOT_MODIFIED_SINCE_PREFIX_LEN \
    (sizeof(IF_NOT_MODIFIED_SINCE_PREFIX) - 1)
#define IF_MATCH_PREFIX "ifMatch="
#define IF_MATCH_PREFIX_LEN (sizeof(IF_MATCH_PREFIX) - 1)
#define IF_NOT_MATCH_PREFIX "ifNotMatch="
#define IF_NOT_MATCH_PREFIX_LEN (sizeof(IF_NOT_MATCH_PREFIX) - 1)
#define START_BYTE_PREFIX "startByte="
#define START_BYTE_PREFIX_LEN (sizeof(START_BYTE_PREFIX) - 1)
#define BYTE_COUNT_PREFIX "byteCount="
#define BYTE_COUNT_PREFIX_LEN (sizeof(BYTE_COUNT_PREFIX) - 1)
#define ALL_DETAILS_PREFIX "allDetails="
#define ALL_DETAILS_PREFIX_LEN (sizeof(ALL_DETAILS_PREFIX) - 1)
#define NO_STATUS_PREFIX "noStatus="
#define NO_STATUS_PREFIX_LEN (sizeof(NO_STATUS_PREFIX) - 1)
#define RESOURCE_PREFIX "resource="
#define RESOURCE_PREFIX_LEN (sizeof(RESOURCE_PREFIX) - 1)
#define TARGET_BUCKET_PREFIX "targetBucket="
#define TARGET_BUCKET_PREFIX_LEN (sizeof(TARGET_BUCKET_PREFIX) - 1)
#define TARGET_PREFIX_PREFIX "targetPrefix="
#define TARGET_PREFIX_PREFIX_LEN (sizeof(TARGET_PREFIX_PREFIX) - 1)


// util ----------------------------------------------------------------------

int s3fs_init_credentials() {
    accessKeyIdG = getenv("S3_ACCESS_KEY_ID");
    if (!accessKeyIdG) {
        fprintf(stderr, "Missing environment variable: S3_ACCESS_KEY_ID\n");
        return -1;
    }
    secretAccessKeyG = getenv("S3_SECRET_ACCESS_KEY");
    if (!secretAccessKeyG) {
        fprintf(stderr, 
                "Missing environment variable: S3_SECRET_ACCESS_KEY\n");
        return -1;
    }
    return 0;
}

static void S3_init()
{
    S3Status status;
    const char *hostname = getenv("S3_HOSTNAME");
    
    if ((status = S3_initialize("s3", S3_INIT_ALL, hostname))
        != S3StatusOK) {
        fprintf(stderr, "Failed to initialize libs3: %s\n", 
                S3_get_status_name(status));
        exit(-1);
    }
}

static void printError()
{
    if (statusG < S3StatusErrorAccessDenied) {
        fprintf(stderr, "\nERROR: %s\n", S3_get_status_name(statusG));
    }
    else {
        fprintf(stderr, "\nERROR: %s\n", S3_get_status_name(statusG));
        fprintf(stderr, "%s\n", errorDetailsG);
    }
}

static int should_retry()
{
    if (retriesG--) {
        // Sleep before next retry; start out with a 1 second sleep
        static int retrySleepInterval = 1 * SLEEP_UNITS_PER_SECOND;
        sleep(retrySleepInterval);
        // Next sleep 1 second longer
        retrySleepInterval++;
        return 1;
    }

    return 0;
}

// response properties callback ----------------------------------------------

// This callback does the same thing for every request type: prints out the
// properties if the user has requested them to be so
static S3Status responsePropertiesCallback
    (const S3ResponseProperties *properties, void *callbackData)
{
    (void) callbackData;

    if (!showResponsePropertiesG) {
        return S3StatusOK;
    }

#define print_nonnull(name, field)                                 \
    do {                                                           \
        if (properties-> field) {                                  \
            printf("%s: %s\n", name, properties-> field);          \
        }                                                          \
    } while (0)
    
    print_nonnull("Content-Type", contentType);
    print_nonnull("Request-Id", requestId);
    print_nonnull("Request-Id-2", requestId2);
    if (properties->contentLength > 0) {
        printf("Content-Length: %lld\n", 
               (unsigned long long) properties->contentLength);
    }
    print_nonnull("Server", server);
    print_nonnull("ETag", eTag);
    if (properties->lastModified > 0) {
        char timebuf[256];
        time_t t = (time_t) properties->lastModified;
        // gmtime is not thread-safe but we don't care here.
        strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
        printf("Last-Modified: %s\n", timebuf);
    }
    int i;
    for (i = 0; i < properties->metaDataCount; i++) {
        printf("x-amz-meta-%s: %s\n", properties->metaData[i].name,
               properties->metaData[i].value);
    }

    return S3StatusOK;
}

// response complete callback ------------------------------------------------

// This callback does the same thing for every request type: saves the status
// and error stuff in global variables
static void responseCompleteCallback(S3Status status,
                                     const S3ErrorDetails *error, 
                                     void *callbackData)
{
    (void) callbackData;

    statusG = status;
    // Compose the error details message now, although we might not use it.
    // Can't just save a pointer to [error] since it's not guaranteed to last
    // beyond this callback
    int len = 0;
    if (error && error->message) {
        len += snprintf(&(errorDetailsG[len]), sizeof(errorDetailsG) - len,
                        "  Message: %s\n", error->message);
    }
    if (error && error->resource) {
        len += snprintf(&(errorDetailsG[len]), sizeof(errorDetailsG) - len,
                        "  Resource: %s\n", error->resource);
    }
    if (error && error->furtherDetails) {
        len += snprintf(&(errorDetailsG[len]), sizeof(errorDetailsG) - len,
                        "  Further Details: %s\n", error->furtherDetails);
    }
    if (error && error->extraDetailsCount) {
        len += snprintf(&(errorDetailsG[len]), sizeof(errorDetailsG) - len,
                        "%s", "  Extra Details:\n");
        int i;
        for (i = 0; i < error->extraDetailsCount; i++) {
            len += snprintf(&(errorDetailsG[len]), 
                            sizeof(errorDetailsG) - len, "    %s: %s\n", 
                            error->extraDetails[i].name,
                            error->extraDetails[i].value);
        }
    }
}


int s3fs_test_bucket(const char *bucketName) {
    s3fs_lock();
    int rv = __s3fs_test_bucket(bucketName);
    s3fs_unlock();
    return rv;
}

int __s3fs_test_bucket(const char *bucketName)
{
    S3_init();

    S3ResponseHandler responseHandler =
    {
        &responsePropertiesCallback, &responseCompleteCallback
    };

    char locationConstraint[64];
    do {
        S3_test_bucket(protocolG, uriStyleG, accessKeyIdG, secretAccessKeyG,
                       0, bucketName, sizeof(locationConstraint),
                       locationConstraint, 0, &responseHandler, 0);
    } while (S3_status_is_retryable(statusG) && should_retry());

    const char *reason = "Unknown";
    int result = statusG == S3StatusOK ? 1 : 0;

    switch (statusG) {
    case S3StatusOK:
        // bucket exists
        reason = locationConstraint[0] ? locationConstraint : "USA";
        break;
    case S3StatusErrorNoSuchBucket:
        reason = "Does Not Exist";
        break;
    case S3StatusErrorAccessDenied:
        reason = "Access Denied";
        break;
    default:
        break;
    }

    fprintf(stderr, "S3 test_bucket: %s\n", reason);

    S3_deinitialize();

    return result;
}


// list bucket ---------------------------------------------------------------
// JS: well, it's really remove bucket.  doesn't that make sense?

struct node {
    char *key;
    struct node *next;
};

typedef struct traverse_bucket_callback_data
{
    int isTruncated;
    char nextMarker[1024];
    int keyCount;
    int allDetails;
    struct node *keylist;
} traverse_bucket_callback_data;


static S3Status traverseBucketCallback(int isTruncated, const char *nextMarker,
                                   int contentsCount, 
                                   const S3ListBucketContent *contents,
                                   int commonPrefixesCount,
                                   const char **commonPrefixes,
                                   void *callbackData)
{
    traverse_bucket_callback_data *data = 
        (traverse_bucket_callback_data *) callbackData;

    data->isTruncated = isTruncated;
    // This is tricky.  S3 doesn't return the NextMarker if there is no
    // delimiter.  Why, I don't know, since it's still useful for paging
    // through results.  We want NextMarker to be the last content in the
    // list, so set it to that if necessary.
    if ((!nextMarker || !nextMarker[0]) && contentsCount) {
        nextMarker = contents[contentsCount - 1].key;
    }
    if (nextMarker) {
        snprintf(data->nextMarker, sizeof(data->nextMarker), "%s", 
                 nextMarker);
    }
    else {
        data->nextMarker[0] = 0;
    }
    
    int i;
    for (i = 0; i < contentsCount; i++) {
        const S3ListBucketContent *content = &(contents[i]);

        // add key onto linked list; push at head
        struct node *el = malloc(sizeof(struct node));
        el->key = strdup(content->key);
        el->next = data->keylist;
        data->keylist = el;
    }

    data->keyCount += contentsCount;

    return S3StatusOK;
}


// JS: for s3fs project; adapted from original list_bucket.
// (Makes sense, right?  Instead of listing, we just remove everything :-)

int s3fs_clear_bucket(const char *bucketName) {
    s3fs_lock();
    int rv = __s3fs_clear_bucket(bucketName);
    s3fs_unlock();
    return rv;
}

int __s3fs_clear_bucket(const char *bucketName) {
    S3_init();

    const char *prefix = 0, *marker = 0, *delimiter = 0;
    int maxkeys = 0, allDetails = 0;
    
    S3BucketContext bucketContext =
    {
        0,
        bucketName,
        protocolG,
        uriStyleG,
        accessKeyIdG,
        secretAccessKeyG
    };

    S3ListBucketHandler listBucketHandler =
    {
        { &responsePropertiesCallback, &responseCompleteCallback },
        &traverseBucketCallback
    };

    traverse_bucket_callback_data data;

    snprintf(data.nextMarker, sizeof(data.nextMarker), "%s", marker);
    data.keyCount = 0;
    data.keylist = NULL;
    data.allDetails = allDetails;

    do {
        data.isTruncated = 0;
        do {
            S3_list_bucket(&bucketContext, prefix, data.nextMarker,
                           delimiter, maxkeys, 0, &listBucketHandler, &data);
        } while (S3_status_is_retryable(statusG) && should_retry());
        if (statusG != S3StatusOK) {
            break;
        }
    } while (data.isTruncated && (!maxkeys || (data.keyCount < maxkeys)));

    int rv = statusG == S3StatusOK ? 0 : -1;

    S3_deinitialize();

    struct node *klist = data.keylist;

    // try to remove objects
    if (rv == 0) {
        while (klist) {
            struct node *el = klist;
            int thisrv = __s3fs_remove_object(bucketName, el->key);
            if (thisrv < 0) {
                rv = -1;
            }
            klist = klist->next;
        }
    }

    // free keylist
    klist = data.keylist;
    while (klist) {
        struct node *el = klist;
        klist = klist->next;
        free(el->key);
        free(el);
    }

    return rv;
}

// put object ----------------------------------------------------------------

typedef struct put_object_callback_data
{
    const uint8_t *data;
    uint64_t contentLength, originalContentLength;
    int written;
    int noStatus;
} put_object_callback_data;


int putObjectDataCallback(int bufferSize, char *buffer,
                                 void *callbackData)
{
    put_object_callback_data *data = 
        (put_object_callback_data *) callbackData;
    
    int ret = 0;

    if (data->contentLength) {
        int toRead = ((data->contentLength > (unsigned) bufferSize) ?
                      (unsigned) bufferSize : data->contentLength);
        const unsigned char *tmp = data->data;
        memcpy(buffer, tmp, toRead);
        tmp += toRead;
        data->data = tmp; // advance data ptr
        ret += toRead;
    }
    data->written += ret;
    data->contentLength -= ret;

    if (data->contentLength && !data->noStatus) {
        // Avoid a weird bug in MingW, which won't print the second integer
        // value properly when it's in the same call, so print separately
        printf("%llu bytes remaining ", 
               (unsigned long long) data->contentLength);
        printf("(%d%% complete) ...\n",
               (int) (((data->originalContentLength - 
                        data->contentLength) * 100) /
                      data->originalContentLength));
    }

    return ret;
}

ssize_t s3fs_put_object(const char *bucketName, const char *key, const uint8_t *buf, ssize_t contentLength) {
    s3fs_lock();
    ssize_t rv = __s3fs_put_object(bucketName, key, buf, contentLength);
    s3fs_unlock();
    return rv;
}

ssize_t __s3fs_put_object(const char *bucketName, const char *key, const uint8_t *buf, ssize_t contentLength)
{
    const char *cacheControl = 0, *contentType = 0, *md5 = 0;
    const char *contentDispositionFilename = 0, *contentEncoding = 0;
    int64_t expires = -1;
    S3CannedAcl cannedAcl = S3CannedAclPrivate;
    int metaPropertiesCount = 0;
    S3NameValue metaProperties[S3_MAX_METADATA_COUNT];
    int noStatus = 0;

    put_object_callback_data data;
    memset(&data, 0, sizeof(put_object_callback_data));
    data.data = buf;
    // data.gb = 0;
    data.noStatus = noStatus;

    data.contentLength = data.originalContentLength = contentLength;

    S3_init();
    
    S3BucketContext bucketContext =
    {
        0,
        bucketName,
        protocolG,
        uriStyleG,
        accessKeyIdG,
        secretAccessKeyG
    };

    S3PutProperties putProperties =
    {
        contentType,
        md5,
        cacheControl,
        contentDispositionFilename,
        contentEncoding,
        expires,
        cannedAcl,
        metaPropertiesCount,
        metaProperties
    };

    S3PutObjectHandler putObjectHandler =
    {
        { &responsePropertiesCallback, &responseCompleteCallback },
        &putObjectDataCallback
    };

    do {
        S3_put_object(&bucketContext, key, contentLength, &putProperties, 0,
                      &putObjectHandler, &data);
    } while (S3_status_is_retryable(statusG) && should_retry());

    int result = data.written;

    if (statusG != S3StatusOK) {
        printError();
        result = -1;
    }
    else if (data.contentLength) {
        fprintf(stderr, "\nERROR: Failed to read remaining %llu bytes from "
                "input\n", (unsigned long long) data.contentLength);
    }

    S3_deinitialize();
    return result;
}

// get object ----------------------------------------------------------------

struct get_callback_data {
    uint8_t *buf;
    ssize_t bytes_read;
};

S3Status getObjectDataCallback(int bufferSize, const char *buffer,
                               void *callbackData) {
    struct get_callback_data *get_context = (struct get_callback_data*)callbackData;
    if (bufferSize > 0) {
        if (get_context->buf == NULL) {
            // make new buffer
            get_context->buf = malloc(sizeof(uint8_t) * bufferSize);
            if (!get_context->buf) {
                return S3StatusAbortedByCallback;
            }

            memcpy(get_context->buf, buffer, bufferSize);
        } else {
            // reallocate buffer
            uint8_t *tmp = malloc(sizeof(uint8_t) * (bufferSize + get_context->bytes_read));
            if (!tmp) {
                return S3StatusAbortedByCallback;
            }
            
            memcpy(tmp, get_context->buf, get_context->bytes_read);
            memcpy(tmp + get_context->bytes_read, buffer, bufferSize);
            free(get_context->buf);
            get_context->buf = tmp;
        }
    }

    get_context->bytes_read += bufferSize;

    return S3StatusOK;
}


ssize_t s3fs_get_object(const char *bucketName, const char *key, uint8_t **buf, 
                        ssize_t start_byte, ssize_t byte_count) {
    s3fs_lock();
    ssize_t rv = __s3fs_get_object(bucketName, key, buf, start_byte, byte_count);
    s3fs_unlock();
    return rv;
}

ssize_t __s3fs_get_object(const char *bucketName, const char *key, uint8_t **buf, 
                        ssize_t start_byte, ssize_t byte_count) {

    int64_t ifModifiedSince = -1, ifNotModifiedSince = -1;
    const char *ifMatch = 0, *ifNotMatch = 0;
    uint64_t startByte = start_byte, byteCount = byte_count;

    S3_init();

    struct get_callback_data get_context;
    get_context.buf = NULL;
    get_context.bytes_read = 0;
    
    S3BucketContext bucketContext =
    {
        0,
        bucketName,
        protocolG,
        uriStyleG,
        accessKeyIdG,
        secretAccessKeyG
    };

    S3GetConditions getConditions =
    {
        ifModifiedSince,
        ifNotModifiedSince,
        ifMatch,
        ifNotMatch
    };

    S3GetObjectHandler getObjectHandler =
    {
        { &responsePropertiesCallback, &responseCompleteCallback },
        &getObjectDataCallback
    };

    do {
        S3_get_object(&bucketContext, key, &getConditions, startByte,
                      byteCount, 0, &getObjectHandler, &get_context);
    } while (S3_status_is_retryable(statusG) && should_retry());

    ssize_t status = get_context.bytes_read;
    if (statusG != S3StatusOK) {
        status = -1;
        if (get_context.buf) {
            free (get_context.buf);
        }
        printError();
    } else {
        *buf = get_context.buf; 
    }

    S3_deinitialize();

    return status;
}


int s3fs_remove_object(const char *bucketName, const char *key) {
    s3fs_lock();
    int rv = __s3fs_remove_object(bucketName, key);
    s3fs_unlock();
    return rv;
}

int __s3fs_remove_object(const char *bucketName, const char *key) {
    S3_init();
    S3BucketContext bucketContext =
    {
        0,
        bucketName,
        protocolG,
        uriStyleG,
        accessKeyIdG,
        secretAccessKeyG
    };

    S3ResponseHandler responseHandler =
    { 
        0,
        &responseCompleteCallback
    };

    do {
        S3_delete_object(&bucketContext, key, 0, &responseHandler, 0);
    } while (S3_status_is_retryable(statusG) && should_retry());

    int result = statusG == S3StatusOK ? 0 : -1;

    if ((statusG != S3StatusOK) &&
        (statusG != S3StatusErrorPreconditionFailed)) {
        printError();
    }

    S3_deinitialize();

    return result;    
}
