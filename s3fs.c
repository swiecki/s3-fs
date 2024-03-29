                                                                     
                                                                     
                                                                     
                                             
/* This code is based on the fine code written by Joseph Pfeiffer for his
   fuse system tutorial. */

#include "s3fs.h"
#include "libs3_wrapper.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <sys/stat.h>
#include <unistd.h>

#define GET_PRIVATE_DATA ((s3context_t *) fuse_get_context()->private_data)

/*
 * For each function below, if you need to return an error,
 * read the appropriate man page for the call and see what
 * error codes make sense for the type of failure you want
 * to convey.  For example, many of the calls below return
 * -EIO (an I/O error), since there are no S3 calls yet
 * implemented.  (Note that you need to return the negative
 * value for an error code.)
 */


/* 
 * Get file attributes.  Similar to the stat() call
 * (and uses the same structure).  The st_dev, st_blksize,
 * and st_ino fields are ignored in the struct (and 
 * do not need to be filled in).
 */


int fs_getattr(const char *path, struct stat *statbuf) {
    
    fprintf(stderr, "fs_getattr(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;

    char *str1 = strdup(path);
    char *str2 = strdup(path);

    char *dirpath = dirname(str1);
    char *end = basename(str2);

    //get preliminary dir path- this will either get us to the directory we need
    //if we have a non-blank basename, we will then get the file/dirpath we want
    uint8_t *enddir = NULL;
    int rv = s3fs_get_object(ctx->s3bucket, dirpath, &enddir, 0, 0);

    s3dirent_t *dir = (s3dirent_t *) enddir;

    //get number of entries in dir
    int numEntries = (int) rv / sizeof(s3dirent_t);
    fprintf(stderr, "number entries is %i\n",numEntries);

    //no basename, we want the . entry. go home basename, you're drunk.
    if(end[0] == '\0' || !strcasecmp(end, "/")){
        *statbuf = dir[0].metadata;
        fprintf(stderr, "root metadata succesfully returned\n");
        free(str1);
        free(str2);
        free(enddir);
        return 0;
    }

    //get the dirpath of the thing we're looking for-
    //if the basename is empty, get dir metadata
    //otherwise iterate through and find proper file metadata

    int i = 0;
    for (;i<numEntries;i++){
        fprintf(stderr, "%i:%s::%s\n",i,end,dir[i].name);
      if(!strcasecmp(end,dir[i].name)){
        if(dir[i].type == 'f'){
            //it's a file. return its metadata
            *statbuf = dir[i].metadata;
            free(str1);
            free(str2);
            free(enddir);
            return 0;
        } else if(dir[i].type == 'd'){
            //get that directory and return its . entry
            fprintf(stderr, "it's a directory.\n");
            s3dirent_t *retrieved_object = NULL;
            char buf[256];
            snprintf(buf, sizeof(buf), "%s", path);
            fprintf(stderr, "getting object %s\n", (char*)&buf);
            // download ALL THE OBJECTS!
            // 
            int rv2 = s3fs_get_object(ctx->s3bucket, (char *) &buf, (uint8_t **)&retrieved_object, 0, 0);
            if (rv2 < 0) {
                printf("Failure in s3fs_get_object\n");
            } else if (rv2 < sizeof(s3dirent_t)) {
                printf("Failed to retrieve entire object (s3fs_get_object %d)\n", rv);
            } else {
                printf("Successfully retrieved test object from s3 (s3fs_get_object)\n");
                *statbuf = retrieved_object[0].metadata;
                free(str1);
                free(str2);
                free(enddir);
                free(retrieved_object);
                return 0;
            }
        }
      }
    }
    free(str1);
    free(str2);
    free(enddir);
    return -ENOENT;
}


/* 
 * Create a file "node".  When a new file is created, this
 * function will get called.  
 * This is called for creation of all non-directory, non-symlink
 * nodes.  You *only* need to handle creation of regular
 * files here.  (See the man page for mknod (2).)
 */
int fs_mknod(const char *path, mode_t mode, dev_t dev) {
    fprintf(stderr, "fs_mknod(path=\"%s\", mode=0%3o)\n", path, mode);
    s3context_t *ctx = GET_PRIVATE_DATA;
    
	//Pull down the appropriate directory to check whether it exists.
    char *parentPath = strdup(path);
    char *childName = strdup(path);
    parentPath = dirname(parentPath);
    childName = basename(childName);
    s3dirent_t *parent = NULL;
    ssize_t parentSize = s3fs_get_object(ctx->s3bucket,parentPath,(uint8_t**)&parent,0,0);
    if(parentSize < 0){
        free(parentPath);
        free(childName);
        free(parent);
		return -ENOENT;//Part of the directory does not exist.
    }
	//Add an entry for the file in the directory and give it correct metadata.
    int numEntries = (int) parentSize / sizeof(s3dirent_t);
    parent = realloc(parent,(sizeof(s3dirent_t)*(numEntries+1)));
    strcpy(parent[numEntries].name,childName);
    parent[numEntries].type = 'f';
    struct stat md;
    md.st_blocks = (blkcnt_t) 0;
    md.st_mode = mode;
    md.st_size = 0;
    time_t now = time(NULL) - 18000;
    md.st_mtime = now;
    md.st_atime = now;
    md.st_ctime = now;
    md.st_nlink = (nlink_t) 1;
    md.st_uid = getuid();
    md.st_gid = getgid();
    parent[numEntries].metadata = md;
	//Edit the directory's metadata.
    parent[0].metadata.st_size += sizeof(s3dirent_t);
    parent[0].metadata.st_mtime = now;
    parent[0].metadata.st_atime = now;
	//Wipe the old parent, then push the parent directory and new file up.
    s3fs_remove_object(ctx->s3bucket,parentPath);
    s3fs_put_object(ctx->s3bucket,parentPath,(uint8_t*)parent,(sizeof(s3dirent_t)*(numEntries+1)));
    s3fs_put_object(ctx->s3bucket,path,(uint8_t*)parent,0);

    free(parentPath);
    free(childName);
    free(parent);
    return 0;
}

/* 
 * Create a new directory.
 *
 * Note that the mode argument may not have the type specification
 * bits set, i.e. S_ISDIR(mode) can be false.  To obtain the
 * correct directory type bits (for setting in the metadata)
 * use mode|S_IFDIR.
 */
int fs_mkdir(const char *path, mode_t mode) {
    fprintf(stderr, "fs_mkdir(path=\"%s\", mode=0%3o)\n", path, mode);
    s3context_t *ctx = GET_PRIVATE_DATA;
    mode |= S_IFDIR;

    //Get the directory (just root for now).
    s3dirent_t *parent = NULL;
    char *str1 = strdup(path);
    char *str2 = strdup(path);

    char *target = basename(str1);
    char *parentpath = dirname(str2);
    fprintf(stderr, ":%s:\n",target);
    ssize_t size = s3fs_get_object(ctx->s3bucket, parentpath,(uint8_t **) &parent, 0,0);
    int numEntries = (int) size / sizeof(s3dirent_t);
    int i = 0;
    //Check whether the target directory exists.
    for (;i<numEntries;i++){
      if(!strcasecmp(target,parent[i].name)){
        free(str1);
        free(str2);
        free(parent);
        return -EEXIST;
      }
    }
    //Doesn't exist, so make it.
    s3dirent_t *newEntry = malloc(sizeof(s3dirent_t));
    strcpy(newEntry->name, ".");
    newEntry->type = 'd';
    struct stat md;
    md.st_blocks = (blkcnt_t) 0;
    md.st_mode = mode;
    md.st_size = (off_t) sizeof(s3dirent_t);
    time_t now = time(NULL) - 18000;
    md.st_mtime = now;
    md.st_atime = now;
    md.st_ctime = now;
    md.st_nlink = (nlink_t) 1;
    md.st_uid = getuid();
    md.st_gid = getgid();
    newEntry->metadata = md;
		
    //Reallocate the s3 directory object and add the new entry's information.
    parent = realloc(parent, (numEntries+1)*sizeof(s3dirent_t));
		
    //New entry and metadata set up, modify parent directory's metadata.
    parent[0].metadata.st_size = (off_t) (numEntries+1)*sizeof(s3dirent_t);
    parent[0].metadata.st_mtime = now;
    parent[0].metadata.st_atime = now;

    strcpy(parent[numEntries].name, target);
    parent[numEntries].type = 'd';
    printf("Number of entries: %i",numEntries);
    
    //Remove the old directory object, push the new one, then push the created object.
    s3fs_remove_object(ctx->s3bucket,parentpath);
    s3fs_put_object(ctx->s3bucket, parentpath, (uint8_t*)parent, ((numEntries+1)*sizeof(s3dirent_t)));
    s3fs_put_object(ctx->s3bucket,path,(uint8_t*)newEntry,sizeof(s3dirent_t));
    free(str1);
    free(str2);
    free(parent);
    free(newEntry);
    return 0;
}
/*
 * Remove a file.
 */
int fs_unlink(const char *path) {
    fprintf(stderr, "fs_unlink(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
    //Make sure the file exists.
    s3dirent_t *target = NULL;
    s3dirent_t *parent = NULL;
    char *str1 = strdup(path);
    char *str2 = strdup(path);
    
    char *childName = basename(str1);
    char *parentName = dirname(str2);
    ssize_t childSize = s3fs_get_object(ctx->s3bucket,path,(uint8_t**)&target,0,0);
    if(childSize < 0){
        free(target);
        free(str1);
        free(str2);
        return -ENOENT;//file does not exist
    }
    //Make sure it isn't a directory.
    if(target[0].type == 'd'){
        free(target);
        free(str1);
        free(str2);
        return -EISDIR;
    }
    //If it does, pull down the parent directory.
    ssize_t parentSize = s3fs_get_object(ctx->s3bucket,parentName,(uint8_t**)&parent,0,0);
    int numEntries = (int) parentSize / sizeof(s3dirent_t);
    //Remove its entry from the parent directory.
    int index = -1;
    int i = 0;
    for(;i<numEntries;i++){
        if(!strcasecmp(parent[i].name,childName)){
            index = i;
        }
    }
    if(index<0){
        free(target);
        free(str1);
        free(str2);
        free(parent);
        return -ENOENT;//File doesn't exist in the directory; problem.
    }
    //Update the parent's metadata.
    s3dirent_t finalEntry = parent[numEntries-1];
    parent = realloc(parent,(numEntries-1)*sizeof(s3dirent_t));
    if(index<(numEntries-1)){
        parent[index] = finalEntry;
    }
    //Push the parent back up.
    s3fs_put_object(ctx->s3bucket,parentName,(uint8_t*)parent,((numEntries-1)*sizeof(s3dirent_t)));
    //Erase the file from the bucket.
    s3fs_remove_object(ctx->s3bucket,path);
    free(target);
    free(str1);
    free(str2);
    free(parent);
    return 0;
}

/*
 * Remove a directory. 
 */
int fs_rmdir(const char *path) {
    fprintf(stderr, "fs_rmdir(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;

    //Get the directory as a check
    s3dirent_t *targetDir = NULL;
    ssize_t dirSize = s3fs_get_object(ctx->s3bucket,path,(uint8_t **)&targetDir,0,0);
    if(dirSize < 0){
        //Directory doesn't exist.
        free(targetDir);
        return 0; //Well, it's not there anymore...
    }
    if(dirSize != sizeof(s3dirent_t)){
        //Directory is too full to delete.
        free(targetDir);
        return -ENOTEMPTY;
    }
    //Directory exists and is empty.
    //Get the parent directory.
    char *str1 = strdup(path);
    char *str2 = strdup(path);

    char* base = basename(str1);
    char *parentName = dirname(str2);

    s3dirent_t *parent = NULL;
    ssize_t parentSize = s3fs_get_object(ctx->s3bucket,parentName,(uint8_t **)&parent,0,0);
    //Modify the parent's metadata.
    time_t now = time(NULL)-18000;
    parent[0].metadata.st_mtime = now;
    parent[0].metadata.st_atime = now;
    parent[0].metadata.st_size -= (off_t) sizeof(s3dirent_t);
    int index = 0;
    int entries = (int) parentSize/sizeof(s3dirent_t);
    int i = 0;
    for (;i<entries;i++){
        if(!strcasecmp(parent[i].name,base)){
            //This is where the directory is
            index = i;
        }
    }
    //Pull the last item out of the parent directory, reallocate, and juggle entries around.
    s3dirent_t finalEntry = parent[entries-1];
    parent = realloc(parent, (entries-1)*sizeof(s3dirent_t));
    if(index<(entries-1)){
        parent[index] = finalEntry;
    }
    //Erase the item from the s3 bucket and push the parent back up.
    s3fs_remove_object(ctx->s3bucket,path);
    s3fs_remove_object(ctx->s3bucket,parentName);
    s3fs_put_object(ctx->s3bucket,parentName,(uint8_t*)parent,((entries-1)*sizeof(s3dirent_t)));

    free(str1);
    free(str2);
    free(targetDir);
    free(parent);
    return 0;
}

/*
 * Rename a file.
 */
int fs_rename(const char *path, const char *newpath) {
    fprintf(stderr, "fs_rename(fpath=\"%s\", newpath=\"%s\")\n", path, newpath);
    s3context_t *ctx = GET_PRIVATE_DATA;
    //pull down parent directory, change name of file there
    char *str1 = strdup(path);
    char *str2 = strdup(path);
    char *str3 = strdup(newpath);

    char *base = basename(str1);
    char *parentName = dirname(str2);
    char *newname = basename(str3);

    s3dirent_t *parent = NULL;
    ssize_t parentSize = s3fs_get_object(ctx->s3bucket,parentName,(uint8_t **)&parent,0,0);
    if(parentSize < 0){
        free(str1);
        free(str2);
        free(str3);
        free(parent);
        return -ENOENT;//Part of the directory does not exist.
    }
    //Modify the parent's metadata.
    time_t now = time(NULL)-18000;
    parent[0].metadata.st_mtime = now;
    parent[0].metadata.st_atime = now;
    int numEntries = (int) parentSize/sizeof(s3dirent_t);
    int i = 0;
    for (;i<numEntries;i++){
        if(!strcasecmp(parent[i].name,base)){
            strcpy(parent[i].name, newname);
        }
    }
    uint8_t  *entry = NULL;
    ssize_t fileSize = s3fs_get_object(ctx->s3bucket,path,(uint8_t**)&entry,0,0);
    if(fileSize < 0){
        free(str1);
        free(str2);
        free(str3);
        free(parent);
        free(entry);
        return -ENOENT;//Part of the directory does not exist.
    }

    s3fs_put_object(ctx->s3bucket,newpath,(uint8_t*)entry,fileSize);
    s3fs_remove_object(ctx->s3bucket,path);
    s3fs_remove_object(ctx->s3bucket,parentName);
    s3fs_put_object(ctx->s3bucket, parentName, (uint8_t*)parent, ((numEntries)*sizeof(s3dirent_t)));
    free(str1);
    free(str2);
    free(str3);
    free(parent);
    free(entry);
    return 0;
}
/*
 * Change the permission bits of a file.
 */
int fs_chmod(const char *path, mode_t mode) {
    fprintf(stderr, "fs_chmod(fpath=\"%s\", mode=0%03o)\n", path, mode);
    //s3context_t *ctx = GET_PRIVATE_DATA;
    //pull down parent directory, if path leads to a directory, change mode of . entry
    //if path leads to file, change file mode metadata
    return -EIO;
}

/*
 * Change the owner and group of a file.
 */
int fs_chown(const char *path, uid_t uid, gid_t gid) {
    fprintf(stderr, "fs_chown(path=\"%s\", uid=%d, gid=%d)\n", path, uid, gid);
    //s3context_t *ctx = GET_PRIVATE_DATA;
    //pull down parent directory, if path leads to a directory, change mode of . entry
    //if path leads to file, change file mode metadata
    return -EIO;
}

/*
 * Change the size of a file.
 */
int fs_truncate(const char *path, off_t newsize) {
    fprintf(stderr, "fs_truncate(path=\"%s\", newsize=%d)\n", path, (int)newsize);
    s3context_t *ctx = GET_PRIVATE_DATA;
    
    uint8_t *target = NULL;
    s3dirent_t *parent = NULL;
    char *str1 = strdup(path);
    char *str2 = strdup(path);
    char *childName = basename(str1);
    char *parentName = dirname(str2);
    ssize_t childSize = s3fs_get_object(ctx->s3bucket,path,(uint8_t**)&target,0,0);
    if(childSize < 0){
        free(target);
        free(str1);
        free(str2);
        return -ENOENT;//file does not exist
    }
    //If it does, pull down the parent directory.
    ssize_t parentSize = s3fs_get_object(ctx->s3bucket,parentName,(uint8_t**)&parent,0,0);
    int numEntries = (int) parentSize / sizeof(s3dirent_t);
    //Modify the metadata to reflect the new file attributes.
    int index = 0;
    int i = 0;
    for (;i<numEntries;i++){
        if(!strcasecmp(parent[i].name,childName)){
            index = i;
        }
    }
    //Make sure it isn't a directory.
    if(parent[i].type == 'd'){
        free(target);
        free(str1);
        free(str2);
        free(parent);
        return -EISDIR;
    }
    time_t now = time(NULL) - 18000;
    parent[i].metadata.st_size = 0;
    parent[i].metadata.st_mtime = now;
    parent[i].metadata.st_atime = now;
    parent[0].metadata.st_size -= childSize;
    parent[0].metadata.st_mtime = now;
    parent[0].metadata.st_atime = now;
    //Replace old files with the modified directory and new (empty) file.
    s3fs_put_object(ctx->s3bucket,path,(uint8_t*)target,0);
    s3fs_put_object(ctx->s3bucket,parentName,(uint8_t*)parent,parentSize);
    free(target);
    free(str1);
    free(str2);
    free(parent);
    return 0;
}
/*
 * Change the access and/or modification times of a file. 
 */
int fs_utime(const char *path, struct utimbuf *ubuf) {
    fprintf(stderr, "fs_utime(path=\"%s\")\n", path);
    //s3context_t *ctx = GET_PRIVATE_DATA;
    return 0;
}


/* 
 * File open operation
 * No creation, or truncation flags (O_CREAT, O_EXCL, O_TRUNC)
 * will be passed to open().  Open should check if the operation
 * is permitted for the given flags.  
 * 
 * Optionally open may also return an arbitrary filehandle in the 
 * fuse_file_info structure (fi->fh).
 * which will be passed to all file operations.
 * (In stages 1 and 2, you are advised to keep this function very,
 * very simple.)
 */
int fs_open(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_open(path\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
	//Check whether the file exists.
    s3dirent_t  *entry = NULL;
    char *str1 = strdup(path);
    char *str2 = strdup(path);
    char *dirpath = dirname(str1);
    char *end = basename(str2);
    int size = s3fs_get_object(ctx->s3bucket,dirpath,(uint8_t**)&entry,0,0);
    int numEntries = (int) size / sizeof(s3dirent_t);

    int i = 0;
    for (;i<numEntries;i++){
        fprintf(stderr, "%i:%s::%s\n",i,end,entry[i].name);
      if(!strcasecmp(end,entry[i].name)){
        if(entry[i].type == 'f'){
            free(str1);
            free(str2);
            free(entry);
            return 0;
        } 
      }
    }
    free(str1);
    free(str2);
    free(entry);
    return -ENOENT;
}

/* 
 * Read data from an open file
 *
 * Read should return exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  
 */
int fs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_read(path=\"%s\", buf=%p, size=%d, offset=%d)\n",
      path, buf, (int)size, (int)offset);
    s3context_t *ctx = GET_PRIVATE_DATA;
	//Check whether the file exists.
    uint8_t  *entry = NULL;
    int fileSize = s3fs_get_object(ctx->s3bucket,path,(uint8_t**)&entry,0,0);
    if(fileSize<0){
        free(entry);
        return -ENOENT;
    }
    memset(buf,0,size);
    memcpy(buf,(void *)&entry[offset],fileSize);
    free(entry);
    return fileSize;
}

/*
 * Write data to an open file
 *
 * Write should return exactly the number of bytes requested
 * except on error.
 */
int fs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_write(path=\"%s\", buf=%p, size=%d, offset=%d)\n",
          path, buf, (int)size, (int)offset);
    s3context_t *ctx = GET_PRIVATE_DATA;

    char *str1 = strdup(path);
    char *str2 = strdup(path);

    char *base = basename(str1);
    char *dir = dirname(str2);

    uint8_t *olddata = NULL;
    int rv = s3fs_get_object(ctx->s3bucket, path, (uint8_t **) &olddata, 0, offset);
    if(rv == -1){
        free(str1);
        free(str2);
        free(olddata);
        return -EIO; //no file
    }    
    int realsize = (rv - offset) + rv + size;
    olddata = realloc(olddata,realsize);
    memcpy((void *)&olddata[offset],(void *)buf,size);
    s3fs_remove_object(ctx->s3bucket, path);
    s3fs_put_object(ctx->s3bucket, path, (uint8_t *)olddata, realsize);    s3dirent_t *parent = NULL; //updating metadata
    
    int parentSize = s3fs_get_object(ctx->s3bucket, (char *) dir, (uint8_t **) &parent, 0, 0);
    int numEntries = (int) parentSize / sizeof(s3dirent_t);    time_t now = time(NULL)-18000;
    parent[0].metadata.st_mtime = now;
    parent[0].metadata.st_atime = now;

    int i = 0;
    for (;i<numEntries;i++){
        if(!strcasecmp(base,parent[i].name)){
            parent[i].metadata.st_size = realsize;
        }
    }
        s3fs_put_object(ctx->s3bucket, (char *) dir, (uint8_t*)parent, parentSize);    
        free(str1);
        free(str2);
        free(olddata);
        free(parent);
        return size;
}

/* 
 * Possibly flush cached data for one file.
 *
 * Flush is called on each close() of a file descriptor.  So if a
 * filesystem wants to return write errors in close() and the file
 * has cached dirty data, this is a good place to write back data
 * and return any errors.  Since many applications ignore close()
 * errors this is not always useful.
 */
int fs_flush(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_flush(path=\"%s\", fi=%p)\n", path, fi);
    //s3context_t *ctx = GET_PRIVATE_DATA;
    return 0;
}

/*
 * Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.  
 *
 * For every open() call there will be exactly one release() call
 * with the same flags and file descriptor.  It is possible to
 * have a file opened more than once, in which case only the last
 * release will mean, that no more reads/writes will happen on the
 * file.  The return value of release is ignored.
 */
int fs_release(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_release(path=\"%s\")\n", path);
    //s3context_t *ctx = GET_PRIVATE_DATA;
    return 0;
}
/*
 * Synchronize file contents; any cached data should be written back to 
 * stable storage.
 */
int fs_fsync(const char *path, int datasync, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_fsync(path=\"%s\")\n", path);
    return 0;
}

/*
 * Open directory
 *
 * This method should check if the open operation is permitted for
 * this directory
 */
int fs_opendir(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_opendir(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;

    uint8_t *retrieved_object = NULL;
    int rv = s3fs_get_object(ctx->s3bucket, path, &retrieved_object, 0, 0);

    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
        free(retrieved_object);
        return -ENOENT;
    } else if (rv < sizeof(s3dirent_t)) {
        printf("Failed to retrieve entire object (s3fs_get_object %d)\n", rv);
        free(retrieved_object);
        return -EIO;
    } else {
        printf("Successfully retrieved test object from s3 (s3fs_get_object)\n");
        int numEntries = (int) rv / sizeof(s3dirent_t);
        fprintf(stderr, "There are currently %i entries.\n", numEntries);
        free(retrieved_object);
        return 0;
    }
}

/*
 * Read directory.  See the project description for how to use the filler
 * function for filling in directory items.
 */
int fs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
         struct fuse_file_info *fi)
{
    fprintf(stderr, "fs_readdir(path=\"%s\", buf=%p, offset=%d)\n",
          path, buf, (int)offset);
    s3context_t *ctx = GET_PRIVATE_DATA;

    uint8_t *retrieved_object = NULL;
    int rv = s3fs_get_object(ctx->s3bucket, path, &retrieved_object, 0, 0);
    s3dirent_t *dir = (s3dirent_t *) retrieved_object;
    if (rv < 0) {
        printf("Failure in s3fs_get_object\n");
        free(retrieved_object);
        return -ENOENT;
    } else if (rv < sizeof(s3dirent_t)) {
        printf("Failed to retrieve entire object (s3fs_get_object %d)\n", rv);
        free(retrieved_object);
        return -EIO;
    } else {
        printf("Successfully retrieved test object from s3 (s3fs_get_object)\n");
        int numEntries = (int) rv / sizeof(s3dirent_t);
        fprintf(stderr, "There are currently %i entries.\n", numEntries);
        int i = 0;

        for (; i < numEntries; i++) {
            // call filler function to fill in directory name
            // to the supplied buffer
            if (filler(buf, dir[i].name, NULL, 0) != 0) {
                free(retrieved_object);
                return -ENOMEM;
            }
        }
        free(retrieved_object);
        return 0;
    }
    free(retrieved_object);
    return -EIO;
}
/*
 * Release directory.
 */
int fs_releasedir(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_releasedir(path=\"%s\")\n", path);
    //s3context_t *ctx = GET_PRIVATE_DATA;
    return 0;
}

/*
 * Synchronize directory contents; cached data should be saved to 
 * stable storage.
 */
int fs_fsyncdir(const char *path, int datasync, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_fsyncdir(path=\"%s\")\n", path);
    //s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}
/*
 * Initialize the file system.  This is called once upon
 * file system startup.
 */
void *fs_init(struct fuse_conn_info *conn)
{
    fprintf(stderr, "fs_init --- initializing file system.\n");
    s3context_t *ctx = GET_PRIVATE_DATA;
    fprintf(stderr, "%s\n",ctx->s3bucket);
    //clear bucket
    s3fs_clear_bucket(ctx->s3bucket);
    //create directory object for root
    s3dirent_t *root = malloc(sizeof(s3dirent_t));
    strcpy(root->name, ".");
    root->type = 'd';
    struct stat md;
    md.st_blocks = (blkcnt_t) 0;
    md.st_mode = (S_IFDIR | S_IRUSR | S_IWUSR | S_IXUSR);
    md.st_size = (off_t) sizeof(s3dirent_t);
    time_t now = time(NULL) - 18000;
    md.st_mtime = now;
    md.st_atime = now;
    md.st_ctime = now;
    md.st_nlink = (nlink_t) 1;
    md.st_uid = getuid();
    md.st_gid = getgid();
    root->metadata = md;
    //store directory object to S3
    if(sizeof(s3dirent_t) == s3fs_put_object(ctx->s3bucket, (char*)"/", (uint8_t*)root, sizeof(s3dirent_t))){
      fprintf(stderr, "fs_init --- file sysetem initalized.\n");
    }else{
      printf("\n"); 
      free(root);
      return NULL;
    }
    free(root);
    return ctx;
}

/*
 * Clean up filesystem -- free any allocated data.
 * Called once on filesystem exit.
 */
void fs_destroy(void *userdata) {
    fprintf(stderr, "fs_destroy --- shutting down file system.\n");
    free(userdata);
}

/*
 * Check file access permissions.  For now, just return 0 (success!)
 * Later, actually check permissions (don't bother initially).
 */
int fs_access(const char *path, int mask) {
    fprintf(stderr, "fs_access(path=\"%s\", mask=0%o)\n", path, mask);
    //s3context_t *ctx = GET_PRIVATE_DATA;
    return 0;
}

/*
 * Change the size of an open file.  Very similar to fs_truncate (and,
 * depending on your implementation), you could possibly treat it the
 * same as fs_truncate.
 */
int fs_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_ftruncate(path=\"%s\", offset=%d)\n", path, (int)offset);
    s3context_t *ctx = GET_PRIVATE_DATA;
    
    uint8_t *target = NULL;
    s3dirent_t *parent = NULL;

    char *str1 = strdup(path);
    char *str2 = strdup(path);

    char *childName = basename(str1);
    char *parentName = dirname(str2);
    ssize_t childSize = s3fs_get_object(ctx->s3bucket,path,(uint8_t**)&target,0,0);
    if(childSize < 0){
        free(str1);
        free(str2);
        free(target);
        return -ENOENT;//file does not exist
    }
    //If it does, pull down the parent directory.
    ssize_t parentSize = s3fs_get_object(ctx->s3bucket,parentName,(uint8_t**)&parent,0,0);
    int numEntries = (int) parentSize / sizeof(s3dirent_t);
    //Modify the metadata to reflect the new file attributes.
    int index = 0;
    int i = 0;
    for (;i<numEntries;i++){
        if(!strcasecmp(parent[i].name,childName)){
            index = i;
        }
    }
    //Make sure it isn't a directory.
    if(parent[i].type == 'd'){
        free(str1);
        free(str2);
        free(target);
        free(parent);
        return -EISDIR;
    }
    time_t now = time(NULL) - 18000;
    parent[i].metadata.st_size = 0;
    parent[i].metadata.st_mtime = now;
    parent[i].metadata.st_atime = now;
    parent[0].metadata.st_size -= childSize;
    parent[0].metadata.st_mtime = now;
    parent[0].metadata.st_atime = now;
    //Replace old files with the modified directory and new (empty) file.
    s3fs_put_object(ctx->s3bucket,path,(uint8_t*)target,0);
    s3fs_put_object(ctx->s3bucket,parentName,(uint8_t*)parent,parentSize);
    free(str1);
    free(str2);
    free(target);
    free(parent);
    return 0;
}
/*
 * The struct that contains pointers to all our callback
 * functions.  Those that are currently NULL aren't 
 * intended to be implemented in this project.
 */
struct fuse_operations s3fs_ops = {
  .getattr     = fs_getattr,    // get file attributes
  .readlink    = NULL,          // read a symbolic link
  .getdir      = NULL,          // deprecated function
  .mknod       = fs_mknod,      // create a file
  .mkdir       = fs_mkdir,      // create a directory
  .unlink      = fs_unlink,     // remove/unlink a file
  .rmdir       = fs_rmdir,      // remove a directory
  .symlink     = NULL,          // create a symbolic link
  .rename      = fs_rename,     // rename a file
  .link        = NULL,          // we don't support hard links
  .chmod       = fs_chmod,      // change mode bits
  .chown       = fs_chown,      // change ownership
  .truncate    = fs_truncate,   // truncate a file's size
  .utime       = fs_utime,      // update stat times for a file
  .open        = fs_open,       // open a file
  .read        = fs_read,       // read contents from an open file
  .write       = fs_write,      // write contents to an open file
  .statfs      = NULL,          // file sys stat: not implemented
  .flush       = fs_flush,      // flush file to stable storage
  .release     = fs_release,    // release/close file
  .fsync       = fs_fsync,      // sync file to disk
  .setxattr    = NULL,          // not implemented
  .getxattr    = NULL,          // not implemented
  .listxattr   = NULL,          // not implemented
  .removexattr = NULL,          // not implemented
  .opendir     = fs_opendir,    // open directory entry
  .readdir     = fs_readdir,    // read directory entry
  .releasedir  = fs_releasedir, // release/close directory
  .fsyncdir    = fs_fsyncdir,   // sync dirent to disk
  .init        = fs_init,       // initialize filesystem
  .destroy     = fs_destroy,    // cleanup/destroy filesystem
  .access      = fs_access,     // check access permissions for a file
  .create      = NULL,          // not implemented
  .ftruncate   = fs_ftruncate,  // truncate the file
  .fgetattr    = NULL           // not implemented
};



/* 
 * You shouldn't need to change anything here.  If you need to
 * add more items to the filesystem context object (which currently
 * only has the S3 bucket name), you might want to initialize that
 * here (but you could also reasonably do that in fs_init).
 */
int main(int argc, char *argv[]) {
    // don't allow anything to continue if we're running as root.  bad stuff.
    if ((getuid() == 0) || (geteuid() == 0)) {
      fprintf(stderr, "Don't run this as root.\n");
      return -1;
    }
    s3context_t *stateinfo = malloc(sizeof(s3context_t));
    memset(stateinfo, 0, sizeof(s3context_t));

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
    strncpy((*stateinfo).s3bucket, s3bucket, BUFFERSIZE);

    fprintf(stderr, "Initializing s3 credentials\n");
    s3fs_init_credentials(s3key, s3secret);

    fprintf(stderr, "Totally clearing s3 bucket\n");
    s3fs_clear_bucket(s3bucket);
    fprintf(stderr, "Starting up FUSE file system.\n");
    int fuse_stat = fuse_main(argc, argv, &s3fs_ops, stateinfo);
    fprintf(stderr, "Startup function (fuse_main) returned %d\n", fuse_stat);

    return fuse_stat;
}
