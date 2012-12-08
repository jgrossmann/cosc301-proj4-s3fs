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
	uint8_t *dir = NULL, *dir1 = NULL;
	int i = 0;
	char *temp_path1 = strdup(path), *temp_path2 = strdup(path), *temp_path3 = strdup(path);
	char *dir_name = strdup(dirname(temp_path1)), *base_name = strdup(basename(temp_path2));
	free(temp_path1); free(temp_path2);
	ssize_t dir_size;
	if(s3fs_get_object(ctx->s3bucket, temp_path3, &dir1, 0,0)< 0) {
		fprintf(stderr,"INVALID PATH: fs_getattr\n");
		free(dir_name); free(base_name); free(temp_path3);
		return -ENOENT;
	}
	if(!strcmp(path, "/")) {
		s3dirent_t *directory = (s3dirent_t *)dir1;
		*statbuf = directory[0].metadata;
		free(directory); free(dir_name); free(base_name);
		return 0;
	}
	if((dir_size = s3fs_get_object(ctx->s3bucket, dir_name, &dir, 0,0))<0) {
		fprintf(stderr, "INVALID PATH: fs_getattr\n");
		free(temp_path3); free(base_name); free(dir_name);
		return -ENOENT;
	}else {
		s3dirent_t *direct = (s3dirent_t*)dir;
		for(;i<dir_size/sizeof(s3dirent_t);i++) {
			if(!strcmp(direct[i].name,base_name)) {
				if(direct[i].type == 'D') {
					s3dirent_t *new_dirent = (s3dirent_t *)dir1;
					*statbuf = new_dirent[0].metadata;
					free(new_dirent); free(direct); free(temp_path3); free(base_name); free(dir_name);
					return 0;
				}
				printf("FOUND A FILE\n");
				*statbuf = direct[i].metadata;
				free(direct); free(temp_path3); free(base_name); free(dir_name); free(dir1);
				return 0;
			}
		}
				
		fprintf(stderr, "FS_GETATTR: path not found\n");
		free(direct);free(temp_path3); free(base_name); free(dir_name);free(dir1);
		return -ENOENT;
	}
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
	printf("after initial function print\n");
    s3context_t *ctx = GET_PRIVATE_DATA;
	printf("after get private data: fs_mknod\n");
	uint8_t *file = NULL,*dir = NULL;
	if(s3fs_get_object(ctx->s3bucket, path, &file, 0, 0)>=0) {
		fprintf(stderr, "ALREADY A FILE WITH THAT NAME: fs_mknod\n");
		free(file);
		return -EEXIST;
	}
	printf("after check for duplicate name\n");
	char *temp_path1 = strdup(path), *temp_path2 = strdup(path), *temp_path3 = strdup(path);
	char *dir_name = strdup(dirname(temp_path1)), *base_name = strdup(basename(temp_path2));
	ssize_t dir_size, new_dir_size;
	if((dir_size = s3fs_get_object(ctx->s3bucket, dir_name, &dir, 0,0))<0) {
		fprintf(stderr, "PARENT DIRECTORY NOT FOUND: fs_mknod\n");
		free(temp_path1); free(temp_path2); free(temp_path3); free(dir_name); free(base_name);
		return -ENOENT;
	}
	printf("after get parent directory: fs_mknod");
	s3dirent_t *parent_dir, file_entry;
	parent_dir = (s3dirent_t*)dir; //file_entry= malloc(sizeof(s3dirent_t));
	if(parent_dir->type != 'D') {
		fprintf(stderr, "PARENT MUST BE A DIRECTORY: fs_mknod\n");
		free(temp_path1); free(temp_path2); free(temp_path3); free(dir_name); free(base_name);free(parent_dir);
		//free(file_entry);
		return ENOTDIR;
	}
	printf("after check if parent dir is dir: fs_mknod\n");
	struct stat meta;
	meta.st_dev = dev;
	meta.st_mode = mode;
	meta.st_nlink = 1;
	meta.st_uid = getuid();
	meta.st_gid = getgid();
	struct timeval tv;
	time_t the_time;
	gettimeofday(&tv, NULL);
	the_time = tv.tv_sec;
	meta.st_atime = the_time; meta.st_mtime = the_time; meta.st_ctime = the_time;
	file_entry.type = 'F'; strcpy(file_entry.name, base_name);
	meta.st_size = 0;
	file_entry.metadata = meta; new_dir_size = dir_size + sizeof(s3dirent_t);
	parent_dir = realloc(parent_dir, new_dir_size);
	parent_dir[dir_size/sizeof(s3dirent_t)] = file_entry;//TRY TO DO = *FILE_ENTRY, OR MAKE IT A STACK VARIABLE
	printf("after assign new file_entry to dir, before put update array back: fs_mknod\n");
	printf("dir_name: %s\n",dir_name);
	printf("after dir_name\n");
	if(s3fs_put_object(ctx->s3bucket, dir_name, (uint8_t*)parent_dir, new_dir_size)!= new_dir_size) {
		fprintf(stderr, "UPDATED DIRECTORY DID NOT GET PUT BACK CORRECTLY, REMOVE AND TRY AGAIN: fs_mknod");
		free(temp_path1); free(temp_path2); free(temp_path3); free(dir_name); free(base_name); free(parent_dir);
		return -EIO;
	}
	uint8_t *new_file = NULL;
	if(s3fs_put_object(ctx->s3bucket, path, new_file, 0) != 0) {
		fprintf(stderr, "NEW FILE DID NOT GET PUT BACK CORRECTLY: fs_mknod\n");
		free(temp_path1); free(temp_path2); free(temp_path3); free(dir_name); free(base_name); free(parent_dir);
		return -EIO;
	}
	printf("after put back updated dir: fs_mknod\n");
	free(temp_path1); free(temp_path2); free(temp_path3); free(dir_name); free(base_name); free(parent_dir);
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
	printf("FIRST CHECK PATH TO MKDIR: %s\n\n",path);
	char *temp_path1 = strdup(path), *temp_path2 = strdup(path), *temp_path3 = strdup(path);
	printf("FIRST CHECK OF TEMP_PATH MKDRI: %s\n\n",temp_path1);
	char *dir_name = strdup(dirname(temp_path1));
	printf("DIR NAME: %s\n\n", dir_name);
	char *base_name = strdup(basename(temp_path2));
	printf("BASE NAME: %s\n\n", base_name);
	uint8_t *directory = NULL, *temp = NULL;
	printf("PATH TO MKDIR CHECK: %s\n\n", path);
	if(s3fs_get_object(ctx->s3bucket, temp_path3, &temp, 0, 0) >= 0) {
		fprintf(stderr, "Directory name already exists");
		free(temp); free(temp_path1); free(temp_path2); free(temp_path3);
		free(dir_name); free(base_name);
		return -EEXIST;
	}
	free(temp);
	printf("PAST GET OBJECT IN MKDIR CHECK\n\n");
	ssize_t directory_size = NULL;
	struct stat meta;
	if((directory_size = s3fs_get_object(ctx->s3bucket, dir_name, &directory, 0, 0)) < 0) {
		fprintf(stderr, "Could not find parent directory: fs_mkdir");
		free(temp_path1); free(temp_path2); free(temp_path3); free(base_name); free(dir_name);
		return -ENOENT;
	}
	s3dirent_t *dir_update = (s3dirent_t*)directory;
	printf("PAST GET PARENT DIRECTORY IN MKDIR CHECK\n\n");
	meta.st_mode = (S_IFDIR | S_IRWXU);
	printf("past mode bitwise or set\n");
	meta.st_nlink = 1;
	meta.st_uid = getuid();
	meta.st_gid = getgid();
	struct timeval tv;
	time_t the_time;
	gettimeofday(&tv, NULL);
	the_time = tv.tv_sec;
	meta.st_atime = the_time; meta.st_mtime = the_time; meta.st_ctime = the_time;
	printf("before malloc\n");
	s3dirent_t *new_dir_self = malloc(sizeof(s3dirent_t)), new_dir;
	printf("before strcpy\n");
	strcpy(new_dir_self->name, "."); printf("after strcpy on same line\n");new_dir_self->type = 'D'; new_dir_self->num_entries = 1;
	printf("in between strncpys\n");
	strcpy(new_dir.name,base_name); new_dir.type = 'D'; new_dir.num_entries = 0;
	printf("new_dir name: %s \n",new_dir.name);
	printf("after second strncpy\n");
	meta.st_size = sizeof(*new_dir_self);
	printf("size of new dir: %d\n", meta.st_size);
	printf("same line as metadata assign\n");new_dir_self->metadata = meta;
	printf("right before realloc\n");
	dir_update=realloc(dir_update, directory_size + sizeof(new_dir));
	printf("after realloc\n");
	printf("right before array index input\n");
	printf("dir_update size then index: %d, %d\n",directory_size + sizeof(new_dir),directory_size/sizeof(s3dirent_t));
	dir_update[directory_size/sizeof(s3dirent_t)] = new_dir;
	ssize_t dir_update_size = directory_size + sizeof(s3dirent_t);
	dir_update[0].metadata.st_size = (off_t)dir_update_size;
	printf("right before put back updated directory\n");
	printf("updated directory path: %s \n",dir_name);
	if(s3fs_put_object(ctx->s3bucket, dir_name, (uint8_t *)dir_update, dir_update_size) != dir_update_size) {
		fprintf(stderr, "OBJECT DID NOT TRANSFER CORRECTLY: fs_mkdir");
		free(temp_path1); free(temp_path2); free(temp_path3); free(new_dir_self); free(dir_update);
		free(base_name); free(dir_name);
		return -EIO;
	}
	printf("after updated directory put, before new directory put\n");
	printf("new directory path: %s \n", path);
	if(s3fs_put_object(ctx->s3bucket, path, (uint8_t *)new_dir_self, sizeof(s3dirent_t)) != sizeof(s3dirent_t)) {
		fprintf(stderr, "OBJECT DID NOT TRANSFER CORRECTLY: fs_mkdir");
		free(temp_path1); free(temp_path2); free(temp_path3); free(new_dir_self); free(dir_update);
		free(base_name); free(dir_name);
		return -EIO;
	}
	printf("FINISHED MKDIR\n\n");
	free(base_name); free(dir_name);
	free(dir_update);free(temp_path1); free(temp_path2); free(temp_path3); free(new_dir_self);
    return 0;
}

/*
 * Remove a file.
 */
int fs_unlink(const char *path) {
    fprintf(stderr, "fs_unlink(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
	ssize_t dir_size;
	int i = 0, j = 0;
	uint8_t *dir;
	if(fs_open(path, (struct fuse_file_info *)NULL) != 0) {
		fprintf(stderr, "INVALID PATH\n");
		return -ENOENT;
	}
	char *temp_path1 = strdup(path), *temp_path2 = strdup(path);
	char *dir_name = strdup(dirname(temp_path1)), *base_name = strdup(basename(temp_path2));
	free(temp_path1); free(temp_path2);
	dir_size=s3fs_get_object(ctx->s3bucket, dir_name, &dir, 0, 0);
	s3dirent_t* direct = (s3dirent_t*)dir;
	for(;i<dir_size/sizeof(s3dirent_t);i++) {
		if(!strcmp(direct[i].name, base_name)) {
			i++; break;
		}
	}
	for(;i<dir_size/sizeof(s3dirent_t);i++) {
		j = i-1;
		direct[j] = direct[i];
	}
	s3dirent_t* new_dir;
	ssize_t new_dir_size;
	new_dir = malloc(dir_size - sizeof(s3dirent_t));
	memcpy(new_dir, direct, dir_size - sizeof(s3dirent_t));
	new_dir_size = dir_size - sizeof(s3dirent_t);
	printf("dir_size: %d   new_dir_size: %d\n", dir_size, new_dir_size);
	printf("updated directory from file remove path: %s\n",dir_name);
	if(s3fs_put_object(ctx->s3bucket, dir_name, (uint8_t*)new_dir, new_dir_size)!= new_dir_size) {
		fprintf(stderr, "SOMETHING WENT WRONG WHILE PUTTING TO S3: fs_unlink\n");
		free(new_dir); free(direct); free(base_name); free(dir_name);
		return -EIO;
	}
	if(s3fs_remove_object(ctx->s3bucket, path) != 0) {
		fprintf(stderr, "ERROR WHILE TRYING TO DELTE FILE: fs_unlink");
		free(new_dir); free(direct); free(base_name); free(dir_name);
		return -EIO;
	}
	free(new_dir); free(direct); free(base_name); free(dir_name);
    return 0;
}

/*
 * Remove a directory. 
 */
int fs_rmdir(const char *path) {
    fprintf(stderr, "fs_rmdir(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
	uint8_t *dir = NULL;
	char *temp_path = strdup(path), *temp_path1 = strdup(path), *temp_path2 = strdup(path);
	char* dir_name = strdup(dirname(temp_path)), *base_name = strdup(basename(temp_path1));
	printf("DIR_NAME: %s , BASE_NAME: %s\n",dir_name, base_name);
	ssize_t dir_size;
	if((dir_size = s3fs_get_object(ctx->s3bucket, temp_path2, &dir, 0, 0)) < 0){
		fprintf(stderr, "INVALID DIRECTORY: fs_rmdir\n");
		free(temp_path); free(temp_path1); free(temp_path2);free(dir_name); free(base_name);
		return -ENOENT;
	}
	printf("after getting dir to remove\n");
	s3dirent_t *directory = (s3dirent_t *)dir;
	if(dir_size/sizeof(s3dirent_t) > 1) {
		fprintf(stderr, "CANT DELETE DIRECTORY WITH OTHER THINGS IN IT: fs_rmdir\n");
		free(directory);free(temp_path); free(temp_path1); free(temp_path2);free(dir_name); free(base_name);
		return -ENOTEMPTY;
	}
	printf("before removing dir\n");
	if(s3fs_remove_object(ctx->s3bucket, temp_path2) != 0) {
		fprintf(stderr, "COULD NOT DELETE OBJECT: fs_rmdir\n");
		free(directory);free(temp_path); free(temp_path1); free(temp_path2);free(dir_name); free(base_name);
		return -EIO;
	}
	printf("after removing dir, before getting parent dir\n");
	if((dir_size = s3fs_get_object(ctx->s3bucket, dir_name, &dir, 0, 0)) < 0){
		fprintf(stderr, "INVALID DIRECTORY: fs_rmdir\n");
		free(directory); free(temp_path); free(temp_path1); free(temp_path2);free(dir_name); free(base_name);
		return -ENOENT;
	}
	s3dirent_t *dirent = (s3dirent_t *)dir;
	int numdirents = dir_size/sizeof(s3dirent_t), i = 0;
	printf("before for loop to find entry in parent to remove\n");
	for(;i<numdirents;i++) {
		if(!strcmp(dirent[i].name, base_name)) {
			break;
		}
	}
	if(i>=numdirents) {
		fprintf(stderr, "NO DIRECTORY ENTRY TO REMOVE IN PARENT: fs_rmdir\n");
		free(dirent);free(temp_path); free(temp_path1); free(temp_path2);free(dir_name); free(base_name);
		return -EIO;
	}
	i++;
	int j = 0;
	for(;i<numdirents;i++) {
		j = i-1;
		dirent[j] = dirent[i];
	}
	printf("before updating entry\n");
	ssize_t new_dir_size;
	s3dirent_t *new_dirent = malloc(dir_size - sizeof(s3dirent_t));
	memcpy(new_dirent, dirent, dir_size - sizeof(s3dirent_t));
	new_dir_size = dir_size - sizeof(s3dirent_t);
	if((dir_size = s3fs_put_object(ctx->s3bucket, dir_name, (uint8_t *)new_dirent, new_dir_size)) !=new_dir_size) {
		fprintf(stderr, "DID NOT PUT OBJECT BACK CORRECTLY\n");
		free(new_dirent); free(dirent);free(temp_path); free(temp_path1); free(temp_path2);free(dir_name); free(base_name);
		return -EIO;
	}
	free(dirent); free(new_dirent);free(temp_path); free(temp_path1); free(temp_path2);free(dir_name); free(base_name);
	printf("SUCCESSFULLY REMOVED DIRECTORY!\n");
		
	return 0;
}

/*
 * Rename a file.
 */
int fs_rename(const char *path, const char *newpath) {
    fprintf(stderr, "fs_rename(fpath=\"%s\", newpath=\"%s\")\n", path, newpath);
    s3context_t *ctx = GET_PRIVATE_DATA;
	ssize_t dir_size, dir_size1;
	uint8_t *dir = NULL, *dir1 = NULL;
	char type;
	int i = 0, j = NULL;
	char *temp_path = strdup(path), *temp_path1 = strdup(path), *temp_new_path1 = strdup(newpath), *temp_new_path2=strdup(newpath);
	char* dir_name = strdup(dirname(temp_path)), *base_name = strdup(basename(temp_path1));
	char *new_dir_name=strdup(dirname(temp_new_path1)), *new_base_name = strdup(basename(temp_new_path2));
	char *temp_new_path3=strdup(newpath);
	free(temp_path); free(temp_path1); free(temp_new_path1); free(temp_new_path2);
	if(!strcmp("/", path)) {
		fprintf(stderr,"CANNOT RENAME ROOT DIRECTORY: fs_rename\n");
		free(dir_name); free(base_name); free(new_dir_name); free(new_base_name);
		return -EPERM;
	}
	if((dir_size1=s3fs_get_object(ctx->s3bucket, path, &dir1, 0, 0))>=0) {
		printf("PATH TO REMOVE IN FS_RENAME DIRECTORY: %s\n",path);
		if((dir_size=s3fs_get_object(ctx->s3bucket, dir_name, &dir, 0, 0))<0) {
			fprintf(stderr, "INVALID PATH: fs_rename\n");
			free(base_name); free(dir_name); free(new_dir_name); free(new_base_name);free(temp_new_path3);free(dir1);
			return -ENOENT;
		}
	s3dirent_t *dirent = (s3dirent_t*)dir;
	for(;i<dir_size/sizeof(s3dirent_t);i++) {
		if(!strcmp(dirent[i].name, base_name)) {
			j = i;
		}else if(!strcmp(dirent[i].name, new_base_name)) {
			fprintf(stderr, "FILE NAME ALREADY USED: fs_rename\n");
			free(base_name); free(dir_name); free(new_dir_name); free(new_base_name); free(dirent);free(temp_new_path3);
			free(dir1);
			return -EEXIST;
		}
	}
	if(j==NULL) {
		fprintf(stderr, "INVALID PATH: fs_rename\n");
		free(base_name); free(dir_name); free(new_dir_name); free(new_base_name);free(temp_new_path3);free(dir1);
		return -ENOENT;
	}
	strcpy(dirent[j].name,new_base_name);
	if(s3fs_put_object(ctx->s3bucket, new_dir_name, (uint8_t *)dirent, dir_size)!=dir_size) {
		fprintf(stderr, "ERROR WHILE PUTTING UPDATED DIRECTORY TO S3: fs_rename\n");
		free(base_name); free(dir_name); free(new_dir_name); free(new_base_name); free(dirent);
		free(temp_new_path3); free(dir1);
		return -EIO;
	}
	if(s3fs_remove_object(ctx->s3bucket, path)!=0) {
		fprintf(stderr, "ERROR WHILE ERASING ORIGINAL DIRECTORY TO RENAME: fs_rename\n");
		free(dirent); free(base_name); free(dir_name); free(new_dir_name); free(new_base_name);free(temp_new_path3);
		free(dir1);
		return -EIO;
	}
	if(s3fs_put_object(ctx->s3bucket, newpath, dir1, dir_size1)!=dir_size1) {
		fprintf(stderr, "ERROR WHILE PUTTING RENAMED OBJECT BACK: fs_rename\n");
		free(dirent); free(base_name); free(dir_name); free(new_dir_name); free(new_base_name);free(temp_new_path3);
		free(dir1);
		return -EIO;
	}
	free(dirent); free(base_name); free(dir_name); free(new_dir_name); free(new_base_name);free(temp_new_path3);
	free(dir1);
			
    return 0;
	}else {
		fprintf(stderr, "INVALID PATH:fs_rename\n");
		free(base_name); free(dir_name); free(new_dir_name); free(new_base_name); free(temp_new_path3);
		return -ENOENT;
	}
}

/*
 * Change the permission bits of a file.
 */
int fs_chmod(const char *path, mode_t mode) {
    fprintf(stderr, "fs_chmod(fpath=\"%s\", mode=0%03o)\n", path, mode);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}

/*
 * Change the owner and group of a file.
 */
int fs_chown(const char *path, uid_t uid, gid_t gid) {
    fprintf(stderr, "fs_chown(path=\"%s\", uid=%d, gid=%d)\n", path, uid, gid);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}

/*
 * Change the size of a file.
 */
int fs_truncate(const char *path, off_t newsize) {
    fprintf(stderr, "fs_truncate(path=\"%s\", newsize=%d)\n", path, (int)newsize);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}

/*
 * Change the access and/or modification times of a file. 
 */
int fs_utime(const char *path, struct utimbuf *ubuf) {
    fprintf(stderr, "fs_utime(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
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
	uint8_t* file;
	if(s3fs_get_object(ctx->s3bucket, path, &file, 0,0)<0) {
		fprintf(stderr, "INVALID PATH\n");
		return -ENOENT;
	}
	free(file); 
    return 0;
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
	uint8_t *file = NULL;
	ssize_t file_size, new_size;
	int i = offset;
	if((file_size = s3fs_get_object(ctx->s3bucket, path, &file, 0,0))<0) {
		fprintf(stderr, "INVALID PATH: fs_read\n");
		return -ENOENT;
	}
	char *file_data = strdup((char *)file);
	if(offset+size > file_size) {
		new_size = offset + size;
		for(;i<file_size;i++) {
			buf[i] = file_data[i];
		}
		for(;i<new_size;i++) {
			buf[i] = 0;
		}
	}else {
		new_size = file_size;
		for(;i<new_size;i++) {
			buf[i] = file_data[i];
		}
	}
	free(file_data);
    return new_size;
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
	if(!strcmp(path, "/")) {
		fprintf(stderr,"CANNOT WRITE TO A DIRECTORY ESPECIALLY ROOT: fs_write\n");
		return -EPERM;
	}
	uint8_t* file = NULL;
	ssize_t file_size;
	printf("before grab the file\n");
	if((file_size=s3fs_get_object(ctx->s3bucket, path, &file, 0, 0))<0) {
		fprintf(stderr, "INVALID PATH: fs_write\n");
		return -ENOENT;
	}
	printf("grabbed file\n");
	ssize_t new_size;
	char *file_data = (char *)file;
	printf("strdup file data\n");
	if(file_size <= offset + size) {
		new_size = offset + size - file_size;
		file_data = realloc(file_data, new_size);
	}else {
		new_size = file_size;
	}
	printf("after assigning new size/ realloc\n");
	int i = offset;
	for(;i<new_size;i++) {
		file_data[i] = buf[i];
	}
	printf("after transfering data\n");
	if(s3fs_put_object(ctx->s3bucket, path, (uint8_t *)file_data, new_size) != new_size) {
		fprintf(stderr,"WRITTEN FILE DID NOT PUT BACK CORRECTLY: fs_write\n");
		free(file_data);
		return -EIO;
	}
	free(file_data);
	uint8_t *file_entry = NULL;
	ssize_t file_entry_size;
	char *temp_path = strdup(path), *temp_path1 = strdup(path);
	char *dir_name = strdup(dirname(temp_path)), *base_name = strdup(basename(base_name));
	free(temp_path); free(temp_path1);
	if((file_entry_size = s3fs_get_object(ctx->s3bucket, dir_name, &file_entry, 0, 0))< 0) {
		fprintf(stderr, "COULD NOT UPDATE FILE ENTRY AFTER WRITE: fs_write\n");
		free(dir_name); free(base_name);
		return -EIO;
	}
	printf("after grabbed directory to update entry size meta\n");
	s3dirent_t *update_file_entry = (s3dirent_t *)file_entry;
	printf("update size: %d\n",new_size);
	for(;i<file_entry_size/sizeof(s3dirent_t);i++) {
		if(!strcmp(update_file_entry[i].name, base_name)) {
			update_file_entry[i].metadata.st_size = new_size;
		}
	}
	if(s3fs_put_object(ctx->s3bucket, path, (uint8_t *)update_file_entry, file_entry_size)!=file_entry_size) {
		fprintf(stderr, "COULD NOT PUT UPDATED DIRECTORY ENTRY BACK: fs_write\n");
		free(dir_name); free(base_name); free(update_file_entry);
		return -EIO;
	}
	printf("yay wrote to a file\n");	
    return new_size;
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
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
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
    s3context_t *ctx = GET_PRIVATE_DATA;
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
	uint8_t *dir;
	if(s3fs_get_object(ctx->s3bucket, path, &dir, 0,0) < 0) {
		fprintf(stderr, "CANNOT ACCESS DIRECTORY: fs_opendir fail\n");
		return -EIO;
	}
	free(dir);
	return 0;
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
	char* temp_path = strdup(path);
	uint8_t *dir = NULL;
	ssize_t dir_size = NULL;
	if((dir_size = s3fs_get_object(ctx->s3bucket, temp_path, &dir, 0,0))<0) {
		fprintf(stderr, "CANNOT READ FILE: fs_readdir\n");
		free(temp_path);
		return -EIO;
	}
	s3dirent_t *directory = (s3dirent_t *)dir;
	if(directory[0].type != 'D') {
		fprintf(stderr, "NOT A DIRECTORY!:fs_readdir\n");
		free(directory); free(temp_path);return -EIO;
	}
	int numdirent = dir_size/sizeof(s3dirent_t), i = 0;
	for(;i<numdirent;i++) {
		if(filler(buf, directory[i].name, NULL, 0) != 0) {
			free(directory);free(temp_path);
			return -ENOMEM;
		}
	}
	free(directory); free(temp_path);

    return 0;
}

/*
 * Release directory.
 */
int fs_releasedir(const char *path, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_releasedir(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return 0;
}

/*
 * Synchronize directory contents; cached data should be saved to 
 * stable storage.
 */
int fs_fsyncdir(const char *path, int datasync, struct fuse_file_info *fi) {
    fprintf(stderr, "fs_fsyncdir(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
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
	if(s3fs_clear_bucket(ctx->s3bucket)!=0) {
		fprintf(stderr, "Failed to initialize file system: Error clearing bucket");
		return -EIO;
	}
	s3dirent_t root;
	struct stat meta;//= malloc(sizeof(struct stat));
	meta.st_mode = (S_IFDIR | S_IRWXU);
	meta.st_nlink = 1;
	meta.st_uid = getuid();
	meta.st_gid = getgid();
	struct timeval tv;
	time_t the_time;
	gettimeofday(&tv, NULL);
	the_time = tv.tv_sec;
	meta.st_atime = the_time; meta.st_mtime = the_time; meta.st_ctime = the_time;
	strcpy(root.name, "."); root.num_entries = 1; root.type = 'D';
	root.metadata = meta; root.metadata.st_size = sizeof(s3dirent_t);
	printf("ROOT PERMISSIONS: %o\n",root.metadata.st_mode);
	if(s3fs_put_object(ctx->s3bucket, "/", (uint8_t *)&root, sizeof(root)) != sizeof(root)) {
		fprintf(stderr, "PUT OBJECT FAILED IN FS_INIT");
		return ctx;
	}
	
	printf("init worked? \n");
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
    s3context_t *ctx = GET_PRIVATE_DATA;
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
    return -EIO;
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
