
#include "s3fs.h"
#include "libs3_wrapper.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/xattr.h>

typedef struct {
	char a[100];
} test_struct;






int main(void) {
	s3dirent_t *a = malloc(sizeof(s3dirent_t));
	printf("%d\n",sizeof(s3dirent_t));
	a->metadata.st_size = 100;
	printf("%d\n",sizeof(*a));
	return 0;
}
