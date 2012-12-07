#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <libgen.h>

typedef struct {
	char a[100];
} test_struct;

typedef struct {
	char name[1024];
	int num_entries;
	unsigned char type;
	struct stat *metadata;
} s3dirent_t;


s3dirent_t* directory() {
	struct stat meta;
	meta.st_mode = (S_IFDIR | S_IRUSR | S_IWUSR | S_IXUSR);
	printf("%d\n", meta.st_mode);
	s3dirent_t* dir = malloc(sizeof(s3dirent_t));
	dir->metadata = &meta;
	return dir;
}

int main(void) {
	char *path = "/hello";
	char *temp_path = path;
	printf("%s\n",temp_path);
	char* dir = dirname(temp_path); //base = basename(path);
	printf("after dirname\n");
	//printf("%s\n",dir);
	//printf("%s\n",base);
	//printf("%s\n",temp_path);
	printf("%s\n",path);
	return 0;
	
}
