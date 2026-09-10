#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Inject one ENOSPC only into this process's staged native snapshot data file.
 * Do not fill the shared laboratory disk or affect another process/root. */
ssize_t write(int fd, const void *buffer, size_t count) {
    ssize_t (*next_write)(int, const void *, size_t) = dlsym(RTLD_NEXT, "write");
    static atomic_bool fired = false;
    const char *root = getenv("NATIVE_ENOSPC_ROOT");
    if (root != NULL && count != 0) {
        char link[64], path[4096];
        snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
        ssize_t length = readlink(link, path, sizeof(path) - 1);
        if (length > 0) {
            path[length] = '\0';
            size_t root_len = strlen(root), path_len = strlen(path);
            if (path_len > root_len && strncmp(path, root, root_len) == 0 && path[root_len] == '/'
                && strstr(path, "/snapshots/") != NULL && strstr(path, "/.stage-") != NULL
                && path_len >= 9 && strcmp(path + path_len - 9, "/data.bin") == 0
                && !atomic_exchange(&fired, true)) {
                const char marker[] = "NATIVE_ENOSPC_INJECTED\n";
                next_write(STDERR_FILENO, marker, sizeof(marker) - 1);
                errno = ENOSPC;
                return -1;
            }
        }
    }
    return next_write(fd, buffer, count);
}
