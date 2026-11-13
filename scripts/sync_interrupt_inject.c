/* Test-only Linux interposer. Perform a real successful sync, report EINTR,
 * then allow or fail the retry. This library is never linked into the engine. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

static unsigned long calls;

static int is_target(int fd) {
    const char *target = getenv("EMDA_SYNC_TARGET");
    if (!target || target[0] != '/') return 0;
    char link[64], actual[PATH_MAX];
    int n = snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    if (n <= 0 || (size_t)n >= sizeof(link)) return 0;
    ssize_t length = syscall(SYS_readlink, link, actual, sizeof(actual) - 1);
    if (length < 0) return 0;
    actual[length] = '\0';
    return strcmp(actual, target) == 0;
}

static void trace(const char *name, unsigned long sequence, int actual,
                  int reported, int error) {
    int original_errno = errno;
    const char *path = getenv("EMDA_SYNC_TRACE");
    if (path) {
        int fd = syscall(SYS_openat, AT_FDCWD, path,
                         O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
        if (fd >= 0) {
            char line[160];
            int length = snprintf(line, sizeof(line), "%s %lu %d %d %d\n",
                                  name, sequence, actual, reported, error);
            if (length > 0 && (size_t)length < sizeof(line))
                (void)syscall(SYS_write, fd, line, (size_t)length);
            (void)syscall(SYS_close, fd);
        }
    }
    errno = original_errno;
}

static int synchronize(int fd, long number, const char *name) {
    if (!is_target(fd)) return (int)syscall(number, fd);
    unsigned long sequence = __atomic_add_fetch(&calls, 1, __ATOMIC_SEQ_CST);
    const char *mode = getenv("EMDA_SYNC_MODE");
    if (sequence == 2 && mode && strcmp(mode, "error") == 0) {
        trace(name, sequence, -2, -1, EIO); /* -2 means no syscall issued. */
        errno = EIO;
        return -1;
    }
    int actual = (int)syscall(number, fd);
    int saved_errno = actual < 0 ? errno : 0;
    if (sequence == 1 && actual == 0) {
        trace(name, sequence, actual, -1, EINTR);
        errno = EINTR;
        return -1;
    }
    trace(name, sequence, actual, actual, saved_errno);
    if (actual < 0) errno = saved_errno;
    return actual;
}

int fsync(int fd) { return synchronize(fd, SYS_fsync, "fsync"); }
int fdatasync(int fd) { return synchronize(fd, SYS_fdatasync, "fdatasync"); }
