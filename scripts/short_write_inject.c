/* Test-only Linux interposer: perform real short writes to one exact file.
 * No function here is linked into the database engine. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

static int short_written;
static int delayed_write;

static int target_fd(int fd) {
    const char *target = getenv("EMDA_SHORT_TARGET");
    if (!target || target[0] != '/') return 0;
    char link[64], actual[PATH_MAX];
    int n = snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    if (n <= 0 || (size_t)n >= sizeof(link)) return 0;
    ssize_t length = syscall(SYS_readlink, link, actual, sizeof(actual) - 1);
    if (length < 0) return 0;
    actual[length] = '\0';
    return strcmp(actual, target) == 0;
}

static size_t limit_bytes(void) {
    const char *value = getenv("EMDA_SHORT_LIMIT");
    if (!value || !*value) return 0;
    char *end;
    unsigned long parsed = strtoul(value, &end, 10);
    if (*end || parsed == 0 || parsed > 1048576) return 0;
    return (size_t)parsed;
}

static int fail_next(void) {
    const char *mode = getenv("EMDA_SHORT_MODE");
    return mode && strcmp(mode, "error") == 0 &&
        __atomic_load_n(&short_written, __ATOMIC_SEQ_CST);
}

static int enabled_for(const char *name) {
    const char *only = getenv("EMDA_SHORT_ONLY");
    return !only || !*only || strcmp(only, name) == 0;
}

static void delay_first_write(void) {
    const char *value = getenv("EMDA_SHORT_DELAY_FIRST_MS");
    if (!value || __atomic_exchange_n(&delayed_write, 1, __ATOMIC_SEQ_CST)) return;
    char *end;
    unsigned long ms = strtoul(value, &end, 10);
    if (*end || ms == 0 || ms > 1000) return;
    struct timespec duration = {(time_t)(ms / 1000), (long)(ms % 1000) * 1000000};
    while (nanosleep(&duration, &duration) == -1 && errno == EINTR) {}
}

static void trace(const char *name, size_t requested, ssize_t result, int saved_errno) {
    int original_errno = errno;
    const char *path = getenv("EMDA_SHORT_TRACE");
    if (path) {
        int fd = syscall(SYS_openat, AT_FDCWD, path,
                         O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
        if (fd >= 0) {
            char line[160];
            int length = snprintf(line, sizeof(line), "%s %zu %zd %d\n",
                                  name, requested, result, saved_errno);
            if (length > 0 && (size_t)length < sizeof(line))
                (void)syscall(SYS_write, fd, line, (size_t)length);
            (void)syscall(SYS_close, fd);
        }
    }
    errno = original_errno;
}

static ssize_t one_write(int fd, const void *buffer, size_t count, off_t offset,
                         int positioned) {
    int matching = target_fd(fd);
    const char *name = positioned ? "pwrite" : "write";
    if (matching && !positioned) delay_first_write();
    size_t limit = matching && enabled_for(name) ? limit_bytes() : 0;
    if (limit && fail_next()) {
        trace(name, count, -1, EIO);
        errno = EIO;
        return -1;
    }
    size_t actual = limit && count > limit ? limit : count;
    ssize_t result = positioned ? syscall(SYS_pwrite64, fd, buffer, actual, offset)
                                : syscall(SYS_write, fd, buffer, actual);
    int saved_errno = result < 0 ? errno : 0;
    if (limit && result > 0 && (size_t)result < count)
        __atomic_store_n(&short_written, 1, __ATOMIC_SEQ_CST);
    if (limit) trace(name, count, result, saved_errno);
    if (result < 0) errno = saved_errno;
    return result;
}

ssize_t write(int fd, const void *buffer, size_t count) {
    return one_write(fd, buffer, count, 0, 0);
}

ssize_t pwrite(int fd, const void *buffer, size_t count, off_t offset) {
    return one_write(fd, buffer, count, offset, 1);
}

ssize_t pwrite64(int fd, const void *buffer, size_t count, off64_t offset) {
    return one_write(fd, buffer, count, (off_t)offset, 1);
}

ssize_t writev(int fd, const struct iovec *buffers, int count) {
    size_t limit = target_fd(fd) && enabled_for("writev") ? limit_bytes() : 0;
    if (!limit || count <= 0 || count > IOV_MAX)
        return syscall(SYS_writev, fd, buffers, count);
    size_t requested = 0;
    for (int i = 0; i < count; i++) {
        if (buffers[i].iov_len > SIZE_MAX - requested) {
            errno = EINVAL;
            return -1;
        }
        requested += buffers[i].iov_len;
    }
    if (fail_next()) {
        trace("writev", requested, -1, EIO);
        errno = EIO;
        return -1;
    }
    struct iovec limited[IOV_MAX];
    size_t left = limit;
    int used = 0;
    for (int i = 0; i < count && left; i++) {
        size_t part = buffers[i].iov_len < left ? buffers[i].iov_len : left;
        if (!part) continue;
        limited[used++] = (struct iovec){buffers[i].iov_base, part};
        left -= part;
    }
    ssize_t result = syscall(SYS_writev, fd, limited, used);
    int saved_errno = result < 0 ? errno : 0;
    if (result > 0 && (size_t)result < requested)
        __atomic_store_n(&short_written, 1, __ATOMIC_SEQ_CST);
    trace("writev", requested, result, saved_errno);
    if (result < 0) errno = saved_errno;
    return result;
}
