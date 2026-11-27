//! The log's minimal I/O boundary, allowing deterministic failure tests.

use std::fs::File;
use std::io::{self, IoSlice, Write};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::Arc;

#[cfg(target_os = "macos")]
pub(crate) mod macos_sync {
    use rustix::io::Errno;
    use std::ffi::{c_char, c_int, c_void};
    use std::fs::File;
    use std::io;
    use std::os::fd::AsRawFd;

    const MNT_LOCAL: u32 = 0x0000_1000;
    const ATTR_BIT_MAP_COUNT: u16 = 5;
    const ATTR_VOL_CAPABILITIES: u32 = 0x0002_0000;
    const ATTR_VOL_INFO: u32 = 0x8000_0000;
    const VOL_CAPABILITIES_INTERFACES: usize = 1;
    const VOL_CAP_INT_BARRIERFSYNC: u32 = 0x0100_0000;
    const F_BARRIERFSYNC: c_int = 85;

    #[repr(C)]
    struct AttrList {
        bitmapcount: u16,
        reserved: u16,
        commonattr: u32,
        volattr: u32,
        dirattr: u32,
        fileattr: u32,
        forkattr: u32,
    }

    #[repr(C)]
    struct VolCapabilitiesAttr {
        capabilities: [u32; 4],
        valid: [u32; 4],
    }

    #[repr(C)]
    struct CapBuf {
        length: u32,
        caps: VolCapabilitiesAttr,
    }

    unsafe extern "C" {
        fn fgetattrlist(
            fd: c_int,
            attr_list: *const AttrList,
            attr_buf: *mut c_void,
            attr_buf_size: usize,
            options: u32,
        ) -> c_int;
        fn fcntl(fd: c_int, cmd: c_int, ...) -> c_int;
    }

    fn is_supported_local_fs(flags: u32, fstypename: &[c_char]) -> bool {
        if (flags & MNT_LOCAL) == 0 {
            return false;
        }
        let bytes: Vec<u8> = fstypename
            .iter()
            .take_while(|&&byte| byte != 0)
            .map(|&byte| byte as u8)
            .collect();
        matches!(bytes.as_slice(), b"apfs" | b"hfs")
    }

    /// Queries whether an open descriptor resides on a local APFS or HFS+
    /// volume reporting `VOL_CAP_INT_BARRIERFSYNC`.
    ///
    /// Restricts barrier-ordered V2 log flushes to local APFS and HFS+ volumes
    /// whose `fcntl(2)` semantics guarantee that `F_BARRIERFSYNC` orders log
    /// writes ahead of a subsequent same-device `F_FULLFSYNC` on the witness.
    pub(crate) fn supports_barrier_fsync(file: &File) -> io::Result<bool> {
        let stat = loop {
            match rustix::fs::fstatfs(file) {
                Err(Errno::INTR) => continue,
                result => break result,
            }
        }?;
        if !is_supported_local_fs(stat.f_flags, &stat.f_fstypename) {
            return Ok(false);
        }
        let attr_list = AttrList {
            bitmapcount: ATTR_BIT_MAP_COUNT,
            reserved: 0,
            commonattr: 0,
            volattr: ATTR_VOL_INFO | ATTR_VOL_CAPABILITIES,
            dirattr: 0,
            fileattr: 0,
            forkattr: 0,
        };
        let mut buf = CapBuf {
            length: 0,
            caps: VolCapabilitiesAttr {
                capabilities: [0; 4],
                valid: [0; 4],
            },
        };
        loop {
            let rc = unsafe {
                fgetattrlist(
                    file.as_raw_fd(),
                    &attr_list,
                    (&raw mut buf).cast::<c_void>(),
                    std::mem::size_of::<CapBuf>(),
                    0,
                )
            };
            if rc == 0 {
                break;
            }
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if matches!(
                Errno::from_io_error(&error),
                Some(Errno::NOTSUP | Errno::OPNOTSUPP | Errno::INVAL | Errno::NOTTY)
            ) {
                return Ok(false);
            }
            return Err(error);
        }
        if (buf.length as usize) < std::mem::size_of::<CapBuf>() {
            return Ok(false);
        }
        let supported = buf.caps.capabilities[VOL_CAPABILITIES_INTERFACES]
            & buf.caps.valid[VOL_CAPABILITIES_INTERFACES];
        Ok((supported & VOL_CAP_INT_BARRIERFSYNC) != 0)
    }

    pub(crate) fn fullfsync_with(mut full: impl FnMut() -> Result<(), Errno>) -> io::Result<()> {
        loop {
            match full() {
                Ok(()) => return Ok(()),
                Err(Errno::INTR) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Performs `fcntl(fd, F_FULLFSYNC)`, retrying only `EINTR` and never
    /// silently degrading to plain `fsync(2)`.
    pub(crate) fn fullfsync(file: &File) -> io::Result<()> {
        fullfsync_with(|| rustix::fs::fcntl_fullfsync(file))
    }

    fn raw_barrierfsync(file: &File) -> Result<(), Errno> {
        let rc = unsafe { fcntl(file.as_raw_fd(), F_BARRIERFSYNC) };
        if rc == 0 {
            Ok(())
        } else {
            Err(Errno::from_io_error(&io::Error::last_os_error()).unwrap_or(Errno::IO))
        }
    }

    pub(crate) fn barrier_or_fallback(
        mut barrier: impl FnMut() -> Result<(), Errno>,
        mut full: impl FnMut() -> io::Result<()>,
    ) -> io::Result<()> {
        loop {
            match barrier() {
                Ok(()) => return Ok(()),
                Err(Errno::INTR) => continue,
                Err(Errno::NOTSUP | Errno::OPNOTSUPP | Errno::INVAL | Errno::NOTTY) => {
                    return full();
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Orders Phase 1 (`records.log`) ahead of Phase 2 (`commit.state`) on the
    /// same device using `F_BARRIERFSYNC`. Falls back strictly to `F_FULLFSYNC`
    /// if the barrier command is unsupported; never falls back to plain `fsync`.
    pub(crate) fn barrier_fsync_or_fullfsync(file: &File) -> io::Result<()> {
        barrier_or_fallback(|| raw_barrierfsync(file), || fullfsync(file))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn supported_local_fs_requires_local_apfs_or_hfs() {
            let mut apfs = [0 as c_char; 16];
            for (dst, &src) in apfs.iter_mut().zip(b"apfs") {
                *dst = src as c_char;
            }
            let mut hfs = [0 as c_char; 16];
            for (dst, &src) in hfs.iter_mut().zip(b"hfs") {
                *dst = src as c_char;
            }
            let mut msdos = [0 as c_char; 16];
            for (dst, &src) in msdos.iter_mut().zip(b"msdos") {
                *dst = src as c_char;
            }
            assert!(is_supported_local_fs(MNT_LOCAL, &apfs));
            assert!(is_supported_local_fs(MNT_LOCAL, &hfs));
            assert!(!is_supported_local_fs(0, &apfs));
            assert!(!is_supported_local_fs(MNT_LOCAL, &msdos));
        }

        #[test]
        fn barrier_or_fallback_retries_eintr_falls_back_on_unsupported_and_propagates_eio() {
            let mut barrier_calls = 0;
            let mut full_calls = 0;
            barrier_or_fallback(
                || {
                    barrier_calls += 1;
                    if barrier_calls <= 2 {
                        Err(Errno::INTR)
                    } else {
                        Ok(())
                    }
                },
                || {
                    full_calls += 1;
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!((barrier_calls, full_calls), (3, 0));

            for unsupported in [Errno::NOTSUP, Errno::OPNOTSUPP, Errno::INVAL, Errno::NOTTY] {
                let mut barrier_calls = 0;
                let mut full_calls = 0;
                barrier_or_fallback(
                    || {
                        barrier_calls += 1;
                        Err(unsupported)
                    },
                    || {
                        full_calls += 1;
                        Ok(())
                    },
                )
                .unwrap();
                assert_eq!((barrier_calls, full_calls), (1, 1));
            }

            let mut full_calls = 0;
            let err = barrier_or_fallback(
                || Err(Errno::IO),
                || {
                    full_calls += 1;
                    Ok(())
                },
            )
            .unwrap_err();
            assert_eq!(full_calls, 0);
            assert_eq!(Errno::from_io_error(&err), Some(Errno::IO));

            // EINTR followed by EIO propagates EIO without invoking fallback.
            let mut barrier_calls = 0;
            let mut full_calls = 0;
            let err = barrier_or_fallback(
                || {
                    barrier_calls += 1;
                    if barrier_calls == 1 {
                        Err(Errno::INTR)
                    } else {
                        Err(Errno::IO)
                    }
                },
                || {
                    full_calls += 1;
                    Ok(())
                },
            )
            .unwrap_err();
            assert_eq!((barrier_calls, full_calls), (2, 0));
            assert_eq!(Errno::from_io_error(&err), Some(Errno::IO));

            // EINTR followed by ENOTSUP falls back once to strict F_FULLFSYNC.
            let mut barrier_calls = 0;
            let mut full_calls = 0;
            barrier_or_fallback(
                || {
                    barrier_calls += 1;
                    if barrier_calls == 1 {
                        Err(Errno::INTR)
                    } else {
                        Err(Errno::NOTSUP)
                    }
                },
                || {
                    full_calls += 1;
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!((barrier_calls, full_calls), (2, 1));
        }

        #[test]
        fn barrier_or_fallback_propagates_fallback_fullfsync_error() {
            for fallback_errno in [Errno::IO, Errno::NOTSUP, Errno::INVAL] {
                let mut barrier_calls = 0;
                let mut full_calls = 0;
                let err = barrier_or_fallback(
                    || {
                        barrier_calls += 1;
                        Err(Errno::NOTSUP)
                    },
                    || {
                        full_calls += 1;
                        Err(fallback_errno.into())
                    },
                )
                .unwrap_err();
                assert_eq!((barrier_calls, full_calls), (1, 1));
                assert_eq!(Errno::from_io_error(&err), Some(fallback_errno));
            }
        }

        #[test]
        fn fullfsync_with_retries_eintr_and_never_falls_back_on_errors() {
            let mut calls = 0;
            fullfsync_with(|| {
                calls += 1;
                if calls <= 2 { Err(Errno::INTR) } else { Ok(()) }
            })
            .unwrap();
            assert_eq!(calls, 3);

            for non_retryable in [
                Errno::IO,
                Errno::NOTSUP,
                Errno::OPNOTSUPP,
                Errno::INVAL,
                Errno::NOTTY,
            ] {
                let mut calls = 0;
                let err = fullfsync_with(|| {
                    calls += 1;
                    if calls == 1 {
                        Err(Errno::INTR)
                    } else {
                        Err(non_retryable)
                    }
                })
                .unwrap_err();
                assert_eq!(calls, 2);
                assert_eq!(Errno::from_io_error(&err), Some(non_retryable));
            }
        }

        #[test]
        fn local_temp_volume_reports_barrier_capability_and_syncs() {
            let file = tempfile::tempfile().unwrap();
            let barrier = supports_barrier_fsync(&file).unwrap();
            // On macOS APFS/HFS+ development volumes barrier sync is supported;
            // either way fullfsync and barrier_fsync_or_fullfsync must succeed.
            barrier_fsync_or_fullfsync(&file).unwrap();
            fullfsync(&file).unwrap();
            // Barrier capability is reported per volume: local APFS on some
            // virtual machines (such as hosted CI runners) does not report
            // VOL_CAP_INT_BARRIERFSYNC. It must never be reported elsewhere.
            let stat = rustix::fs::fstatfs(&file).unwrap();
            if !is_supported_local_fs(stat.f_flags, &stat.f_fstypename) {
                assert!(!barrier);
            }
        }

        #[test]
        fn non_apfs_descriptor_reports_no_barrier_capability() {
            let dev_null = File::open("/dev/null").unwrap();
            assert!(!supports_barrier_fsync(&dev_null).unwrap());
        }
    }
}

pub(crate) trait LogIo: Send + Sync {
    fn len(&self) -> io::Result<u64>;
    fn read_exact_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<()>;
    fn append_all(&self, bytes: &[u8]) -> io::Result<()>;
    // Implementations that do not support vectored I/O preserve append order
    // by writing one record at a time. Any error leaves the outcome uncertain.
    fn append_vectored(&self, buffers: &mut [IoSlice<'_>]) -> io::Result<()> {
        for buffer in buffers {
            self.append_all(buffer)?;
        }
        Ok(())
    }
    fn sync_data(&self) -> io::Result<()>;
    // Whether the log handle supports a hardware write-barrier flush that
    // orders log data ahead of a subsequent same-device witness F_FULLFSYNC.
    fn supports_ordered_witness_barrier(&self) -> io::Result<bool> {
        Ok(false)
    }
    // Synchronize the log with an ordering barrier before a mandatory
    // same-device full flush on the commit witness. Must never be called
    // without that following full flush (e.g., never for unwitnessed logs).
    fn sync_ordered_before_witness(&self) -> io::Result<()> {
        self.sync_data()
    }

    // Optional identity and shared ownership support for real file handles.
    // Test and other I/O adapters retain their existing independent path.
    fn file_identity(&self) -> io::Result<Option<(u64, u64)>> {
        Ok(None)
    }
    fn shared_file(&self) -> Option<Arc<File>> {
        None
    }
}

impl LogIo for File {
    fn len(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }

    fn read_exact_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
        FileExt::read_exact_at(self, buffer, offset)
    }

    fn append_all(&self, bytes: &[u8]) -> io::Result<()> {
        let mut file = self;
        file.write_all(bytes)
    }

    fn append_vectored(&self, buffers: &mut [IoSlice<'_>]) -> io::Result<()> {
        write_all_vectored(self, buffers)
    }

    fn sync_data(&self) -> io::Result<()> {
        File::sync_data(self)
    }

    #[cfg(target_os = "macos")]
    fn supports_ordered_witness_barrier(&self) -> io::Result<bool> {
        macos_sync::supports_barrier_fsync(self)
    }

    #[cfg(target_os = "macos")]
    fn sync_ordered_before_witness(&self) -> io::Result<()> {
        macos_sync::barrier_fsync_or_fullfsync(self)
    }

    fn file_identity(&self) -> io::Result<Option<(u64, u64)>> {
        let metadata = self.metadata()?;
        Ok(Some((metadata.dev(), metadata.ino())))
    }
}

// Sharing ownership keeps one descriptor while the log and its metadata
// witness refer to the same open file description.
impl LogIo for Arc<File> {
    fn len(&self) -> io::Result<u64> {
        LogIo::len(self.as_ref())
    }

    fn read_exact_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
        LogIo::read_exact_at(self.as_ref(), buffer, offset)
    }

    fn append_all(&self, bytes: &[u8]) -> io::Result<()> {
        LogIo::append_all(self.as_ref(), bytes)
    }

    fn append_vectored(&self, buffers: &mut [IoSlice<'_>]) -> io::Result<()> {
        LogIo::append_vectored(self.as_ref(), buffers)
    }

    fn sync_data(&self) -> io::Result<()> {
        LogIo::sync_data(self.as_ref())
    }

    fn supports_ordered_witness_barrier(&self) -> io::Result<bool> {
        LogIo::supports_ordered_witness_barrier(self.as_ref())
    }

    fn sync_ordered_before_witness(&self) -> io::Result<()> {
        LogIo::sync_ordered_before_witness(self.as_ref())
    }

    fn file_identity(&self) -> io::Result<Option<(u64, u64)>> {
        LogIo::file_identity(self.as_ref())
    }

    fn shared_file(&self) -> Option<Arc<File>> {
        Some(Arc::clone(self))
    }
}

fn write_all_vectored(mut writer: impl Write, mut buffers: &mut [IoSlice<'_>]) -> io::Result<()> {
    while !buffers.is_empty() {
        match writer.write_vectored(buffers) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to append log",
                ));
            }
            Ok(written) => IoSlice::advance_slices(&mut buffers, written),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod replay_fault {
    use super::*;
    use std::ops::Range;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Replace a record as soon as the scanner has read the original bytes.
    pub(crate) struct ReplaceAfterRead {
        pub file: File,
        pub observed: Range<u64>,
        pub replace_at: u64,
        pub replacement: Vec<u8>,
        pub fired: Arc<AtomicBool>,
    }

    impl LogIo for ReplaceAfterRead {
        fn len(&self) -> io::Result<u64> {
            LogIo::len(&self.file)
        }

        fn read_exact_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<()> {
            FileExt::read_exact_at(&self.file, buffer, offset)?;
            if offset == self.observed.start
                && buffer.len() as u64 == self.observed.end - self.observed.start
                && !self.fired.swap(true, Ordering::Relaxed)
            {
                self.file.write_all_at(&self.replacement, self.replace_at)?;
            }
            Ok(())
        }

        fn append_all(&self, _: &[u8]) -> io::Result<()> {
            panic!("checkpoint tried to append after reading a changed log")
        }

        fn sync_data(&self) -> io::Result<()> {
            self.file.sync_data()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    enum Step {
        Write(usize),
        Interrupted,
        Zero,
        Error,
    }

    struct ShortWriter {
        steps: VecDeque<Step>,
        bytes: Vec<u8>,
    }

    impl Write for ShortWriter {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            unreachable!("the batch path uses vectored writes")
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn write_vectored(&mut self, buffers: &[IoSlice<'_>]) -> io::Result<usize> {
            let mut remaining = match self.steps.pop_front().expect("unexpected extra write") {
                Step::Write(limit) => limit,
                Step::Interrupted => return Err(io::ErrorKind::Interrupted.into()),
                Step::Zero => return Ok(0),
                Step::Error => return Err(io::ErrorKind::BrokenPipe.into()),
            };
            let mut written = 0;
            for buffer in buffers {
                let count = buffer.len().min(remaining);
                self.bytes.extend_from_slice(&buffer[..count]);
                written += count;
                remaining -= count;
                if remaining == 0 {
                    break;
                }
            }
            Ok(written)
        }
    }

    #[test]
    fn vectored_append_retries_interruptions_and_short_writes_across_records() {
        let mut writer = ShortWriter {
            steps: VecDeque::from([
                Step::Interrupted,
                Step::Write(1),
                Step::Write(3),
                Step::Interrupted,
                Step::Write(2),
                Step::Write(100),
            ]),
            bytes: Vec::new(),
        };
        let mut buffers = [
            IoSlice::new(b"abc"),
            IoSlice::new(b"de"),
            IoSlice::new(b"fghi"),
        ];
        write_all_vectored(&mut writer, &mut buffers).unwrap();
        assert_eq!(writer.bytes, b"abcdefghi");
        assert!(writer.steps.is_empty());
    }

    #[test]
    fn vectored_append_propagates_zero_and_errors_after_a_partial_write() {
        for (step, kind) in [
            (Step::Zero, io::ErrorKind::WriteZero),
            (Step::Error, io::ErrorKind::BrokenPipe),
        ] {
            let mut writer = ShortWriter {
                steps: VecDeque::from([Step::Write(4), step]),
                bytes: Vec::new(),
            };
            let mut buffers = [IoSlice::new(b"abc"), IoSlice::new(b"def")];
            assert_eq!(
                write_all_vectored(&mut writer, &mut buffers)
                    .unwrap_err()
                    .kind(),
                kind
            );
            assert_eq!(writer.bytes, b"abcd");
        }
    }
}
