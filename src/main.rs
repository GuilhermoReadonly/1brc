// #![allow(unused_variables, unused_imports, dead_code)]

use std::{
    error::Error,
    fmt::Display,
    fs::{self, File},
    io::BufRead,
    sync::Arc,
    thread::{self, JoinHandle},
    time::Instant,
};

use crate::mmap::MmapOptions;

#[derive(Debug, Clone, Copy)]
struct Stats {
    min: f64,
    max: f64,
    count: u64,
    sum: f64,
}

impl Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            self.min,
            self.max,
            self.sum / (self.count as f64)
        )
    }
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            count: Default::default(),
            sum: Default::default(),
        }
    }
}

// type Map<K, V> = std::collections::HashMap<K, V>;
type Map = std::collections::BTreeMap<String, Stats>;

fn main() -> Result<(), Box<dyn Error>> {
    let cores: usize = std::thread::available_parallelism().unwrap().into();

    let path = match std::env::args().skip(1).next() {
        Some(path) => path,
        None => "measurements.txt".to_owned(),
    };

    let now = Instant::now();

    let stations_stats = read(cores, path)?;
    println!("Running read() took {} us.", now.elapsed().as_micros());

    let now = Instant::now();
    write_result(stations_stats)?;
    println!(
        "Running write_result() took {} us.",
        now.elapsed().as_micros()
    );

    Ok(())
}

fn read(nb_cores: usize, path: String) -> Result<Map, Box<dyn Error>> {
    let metadata = fs::metadata(&path)?;
    println!(
        "{:?}: File size = {}",
        thread::current().id(),
        metadata.len()
    );

    let file = File::open(&path)?;
    let mmap = unsafe { MmapOptions::new().map(&file)? };

    let mmap = Arc::new(mmap);

    let chunk_size = mmap.len() / nb_cores;

    let mut threads: Vec<JoinHandle<Map>> = vec![];

    let mut mmap_slices = vec![];

    for i in 0..nb_cores {
        let start = i * chunk_size;
        let end = if start + chunk_size > mmap.len() {
            mmap.len()
        } else {
            start + chunk_size
        };
        let m = (start, end);
        mmap_slices.push(m);
    }

    println!(
        "{:?}: cores = {}",
        thread::current().id(),
        mmap_slices.len()
    );

    let mut result: Map = Map::new();

    for (start, end) in mmap_slices {
        let mmap = mmap.clone();
        let thread_handle = thread::spawn(move || {
            let mut s: Map = Map::new();
            for (i, line) in mmap[start..end].lines().enumerate() {
                let line = match line {
                    Ok(l) => l,
                    Err(e) => {
                        println!("{:?}: {e}", thread::current().id());
                        continue;
                    }
                };

                let (city, value) = match line.split_once(';') {
                    Some((city, value)) => (city, value),
                    None => {
                        println!("{:?}: split failed on line {i}", thread::current().id());
                        continue;
                    }
                };
                let value: f64 = match value.parse() {
                    Ok(v) => v,
                    Err(e) => {
                        println!("{:?}: {e}", thread::current().id());
                        continue;
                    }
                };

                let stat = s.entry(city.to_string()).or_insert(Stats::default());

                stat.sum += value;
                stat.count += 1;
                stat.min = stat.min.min(value);
                stat.max = stat.max.max(value);
            }
            s
        });

        threads.push(thread_handle);
    }

    for t in threads {
        let mut partial_res = t.join().unwrap();
        result.append(&mut partial_res);
    }

    Ok(result)
}

fn write_result(stations_stats: Map) -> Result<(), Box<dyn Error>> {
    print!("{{");

    let mut stations_iter = stations_stats.iter() ;
    let first_elt = stations_iter.next().unwrap();
    print!("{}={}", first_elt.0, first_elt.1);
    for (station, state) in stations_iter {
        print!(",{station}={state}");
    }
    println!("}}");
    Ok(())
}

////// unshamely copy-pasted from https://github.com/danburkert/memmap-rs
mod mmap {

    #[cfg(unix)]
    use unix::MmapInner;

    use std::fmt;
    use std::fs::File;
    use std::io::{Error, ErrorKind, Result};
    use std::ops::Deref;
    use std::slice;
    use std::usize;

    /// A memory map builder, providing advanced options and flags for specifying memory map behavior.
    ///
    /// `MmapOptions` can be used to create an anonymous memory map using [`map_anon()`], or a
    /// file-backed memory map using one of [`map()`], [`map_mut()`], [`map_exec()`], or
    /// [`map_copy()`].
    ///
    /// ## Safety
    ///
    /// All file-backed memory map constructors are marked `unsafe` because of the potential for
    /// *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
    /// out of process. Applications must consider the risk and take appropriate precautions when
    /// using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
    /// unlinked) files exist but are platform specific and limited.
    ///
    /// [`map_anon()`]: MmapOptions::map_anon()
    /// [`map()`]: MmapOptions::map()
    /// [`map_mut()`]: MmapOptions::map_mut()
    /// [`map_exec()`]: MmapOptions::map_exec()
    /// [`map_copy()`]: MmapOptions::map_copy()
    #[derive(Clone, Debug, Default)]
    pub struct MmapOptions {
        offset: u64,
        len: Option<usize>,
        _stack: bool,
    }

    impl MmapOptions {
        /// Creates a new set of options for configuring and creating a memory map.
        ///
        /// # Example
        ///
        /// ```
        /// use memmap::{MmapMut, MmapOptions};
        /// # use std::io::Result;
        ///
        /// # fn main() -> Result<()> {
        /// // Create a new memory map builder.
        /// let mut mmap_options = MmapOptions::new();
        ///
        /// // Configure the memory map builder using option setters, then create
        /// // a memory map using one of `mmap_options.map_anon`, `mmap_options.map`,
        /// // `mmap_options.map_mut`, `mmap_options.map_exec`, or `mmap_options.map_copy`:
        /// let mut mmap: MmapMut = mmap_options.len(36).map_anon()?;
        ///
        /// // Use the memory map:
        /// mmap.copy_from_slice(b"...data to copy to the memory map...");
        /// # Ok(())
        /// # }
        /// ```
        pub fn new() -> MmapOptions {
            MmapOptions::default()
        }

        /// Configures the memory map to start at byte `offset` from the beginning of the file.
        ///
        /// This option has no effect on anonymous memory maps.
        ///
        /// By default, the offset is 0.
        ///
        /// # Example
        ///
        /// ```
        /// use memmap::MmapOptions;
        /// use std::fs::File;
        ///
        /// # fn main() -> std::io::Result<()> {
        /// let mmap = unsafe {
        ///     MmapOptions::new()
        ///                 .offset(10)
        ///                 .map(&File::open("README.md")?)?
        /// };
        /// assert_eq!(&b"A Rust library for cross-platform memory mapped IO."[..],
        ///            &mmap[..51]);
        /// # Ok(())
        /// # }
        /// ```
        pub fn _offset(&mut self, offset: u64) -> &mut Self {
            self.offset = offset;
            self
        }

        /// Configures the created memory mapped buffer to be `len` bytes long.
        ///
        /// This option is mandatory for anonymous memory maps.
        ///
        /// For file-backed memory maps, the length will default to the file length.
        ///
        /// # Example
        ///
        /// ```
        /// use memmap::MmapOptions;
        /// use std::fs::File;
        ///
        /// # fn main() -> std::io::Result<()> {
        /// let mmap = unsafe {
        ///     MmapOptions::new()
        ///                 .len(8)
        ///                 .map(&File::open("README.md")?)?
        /// };
        /// assert_eq!(&b"# memmap"[..], &mmap[..]);
        /// # Ok(())
        /// # }
        /// ```
        pub fn _len(&mut self, len: usize) -> &mut Self {
            self.len = Some(len);
            self
        }

        /// Returns the configured length, or the length of the provided file.
        fn get_len(&self, file: &File) -> Result<usize> {
            self.len.map(Ok).unwrap_or_else(|| {
                let len = file.metadata()?.len() - self.offset;
                if len > (usize::MAX as u64) {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "memory map length overflows usize",
                    ));
                }
                Ok(len as usize)
            })
        }

        /// Configures the anonymous memory map to be suitable for a process or thread stack.
        ///
        /// This option corresponds to the `MAP_STACK` flag on Linux.
        ///
        /// This option has no effect on file-backed memory maps.
        ///
        /// # Example
        ///
        /// ```
        /// use memmap::MmapOptions;
        ///
        /// # fn main() -> std::io::Result<()> {
        /// let stack = MmapOptions::new().stack().len(4096).map_anon();
        /// # Ok(())
        /// # }
        /// ```
        pub fn _stack(&mut self) -> &mut Self {
            self._stack = true;
            self
        }

        /// Creates a read-only memory map backed by a file.
        ///
        /// # Errors
        ///
        /// This method returns an error when the underlying system call fails, which can happen for a
        /// variety of reasons, such as when the file is not open with read permissions.
        ///
        /// # Example
        ///
        /// ```
        /// use memmap::MmapOptions;
        /// use std::fs::File;
        /// use std::io::Read;
        ///
        /// # fn main() -> std::io::Result<()> {
        /// let mut file = File::open("README.md")?;
        ///
        /// let mut contents = Vec::new();
        /// file.read_to_end(&mut contents)?;
        ///
        /// let mmap = unsafe {
        ///     MmapOptions::new().map(&file)?
        /// };
        ///
        /// assert_eq!(&contents[..], &mmap[..]);
        /// # Ok(())
        /// # }
        /// ```
        pub unsafe fn map(&self, file: &File) -> Result<Mmap> {
            MmapInner::map(self.get_len(file)?, file, self.offset)
                .map(|inner| Mmap { inner: inner })
        }

        /// Creates a readable and executable memory map backed by a file.
        ///
        /// # Errors
        ///
        /// This method returns an error when the underlying system call fails, which can happen for a
        /// variety of reasons, such as when the file is not open with read permissions.
        pub unsafe fn _map_exec(&self, file: &File) -> Result<Mmap> {
            MmapInner::_map_exec(self.get_len(file)?, file, self.offset)
                .map(|inner| Mmap { inner: inner })
        }
    }

    /// A handle to an immutable memory mapped buffer.
    ///
    /// A `Mmap` may be backed by a file, or it can be anonymous map, backed by volatile memory. Use
    /// [`MmapOptions`] or [`map()`] to create a file-backed memory map. To create an immutable
    /// anonymous memory map, first create a mutable anonymous memory map, and then make it immutable
    /// with [`MmapMut::make_read_only()`].
    ///
    /// A file backed `Mmap` is created by `&File` reference, and will remain valid even after the
    /// `File` is dropped. In other words, the `Mmap` handle is completely independent of the `File`
    /// used to create it. For consistency, on some platforms this is achieved by duplicating the
    /// underlying file handle. The memory will be unmapped when the `Mmap` handle is dropped.
    ///
    /// Dereferencing and accessing the bytes of the buffer may result in page faults (e.g. swapping
    /// the mapped pages into physical memory) though the details of this are platform specific.
    ///
    /// `Mmap` is [`Sync`](std::marker::Sync) and [`Send`](std::marker::Send).
    ///
    /// ## Safety
    ///
    /// All file-backed memory map constructors are marked `unsafe` because of the potential for
    /// *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
    /// out of process. Applications must consider the risk and take appropriate precautions when using
    /// file-backed maps. Solutions such as file permissions, locks or process-private (e.g. unlinked)
    /// files exist but are platform specific and limited.
    ///
    /// ## Example
    ///
    /// ```
    /// use memmap::MmapOptions;
    /// use std::io::Write;
    /// use std::fs::File;
    ///
    /// # fn main() -> std::io::Result<()> {
    /// let file = File::open("README.md")?;
    /// let mmap = unsafe { MmapOptions::new().map(&file)? };
    /// assert_eq!(b"# memmap", &mmap[0..8]);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// See [`MmapMut`] for the mutable version.
    ///
    /// [`map()`]: Mmap::map()
    pub struct Mmap {
        inner: MmapInner,
    }

    impl Mmap {
        /// Creates a read-only memory map backed by a file.
        ///
        /// This is equivalent to calling `MmapOptions::new().map(file)`.
        ///
        /// # Errors
        ///
        /// This method returns an error when the underlying system call fails, which can happen for a
        /// variety of reasons, such as when the file is not open with read permissions.
        ///
        /// # Example
        ///
        /// ```
        /// use std::fs::File;
        /// use std::io::Read;
        ///
        /// use memmap::Mmap;
        ///
        /// # fn main() -> std::io::Result<()> {
        /// let mut file = File::open("README.md")?;
        ///
        /// let mut contents = Vec::new();
        /// file.read_to_end(&mut contents)?;
        ///
        /// let mmap = unsafe { Mmap::map(&file)?  };
        ///
        /// assert_eq!(&contents[..], &mmap[..]);
        /// # Ok(())
        /// # }
        /// ```
        pub unsafe fn _map(file: &File) -> Result<Mmap> {
            MmapOptions::new().map(file)
        }
    }

    impl Deref for Mmap {
        type Target = [u8];

        #[inline]
        fn deref(&self) -> &[u8] {
            unsafe { slice::from_raw_parts(self.inner.ptr(), self.inner.len()) }
        }
    }

    impl AsRef<[u8]> for Mmap {
        #[inline]
        fn as_ref(&self) -> &[u8] {
            self.deref()
        }
    }

    impl fmt::Debug for Mmap {
        fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            fmt.debug_struct("Mmap")
                .field("ptr", &self.as_ptr())
                .field("len", &self.len())
                .finish()
        }
    }

    mod unix {
        extern crate libc;

        use std::fs::File;
        use std::os::unix::io::{AsRawFd, RawFd};
        use std::{io, ptr};

        #[cfg(any(
            all(target_os = "linux", not(target_arch = "mips")),
            target_os = "freebsd",
            target_os = "android"
        ))]
        const _MAP_STACK: libc::c_int = libc::MAP_STACK;

        #[cfg(not(any(
            all(target_os = "linux", not(target_arch = "mips")),
            target_os = "freebsd",
            target_os = "android"
        )))]
        const MAP_STACK: libc::c_int = 0;

        pub struct MmapInner {
            ptr: *mut libc::c_void,
            len: usize,
        }

        impl MmapInner {
            /// Creates a new `MmapInner`.
            ///
            /// This is a thin wrapper around the `mmap` sytem call.
            fn new(
                len: usize,
                prot: libc::c_int,
                flags: libc::c_int,
                file: RawFd,
                offset: u64,
            ) -> io::Result<MmapInner> {
                let alignment = offset % page_size() as u64;
                let aligned_offset = offset - alignment;
                let aligned_len = len + alignment as usize;
                if aligned_len == 0 {
                    // Normally the OS would catch this, but it segfaults under QEMU.
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "memory map must have a non-zero length",
                    ));
                }

                unsafe {
                    let ptr = libc::mmap(
                        ptr::null_mut(),
                        aligned_len as libc::size_t,
                        prot,
                        flags,
                        file,
                        aligned_offset as libc::off_t,
                    );

                    if ptr == libc::MAP_FAILED {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(MmapInner {
                            ptr: ptr.offset(alignment as isize),
                            len: len,
                        })
                    }
                }
            }

            pub fn map(len: usize, file: &File, offset: u64) -> io::Result<MmapInner> {
                MmapInner::new(
                    len,
                    libc::PROT_READ,
                    libc::MAP_SHARED,
                    file.as_raw_fd(),
                    offset,
                )
            }

            pub fn _map_exec(len: usize, file: &File, offset: u64) -> io::Result<MmapInner> {
                MmapInner::new(
                    len,
                    libc::PROT_READ | libc::PROT_EXEC,
                    libc::MAP_SHARED,
                    file.as_raw_fd(),
                    offset,
                )
            }

            pub fn _map_mut(len: usize, file: &File, offset: u64) -> io::Result<MmapInner> {
                MmapInner::new(
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED,
                    file.as_raw_fd(),
                    offset,
                )
            }

            pub fn _map_copy(len: usize, file: &File, offset: u64) -> io::Result<MmapInner> {
                MmapInner::new(
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE,
                    file.as_raw_fd(),
                    offset,
                )
            }

            /// Open an anonymous memory map.
            pub fn _map_anon(len: usize, stack: bool) -> io::Result<MmapInner> {
                let stack = if stack { _MAP_STACK } else { 0 };
                MmapInner::new(
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED | libc::MAP_ANON | stack,
                    -1,
                    0,
                )
            }

            pub fn _flush(&self, offset: usize, len: usize) -> io::Result<()> {
                let alignment = (self.ptr as usize + offset) % page_size();
                let offset = offset as isize - alignment as isize;
                let len = len + alignment;
                let result = unsafe {
                    libc::msync(self.ptr.offset(offset), len as libc::size_t, libc::MS_SYNC)
                };
                if result == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            }

            pub fn _flush_async(&self, offset: usize, len: usize) -> io::Result<()> {
                let alignment = offset % page_size();
                let aligned_offset = offset - alignment;
                let aligned_len = len + alignment;
                let result = unsafe {
                    libc::msync(
                        self.ptr.offset(aligned_offset as isize),
                        aligned_len as libc::size_t,
                        libc::MS_ASYNC,
                    )
                };
                if result == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            }

            fn _mprotect(&mut self, prot: libc::c_int) -> io::Result<()> {
                unsafe {
                    let alignment = self.ptr as usize % page_size();
                    let ptr = self.ptr.offset(-(alignment as isize));
                    let len = self.len + alignment;
                    if libc::mprotect(ptr, len, prot) == 0 {
                        Ok(())
                    } else {
                        Err(io::Error::last_os_error())
                    }
                }
            }

            pub fn _make_read_only(&mut self) -> io::Result<()> {
                self._mprotect(libc::PROT_READ)
            }

            pub fn _make_exec(&mut self) -> io::Result<()> {
                self._mprotect(libc::PROT_READ | libc::PROT_EXEC)
            }

            pub fn _make_mut(&mut self) -> io::Result<()> {
                self._mprotect(libc::PROT_READ | libc::PROT_WRITE)
            }

            #[inline]
            pub fn ptr(&self) -> *const u8 {
                self.ptr as *const u8
            }

            #[inline]
            pub fn _mut_ptr(&mut self) -> *mut u8 {
                self.ptr as *mut u8
            }

            #[inline]
            pub fn len(&self) -> usize {
                self.len
            }
        }

        impl Drop for MmapInner {
            fn drop(&mut self) {
                let alignment = self.ptr as usize % page_size();
                unsafe {
                    assert!(
                        libc::munmap(
                            self.ptr.offset(-(alignment as isize)),
                            (self.len + alignment) as libc::size_t
                        ) == 0,
                        "unable to unmap mmap: {}",
                        io::Error::last_os_error()
                    );
                }
            }
        }

        unsafe impl Sync for MmapInner {}
        unsafe impl Send for MmapInner {}

        fn page_size() -> usize {
            unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
        }
    }
}
