use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::path::Path;

const BLOCK_SIZE: usize = 4096;

/// Create a test file with the specified size filled with random-ish data
fn create_test_file(path: &Path, size: usize) {
    let mut file = File::create(path).expect("Failed to create test file");
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    file.write_all(&data).expect("Failed to write test data");
    file.sync_all().expect("Failed to sync file");
}

/// Open a file with F_NOCACHE to bypass OS cache (macOS only)
fn open_nocache(path: &Path) -> std::io::Result<File> {
    let file = File::open(path)?;
    unsafe {
        if libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) == -1 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(file)
}

/// Read specified number of bytes from a file using F_NOCACHE
fn read_nocache(path: &Path, size: usize) -> std::io::Result<Vec<u8>> {
    let file = open_nocache(path)?;
    let mut buffer = vec![0u8; size];

    let bytes_read = unsafe {
        libc::pread(
            file.as_raw_fd(),
            buffer.as_mut_ptr() as *mut libc::c_void,
            size,
            0,
        )
    };

    if bytes_read < 0 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(buffer)
}

/// Read specified number of bytes from a file using normal cached I/O
fn read_cached(path: &Path, size: usize) -> std::io::Result<Vec<u8>> {
    let file = File::open(path)?;
    let mut buffer = vec![0u8; size];

    let bytes_read = unsafe {
        libc::pread(
            file.as_raw_fd(),
            buffer.as_mut_ptr() as *mut libc::c_void,
            size,
            0,
        )
    };

    if bytes_read < 0 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(buffer)
}

/// Warm the OS page cache by reading the file
fn warm_cache(path: &Path, size: usize) {
    for _ in 0..10 {
        let _ = read_cached(path, size);
    }
}

/// Benchmark comparing sub-block reads (2KB vs 4KB)
///
/// This demonstrates that reading 2KB and 4KB from disk takes the same time
/// because the disk reads in 4KB blocks - reading 2KB still fetches the full block.
fn benchmark_subblock_reads(c: &mut Criterion) {
    let test_dir = Path::new("/tmp/disk_io_bench_subblock");
    fs::create_dir_all(test_dir).expect("Failed to create test directory");

    // Create a test file large enough for our reads
    let path = test_dir.join("test_file.dat");
    create_test_file(&path, BLOCK_SIZE); // 4KB file

    let mut group = c.benchmark_group("subblock_reads");

    // Test various read sizes within a single 4KB block
    let sizes: [(usize, &str); 4] = [(1024, "1KB"), (2048, "2KB"), (3072, "3KB"), (4096, "4KB")];

    for (size, name) in &sizes {
        group.bench_with_input(BenchmarkId::from_parameter(*name), size, |b, &size| {
            b.iter(|| {
                let result = read_nocache(&path, size);
                black_box(result.expect("Read failed"))
            })
        });
    }

    group.finish();

    // Cleanup
    let _ = fs::remove_dir_all(test_dir);
}

criterion_group!(benches, benchmark_subblock_reads);
criterion_main!(benches);
