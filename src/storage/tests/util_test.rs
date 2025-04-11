use crate::storage::util::*;
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

#[test]
fn test_is_dir() {
    let dir_path = "test_dir";
    fs::create_dir(dir_path).unwrap();
    let result = is_dir(dir_path);
    assert!(result.is_ok());
    assert!(result.unwrap());

    fs::remove_dir(dir_path).unwrap();
}

#[test]
fn test_mkdir_with_path() {
    let dir_path = "nested/test/dir";
    let mode = 0o755; // Unix specific mode (read, write, exec for owner; read, exec for others)
    let result = mkdir_with_path(dir_path, mode);
    assert!(result.is_ok());
    // Check if the directory exists and the permissions are set correctly
    let metadata = fs::metadata(dir_path).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o777, mode);
    // Clean up
    fs::remove_dir_all("nested").unwrap();
}

#[test]
fn test_delete_dir() {
    let dir_path = "test_delete_dir";
    fs::create_dir_all(format!("{}/subdir", dir_path)).unwrap();
    fs::File::create(format!("{}/subdir/file.txt", dir_path))
        .unwrap()
        .write_all(b"data")
        .unwrap();
    assert!(Path::new(&format!("{}/subdir", dir_path)).exists());
    assert!(Path::new(&format!("{}/subdir/file.txt", dir_path)).exists());
    let result = delete_dir(dir_path);
    assert!(result.is_ok());

    assert!(!Path::new(dir_path).exists());
}
