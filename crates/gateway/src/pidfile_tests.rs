use super::*;

#[test]
fn write_then_read_round_trips() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("gateway.pid");
    write_to(&path, 4242).expect("write");
    assert_eq!(read_from(&path).expect("read"), Some(4242));
}

#[test]
fn read_absent_is_none() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("does-not-exist.pid");
    assert_eq!(read_from(&path).expect("read"), None);
}

#[test]
fn read_garbage_is_err() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("gateway.pid");
    std::fs::write(&path, "not-a-pid").expect("seed");
    assert!(read_from(&path).is_err());
}

#[test]
fn remove_is_idempotent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("gateway.pid");
    // removing an absent file is fine
    remove(&path);
    write_to(&path, 1).expect("write");
    remove(&path);
    assert_eq!(read_from(&path).expect("read"), None);
}

#[test]
fn write_creates_parent_dir() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("nested").join("sub").join("gateway.pid");
    write_to(&path, 99).expect("write creates parents");
    assert_eq!(read_from(&path).expect("read"), Some(99));
}
