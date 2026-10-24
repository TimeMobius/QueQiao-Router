use super::{artifact_path, replace_active};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn refuses_nonempty_target_wal_without_replacing_original() {
    let directory = std::env::temp_dir().join(format!(
        "qq-cutover-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&directory).unwrap();
    let active = directory.join("record.db");
    let target = directory.join("record.db.v8-target");
    let wal = artifact_path(&target, "-wal");
    fs::write(&active, b"original").unwrap();
    fs::write(&target, b"replacement").unwrap();
    fs::write(&wal, b"uncheckpointed data").unwrap();
    let permissions = fs::metadata(&active).unwrap().permissions();

    let result = replace_active(&active, &target, permissions.clone());

    assert!(result.is_err());
    assert_eq!(fs::read(&active).unwrap(), b"original");
    assert_eq!(fs::read(&target).unwrap(), b"replacement");
    assert!(!artifact_path(&active, ".bak").exists());

    fs::write(&wal, b"").unwrap();
    replace_active(&active, &target, permissions).unwrap();

    assert_eq!(fs::read(&active).unwrap(), b"replacement");
    assert_eq!(
        fs::read(artifact_path(&active, ".bak")).unwrap(),
        b"original"
    );
    assert!(!wal.exists());
    fs::remove_dir_all(directory).unwrap();
}
