//! Fail-closed placement guard for explicitly invoked laboratory cases.
use std::{
    path::{Path, PathBuf},
    process::Command,
};

pub fn require_lab(root: &Path) -> PathBuf {
    assert_eq!(std::env::consts::OS, "linux", "dedicated laboratory only");
    let hostname = Command::new("hostname").output().unwrap();
    assert_eq!(
        String::from_utf8(hostname.stdout).unwrap().trim(),
        "iZk1ah3k883dc7jcznm36hZ"
    );
    let login = Command::new("id").arg("-un").output().unwrap();
    assert_eq!(String::from_utf8(login.stdout).unwrap().trim(), "ecs-user");
    let root = root
        .canonicalize()
        .expect("laboratory preflight must create the root");
    assert!(root.starts_with("/srv/tornado-message-data"));
    assert!(std::env::current_exe()
        .unwrap()
        .canonicalize()
        .unwrap()
        .starts_with("/srv/tornado-message-data"));
    let mount = Command::new("findmnt")
        .args(["-n", "-o", "SOURCE,TARGET", "-T"])
        .arg(&root)
        .output()
        .unwrap();
    assert!(mount.status.success());
    let mount = String::from_utf8(mount.stdout).unwrap();
    assert_eq!(
        mount.split_whitespace().collect::<Vec<_>>(),
        vec!["/dev/vdb1", "/srv/tornado-message-data"]
    );
    root
}
