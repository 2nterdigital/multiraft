//! Kill the complete native runtime at an observed durable protocol boundary.
use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver},
    thread::JoinHandle,
    time::Duration,
};

pub struct Process {
    child: Child,
    phases: Receiver<String>,
    reader: Option<JoinHandle<()>>,
}

impl Process {
    pub fn start(root: &Path, scenario: &str, resume: bool) -> Self {
        let stage = if resume { "resume" } else { "initial" };
        let stderr = File::create(root.join(format!("{scenario}-{stage}.stderr")))
            .expect("child stderr evidence");
        let mut stdout = File::create(root.join(format!("{scenario}-{stage}.stdout")))
            .expect("child stdout evidence");
        let mut child = Command::new(std::env::current_exe().expect("current test binary"))
            .args([
                "--ignored",
                "--exact",
                "controlled_protocol_child",
                "--nocapture",
            ])
            .env("CONTROLLED_PROTOCOL_ROOT", root)
            .env("CONTROLLED_PROTOCOL_SCENARIO", scenario)
            .env("CONTROLLED_PROTOCOL_RESUME", if resume { "1" } else { "0" })
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("spawn native trajectory child");
        let output = child.stdout.take().expect("child stdout pipe");
        let (sender, phases) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                let line = line.expect("child output line");
                writeln!(stdout, "{line}").expect("record child stdout");
                stdout.flush().expect("flush child evidence");
                if let Some(phase) = line.strip_prefix("CONTROLLED_PHASE ") {
                    let _ = sender.send(phase.to_owned());
                }
            }
        });
        Self {
            child,
            phases,
            reader: Some(reader),
        }
    }

    pub fn wait_phase(&self, expected: &str) {
        let observed = self
            .phases
            .recv_timeout(Duration::from_secs(90))
            .expect("bounded persisted protocol phase; inspect child evidence");
        assert_eq!(
            observed, expected,
            "kill boundary must be explicitly observed"
        );
    }

    pub fn kill(mut self) {
        self.child.kill().expect("SIGKILL native runtime child");
        let status = self.child.wait().expect("reap killed native runtime");
        assert!(!status.success(), "crash must not be a graceful shutdown");
        self.reader
            .take()
            .expect("stdout reader")
            .join()
            .expect("join evidence reader");
    }

    pub fn finish(mut self) {
        self.wait_phase("finished");
        let status = self.child.wait().expect("reap completed native trajectory");
        assert!(status.success(), "native trajectory child failed: {status}");
        self.reader
            .take()
            .expect("stdout reader")
            .join()
            .expect("join evidence reader");
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

pub fn persisted_phase(phase: &str) {
    println!("CONTROLLED_PHASE {phase}");
    std::io::stdout()
        .flush()
        .expect("flush persisted phase marker");
    let mut input = String::new();
    std::io::stdin()
        .read_line(&mut input)
        .expect("await process kill at durable boundary");
    panic!("protocol child must be killed while parked at {phase}");
}
