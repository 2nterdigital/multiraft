//! Process-crash injection at production durability observations; no production fault hook.
use tracing_subscriber::prelude::*;

pub fn kill_at(target: &'static str, phase: String) -> impl tracing::Subscriber + Send + Sync {
    struct KillAt {
        target: &'static str,
        phase: String,
    }
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for KillAt {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if event.metadata().target() != self.target {
                return;
            }
            struct Phase(Option<String>);
            impl tracing::field::Visit for Phase {
                fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    if field.name() == "phase" {
                        self.0 = Some(value.to_owned());
                    }
                }
            }
            let mut phase = Phase(None);
            event.record(&mut phase);
            if phase.0.as_deref() == Some(self.phase.as_str()) {
                std::process::Command::new("/bin/kill")
                    .args(["-KILL", &std::process::id().to_string()])
                    .status()
                    .unwrap();
                panic!("SIGKILL did not terminate writer");
            }
        }
    }
    tracing_subscriber::registry().with(KillAt { target, phase })
}
