//! Environment fence for spawned agent (buzz-acp → harness) children.
//!
//! When Buzz runs as an AppImage, AppRun exports `APPDIR`, `LD_LIBRARY_PATH`,
//! `GTK_PATH`, `GIO_EXTRA_MODULES` and `XDG_DATA_DIRS` pointing into the
//! read-only squashfs mount (`/tmp/.mount_buzz-*`). The agent spawn path
//! (runtime.rs `spawn_agent_child`) inherits Buzz's own environment
//! wholesale — `std::process::Command` starts from the parent env — so every
//! agent child, and through it every sandboxed tool child (Codex seccomp
//! wrappers), also carries the AppImage env.
//!
//! That combination is fatal for Chromium-based tooling: a sandboxed
//! headless Chromium (Playwright under a Codex sandbox) loading the
//! AppImage's bundled glib/GTK libs while its syscall filter denies the
//! early-startup syscalls those libs make hits Chromium's own
//! `CHECK()`/`IMMEDIATE_CRASH()` abort — SIGTRAP in `ChromeMain` within
//! ~0.4s of launch, byte-identical across 11 coredumps (2026-09-27/28).
//! The same Chromium binary, same flags, runs cleanly outside the sandbox.
//!
//! Unlike the terminal fence (`buzz-terminal/src/env_fence.rs`), this is a
//! narrow denylist, not an allowlist: agent harnesses legitimately need the
//! full parent environment (relay credentials, provider keys, PATH layers
//! assembled by `build_augmented_path`), and the AppImage keys are a small,
//! closed set we fully control. If the set grows stale it degrades to today's
//! behavior — it cannot strand a spawn on a missing legitimate key.
//!
//! `XDG_DATA_DIRS` is not dropped wholesale: it is rewritten to the host
//! entries only (the AppImage prepends its own share dir). Dropping it
//! entirely would break tools that rely on the host's data dirs.

/// AppImage keys that must not leak into agent children.
const APPIMAGE_ENV_KEYS: &[&str] = &[
    "APPDIR",
    "LD_LIBRARY_PATH",
    "GTK_PATH",
    "GTK_PATH3",
    "GTK_EXE_PREFIX",
    "GIO_EXTRA_MODULES",
    "GDK_PIXBUF_MODULE_FILE",
    "GDK_PIXBUF_MODULEDIR",
];

/// Rewrite `XDG_DATA_DIRS` to its host-only entries (drops the AppImage
/// prepend). A missing or host-shaped value passes through untouched.
fn host_xdg_data_dirs(value: &std::ffi::OsStr) -> Option<String> {
    let text = value.to_str()?;
    let mut out = Vec::new();
    for entry in text.split(':') {
        if entry.is_empty() {
            continue;
        }
        if entry.contains("/.mount_") || entry.starts_with("/tmp/.mount_") {
            continue;
        }
        if !out.iter().any(|e: &String| e == entry) {
            out.push(entry.to_string());
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out.join(":"))
    }
}

/// Strip/rewrite AppImage environment keys on `cmd`.
/// Called AFTER all `command.env(...)` writes in `spawn_agent_child` — an
/// explicit `env()` from the layers above must win over this fence.
pub fn fence_appimage_env(cmd: &mut std::process::Command) {
    for key in APPIMAGE_ENV_KEYS {
        cmd.env_remove(key);
    }
    if let Some(value) = std::env::var_os("XDG_DATA_DIRS") {
        if let Some(host) = host_xdg_data_dirs(&value) {
            cmd.env("XDG_DATA_DIRS", host);
        } else {
            cmd.env_remove("XDG_DATA_DIRS");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fence_drops_appimage_keys() {
        // CommandBuilder starts from the parent env; simulate the AppImage keys.
        unsafe { std::env::set_var("APPDIR", "/tmp/.mount_buzz-test/usr") };
        unsafe { std::env::set_var("LD_LIBRARY_PATH", "/tmp/.mount_buzz-test/usr/lib") };
        let mut cmd = std::process::Command::new("env");
        fence_appimage_env(&mut cmd);
        let out = cmd.output().unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(!stdout.contains("APPDIR="), "APPDIR leaked: {stdout}");
        assert!(
            !stdout.contains("LD_LIBRARY_PATH="),
            "LD_LIBRARY_PATH leaked: {stdout}"
        );
        let _ = std::env::remove_var("APPDIR");
        let _ = std::env::remove_var("LD_LIBRARY_PATH");
    }

    #[test]
    fn fence_rewrites_mount_prefixed_xdg_data_dirs() {
        unsafe {
            std::env::set_var(
                "XDG_DATA_DIRS",
                "/tmp/.mount_buzz-x/usr/share:/usr/share:/usr/local/share",
            );
        }
        let mut cmd = std::process::Command::new("env");
        fence_appimage_env(&mut cmd);
        let out = cmd.output().unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        let line = stdout
            .lines()
            .find(|l| l.starts_with("XDG_DATA_DIRS="))
            .expect("XDG_DATA_DIRS present");
        assert!(
            !line.contains(".mount_"),
            "mount entry leaked into XDG_DATA_DIRS: {line}"
        );
        assert!(
            line.contains("/usr/share"),
            "host entries must survive: {line}"
        );
        let _ = std::env::remove_var("XDG_DATA_DIRS");
    }

    #[test]
    fn fence_keeps_unrelated_keys() {
        unsafe { std::env::set_var("BUZZ_FENCE_KEEP_PROBE", "1") };
        let mut cmd = std::process::Command::new("env");
        fence_appimage_env(&mut cmd);
        let out = cmd.output().unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.contains("BUZZ_FENCE_KEEP_PROBE=1"));
        let _ = std::env::remove_var("BUZZ_FENCE_KEEP_PROBE");
    }
}
