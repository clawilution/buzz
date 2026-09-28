use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tauri::{AppHandle, Emitter, Manager};

use super::{
    agent_readiness, append_log_marker, current_instance_id, find_managed_agent_mut,
    load_global_agent_config, load_managed_agents, load_personas, managed_agent_runtime_log_path,
    process_is_running, record_agent_command, resolve_effective_agent_env, save_managed_agents,
    spawn_agent_child, terminate_process, terminate_untracked_pair_runtime,
    write_agent_runtime_receipt, AgentReadiness, BackendKind, ManagedAgentPairRuntime,
    ManagedAgentRuntimeKey, ManagedAgentRuntimeLifecycle, ManagedAgentRuntimeReceipt,
    ManagedAgentRuntimeStatus, RuntimeRecovery,
};
use crate::app_state::AppState;

const STATUS_EVENT: &str = "managed-agent-runtime-status";
const RECOVERY_POLL: Duration = Duration::from_secs(10);
const HEALTHY_RUN: Duration = Duration::from_secs(300);
const RECOVERY_DELAYS: [Duration; 3] = [
    Duration::from_secs(5),
    Duration::from_secs(15),
    Duration::from_secs(45),
];

fn status_for(
    app: &AppHandle,
    record: &super::ManagedAgentRecord,
    key: &ManagedAgentRuntimeKey,
    runtime: Option<&ManagedAgentPairRuntime>,
    requested_relay_url: Option<String>,
) -> ManagedAgentRuntimeStatus {
    let personas = load_personas(app).unwrap_or_default();
    let global = load_global_agent_config(app).unwrap_or_default();
    status_for_with(
        app,
        record,
        key,
        runtime,
        requested_relay_url,
        StatusInputs {
            personas: &personas,
            global: &global,
        },
    )
}

/// Preloaded per-call-site inputs for [`status_for_with`], so multi-row
/// callers (list, reconcile) hit disk once instead of once per row.
struct StatusInputs<'a> {
    personas: &'a [super::AgentDefinition],
    global: &'a super::GlobalAgentConfig,
}

fn status_for_with(
    app: &AppHandle,
    record: &super::ManagedAgentRecord,
    key: &ManagedAgentRuntimeKey,
    runtime: Option<&ManagedAgentPairRuntime>,
    requested_relay_url: Option<String>,
    inputs: StatusInputs<'_>,
) -> ManagedAgentRuntimeStatus {
    let StatusInputs { personas, global } = inputs;
    let command = record_agent_command(record, personas);
    let metadata = super::known_acp_runtime(&command);
    let effective = resolve_effective_agent_env(record, personas, metadata, global);
    let local_setup = matches!(agent_readiness(&effective), AgentReadiness::Ready);
    ManagedAgentRuntimeStatus {
        pubkey: key.pubkey.clone(),
        relay_url: key.relay_url.clone(),
        requested_relay_url,
        local_setup,
        lifecycle: runtime
            .map(|runtime| runtime.lifecycle.clone())
            .unwrap_or(ManagedAgentRuntimeLifecycle::Stopped),
        pid: runtime.map(|runtime| runtime.child.id()),
        error: runtime.and_then(|runtime| runtime.error.clone()),
        log_path: managed_agent_runtime_log_path(app, key)
            .ok()
            .map(|path| path.display().to_string()),
    }
}

fn emit_status(app: &AppHandle, status: &ManagedAgentRuntimeStatus) {
    let _ = app.emit(STATUS_EVENT, status);
}

/// Polls independently of the Agents screen. A clean exit is still unexpected
/// while the pair remains tracked; explicit stop removes it before this poll.
pub(crate) fn spawn_runtime_supervisor(app: AppHandle) {
    tauri::async_runtime::spawn(async move {
        loop {
            tokio::time::sleep(RECOVERY_POLL).await;
            if app
                .state::<AppState>()
                .shutdown_started
                .load(Ordering::Acquire)
            {
                break;
            }
            let tick_app = app.clone();
            match tokio::task::spawn_blocking(move || supervise_runtime_tick(&tick_app)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => eprintln!("buzz-desktop: runtime recovery: {error}"),
                Err(error) => eprintln!("buzz-desktop: runtime recovery task failed: {error}"),
            }
        }
    });
}

fn recovery_failure(recovery: &mut RuntimeRecovery, now: Instant) -> Option<Duration> {
    if recovery
        .started_at
        .is_some_and(|start| now.duration_since(start) >= HEALTHY_RUN)
    {
        recovery.failures = 0;
    }
    recovery.started_at = None;
    recovery.failures = recovery.failures.saturating_add(1);
    let delay = RECOVERY_DELAYS
        .get(usize::from(recovery.failures - 1))
        .copied();
    recovery.next_retry = delay.map(|delay| now + delay);
    recovery.exhausted = delay.is_none();
    delay
}

fn poll_child_exit(child: &mut std::process::Child) -> Option<(String, Option<i32>)> {
    match child.try_wait() {
        Ok(Some(status)) => Some((
            format!("harness exited with status {status}"),
            status.code(),
        )),
        Err(error) => Some((format!("failed to inspect harness: {error}"), None)),
        Ok(None) => None,
    }
}

fn due_recovery_keys(
    recovery: &std::collections::HashMap<ManagedAgentRuntimeKey, RuntimeRecovery>,
    is_running: impl Fn(&ManagedAgentRuntimeKey) -> bool,
    now: Instant,
) -> Vec<(ManagedAgentRuntimeKey, u8)> {
    recovery
        .iter()
        .filter(|(key, entry)| {
            !entry.exhausted
                && entry.next_retry.is_some_and(|deadline| deadline <= now)
                && !is_running(key)
        })
        .map(|(key, entry)| (key.clone(), entry.failures))
        .collect()
}

fn supervise_runtime_tick(app: &AppHandle) -> Result<(), String> {
    let state = app.state::<AppState>();
    let _transition = state
        .managed_agent_runtime_transition
        .lock()
        .map_err(|e| e.to_string())?;
    if state.shutdown_started.load(Ordering::Acquire) {
        return Ok(());
    }
    let _store = state
        .managed_agents_store_lock
        .lock()
        .map_err(|e| e.to_string())?;
    let mut records = load_managed_agents(app)?;
    let mut runtimes = state
        .managed_agent_processes
        .lock()
        .map_err(|e| e.to_string())?;
    let mut recovery = state
        .managed_agent_recovery
        .lock()
        .map_err(|e| e.to_string())?;
    recovery.retain(|key, _| {
        records.iter().any(|record| {
            record.pubkey.eq_ignore_ascii_case(&key.pubkey) && record.backend == BackendKind::Local
        })
    });
    let now = Instant::now();
    let mut changed = false;
    let mut exited = Vec::new();
    for (key, runtime) in runtimes.iter_mut() {
        if let Some((reason, code)) = poll_child_exit(&mut runtime.child) {
            exited.push((key.clone(), reason, code));
        } else if recovery.get(key).is_some_and(|entry| {
            entry
                .started_at
                .is_some_and(|start| now.duration_since(start) >= HEALTHY_RUN)
        }) {
            recovery.remove(key);
        }
    }
    for (key, exit_error, exit_code) in exited {
        let Some(runtime) = runtimes.remove(&key) else {
            continue;
        };
        super::remove_agent_runtime_receipt(app, &key);
        state.clear_agent_session_cache(&key);
        let entry = recovery
            .entry(key.clone())
            .or_insert_with(RuntimeRecovery::new);
        let delay = recovery_failure(entry, now);
        let detail = super::meaningful_agent_error_from_log(&runtime.log_path)
            .map(|error| error.message)
            .unwrap_or(exit_error);
        let error = match delay {
            Some(delay) => format!("{detail}; restarting in {}s (attempt {}/3)", delay.as_secs(), entry.failures),
            None => format!("{detail}; automatic recovery exhausted after {} exits. Start the agent manually to retry.", entry.failures),
        };
        if let Some(record) = records
            .iter_mut()
            .find(|record| record.pubkey.eq_ignore_ascii_case(&key.pubkey))
        {
            record.updated_at = crate::util::now_iso();
            record.last_stopped_at = Some(record.updated_at.clone());
            record.last_exit_code = exit_code;
            record.last_error = Some(error.clone());
            record.last_error_code = None;
            let mut status = status_for(app, record, &key, None, None);
            status.lifecycle = ManagedAgentRuntimeLifecycle::Failed;
            status.error = Some(error);
            emit_status(app, &status);
            changed = true;
        }
    }
    let due = due_recovery_keys(&recovery, |key| runtimes.contains_key(key), now);
    if changed {
        save_managed_agents(app, &records)?;
    }
    drop(recovery);
    drop(runtimes);
    drop(_store);
    drop(_transition);

    for (key, generation) in due {
        match start_pair(
            key.pubkey.clone(),
            key.relay_url.clone(),
            true,
            None,
            Some(generation),
            app.clone(),
        ) {
            Ok(_) => {
                if let Ok(mut recovery) = state.managed_agent_recovery.lock() {
                    if let Some(entry) = recovery.get_mut(&key) {
                        if entry.failures == generation {
                            entry.started_at = Some(Instant::now());
                            entry.next_retry = None;
                        }
                    }
                }
            }
            Err(error) => {
                let mut recovery = state
                    .managed_agent_recovery
                    .lock()
                    .map_err(|e| e.to_string())?;
                let Some(entry) = recovery.get_mut(&key) else {
                    continue;
                };
                if entry.failures != generation {
                    continue;
                }
                let delay = recovery_failure(entry, Instant::now());
                let message = match delay {
                    Some(delay) => format!("restart failed: {error}; retrying in {}s", delay.as_secs()),
                    None => format!("restart failed: {error}; automatic recovery exhausted. Start the agent manually to retry."),
                };
                drop(recovery);
                // The failed state must remain visible even after the dead pair
                // has been removed from the runtime map.
                let _store = state
                    .managed_agents_store_lock
                    .lock()
                    .map_err(|e| e.to_string())?;
                let mut records = load_managed_agents(app)?;
                let failed_status = if let Some(record) = records
                    .iter_mut()
                    .find(|record| record.pubkey.eq_ignore_ascii_case(&key.pubkey))
                {
                    record.updated_at = crate::util::now_iso();
                    record.last_error = Some(message.clone());
                    let mut status = status_for(app, record, &key, None, None);
                    status.lifecycle = ManagedAgentRuntimeLifecycle::Failed;
                    status.error = Some(message);
                    Some(status)
                } else {
                    None
                };
                if let Some(status) = failed_status {
                    save_managed_agents(app, &records)?;
                    emit_status(app, &status);
                }
            }
        }
    }
    Ok(())
}

fn observer_lifecycle_key(
    outer_pubkey: &str,
    payload: &super::ManagedAgentRuntimeLifecycleObserverPayload,
) -> Result<ManagedAgentRuntimeKey, String> {
    if !outer_pubkey.eq_ignore_ascii_case(&payload.pubkey) {
        return Err("observer signer does not match lifecycle payload pubkey".into());
    }
    if matches!(
        payload.lifecycle,
        ManagedAgentRuntimeLifecycle::Starting | ManagedAgentRuntimeLifecycle::Stopped
    ) {
        return Err("observer cannot author starting or stopped lifecycle".into());
    }
    if payload.lifecycle == ManagedAgentRuntimeLifecycle::Failed && payload.error.is_none() {
        return Err("failed lifecycle requires an error".into());
    }
    if payload.lifecycle != ManagedAgentRuntimeLifecycle::Failed && payload.error.is_some() {
        return Err("lifecycle error is only valid for failed".into());
    }
    ManagedAgentRuntimeKey::new(payload.pubkey.clone(), &payload.relay_url)
}

#[tauri::command]
pub fn put_managed_agent_runtime_lifecycle(
    outer_pubkey: String,
    payload: super::ManagedAgentRuntimeLifecycleObserverPayload,
    app: AppHandle,
) -> Result<ManagedAgentRuntimeStatus, String> {
    let key = observer_lifecycle_key(&outer_pubkey, &payload)?;
    let state = app.state::<AppState>();
    let records = load_managed_agents(&app)?;
    let record = records
        .iter()
        .find(|record| record.pubkey.eq_ignore_ascii_case(&key.pubkey))
        .ok_or_else(|| format!("agent {} not found", key.pubkey))?;
    let mut runtimes = state
        .managed_agent_processes
        .lock()
        .map_err(|e| e.to_string())?;
    let runtime = runtimes
        .get_mut(&key)
        .ok_or_else(|| "lifecycle frame does not match a tracked runtime pair".to_string())?;
    if runtime.start_nonce != payload.start_nonce {
        return Err("lifecycle frame does not match the current harness generation".into());
    }
    if runtime
        .child
        .try_wait()
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Err("lifecycle frame arrived after process exit".into());
    }
    runtime.lifecycle = payload.lifecycle;
    runtime.error = payload.error;
    let status = status_for(&app, record, &key, Some(runtime), None);
    emit_status(&app, &status);
    Ok(status)
}

// Keep disk, process, and mutex work off the main thread so opening members cannot stall the UI.
#[tauri::command]
pub async fn list_managed_agent_runtimes(
    app: AppHandle,
) -> Result<Vec<ManagedAgentRuntimeStatus>, String> {
    tokio::task::spawn_blocking(move || {
        // This command is polled whenever the members sidebar opens and refetched
        // on every status event — load the per-row status inputs once, outside
        // the locks, instead of hitting disk per row while holding them.
        let personas = load_personas(&app).unwrap_or_default();
        let global = load_global_agent_config(&app).unwrap_or_default();
        let state = app.state::<AppState>();
        let _transition = state
            .managed_agent_runtime_transition
            .lock()
            .map_err(|e| e.to_string())?;
        let _store = state
            .managed_agents_store_lock
            .lock()
            .map_err(|e| e.to_string())?;
        let records = load_managed_agents(&app)?;
        let mut runtimes = state
            .managed_agent_processes
            .lock()
            .map_err(|e| e.to_string())?;
        let mut statuses = Vec::new();
        statuses.extend(runtimes.iter_mut().filter_map(|(key, runtime)| {
            let record = records
                .iter()
                .find(|record| record.pubkey.eq_ignore_ascii_case(&key.pubkey))?;
            let mut status = status_for_with(
                &app,
                record,
                key,
                Some(runtime),
                None,
                StatusInputs {
                    personas: &personas,
                    global: &global,
                },
            );
            if runtime.child.try_wait().ok().flatten().is_some() {
                status.lifecycle = ManagedAgentRuntimeLifecycle::Failed;
                status.error = Some("Agent process exited; recovery is pending".into());
                status.pid = None;
            }
            Some(status)
        }));
        let recovery = state
            .managed_agent_recovery
            .lock()
            .map_err(|e| e.to_string())?;
        statuses.extend(recovery.iter().filter_map(|(key, _)| {
            if runtimes.contains_key(key) {
                return None;
            }
            let record = records
                .iter()
                .find(|record| record.pubkey.eq_ignore_ascii_case(&key.pubkey))?;
            let mut status = status_for_with(
                &app,
                record,
                key,
                None,
                None,
                StatusInputs {
                    personas: &personas,
                    global: &global,
                },
            );
            status.lifecycle = ManagedAgentRuntimeLifecycle::Failed;
            status.error = record.last_error.clone();
            Some(status)
        }));
        drop(runtimes);
        Ok(statuses)
    })
    .await
    .map_err(|e| format!("spawn_blocking failed: {e}"))?
}

pub(crate) fn start_managed_agent_runtime_pair_lazy(
    pubkey: String,
    relay_url: String,
    app: AppHandle,
) -> Result<ManagedAgentRuntimeStatus, String> {
    let status = start_pair(pubkey, relay_url, true, None, None, app.clone())?;
    app.state::<AppState>()
        .managed_agent_recovery
        .lock()
        .map_err(|e| e.to_string())?
        .remove(&ManagedAgentRuntimeKey::new(
            status.pubkey.clone(),
            &status.relay_url,
        )?);
    Ok(status)
}

#[tauri::command]
pub fn start_managed_agent_runtime(
    pubkey: String,
    relay_url: String,
    app: AppHandle,
) -> Result<ManagedAgentRuntimeStatus, String> {
    start_managed_agent_runtime_pair_lazy(pubkey, relay_url, app)
}

fn start_pair(
    pubkey: String,
    relay_url: String,
    lazy: bool,
    expected_updated_at: Option<&str>,
    recovery_generation: Option<u8>,
    app: AppHandle,
) -> Result<ManagedAgentRuntimeStatus, String> {
    let state = app.state::<AppState>();
    let _transition = state
        .managed_agent_runtime_transition
        .lock()
        .map_err(|e| e.to_string())?;
    if state.shutdown_started.load(Ordering::Acquire) {
        return Err("desktop shutdown has started".into());
    }
    let key = ManagedAgentRuntimeKey::new(pubkey.clone(), &relay_url)?;
    if let Some(generation) = recovery_generation {
        let recovery = state
            .managed_agent_recovery
            .lock()
            .map_err(|e| e.to_string())?;
        if !recovery
            .get(&key)
            .is_some_and(|entry| entry.failures == generation && !entry.exhausted)
        {
            return Err("automatic restart was cancelled".into());
        }
    }
    let _store = state
        .managed_agents_store_lock
        .lock()
        .map_err(|e| e.to_string())?;
    let mut records = load_managed_agents(&app)?;
    let record = find_managed_agent_mut(&mut records, &pubkey)?;
    if record.backend != BackendKind::Local {
        return Err("managed runtime pairs require a local agent".into());
    }
    if expected_updated_at.is_some_and(|expected| record.updated_at != expected) {
        return Err("managed agent changed while runtime reconciliation was in flight".into());
    }
    let mut runtimes = state
        .managed_agent_processes
        .lock()
        .map_err(|e| e.to_string())?;
    if runtimes
        .get_mut(&key)
        .is_some_and(|runtime| runtime.child.try_wait().ok().flatten().is_none())
    {
        let status = status_for(&app, record, &key, runtimes.get(&key), None);
        return Ok(status);
    }
    runtimes.remove(&key);
    terminate_untracked_pair_runtime(&app, &key)?;

    let owner = state
        .keys
        .lock()
        .ok()
        .map(|keys| keys.public_key().to_hex());
    let mut process = spawn_agent_child(&app, record, &key.relay_url, lazy, owner.as_deref())?;
    let now = crate::util::now_iso();
    let receipt = ManagedAgentRuntimeReceipt {
        key: key.clone(),
        pid: process.child.id(),
        desktop_instance_id: current_instance_id(&app),
        started_at: now.clone(),
    };
    if let Err(error) = write_agent_runtime_receipt(&app, &receipt) {
        let _ = terminate_process(process.child.id());
        let _ = process.child.wait();
        return Err(error);
    }
    record.runtime_pid = None;
    record.updated_at = now.clone();
    record.last_started_at = Some(now);
    record.last_stopped_at = None;
    record.last_error = None;
    runtimes.insert(key.clone(), ManagedAgentPairRuntime::starting(process));
    let status = status_for(&app, record, &key, runtimes.get(&key), None);
    drop(runtimes);
    save_managed_agents(&app, &records)?;
    emit_status(&app, &status);
    Ok(status)
}

#[tauri::command]
pub fn stop_managed_agent_runtime(
    pubkey: String,
    relay_url: String,
    app: AppHandle,
) -> Result<ManagedAgentRuntimeStatus, String> {
    let state = app.state::<AppState>();
    let _transition = state
        .managed_agent_runtime_transition
        .lock()
        .map_err(|e| e.to_string())?;
    let _store = state
        .managed_agents_store_lock
        .lock()
        .map_err(|e| e.to_string())?;
    let mut records = load_managed_agents(&app)?;
    let record = find_managed_agent_mut(&mut records, &pubkey)?;
    let key = ManagedAgentRuntimeKey::new(pubkey, &relay_url)?;
    state
        .managed_agent_recovery
        .lock()
        .map_err(|e| e.to_string())?
        .remove(&key);
    let mut runtimes = state
        .managed_agent_processes
        .lock()
        .map_err(|e| e.to_string())?;
    if let Some(mut runtime) = runtimes.remove(&key) {
        let stop_result = if process_is_running(runtime.child.id()) {
            terminate_process(runtime.child.id())
        } else {
            Ok(())
        }
        .and_then(|()| runtime.child.wait().map_err(|e| e.to_string()));
        match stop_result {
            Ok(status) => {
                record.last_exit_code = status.code();
                let _ = append_log_marker(&runtime.log_path, "=== stopped pair runtime ===");
            }
            Err(error) => {
                // Keep failed teardown visible/manageable instead of
                // orphaning it: the child stays tracked and the receipt
                // stays on disk until a stop actually succeeds.
                runtimes.insert(key, runtime);
                return Err(error);
            }
        }
    } else {
        // No runtime is tracked at this key, but a valid prior-session
        // receipt may still point at a live child (e.g. the crash-recovery
        // window for a non-auto-start agent). Terminate that orphan before
        // erasing its receipt — otherwise this "stop" leaves the harness
        // running yet deletes the one artifact sweeps and
        // terminate_untracked_pair_runtime use to find it, and a follow-up
        // start would spawn a duplicate harness for the same pair. On
        // failure the receipt stays on disk (terminate_untracked_pair_runtime
        // only removes it after the child exits), mirroring the tracked
        // path's keep-until-success invariant.
        terminate_untracked_pair_runtime(&app, &key)?;
    }
    super::remove_agent_runtime_receipt(&app, &key);
    state.clear_agent_session_cache(&key);
    record.runtime_pid = None;
    record.updated_at = crate::util::now_iso();
    record.last_stopped_at = Some(record.updated_at.clone());
    record.last_error = None;
    record.last_error_code = None;
    let status = status_for(&app, record, &key, None, None);
    drop(runtimes);
    save_managed_agents(&app, &records)?;
    emit_status(&app, &status);
    Ok(status)
}

#[tauri::command]
pub fn restart_managed_agent_runtime(
    pubkey: String,
    relay_url: String,
    app: AppHandle,
) -> Result<ManagedAgentRuntimeStatus, String> {
    stop_managed_agent_runtime(pubkey.clone(), relay_url.clone(), app.clone())?;
    start_pair(pubkey, relay_url, true, None, None, app)
}

/// Probe whether this agent can operate on `requested_relay_url`.
///
/// Runs a bounded authenticated query with the agent's own keys (NIP-42 +
/// NIP-OA auth tag). Auth success is the spawn-eligibility signal: NIP-29
/// membership (kind 39002) cannot exist before the agent's harness first
/// connects to a relay, so gating on membership *presence* could never
/// bootstrap a pair on a newly configured community — it only rediscovered
/// pairs that had already run. A rejected or timed-out probe surfaces as a
/// Failed status row instead of a silent skip.
async fn probe_agent_relay_access(
    state: &AppState,
    record: super::ManagedAgentRecord,
    requested_relay_url: String,
) -> Result<(super::ManagedAgentRecord, ManagedAgentRuntimeKey, String), String> {
    let key = ManagedAgentRuntimeKey::new(record.pubkey.clone(), &requested_relay_url)?;
    let keys = nostr::Keys::parse(record.private_key_nsec.trim())
        .map_err(|error| format!("invalid managed-agent key: {error}"))?;
    let api_base = crate::relay::relay_http_base_url(&key.relay_url);
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        crate::relay::query_relay_at_with_keys(
            state,
            &api_base,
            &[serde_json::json!({"kinds": [39002], "#p": [record.pubkey]})],
            &keys,
            record.auth_tag.as_deref(),
        ),
    )
    .await
    .map_err(|_| "relay access probe timed out".to_string())??;
    Ok((record, key, requested_relay_url))
}

/// Build the `Failed` status row for a probe failure whose requested relay URL
/// cannot even form a pair key (so there is no canonical `relay_url` to key on).
/// The raw requested URL stands in for both the identity and the requested
/// field so the batch still degrades this one community to a visible row
/// instead of aborting every other community's row.
fn unkeyable_failed_status(
    record: &super::ManagedAgentRecord,
    requested: String,
    error: String,
    personas: &[super::AgentDefinition],
    global: &super::GlobalAgentConfig,
) -> ManagedAgentRuntimeStatus {
    let command = record_agent_command(record, personas);
    let metadata = super::known_acp_runtime(&command);
    let effective = resolve_effective_agent_env(record, personas, metadata, global);
    ManagedAgentRuntimeStatus {
        pubkey: record.pubkey.clone(),
        relay_url: requested.clone(),
        requested_relay_url: Some(requested),
        local_setup: matches!(agent_readiness(&effective), AgentReadiness::Ready),
        lifecycle: ManagedAgentRuntimeLifecycle::Failed,
        pid: None,
        error: Some(error),
        log_path: None,
    }
}

/// Spawn a lazy harness pair for every eligible (agent, community) pair.
///
/// Eligibility is deliberately gated on `start_on_app_launch`: auto-start is
/// the *proactive fan-out* policy — "keep this agent warm in every community" —
/// not a correctness prerequisite. A manual-start agent still works on demand
/// everywhere: attaching it to a channel ensures its pair, an @mention wakes a
/// pair, the members sidebar and Settings controls start pairs, and restore
/// preserves running pairs across relaunch. Fanning out warm-socket pairs for
/// agents the user chose *not* to auto-start would contradict that choice, so
/// reconcile leaves them alone until something explicitly asks for them.
#[tauri::command]
pub async fn reconcile_managed_agent_runtimes(
    communities: Vec<super::ManagedAgentCommunityTarget>,
    app: AppHandle,
) -> Result<Vec<ManagedAgentRuntimeStatus>, String> {
    use futures_util::{stream, StreamExt};

    let records = load_managed_agents(&app)?;
    let mut jobs = Vec::new();
    for community in communities {
        for record in records
            .iter()
            .filter(|record| record.start_on_app_launch && record.backend == BackendKind::Local)
        // The legacy per-record relay pin is deliberately ignored here — see
        // `effective_agent_relay_url`. Every local auto-start agent fans out
        // to every configured community.
        {
            jobs.push((record.clone(), community.relay_url.clone()));
        }
    }
    let probes: Vec<_> = stream::iter(jobs)
        .map(|(record, requested)| {
            let state = app.state::<AppState>();
            async move {
                let fallback_record = record.clone();
                let fallback_requested = requested.clone();
                probe_agent_relay_access(&state, record, requested)
                    .await
                    .map_err(|error| (fallback_record, fallback_requested, error))
            }
        })
        .buffer_unordered(6)
        .collect()
        .await;

    // start_pair does blocking work (std mutexes, process spawn, receipt
    // writes, and up-to-2s exit polling in terminate_untracked_pair_runtime),
    // so run the post-probe start loop off the async workers, matching the
    // restart flows.
    tokio::task::spawn_blocking(move || {
        let personas = load_personas(&app).unwrap_or_default();
        let global = load_global_agent_config(&app).unwrap_or_default();
        let mut rows = Vec::new();
        for probe in probes {
            match probe {
                Ok((record, key, requested)) => {
                    match start_pair(
                        record.pubkey.clone(),
                        key.relay_url.clone(),
                        true,
                        Some(&record.updated_at),
                        None,
                        app.clone(),
                    ) {
                        Ok(mut status) => {
                            status.requested_relay_url = Some(requested);
                            rows.push(status);
                        }
                        Err(error) => {
                            let mut status = status_for_with(
                                &app,
                                &record,
                                &key,
                                None,
                                Some(requested),
                                StatusInputs {
                                    personas: &personas,
                                    global: &global,
                                },
                            );
                            status.lifecycle = ManagedAgentRuntimeLifecycle::Failed;
                            status.error = Some(error);
                            rows.push(status);
                        }
                    }
                }
                Err((record, requested, error)) => {
                    // Per-community degradation: a relay URL that cannot even
                    // form a pair key gets a Failed row (with the raw
                    // requested URL) like any other probe failure, instead of
                    // aborting every other community's row.
                    let status =
                        match ManagedAgentRuntimeKey::new(record.pubkey.clone(), &requested) {
                            Ok(key) => {
                                let mut status = status_for_with(
                                    &app,
                                    &record,
                                    &key,
                                    None,
                                    Some(requested),
                                    StatusInputs {
                                        personas: &personas,
                                        global: &global,
                                    },
                                );
                                status.lifecycle = ManagedAgentRuntimeLifecycle::Failed;
                                status.error = Some(error);
                                status
                            }
                            Err(_) => unkeyable_failed_status(
                                &record, requested, error, &personas, &global,
                            ),
                        };
                    rows.push(status);
                }
            }
        }
        rows
    })
    .await
    .map_err(|e| format!("spawn_blocking failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(unix)]
    fn clean_child_exit_consumes_recovery_budget() {
        // A code-0 ACP exit still strands a live assignment. Exercise the
        // same child poll and crash budget used by the background watchdog.
        let mut child = std::process::Command::new("/usr/bin/true").spawn().unwrap();
        let (reason, code) = loop {
            if let Some(exit) = poll_child_exit(&mut child) {
                break exit;
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        assert_eq!(code, Some(0));
        assert!(reason.contains("exited"));
        let now = Instant::now();
        let mut recovery = RuntimeRecovery::new();
        assert_eq!(
            recovery_failure(&mut recovery, now),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            recovery_failure(&mut recovery, now),
            Some(Duration::from_secs(15))
        );
        assert_eq!(
            recovery_failure(&mut recovery, now),
            Some(Duration::from_secs(45))
        );
        assert_eq!(recovery_failure(&mut recovery, now), None);
        assert!(recovery.exhausted);
    }

    #[test]
    fn healthy_run_resets_crash_budget() {
        let now = Instant::now();
        let mut recovery = RuntimeRecovery::new();
        recovery.failures = 3;
        recovery.started_at = Some(now - HEALTHY_RUN);
        assert_eq!(
            recovery_failure(&mut recovery, now),
            Some(Duration::from_secs(5))
        );
        assert_eq!(recovery.failures, 1);
    }

    #[test]
    fn intentional_stop_cancels_pending_restart() {
        let key = ManagedAgentRuntimeKey::new("aa".repeat(32), "wss://relay.example").unwrap();
        let now = Instant::now();
        let mut recovery = std::collections::HashMap::new();
        let mut attempt = RuntimeRecovery::new();
        recovery_failure(&mut attempt, now - Duration::from_secs(5));
        recovery.insert(key.clone(), attempt);
        assert_eq!(due_recovery_keys(&recovery, |_| false, now).len(), 1);
        recovery.remove(&key); // same cancellation performed by the stop path
        assert!(due_recovery_keys(&recovery, |_| false, now).is_empty());
    }

    #[test]
    fn list_managed_agent_runtimes_returns_a_future() {
        fn assert_async_command<F, Fut>(_command: F)
        where
            F: Fn(AppHandle) -> Fut,
            Fut: std::future::Future<Output = Result<Vec<ManagedAgentRuntimeStatus>, String>>,
        {
        }

        assert_async_command(list_managed_agent_runtimes);
    }

    fn payload(
        relay_url: &str,
        lifecycle: ManagedAgentRuntimeLifecycle,
        error: Option<&str>,
    ) -> super::super::ManagedAgentRuntimeLifecycleObserverPayload {
        super::super::ManagedAgentRuntimeLifecycleObserverPayload {
            pubkey: "aa".repeat(32),
            relay_url: relay_url.into(),
            start_nonce: "test-generation".into(),
            lifecycle,
            error: error.map(str::to_owned),
        }
    }

    fn record_with_relay(relay_url: &str) -> super::super::ManagedAgentRecord {
        serde_json::from_str(&format!(
            r#"{{
                "pubkey": "{}",
                "name": "pin-test",
                "relay_url": "{relay_url}",
                "acp_command": "buzz-acp",
                "agent_command": "goose",
                "agent_args": [],
                "mcp_command": "",
                "turn_timeout_seconds": 320,
                "system_prompt": "",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z"
            }}"#,
            "aa".repeat(32)
        ))
        .unwrap()
    }

    #[test]
    fn legacy_relay_pin_is_ignored_for_fan_out() {
        // Zero-touch cutover (#2122): a record carrying a creation-era
        // `relay_url` pin must fan out exactly like an unpinned one — the
        // stored field is parsed but never consulted. See
        // `effective_agent_relay_url`.
        let unpinned = record_with_relay("");
        let pinned = record_with_relay("wss://one.example");
        for record in [&unpinned, &pinned] {
            assert_eq!(
                crate::relay::effective_agent_relay_url(&record.relay_url, "wss://two.example"),
                "wss://two.example"
            );
        }
    }

    #[test]
    fn unkeyable_relay_degrades_to_failed_row() {
        // A requested URL that cannot form a pair key must still yield a
        // Failed row keyed by the raw requested string, so one bad community
        // never aborts the rest of the reconcile batch.
        let record = record_with_relay("");
        let status = unkeyable_failed_status(
            &record,
            "not a url".to_string(),
            "relay access probe timed out".to_string(),
            &[],
            &super::super::GlobalAgentConfig::default(),
        );
        assert!(matches!(
            status.lifecycle,
            ManagedAgentRuntimeLifecycle::Failed
        ));
        assert_eq!(status.relay_url, "not a url");
        assert_eq!(status.requested_relay_url.as_deref(), Some("not a url"));
        assert_eq!(status.pubkey, record.pubkey);
        assert_eq!(
            status.error.as_deref(),
            Some("relay access probe timed out")
        );
        assert!(status.pid.is_none());
    }

    #[test]
    fn runtime_key_rejects_non_hex_pubkeys() {
        assert!(ManagedAgentRuntimeKey::new("../not-a-key", "wss://relay.example").is_err());
        assert!(ManagedAgentRuntimeKey::new("gg".repeat(32), "wss://relay.example").is_err());
    }

    #[test]
    fn runtime_key_canonicalizes_hex_pubkeys() {
        let key = ManagedAgentRuntimeKey::new("AA".repeat(32), "wss://relay.example").unwrap();
        assert_eq!(key.pubkey, "aa".repeat(32));
    }

    #[test]
    fn observer_lifecycle_key_preserves_exact_canonical_pair() {
        let first = payload(
            "WSS://Relay.Example:443/",
            ManagedAgentRuntimeLifecycle::Ready,
            None,
        );
        let key = observer_lifecycle_key(&first.pubkey, &first).unwrap();
        assert_eq!(key.pubkey, first.pubkey);
        assert_eq!(key.relay_url, "wss://relay.example");

        let other = payload(
            "wss://other.example",
            ManagedAgentRuntimeLifecycle::Ready,
            None,
        );
        assert_ne!(key, observer_lifecycle_key(&other.pubkey, &other).unwrap());
    }

    #[test]
    fn observer_lifecycle_rejects_cross_agent_and_desktop_states() {
        let ready = payload(
            "wss://relay.example",
            ManagedAgentRuntimeLifecycle::Ready,
            None,
        );
        assert!(observer_lifecycle_key(&"bb".repeat(32), &ready).is_err());

        let stopped = payload(
            "wss://relay.example",
            ManagedAgentRuntimeLifecycle::Stopped,
            None,
        );
        assert!(observer_lifecycle_key(&stopped.pubkey, &stopped).is_err());
    }

    #[test]
    fn observer_lifecycle_enforces_failed_error_contract() {
        let failed = payload(
            "wss://relay.example",
            ManagedAgentRuntimeLifecycle::Failed,
            None,
        );
        assert!(observer_lifecycle_key(&failed.pubkey, &failed).is_err());

        let ready_with_error = payload(
            "wss://relay.example",
            ManagedAgentRuntimeLifecycle::Ready,
            Some("unexpected"),
        );
        assert!(observer_lifecycle_key(&ready_with_error.pubkey, &ready_with_error).is_err());
    }
}
