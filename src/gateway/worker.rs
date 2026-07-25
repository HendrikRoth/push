//! Per-thread worker: runs one message through an agent backend, records the
//! outcome in canonical history, and delivers the reply.

use std::future::{pending, Future};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

use crate::agent::{Request, RunError};
use crate::channel::OutboundFile;
use crate::history::{DeliveryStatus, OutboundMessage, OutboundOrigin};
use crate::rehydration::{self, RehydrationPrompt};
use crate::soul;
use crate::voice::MAX_AUDIO_BYTES;

use super::{audit, complete_row, Ctx, Job, WorkerState};

pub(super) const SESSION_SETUP_FAILURE: &str =
    "Push could not prepare this conversation. Check the local logs, then resend.";

/// Processes one thread's jobs strictly in order, exiting when the queue closes.
pub(super) async fn run(
    ctx: Ctx,
    mut rx: mpsc::Receiver<Job>,
    mut cancel: watch::Receiver<i64>,
    state: Arc<std::sync::Mutex<WorkerState>>,
) {
    while let Some(job) = rx.recv().await {
        let row_id = job.row_id;
        {
            let mut state = state.lock().unwrap();
            state.current_row = Some(row_id);
            state.retained_rows.remove(&row_id);
        }
        handle_with_interrupt(&ctx, job, interrupt(&mut cancel, row_id)).await;
        let completed = !ctx.ack.lock().unwrap().in_flight.contains(&row_id);
        let mut state = state.lock().unwrap();
        state.current_row = None;
        if completed {
            state.pending.retain(|job| job.row_id != row_id);
            state.retained_rows.remove(&row_id);
        } else {
            state.retained_rows.insert(row_id);
        }
    }
}

#[cfg(test)]
pub(super) async fn handle(ctx: &Ctx, job: Job) {
    handle_with_interrupt(ctx, job, pending()).await;
}

async fn handle_with_interrupt<I>(ctx: &Ctx, mut job: Job, interrupt: I)
where
    I: Future<Output = ()>,
{
    let existing_outbound = match ctx.history.lock().unwrap().outbound_for(job.inbound_id) {
        Ok(outbound) => outbound,
        Err(error) => {
            history_error(ctx, &job, "read outbound", error);
            return;
        }
    };
    if let Some(outbound) = existing_outbound {
        report_delivery(
            ctx,
            &job,
            deliver_stored(ctx, &job, &outbound).await,
            &outbound.content,
            "recovered_outbound",
            "recover outbound",
        );
        return;
    }

    if job.voice_attachment.is_some() {
        match prepare_voice(ctx, &job).await {
            Ok(transcript) => job.text = transcript,
            Err(VoicePreparationError::History(error)) => {
                history_error(ctx, &job, "store voice transcript", error);
                return;
            }
            Err(VoicePreparationError::User {
                event,
                reply,
                detail,
            }) => {
                warn!("[{}] {detail}", job.thread);
                audit(
                    ctx,
                    ctx.audit
                        .failed(event, job.row_id, &job.thread, Some(job.backend), detail),
                );
                let delivery = record_and_deliver(ctx, &job, OutboundOrigin::Gateway, reply).await;
                report_delivery(ctx, &job, delivery, reply, event, "deliver voice fallback");
                return;
            }
        }
    }

    if let Some(reply) = command(ctx, &job) {
        let delivery = record_and_deliver(ctx, &job, OutboundOrigin::Gateway, &reply).await;
        if delivery.is_ok() {
            info!(
                "[{}] command reply sent via {}",
                job.thread,
                ctx.channel.id()
            );
        }
        report_delivery(
            ctx,
            &job,
            delivery,
            &reply,
            "command",
            "record command reply",
        );
        return;
    }

    if let Some(reply) = job_command(ctx, &job).await {
        let delivery = record_and_deliver(ctx, &job, OutboundOrigin::Gateway, &reply).await;
        if delivery.is_ok() {
            info!("[{}] job command reply sent via {}", job.thread, ctx.channel.id());
        }
        report_delivery(
            ctx,
            &job,
            delivery,
            &reply,
            "job_command",
            "record job command reply",
        );
        return;
    }

    let Some(runner) = ctx.runners.get(&job.backend) else {
        error!(
            "[{}] no runner configured for {}",
            job.thread,
            job.backend.as_str()
        );
        audit(
            ctx,
            ctx.audit.failed(
                "backend_run_failed",
                job.row_id,
                &job.thread,
                Some(job.backend),
                "no runner configured",
            ),
        );
        complete_job(ctx, &job, "missing_runner");
        return;
    };

    let session_result = {
        let initial_session_id = runner.initial_session_id();
        ctx.store.lock().unwrap().session_for(
            &job.thread,
            runner.backend().as_str(),
            initial_session_id,
        )
    };
    let (session_id, is_new) = match session_result {
        Ok(v) => v,
        Err(e) => {
            error!("[{}] session error: {e}", job.thread);
            audit(
                ctx,
                ctx.audit.failed(
                    "backend_setup_failed",
                    job.row_id,
                    &job.thread,
                    Some(job.backend),
                    e.to_string(),
                ),
            );
            complete_setup_failure(ctx, &job, SESSION_SETUP_FAILURE).await;
            return;
        }
    };

    let work_dir = match std::fs::canonicalize(&ctx.assistant_dir) {
        Ok(path) => path.to_string_lossy().to_string(),
        Err(error) => {
            error!("[{}] assistant workspace error: {error}", job.thread);
            audit(
                ctx,
                ctx.audit.failed(
                    "backend_setup_failed",
                    job.row_id,
                    &job.thread,
                    Some(job.backend),
                    error.to_string(),
                ),
            );
            complete_setup_failure(ctx, &job, SESSION_SETUP_FAILURE).await;
            return;
        }
    };

    if !job.files.is_empty() {
        let note = prepare_files(ctx, &job, &work_dir).await;
        if !note.is_empty() {
            job.text = if job.text.trim().is_empty() {
                note
            } else {
                format!("{}\n\n{note}", job.text)
            };
        }
    }

    let instructions = match soul::load(&work_dir) {
        Ok(instructions) => instructions,
        Err(error) => {
            error!("[{}] assistant identity error: {error}", job.thread);
            audit(
                ctx,
                ctx.audit.failed(
                    "backend_setup_failed",
                    job.row_id,
                    &job.thread,
                    Some(job.backend),
                    error.to_string(),
                ),
            );
            complete_setup_failure(ctx, &job, SESSION_SETUP_FAILURE).await;
            return;
        }
    };
    if let Err(error) = ctx.cfg.backend_context_dir() {
        error!("[{}] assistant context error: {error:#}", job.thread);
        audit(
            ctx,
            ctx.audit.failed(
                "backend_setup_failed",
                job.row_id,
                &job.thread,
                Some(job.backend),
                error.to_string(),
            ),
        );
        complete_setup_failure(ctx, &job, SESSION_SETUP_FAILURE).await;
        return;
    }

    let run = async {
        let mut session_id = session_id;
        let mut rehydration = if is_new {
            match rehydration_prompt(ctx, &job) {
                Ok(prompt) => Some(prompt),
                Err(error) => {
                    return Err(RunError::Failed(format!(
                        "load canonical history for rehydration: {error}"
                    )));
                }
            }
        } else {
            None
        };

        // Some backends let push choose the session id. Mark those before the
        // run so a post-create failure does not retry the same create call.
        if is_new && runner.mark_started_before_run() {
            let _ = ctx.store.lock().unwrap().mark_started(&job.thread, None);
        }
        let prompt = rehydration
            .as_ref()
            .filter(|prompt| prompt.message_count > 0)
            .map_or(job.text.as_str(), |prompt| prompt.text.as_str());
        audit(
            ctx,
            ctx.audit.backend_started(
                job.row_id,
                &job.thread,
                job.backend,
                is_new,
                rehydration
                    .as_ref()
                    .map_or(0, |prompt| prompt.message_count),
            ),
        );
        info!(
            "[{}] sending message to {} (new_session={is_new}, rehydrated_messages={})",
            job.thread,
            runner.label(),
            rehydration
                .as_ref()
                .map_or(0, |prompt| prompt.message_count)
        );
        let mut result = runner
            .run(
                backend_request(&session_id, is_new, &work_dir, &instructions, prompt),
                ctx.run_timeout,
            )
            .await;
        // If the session id already exists (e.g. left over from a previous run
        // or a different build), resume it instead of trying to create it again.
        if is_new {
            if let Err(RunError::Failed(msg)) = &result {
                if msg.to_lowercase().contains("already in use") {
                    warn!("[{}] session id already existed, resuming", job.thread);
                    result = runner
                        .run(
                            backend_request(
                                &session_id,
                                false,
                                &work_dir,
                                &instructions,
                                &job.text,
                            ),
                            ctx.run_timeout,
                        )
                        .await;
                }
            }
        } else if matches!(&result, Err(RunError::SessionMissing(_))) {
            warn!(
                "[{}] backend session is missing; rotating and rehydrating",
                job.thread
            );
            audit(
                ctx,
                ctx.audit.failed(
                    "backend_session_missing",
                    job.row_id,
                    &job.thread,
                    Some(job.backend),
                    "backend could not resume the stored session",
                ),
            );
            session_id = runner.initial_session_id();
            if let Err(error) = ctx.store.lock().unwrap().rotate(
                &job.thread,
                runner.backend().as_str(),
                session_id.clone(),
            ) {
                return Err(RunError::Failed(format!(
                    "rotate missing backend session: {error}"
                )));
            }
            rehydration = match rehydration_prompt(ctx, &job) {
                Ok(prompt) => Some(prompt),
                Err(error) => {
                    return Err(RunError::Failed(format!(
                        "load canonical history for rehydration: {error}"
                    )));
                }
            };
            if runner.mark_started_before_run() {
                let _ = ctx.store.lock().unwrap().mark_started(&job.thread, None);
            }
            let count = rehydration
                .as_ref()
                .map_or(0, |prompt| prompt.message_count);
            audit(
                ctx,
                ctx.audit
                    .backend_started(job.row_id, &job.thread, job.backend, true, count),
            );
            let prompt = rehydration
                .as_ref()
                .filter(|prompt| prompt.message_count > 0)
                .map_or(job.text.as_str(), |prompt| prompt.text.as_str());
            result = runner
                .run(
                    backend_request(&session_id, true, &work_dir, &instructions, prompt),
                    ctx.run_timeout,
                )
                .await;
        }
        result
    };
    let run = async {
        if let Some(refresh) = ctx.channel.typing_refresh() {
            let channel = ctx.channel.clone();
            let channel_id = channel.id();
            let target = job.target.clone();
            let thread = job.thread.clone();
            run_with_periodic_activity(run, refresh, move || {
                let channel = channel.clone();
                let target = target.clone();
                let thread = thread.clone();
                async move {
                    if let Err(e) = channel.send_typing(&target).await {
                        warn!("[{thread}] {channel_id} typing update failed: {e}");
                    }
                }
            })
            .await
        } else {
            run.await
        }
    };
    tokio::pin!(run);
    tokio::pin!(interrupt);
    let result = tokio::select! {
        result = &mut run => Some(result),
        _ = &mut interrupt => None,
    };

    match result {
        Some(Ok(out)) => {
            info!(
                "[{}] {} completed; reply_chars={}",
                job.thread,
                runner.label(),
                out.reply.chars().count()
            );
            audit(
                ctx,
                ctx.audit
                    .backend_completed(job.row_id, &job.thread, job.backend, &out.reply),
            );
            // Pull any [[attach: path]] markers out of the reply; the delivered
            // and stored text is the reply without them.
            let attachments = parse_attach_markers(&out.reply);
            let reply = strip_attach_markers(&out.reply);
            let outbound = match ctx.history.lock().unwrap().record_outbound(
                job.inbound_id,
                OutboundOrigin::Backend,
                Some(job.backend.as_str()),
                &reply,
            ) {
                Ok(outbound) => outbound,
                Err(error) => {
                    history_error(ctx, &job, "record backend reply", error);
                    return;
                }
            };
            if let Err(e) = ctx
                .store
                .lock()
                .unwrap()
                .mark_started(&job.thread, out.session_id.as_deref())
            {
                error!("[{}] session save error: {e}", job.thread);
                audit(
                    ctx,
                    ctx.audit.failed(
                        "backend_session_save_failed",
                        job.row_id,
                        &job.thread,
                        Some(job.backend),
                        e.to_string(),
                    ),
                );
                return;
            }
            let delivery = deliver_stored(ctx, &job, &outbound).await;
            if delivery.is_ok() {
                info!("[{}] reply sent via {}", job.thread, ctx.channel.id());
                if !attachments.is_empty() {
                    let files = load_outbound_files(&work_dir, &attachments);
                    if !files.is_empty() {
                        if let Err(e) = ctx.channel.send_files(&job.target, &files).await {
                            warn!("[{}] attachment upload failed: {e:#}", job.thread);
                        }
                    }
                }
                if let Err(e) = ctx.channel.finish_activity(&job.target, true).await {
                    warn!("[{}] {} activity finish failed: {e}", job.thread, ctx.channel.id());
                }
            }
            report_delivery(
                ctx,
                &job,
                delivery,
                &reply,
                "completed",
                "deliver backend reply",
            );
        }
        Some(Err(RunError::Timeout)) => {
            warn!("[{}] {} run timed out", job.thread, runner.label());
            audit(
                ctx,
                ctx.audit.failed(
                    "backend_run_failed",
                    job.row_id,
                    &job.thread,
                    Some(job.backend),
                    format!("{} run timed out", runner.label()),
                ),
            );
            let reply = "That took too long and was stopped. Try again or simplify the request.";
            finish_run_with_gateway_reply(
                ctx,
                &job,
                reply,
                ReplyLabels {
                    record: "record timeout reply",
                    deliver: "deliver timeout reply",
                    completion: "timeout",
                },
            )
            .await;
        }
        None => {
            warn!("[{}] {} run interrupted", job.thread, runner.label());
            audit(
                ctx,
                ctx.audit.failed(
                    "backend_run_interrupted",
                    job.row_id,
                    &job.thread,
                    Some(job.backend),
                    format!("{} run interrupted by user", runner.label()),
                ),
            );
            finish_run_with_gateway_reply(
                ctx,
                &job,
                "Stopped the current request.",
                ReplyLabels {
                    record: "record interrupted reply",
                    deliver: "deliver interrupted reply",
                    completion: "interrupted",
                },
            )
            .await;
        }
        Some(Err(RunError::Failed(msg) | RunError::SessionMissing(msg))) => {
            error!("[{}] {} error: {msg}", job.thread, runner.label());
            audit(
                ctx,
                ctx.audit.failed(
                    "backend_run_failed",
                    job.row_id,
                    &job.thread,
                    Some(job.backend),
                    msg.clone(),
                ),
            );
            let reply = format!("⚠️ {}", short(&msg));
            finish_run_with_gateway_reply(
                ctx,
                &job,
                &reply,
                ReplyLabels {
                    record: "record failure reply",
                    deliver: "deliver failure reply",
                    completion: "backend_failed",
                },
            )
            .await;
        }
    }
}

async fn interrupt(cancel: &mut watch::Receiver<i64>, row_id: i64) {
    while *cancel.borrow() != row_id {
        if cancel.changed().await.is_err() {
            pending::<()>().await;
        }
    }
}

enum VoicePreparationError {
    User {
        event: &'static str,
        reply: &'static str,
        detail: String,
    },
    History(anyhow::Error),
}

async fn prepare_voice(ctx: &Ctx, job: &Job) -> std::result::Result<String, VoicePreparationError> {
    let attachment = job
        .voice_attachment
        .as_ref()
        .expect("voice preparation requires an attachment");
    if attachment
        .file_size
        .is_some_and(|size| size > MAX_AUDIO_BYTES)
    {
        return Err(VoicePreparationError::User {
            event: "voice_too_large",
            reply: "That voice message is too large. The limit is 20 MB.",
            detail: "voice message exceeds the 20 MB limit".to_string(),
        });
    }
    let Some(voice) = &ctx.voice else {
        return Err(VoicePreparationError::User {
            event: "voice_not_configured",
            reply: "Voice messages are unavailable. Set voice.openai_api_key in config or OPENAI_API_KEY, restart Push, or send text instead.",
            detail: "OpenAI API key is not configured".to_string(),
        });
    };
    let clip = ctx
        .channel
        .download_voice(attachment)
        .await
        .map_err(|error| VoicePreparationError::User {
            event: "voice_download_failed",
            reply: "I could not download that voice message. Please try again or send text.",
            detail: format!("voice download failed: {error:#}"),
        })?;
    let transcript = voice
        .transcribe(clip)
        .await
        .map_err(|error| VoicePreparationError::User {
            event: "voice_transcription_failed",
            reply: "I could not transcribe that voice message. Please try again or send text.",
            detail: format!("voice transcription failed: {error:#}"),
        })?;
    ctx.history
        .lock()
        .unwrap()
        .replace_inbound_content(job.inbound_id, &transcript)
        .map_err(VoicePreparationError::History)?;
    Ok(transcript)
}

/// Largest agent-produced file Push will attach to a reply.
const MAX_ATTACH_BYTES: usize = 20 * 1024 * 1024;
/// Most attachments downloaded for a single inbound message.
const MAX_INBOUND_FILES: usize = 10;

/// Downloads the message's whitelisted attachments into `<work_dir>/inbox/` and
/// returns a note listing their relative paths. The agent decides whether and
/// how to use them; a per-file failure is skipped, not fatal.
async fn prepare_files(ctx: &Ctx, job: &Job, work_dir: &str) -> String {
    let inbox = std::path::Path::new(work_dir).join("inbox");
    if let Err(error) = std::fs::create_dir_all(&inbox) {
        warn!("[{}] inbox dir create failed: {error}", job.thread);
        return String::new();
    }
    // Keep uploaded files out of the assistant's git repository.
    let ignore = inbox.join(".gitignore");
    if !ignore.exists() {
        let _ = std::fs::write(&ignore, "*\n");
    }
    let mut lines = Vec::new();
    for file in job.files.iter().take(MAX_INBOUND_FILES) {
        let Some(name) = sanitize_filename(&file.filename) else {
            continue;
        };
        let bytes = match ctx.channel.download_file(file).await {
            Ok(bytes) => bytes,
            Err(error) => {
                warn!(
                    "[{}] attachment {:?} download failed: {error:#}",
                    job.thread, file.filename
                );
                continue;
            }
        };
        if let Err(error) = std::fs::write(inbox.join(&name), &bytes) {
            warn!("[{}] attachment {name:?} write failed: {error}", job.thread);
            continue;
        }
        lines.push(format!("- inbox/{name}"));
    }
    if lines.is_empty() {
        return String::new();
    }
    format!(
        "The user attached the following file(s) in the working directory:\n{}",
        lines.join("\n")
    )
}

/// Reduces a channel-supplied filename to a safe base name inside `inbox/`.
fn sanitize_filename(name: &str) -> Option<String> {
    let base = std::path::Path::new(name).file_name()?.to_str()?;
    (!base.is_empty() && base != "." && base != "..").then(|| base.to_string())
}

/// Extracts the paths from `[[attach: <path>]]` markers in the agent reply.
fn parse_attach_markers(text: &str) -> Vec<String> {
    let mut paths = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("[[attach:") {
        let after = &rest[start + "[[attach:".len()..];
        let Some(end) = after.find("]]") else {
            break;
        };
        let path = after[..end].trim();
        if !path.is_empty() {
            paths.push(path.to_string());
        }
        rest = &after[end + 2..];
    }
    paths
}

/// Removes every `[[attach: …]]` marker from the reply text.
fn strip_attach_markers(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(start) = rest.find("[[attach:") {
        out.push_str(&rest[..start]);
        let after = &rest[start + "[[attach:".len()..];
        match after.find("]]") {
            Some(end) => rest = &after[end + 2..],
            None => {
                rest = "";
                break;
            }
        }
    }
    out.push_str(rest);
    out.trim().to_string()
}

/// Reads agent-produced files named by attach markers, refusing paths that
/// escape the work directory or exceed the size cap.
fn load_outbound_files(work_dir: &str, names: &[String]) -> Vec<OutboundFile> {
    let Ok(root) = std::fs::canonicalize(work_dir) else {
        return Vec::new();
    };
    let mut files = Vec::new();
    for name in names {
        let Ok(resolved) = std::fs::canonicalize(root.join(name)) else {
            continue;
        };
        if !resolved.starts_with(&root) {
            continue;
        }
        let Ok(bytes) = std::fs::read(&resolved) else {
            continue;
        };
        if bytes.is_empty() || bytes.len() > MAX_ATTACH_BYTES {
            continue;
        }
        let Some(filename) = resolved.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        files.push(OutboundFile {
            filename: filename.to_string(),
            bytes,
        });
    }
    files
}

/// Error and completion labels for one gateway-authored reply flow.
struct ReplyLabels {
    record: &'static str,
    deliver: &'static str,
    completion: &'static str,
}

/// Records a gateway-authored reply, delivers it, and completes the row.
/// Shared by the timeout and failure arms of `handle`.
async fn finish_run_with_gateway_reply(ctx: &Ctx, job: &Job, reply: &str, labels: ReplyLabels) {
    let outbound = match ctx.history.lock().unwrap().record_outbound(
        job.inbound_id,
        OutboundOrigin::Gateway,
        Some(job.backend.as_str()),
        reply,
    ) {
        Ok(outbound) => outbound,
        Err(error) => return history_error(ctx, job, labels.record, error),
    };
    report_delivery(
        ctx,
        job,
        deliver_stored(ctx, job, &outbound).await,
        reply,
        labels.completion,
        labels.deliver,
    );
    // A failed, timed-out, or interrupted run still clears its in-progress
    // signal, marking the message as not successfully answered.
    if let Err(e) = ctx.channel.finish_activity(&job.target, false).await {
        warn!("[{}] {} activity finish failed: {e}", job.thread, ctx.channel.id());
    }
}

/// Audits `reply_sent` and completes the row when the reply reached the
/// channel; reports a canonical-history failure otherwise.
fn report_delivery(
    ctx: &Ctx,
    job: &Job,
    delivery: Result<DeliveryOutcome>,
    reply: &str,
    completion: &str,
    deliver_action: &str,
) {
    match delivery {
        Ok(DeliveryOutcome::Delivered | DeliveryOutcome::AlreadyDelivered) => {
            audit(
                ctx,
                ctx.audit.reply_sent(
                    job.row_id,
                    &job.thread,
                    &job.target,
                    Some(job.backend),
                    reply,
                ),
            );
            complete_job(ctx, job, completion);
        }
        Err(error) => history_error(ctx, job, deliver_action, error),
    }
}

pub(super) async fn run_with_periodic_activity<O, A, AF>(
    operation: O,
    refresh: Duration,
    mut activity: A,
) -> O::Output
where
    O: Future,
    A: FnMut() -> AF,
    AF: Future<Output = ()>,
{
    tokio::pin!(operation);
    let mut ticker = tokio::time::interval(refresh);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            output = &mut operation => return output,
            _ = ticker.tick() => {
                let activity = activity();
                tokio::pin!(activity);
                tokio::select! {
                    output = &mut operation => return output,
                    _ = &mut activity => {}
                }
            }
        }
    }
}

/// Setup failures are terminal for the current row. Try to tell the user what
/// happened, then complete the row so one bad setup step cannot wedge ack state.
/// The persisted notification is retried in bounded batches until delivery or
/// shutdown.
async fn complete_setup_failure(ctx: &Ctx, job: &Job, reply: &str) {
    #[cfg(test)]
    ctx.setup_failure_replies
        .lock()
        .unwrap()
        .push(reply.to_string());

    match record_and_deliver(ctx, job, OutboundOrigin::Gateway, reply).await {
        Ok(DeliveryOutcome::Delivered | DeliveryOutcome::AlreadyDelivered) => {
            audit(
                ctx,
                ctx.audit.reply_sent(
                    job.row_id,
                    &job.thread,
                    &job.target,
                    Some(job.backend),
                    reply,
                ),
            );
        }
        Err(error) => {
            history_error(ctx, job, "record setup failure reply", error);
            return;
        }
    }
    audit(ctx, ctx.audit.completed(job.row_id, "setup_failed"));
    complete_row(&ctx.store, &ctx.ack, ctx.channel.id(), job.row_id);
}

fn complete_job(ctx: &Ctx, job: &Job, reason: &str) {
    audit(ctx, ctx.audit.completed(job.row_id, reason));
    complete_row(&ctx.store, &ctx.ack, ctx.channel.id(), job.row_id);
}

/// Handles gateway-level slash commands before anything reaches the agent.
fn command(ctx: &Ctx, job: &Job) -> Option<String> {
    let trimmed = job.text.trim();
    // Handle reminder commands before lowercasing so messages keep their case.
    if trimmed.eq_ignore_ascii_case("/reminders") {
        return Some(list_reminders(ctx, job));
    }
    if trimmed == "/reminder" || trimmed.starts_with("/reminder ") {
        return Some(reminder_command(ctx, job, trimmed["/reminder".len()..].trim()));
    }
    if trimmed == "/remind" || trimmed.starts_with("/remind ") {
        return Some(set_reminder(ctx, job, trimmed["/remind".len()..].trim()));
    }
    match trimmed.to_lowercase().as_str() {
        "/clear" | "/new" | "/reset" => match ctx.store.lock().unwrap().rotate(
            &job.thread,
            job.backend.as_str(),
            ctx.runners
                .get(&job.backend)
                .map(|r| r.initial_session_id())
                .unwrap_or_default(),
        ) {
            Ok(()) => Some("Started a fresh conversation.".to_string()),
            Err(_) => Some("Couldn't reset the conversation.".to_string()),
        },
        "/help" => Some(
            "Commands:\n/clear - start a fresh conversation\n/stop - stop the active request\n/jobs - list configured jobs\n/run <name> - run a job now\n/status - recent job runs\n/remind <when> <message> - remind you later (2h, 15:00, or daily 09:00)\n/reminders - list pending reminders\n/reminder cancel <id> - cancel a reminder\n/help - this message"
                .to_string(),
        ),
        _ => None,
    }
}

/// Splits a slash command into its name and trimmed argument, or None when the
/// text is not a command.
fn parse_slash(text: &str) -> Option<(&str, &str)> {
    let text = text.trim();
    if !text.starts_with('/') {
        return None;
    }
    Some(
        text.split_once(char::is_whitespace)
            .map_or((text, ""), |(name, arg)| (name, arg.trim())),
    )
}

/// Handles job-management chat commands that need async work or arguments.
async fn job_command(ctx: &Ctx, job: &Job) -> Option<String> {
    let (name, arg) = parse_slash(&job.text)?;
    match name.to_ascii_lowercase().as_str() {
        "/jobs" => Some(list_jobs(ctx)),
        "/run" => Some(run_job(ctx, arg).await),
        "/status" => Some(job_status(ctx)),
        _ => None,
    }
}

fn job_status(ctx: &Ctx) -> String {
    let ledger = match crate::jobs::Ledger::open(&ctx.cfg.database_path) {
        Ok(ledger) => ledger,
        Err(error) => return format!("Couldn't open run history: {error}"),
    };
    match ledger.runs(None) {
        Ok(runs) => format_status(&runs, now_ms()),
        Err(error) => format!("Couldn't read run history: {error}"),
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

fn format_status(runs: &[crate::jobs::RunRow], now_ms: i64) -> String {
    if runs.is_empty() {
        return "No job runs yet.".to_string();
    }
    let day_ago = now_ms - 24 * 60 * 60 * 1000;
    let (mut ok, mut failed) = (0, 0);
    for run in runs.iter().filter(|run| run.queued_at_ms >= day_ago) {
        match run.state.as_str() {
            "succeeded" => ok += 1,
            "failed" | "timed_out" => failed += 1,
            _ => {}
        }
    }
    let mut lines = vec![
        format!("Last 24h: {ok} ok, {failed} failed"),
        "Recent runs:".to_string(),
    ];
    for run in runs.iter().take(8) {
        lines.push(format!(
            "{} {} — {} ({})",
            state_icon(&run.state),
            run.job_name,
            run.state,
            relative_time(now_ms - run.queued_at_ms)
        ));
    }
    lines.join("\n")
}

fn state_icon(state: &str) -> &'static str {
    match state {
        "succeeded" => "✅",
        "failed" | "timed_out" => "⚠️",
        "running" => "⏳",
        "queued" => "•",
        _ => "◦",
    }
}

fn relative_time(ms_ago: i64) -> String {
    let seconds = ms_ago.max(0) / 1000;
    if seconds < 60 {
        format!("{seconds}s ago")
    } else if seconds < 3600 {
        format!("{}m ago", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h ago", seconds / 3600)
    } else {
        format!("{}d ago", seconds / 86_400)
    }
}

fn list_jobs(ctx: &Ctx) -> String {
    match crate::jobs::Catalog::load(&ctx.cfg) {
        Ok(catalog) => format_jobs(&catalog),
        Err(error) => format!("Couldn't load jobs: {error}"),
    }
}

fn format_jobs(catalog: &crate::jobs::Catalog) -> String {
    if catalog.jobs.is_empty() {
        return "No jobs are defined.".to_string();
    }
    let mut lines = vec!["Jobs:".to_string()];
    for (name, job) in &catalog.jobs {
        let triggers = if job.triggers.is_empty() {
            "manual only".to_string()
        } else {
            job.triggers
                .iter()
                .map(|trigger| {
                    let state = if trigger.enabled { "on" } else { "off" };
                    format!("{state} `{}`", trigger.schedule)
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        lines.push(format!("• {name} — {triggers}"));
    }
    lines.join("\n")
}

async fn run_job(ctx: &Ctx, name: &str) -> String {
    if name.is_empty() {
        return "Usage: /run <job-name>".to_string();
    }
    let job = match crate::jobs::Catalog::load_named(&ctx.cfg, name) {
        Ok(job) => job,
        Err(error) => return format!("Couldn't load job `{name}`: {error}"),
    };
    match crate::jobs::run_manual(&ctx.cfg, job).await {
        Ok((_, output)) => output,
        Err(error) => format!("Job `{name}` failed: {error}"),
    }
}

const REMIND_USAGE: &str = "Usage: /remind <when> <message>\n<when> is a duration (30m, 2h, 1d), a HH:MM time, or `daily`/`weekdays` HH:MM.";

fn set_reminder(ctx: &Ctx, job: &Job, args: &str) -> String {
    let Some((first, rest)) = args.split_once(char::is_whitespace) else {
        return REMIND_USAGE.to_string();
    };
    let first_lower = first.to_ascii_lowercase();
    let (recurrence, when, message) = if first_lower == "daily" || first_lower == "weekdays" {
        let Some((when, message)) = rest.trim().split_once(char::is_whitespace) else {
            return REMIND_USAGE.to_string();
        };
        (first_lower.as_str(), when.trim(), message.trim())
    } else {
        ("", first, rest.trim())
    };
    if message.is_empty() {
        return REMIND_USAGE.to_string();
    }
    if !recurrence.is_empty() && !when.contains(':') {
        return "Recurring reminders need a HH:MM time, e.g. /remind daily 09:00 <message>."
            .to_string();
    }
    let now = now_ms();
    let Some(fire_at) = parse_when(when, now) else {
        return format!(
            "Couldn't understand the time {when:?}. Use a duration like 2h or a HH:MM time."
        );
    };
    match ctx.history.lock().unwrap().insert_reminder(
        ctx.channel.id(),
        &job.target,
        &job.thread,
        message,
        fire_at,
        recurrence,
        now,
    ) {
        Ok(_) if recurrence.is_empty() => {
            format!("⏰ Reminder set for {}.", format_local(fire_at))
        }
        Ok(_) => format!(
            "⏰ Recurring reminder ({recurrence}) set, next at {}.",
            format_local(fire_at)
        ),
        Err(error) => format!("Couldn't save the reminder: {error}"),
    }
}

fn list_reminders(ctx: &Ctx, job: &Job) -> String {
    let pending = match ctx.history.lock().unwrap().pending_reminders(&job.thread) {
        Ok(pending) => pending,
        Err(error) => return format!("Couldn't read reminders: {error}"),
    };
    if pending.is_empty() {
        return "No pending reminders.".to_string();
    }
    let mut lines = vec!["Pending reminders:".to_string()];
    for reminder in pending {
        let recurrence = if reminder.recurrence.is_empty() {
            String::new()
        } else {
            format!(" [{}]", reminder.recurrence)
        };
        lines.push(format!(
            "#{} · {}{recurrence} · {}",
            reminder.id,
            format_local(reminder.fire_at_ms),
            reminder.message
        ));
    }
    lines.push("Cancel with /reminder cancel <id>.".to_string());
    lines.join("\n")
}

fn reminder_command(ctx: &Ctx, job: &Job, args: &str) -> String {
    let (verb, rest) = args
        .split_once(char::is_whitespace)
        .unwrap_or((args, ""));
    if !verb.eq_ignore_ascii_case("cancel") {
        return "Usage: /reminder cancel <id>".to_string();
    }
    let Ok(id) = rest.trim().parse::<i64>() else {
        return "Usage: /reminder cancel <id>".to_string();
    };
    match ctx.history.lock().unwrap().cancel_reminder(id, &job.thread) {
        Ok(true) => format!("Cancelled reminder #{id}."),
        Ok(false) => format!("No pending reminder #{id} in this chat."),
        Err(error) => format!("Couldn't cancel the reminder: {error}"),
    }
}

/// Computes the next fire time for a recurring reminder, or None for a one-off
/// or unknown recurrence.
pub(super) fn next_recurrence(fire_at_ms: i64, recurrence: &str) -> Option<i64> {
    use chrono::{Datelike, Duration, Local, TimeZone, Weekday};
    let current = Local.timestamp_millis_opt(fire_at_ms).single()?;
    let mut next = current + Duration::days(1);
    match recurrence {
        "daily" => Some(next.timestamp_millis()),
        "weekdays" => {
            while matches!(next.weekday(), Weekday::Sat | Weekday::Sun) {
                next += Duration::days(1);
            }
            Some(next.timestamp_millis())
        }
        _ => None,
    }
}

/// Resolves a reminder time from a duration (`30m`, `2h`, `1d`) or a `HH:MM`
/// local time (the next occurrence today or tomorrow).
fn parse_when(when: &str, now_ms: i64) -> Option<i64> {
    if let Ok(duration) = humantime::parse_duration(when) {
        let millis = i64::try_from(duration.as_millis()).ok()?;
        return Some(now_ms.saturating_add(millis));
    }
    let (hours, minutes) = when.split_once(':')?;
    let hours: u32 = hours.parse().ok()?;
    let minutes: u32 = minutes.parse().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    use chrono::{Local, TimeZone, Timelike};
    let now = Local.timestamp_millis_opt(now_ms).single()?;
    let mut target = now
        .with_hour(hours)?
        .with_minute(minutes)?
        .with_second(0)?
        .with_nanosecond(0)?;
    if target <= now {
        target += chrono::Duration::days(1);
    }
    Some(target.timestamp_millis())
}

fn format_local(ms: i64) -> String {
    use chrono::{Local, TimeZone};
    Local
        .timestamp_millis_opt(ms)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "the scheduled time".to_string())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeliveryOutcome {
    Delivered,
    AlreadyDelivered,
}

pub(super) async fn record_and_deliver(
    ctx: &Ctx,
    job: &Job,
    origin: OutboundOrigin,
    text: &str,
) -> Result<DeliveryOutcome> {
    let outbound = ctx.history.lock().unwrap().record_outbound(
        job.inbound_id,
        origin,
        Some(job.backend.as_str()),
        text,
    )?;
    deliver_stored(ctx, job, &outbound).await
}

/// Records an outbound reply and tries delivery once. Control-path replies use
/// this so a channel outage cannot block the polling loop indefinitely.
pub(super) async fn record_and_deliver_once(
    ctx: &Ctx,
    job: &Job,
    origin: OutboundOrigin,
    text: &str,
) -> Result<bool> {
    let mut outbound = ctx.history.lock().unwrap().record_outbound(
        job.inbound_id,
        origin,
        Some(job.backend.as_str()),
        text,
    )?;
    if outbound.status == DeliveryStatus::Delivered {
        return Ok(true);
    }
    let delivered = deliver_outbound_once(ctx, &job.target, &mut outbound).await?;
    ctx.history.lock().unwrap().mark_delivery(
        outbound.id,
        if delivered {
            DeliveryStatus::Delivered
        } else {
            DeliveryStatus::Failed
        },
    )?;
    Ok(delivered)
}

async fn deliver_stored(
    ctx: &Ctx,
    job: &Job,
    outbound: &OutboundMessage,
) -> Result<DeliveryOutcome> {
    if outbound.status == DeliveryStatus::Delivered {
        return Ok(DeliveryOutcome::AlreadyDelivered);
    }
    let mut outbound = outbound.clone();
    let semantics = ctx.channel.delivery_semantics();
    let mut attempt = 0;
    loop {
        attempt += 1;
        let delivered = deliver_outbound_once(ctx, &job.target, &mut outbound).await?;
        let status = if delivered {
            DeliveryStatus::Delivered
        } else {
            DeliveryStatus::Failed
        };
        ctx.history
            .lock()
            .unwrap()
            .mark_delivery(outbound.id, status)?;
        if delivered {
            deliver_voice_reply(ctx, job, &outbound.content).await;
            return Ok(DeliveryOutcome::Delivered);
        }
        audit(
            ctx,
            ctx.audit.reply_failed(
                job.row_id,
                &job.thread,
                &job.target,
                Some(job.backend),
                "stored outbound delivery attempt failed",
            ),
        );
        if attempt < semantics.retry_attempts {
            warn!(
                "delivery attempt {attempt}/{} failed; retrying stored outbound",
                semantics.retry_attempts
            );
            #[cfg(not(test))]
            tokio::time::sleep(semantics.retry_delay).await;
        } else {
            warn!("delivery attempts exhausted; stored outbound remains failed and will retry");
            attempt = 0;
            #[cfg(not(test))]
            tokio::time::sleep(semantics.exhausted_retry_delay).await;
        }
        #[cfg(test)]
        tokio::task::yield_now().await;
    }
}

async fn deliver_outbound_once(
    ctx: &Ctx,
    target: &str,
    outbound: &mut OutboundMessage,
) -> Result<bool> {
    let chunks = ctx
        .channel
        .outbound_chunks(&outbound.content, &ctx.reply_marker);
    if outbound.delivery_chunk_index > chunks.len() {
        anyhow::bail!(
            "outbound {} has invalid delivery chunk index {} for {} chunks",
            outbound.id,
            outbound.delivery_chunk_index,
            chunks.len()
        );
    }
    for (index, chunk) in chunks
        .iter()
        .enumerate()
        .skip(outbound.delivery_chunk_index)
    {
        if let Err(error) = super::send_reply_chunk(ctx, target, chunk).await {
            error!(
                "outbound {} chunk {index} send error to {target}: {error}",
                outbound.id
            );
            return Ok(false);
        }
        let next_chunk = index + 1;
        ctx.history
            .lock()
            .unwrap()
            .checkpoint_delivery(outbound.id, next_chunk)?;
        outbound.delivery_chunk_index = next_chunk;
    }
    Ok(true)
}

async fn deliver_voice_reply(ctx: &Ctx, job: &Job, text: &str) {
    if !job.reply_with_voice {
        return;
    }
    let Some(voice) = &ctx.voice else {
        return;
    };
    let clip = match voice.synthesize(text).await {
        Ok(clip) => clip,
        Err(error) => {
            warn!(
                "[{}] voice reply synthesis failed; text reply was delivered: {error:#}",
                job.thread
            );
            return;
        }
    };
    #[cfg(test)]
    {
        ctx.sent_voice_replies
            .lock()
            .unwrap()
            .push((job.target.clone(), clip.bytes));
    }
    #[cfg(not(test))]
    if let Err(error) = ctx.channel.send_voice(&job.target, &clip).await {
        warn!(
            "[{}] voice reply delivery failed; text reply was delivered: {error:#}",
            job.thread
        );
    }
}

fn history_error(ctx: &Ctx, job: &Job, action: &str, error: anyhow::Error) {
    error!(
        "[{}] canonical history {action} failed: {error}; refusing unrecorded delivery",
        job.thread
    );
    audit(
        ctx,
        ctx.audit.failed(
            "message_history_failed",
            job.row_id,
            &job.thread,
            Some(job.backend),
            format!("{action}: {error}"),
        ),
    );
}

/// Extracts a short, user-facing reason from an error message.
fn short(msg: &str) -> String {
    let s = msg.rsplit(": ").next().unwrap_or(msg).trim();
    if s.is_empty() {
        "couldn't reach the agent".to_string()
    } else {
        s.to_string()
    }
}

fn rehydration_prompt(ctx: &Ctx, job: &Job) -> Result<RehydrationPrompt> {
    let messages = ctx.history.lock().unwrap().recent_messages_before(
        ctx.channel.id(),
        &job.thread,
        job.inbound_id,
        rehydration::MAX_HISTORY_MESSAGES,
    )?;
    Ok(rehydration::compose(&messages, &job.text))
}

fn backend_request<'a>(
    session_id: &'a str,
    is_new: bool,
    work_dir: &'a str,
    instructions: &'a str,
    prompt: &'a str,
) -> Request<'a> {
    Request {
        session_id,
        is_new,
        work_dir,
        instructions,
        prompt,
    }
}

#[cfg(test)]
mod attach_tests {
    use super::*;

    #[test]
    fn parses_attach_markers_and_ignores_unclosed() {
        assert_eq!(
            parse_attach_markers("done [[attach: out.md]] and [[attach: dir/b.png ]]"),
            vec!["out.md".to_string(), "dir/b.png".to_string()]
        );
        assert_eq!(parse_attach_markers("no markers"), Vec::<String>::new());
        assert_eq!(parse_attach_markers("[[attach: unclosed"), Vec::<String>::new());
    }

    #[test]
    fn strips_attach_markers_from_reply_text() {
        assert_eq!(
            strip_attach_markers("Here you go. [[attach: out.md]]"),
            "Here you go."
        );
        assert_eq!(strip_attach_markers("[[attach: only.md]]"), "");
        assert_eq!(strip_attach_markers("plain text"), "plain text");
    }

    #[test]
    fn sanitize_filename_strips_paths_and_dot_names() {
        assert_eq!(sanitize_filename("../../etc/passwd").as_deref(), Some("passwd"));
        assert_eq!(sanitize_filename("dir/report.md").as_deref(), Some("report.md"));
        assert_eq!(sanitize_filename(".."), None);
        assert_eq!(sanitize_filename(""), None);
    }

    #[test]
    fn load_outbound_files_reads_within_workdir_and_rejects_escapes() {
        let dir = crate::test_support::temp_path("worker-outbox");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("out.md"), b"payload").unwrap();
        let work_dir = dir.to_str().unwrap();

        let files = load_outbound_files(work_dir, &["out.md".to_string()]);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].filename, "out.md");
        assert_eq!(files[0].bytes, b"payload");

        // A traversal path resolves outside the work dir and is rejected.
        assert!(load_outbound_files(work_dir, &["../out.md".to_string()]).is_empty());
        // A missing file is skipped.
        assert!(load_outbound_files(work_dir, &["nope.md".to_string()]).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod command_tests {
    use super::*;
    use crate::config::AgentBackend;
    use crate::jobs::{Catalog, Job, Trigger};
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn job(triggers: Vec<Trigger>) -> Job {
        Job {
            name: "sample".to_string(),
            path: PathBuf::new(),
            body: String::new(),
            timeout: Duration::from_secs(1),
            workdir: PathBuf::new(),
            backend: AgentBackend::Codex,
            snapshot_hash: String::new(),
            evals: Vec::new(),
            triggers,
            notify: crate::jobs::NotifyPolicy::Always,
        }
    }

    fn cron(schedule: &str, enabled: bool) -> Trigger {
        Trigger {
            id: "t".to_string(),
            kind: "cron".to_string(),
            schedule: schedule.to_string(),
            timezone: "UTC".to_string(),
            enabled,
        }
    }

    #[test]
    fn parse_slash_splits_name_and_argument() {
        assert_eq!(parse_slash("/jobs"), Some(("/jobs", "")));
        assert_eq!(parse_slash("  /run  cve-check  "), Some(("/run", "cve-check")));
        assert_eq!(parse_slash("/run a b"), Some(("/run", "a b")));
        assert_eq!(parse_slash("hello"), None);
        assert_eq!(parse_slash(""), None);
    }

    #[test]
    fn format_jobs_lists_names_and_trigger_state() {
        let mut jobs = BTreeMap::new();
        jobs.insert("alpha".to_string(), job(vec![cron("0 8 * * *", true)]));
        jobs.insert("beta".to_string(), job(Vec::new()));
        let catalog = Catalog {
            jobs,
            errors: Vec::new(),
        };

        let listing = format_jobs(&catalog);
        assert!(listing.contains("• alpha — on `0 8 * * *`"));
        assert!(listing.contains("• beta — manual only"));
    }

    #[test]
    fn format_jobs_handles_empty_catalog() {
        let catalog = Catalog {
            jobs: BTreeMap::new(),
            errors: Vec::new(),
        };
        assert_eq!(format_jobs(&catalog), "No jobs are defined.");
    }

    fn run_row(name: &str, state: &str, queued_at_ms: i64) -> crate::jobs::RunRow {
        crate::jobs::RunRow {
            id: "id".to_string(),
            job_name: name.to_string(),
            state: state.to_string(),
            backend: "codex".to_string(),
            queued_at_ms,
            result: None,
            error: None,
            evaluation_state: "not_requested".to_string(),
            evaluation_result: None,
            evaluation_error: None,
            trigger_kind: "cron".to_string(),
            trigger_id: None,
            scheduled_at_ms: None,
            delivery_state: "not_requested".to_string(),
            delivery_attempts: 0,
            delivery_error: None,
            delivery_channel: None,
            delivery_target: None,
        }
    }

    #[test]
    fn relative_time_scales_by_unit() {
        assert_eq!(relative_time(5_000), "5s ago");
        assert_eq!(relative_time(120_000), "2m ago");
        assert_eq!(relative_time(7_200_000), "2h ago");
        assert_eq!(relative_time(3 * 86_400_000), "3d ago");
        assert_eq!(relative_time(-1000), "0s ago");
    }

    #[test]
    fn format_status_summarizes_recent_runs() {
        let now = 100 * 86_400_000;
        let runs = [
            run_row("alpha", "succeeded", now - 60_000),
            run_row("beta", "failed", now - 3_600_000),
            run_row("gamma", "timed_out", now - 2 * 86_400_000), // outside 24h
        ];
        let status = format_status(&runs, now);
        // Only runs within 24h count toward the summary.
        assert!(status.contains("Last 24h: 1 ok, 1 failed"));
        assert!(status.contains("✅ alpha — succeeded (1m ago)"));
        assert!(status.contains("⚠️ beta — failed (1h ago)"));
        assert!(status.contains("gamma — timed_out (2d ago)"));
    }

    #[test]
    fn format_status_handles_no_runs() {
        assert_eq!(format_status(&[], 0), "No job runs yet.");
    }
}

#[cfg(test)]
mod reminder_tests {
    use super::{format_local, parse_when};

    #[test]
    fn parse_when_reads_durations() {
        assert_eq!(parse_when("2h", 1_000), Some(1_000 + 7_200_000));
        assert_eq!(parse_when("30m", 0), Some(1_800_000));
        assert_eq!(parse_when("1d", 0), Some(86_400_000));
    }

    #[test]
    fn parse_when_reads_hh_mm_within_next_day() {
        let now = 1_700_000_000_000;
        let fire = parse_when("15:30", now).unwrap();
        assert!(fire > now, "reminder time must be in the future");
        assert!(fire <= now + 86_400_000, "within the next 24 hours");
        // Seconds are zeroed, so the fire time lands on a whole minute.
        assert_eq!(fire % 60_000, 0);
    }

    #[test]
    fn parse_when_rejects_garbage_and_out_of_range() {
        assert_eq!(parse_when("later", 0), None);
        assert_eq!(parse_when("25:00", 0), None);
        assert_eq!(parse_when("12:60", 0), None);
    }

    #[test]
    fn format_local_renders_a_timestamp() {
        // Any valid epoch renders to a YYYY-MM-DD HH:MM string.
        let rendered = format_local(1_700_000_000_000);
        assert_eq!(rendered.len(), 16);
        assert_eq!(&rendered[4..5], "-");
    }

    #[test]
    fn next_recurrence_advances_daily_and_skips_weekends() {
        use super::next_recurrence;
        use chrono::{Datelike, Local, TimeZone, Weekday};

        let base = 1_700_000_000_000;
        assert_eq!(next_recurrence(base, "daily"), Some(base + 86_400_000));
        assert_eq!(next_recurrence(base, ""), None);
        assert_eq!(next_recurrence(base, "monthly"), None);

        let weekday_next = next_recurrence(base, "weekdays").unwrap();
        assert!(weekday_next >= base + 86_400_000);
        assert!(weekday_next <= base + 3 * 86_400_000);
        let day = Local.timestamp_millis_opt(weekday_next).single().unwrap().weekday();
        assert!(!matches!(day, Weekday::Sat | Weekday::Sun));
    }
}
