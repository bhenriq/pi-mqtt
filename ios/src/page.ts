/**
 * The MQTT app page.
 *
 * One self-contained HTML document: no libraries, no network. It renders whatever
 * snapshot the extension posts and sends actions back with pi.post(). The extension
 * owns all state, so the page is also correct right after a relaunch (app.state).
 */

/** Shape of `settings` in a snapshot; mirrored (not imported) to avoid a cycle. */
export interface SnapshotSettings {
	url: string;
	room: string;
	name: string;
	username: string;
	heartbeatMs: number;
	notify: boolean;
}

export interface Snapshot {
	status: "offline" | "connecting" | "online";
	room: string;
	url: string;
	me: string;
	hasPassword: boolean;
	settings: SnapshotSettings;
	workers: Array<{
		id: string;
		state: string;
		stale: boolean;
		/** Joined but not approved by this phone: not dispatchable. */
		pending?: boolean;
		/** Approved by this phone (its key is on file). */
		approved?: boolean;
		/** The six digits to compare with the worker's screen, while pending. */
		code?: string;
		fingerprint?: string;
		repo?: string;
		branch?: string;
		model?: string;
		cwd?: string;
		activeTaskId?: string;
		age: string;
	}>;
	orchestrators: Array<{ id: string; stale: boolean; age: string; self: boolean }>;
	tasks: Array<{
		taskId: string;
		to: string;
		state: string;
		age: string;
		detail: string;
		text?: string;
		duration?: string;
	}>;
	log: Array<{ at: number; level: string; message: string }>;
	error?: string;
	lastError?: string;
}


// ---------------------------------------------------------------------------
// The app page
// ---------------------------------------------------------------------------

export function appHtml(state: unknown): string {
	const initial = JSON.stringify(state ?? null).replace(/</g, "\\u003c");
	return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<style>
  :root {
    color-scheme: light dark;
    --bg: #f2f2f7; --card: #ffffff; --text: #111114; --muted: #6b6b73; --line: rgba(0,0,0,.08);
    --accent: #0a84ff; --good: #1fa971; --warn: #d98216; --bad: #d7263d; --chip: rgba(120,120,128,.14);
    --mono: ui-monospace, SFMono-Regular, Menlo, monospace;
  }
  @media (prefers-color-scheme: dark) {
    :root { --bg: #0e0e10; --card: #1c1c1f; --text: #f5f5f7; --muted: #9a9aa2; --line: rgba(255,255,255,.09); --chip: rgba(120,120,128,.24); }
  }
  * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
  html, body { margin: 0; background: var(--bg); color: var(--text); font: 15px/1.4 -apple-system, BlinkMacSystemFont, "SF Pro Text", system-ui, sans-serif; }
  body { padding: max(14px, env(safe-area-inset-top)) 16px max(28px, env(safe-area-inset-bottom)); }
  header { margin-bottom: 6px; }
  .hrow { display: flex; align-items: flex-start; justify-content: space-between; gap: 10px; }
  h1 { font-size: 26px; letter-spacing: -.02em; margin: 0; }
  .sub { color: var(--muted); font-size: 12.5px; margin-top: 2px; font-family: var(--mono); word-break: break-all; }
  .actions { display: flex; align-items: center; gap: 8px; }
  .pill { border: 0; border-radius: 999px; padding: 8px 14px; font-size: 13px; font-weight: 600; color: #fff; background: var(--accent); }
  .pill.off { background: var(--chip); color: var(--text); }
  .icon { border: 0; width: 34px; height: 34px; border-radius: 10px; background: var(--chip); color: var(--text); font-size: 17px; }
  .status { display: flex; align-items: center; gap: 7px; margin: 12px 0 18px; color: var(--muted); font-size: 13px; }
  .dot { width: 9px; height: 9px; border-radius: 50%; background: var(--muted); }
  .dot.online { background: var(--good); box-shadow: 0 0 0 4px rgba(31,169,113,.16); }
  .dot.connecting { background: var(--warn); }
  .dot.offline { background: #8e8e93; }
  section { margin-bottom: 22px; }
  .shead { display: flex; align-items: baseline; justify-content: space-between; margin: 0 2px 8px; }
  h2 { font-size: 13px; text-transform: uppercase; letter-spacing: .07em; color: var(--muted); margin: 0; }
  .count { font-size: 12px; color: var(--muted); }
  .list { display: flex; flex-direction: column; gap: 8px; }
  .card { background: var(--card); border-radius: 14px; padding: 12px 14px; border: 1px solid var(--line); }
  .row { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
  .wid { font-weight: 650; font-size: 15.5px; }
  .chip { font-size: 11.5px; font-weight: 650; padding: 3px 9px; border-radius: 999px; background: var(--chip); color: var(--muted); text-transform: lowercase; }
  .chip.idle { color: var(--good); background: rgba(31,169,113,.14); }
  .chip.busy { color: var(--warn); background: rgba(217,130,22,.16); }
  .chip.offline, .chip.offline\\? { color: #8e8e93; }
  .chip.done { color: var(--good); background: rgba(31,169,113,.14); }
  .chip.error, .chip.timeout, .chip.lost, .chip.cancelled { color: var(--bad); background: rgba(215,38,61,.14); }
  .chip.sent, .chip.running, .chip.queued { color: var(--accent); background: rgba(10,132,255,.14); }
  .chip.pending { color: var(--warn); background: rgba(217,130,22,.16); }
  .code { font-family: var(--mono); font-size: 19px; letter-spacing: .10em; font-weight: 600; color: var(--warn); margin-top: 4px; }
  .code .hint { font-family: -apple-system, system-ui, sans-serif; font-size: 11.5px; letter-spacing: 0; font-weight: 400; color: var(--muted); display: block; margin-top: 2px; }
  .meta { color: var(--muted); font-size: 12.5px; margin-top: 4px; }
  .meta.busyline { color: var(--warn); }
  .cwd { color: var(--muted); font-size: 11.5px; margin-top: 4px; opacity: .8; word-break: break-all; }
  .mono { font-family: var(--mono); }
  .cardfoot { margin-top: 10px; display: flex; gap: 8px; }
  .ghost { flex: 1; border: 1px solid var(--line); background: transparent; color: var(--accent); font-weight: 600; font-size: 13px; padding: 8px 10px; border-radius: 10px; }
  .ghost.danger { color: var(--bad); }
  .empty { background: var(--card); border: 1px dashed var(--line); color: var(--muted); border-radius: 14px; padding: 16px; font-size: 13px; text-align: center; }
  .task .detail { margin-top: 6px; color: var(--text); font-size: 13px; white-space: pre-wrap; word-break: break-word; }
  .task .detail.clamped { display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden; }
  .log { background: var(--card); border: 1px solid var(--line); border-radius: 14px; padding: 10px 12px; font-family: var(--mono); font-size: 11.5px; color: var(--muted); max-height: 150px; overflow: auto; }
  .log div { padding: 1px 0; }
  .log .warn { color: var(--warn); } .log .error { color: var(--bad); }
  .sheet { position: fixed; inset: 0; background: rgba(0,0,0,.34); display: flex; align-items: flex-end; z-index: 20; padding: 0; }
  .sheet.hidden { display: none; }
  .panel { width: 100%; background: var(--card); border-radius: 20px 20px 0 0; padding: 18px 16px max(18px, env(safe-area-inset-bottom)); }
  .panel h3 { margin: 0 0 2px; font-size: 18px; }
  .panel .hint { color: var(--muted); font-size: 12.5px; margin-bottom: 12px; }
  textarea, input[type=text] { width: 100%; border: 1px solid var(--line); background: var(--bg); color: var(--text); border-radius: 12px; padding: 11px 12px; font: inherit; font-size: 15px; resize: vertical; }
  textarea { min-height: 108px; }
  label.field { display: block; margin-bottom: 10px; }
  label.field span { display: block; font-size: 12px; color: var(--muted); margin-bottom: 4px; text-transform: uppercase; letter-spacing: .05em; }
  .panel .btns { display: flex; gap: 8px; margin-top: 12px; }
  .primary { flex: 1; border: 0; border-radius: 12px; padding: 12px; font-weight: 650; font-size: 15px; color: #fff; background: var(--accent); }
  .secondary { flex: 1; border: 1px solid var(--line); border-radius: 12px; padding: 12px; font-weight: 600; font-size: 15px; color: var(--text); background: transparent; }
  .switchrow { display: flex; align-items: center; justify-content: space-between; padding: 8px 0; }
  .toast { position: fixed; left: 50%; bottom: max(22px, env(safe-area-inset-bottom)); transform: translateX(-50%) translateY(20px); background: rgba(30,30,32,.94); color: #fff; font-size: 13px; padding: 10px 16px; border-radius: 999px; opacity: 0; transition: opacity .18s, transform .18s; pointer-events: none; max-width: 88%; text-align: center; }
  .toast.show { opacity: 1; transform: translateX(-50%) translateY(0); }
  input[type=checkbox] { width: 22px; height: 22px; accent-color: var(--accent); }
</style>
</head>
<body>
<header>
  <div class="hrow">
    <div>
      <h1>MQTT</h1>
      <div class="sub" id="sub"></div>
    </div>
    <div class="actions">
      <button class="pill" id="power">Connect</button>
      <button class="icon" id="gear" aria-label="Settings">&#9881;</button>
    </div>
  </div>
  <div class="status"><span class="dot" id="dot"></span><span id="statusText">offline</span></div>
</header>
<main>
  <section>
    <div class="shead"><h2>Workers</h2><span class="count" id="wcount"></span></div>
    <div class="list" id="workers"></div>
  </section>
  <section>
    <div class="shead"><h2>Tasks</h2><span class="count" id="tcount"></span></div>
    <div class="list" id="tasks"></div>
  </section>
  <section>
    <div class="shead"><h2>Log</h2></div>
    <div class="log" id="log"></div>
  </section>
</main>

<div class="sheet hidden" id="composer">
  <div class="panel">
    <h3 id="composerTitle">Send a task</h3>
    <div class="hint" id="composerHint">The worker runs this in its own pi session on its own machine.</div>
    <textarea id="prompt" placeholder="What should it do? Say what done looks like."></textarea>
    <div class="switchrow"><span>Start a turn here when the result lands</span><input type="checkbox" id="wake"></div>
    <div class="btns">
      <button class="secondary" id="cancelComposer">Cancel</button>
      <button class="primary" id="send">Send</button>
    </div>
  </div>
</div>

<div class="sheet hidden" id="settings">
  <div class="panel">
    <h3>Settings</h3>
    <div class="hint">Synced across your devices. The password lives in the Keychain, never in this page.</div>
    <label class="field"><span>Broker (WebSocket)</span><input type="text" id="fUrl" autocapitalize="off" autocorrect="off" spellcheck="false"></label>
    <label class="field"><span>Room</span><input type="text" id="fRoom" autocapitalize="off" autocorrect="off" spellcheck="false"></label>
    <label class="field"><span>This phone's name</span><input type="text" id="fName" autocapitalize="off" autocorrect="off" spellcheck="false"></label>
    <label class="field"><span>Broker user (orchestrator role)</span><input type="text" id="fUser" autocapitalize="off" autocorrect="off" spellcheck="false"></label>
    <div class="switchrow"><span>Notify me when a task finishes</span><input type="checkbox" id="fNotify"></div>
    <div class="btns">
      <button class="secondary" id="password">Set password</button>
      <button class="primary" id="save">Save</button>
    </div>
    <div class="btns"><button class="secondary" id="closeSettings">Close</button></div>
  </div>
</div>

<div class="toast" id="toast"></div>
<script id="initial" type="application/json">__INITIAL__</script>
<script>
(function () {
  var initial = null;
  try { initial = JSON.parse(document.getElementById("initial").textContent || "null"); } catch (e) {}
  var snap = (pi.state.get && pi.state.get()) || initial || { status: "offline", workers: [], tasks: [], log: [], settings: {} };
  var composerTo = null;

  function $(id) { return document.getElementById(id); }
  function esc(v) {
    return String(v == null ? "" : v).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }
  var toastTimer = null;
  function toast(text) {
    var el = $("toast");
    el.textContent = text;
    el.classList.add("show");
    if (toastTimer) clearTimeout(toastTimer);
    toastTimer = setTimeout(function () { el.classList.remove("show"); }, 2400);
  }

  function renderHeader() {
    var online = snap.status === "online";
    $("power").textContent = online ? "Disconnect" : "Connect";
    $("power").className = "pill" + (online ? "" : " off");
    var dot = $("dot");
    dot.className = "dot " + (snap.status || "offline");
    $("sub").textContent = (snap.me ? snap.me + " · " : "") + (snap.room || "") + " · " + (snap.url || "");
    var text = snap.status || "offline";
    if (snap.lastError && !online) text += " · last drop " + snap.lastError;
    else if (snap.hasPassword === false) text += " · no room password yet";
    $("statusText").textContent = text;
  }

  function renderWorkers() {
    var list = snap.workers || [];
    $("wcount").textContent = list.length ? String(list.length) : "";
    if (!list.length) {
      $("workers").innerHTML = '<div class="empty">No workers in this room yet.<br><span class="mono">pi --pi-mqtt-role worker --name lap-b</span></div>';
      return;
    }
    var html = "";
    list.forEach(function (w) {
      var state = w.stale ? w.state + "?" : w.state;
      var repo = w.repo ? w.repo + (w.branch ? "@" + w.branch : "") : "";
      var foot;
      if (w.pending) {
        foot = '<div class="cardfoot">'
          + (w.code ? '<button class="ghost" data-action="admit" data-worker="' + esc(w.id) + '" data-code="' + esc(w.code) + '">Approve</button>' : '')
          + '<button class="ghost danger" data-action="revoke" data-worker="' + esc(w.id) + '">Deny</button>'
          + '</div>';
      } else {
        foot = '<div class="cardfoot">'
          + '<button class="ghost" data-action="compose" data-worker="' + esc(w.id) + '">Send a task</button>'
          + (w.approved ? '<button class="ghost danger" data-action="revoke" data-worker="' + esc(w.id) + '">Revoke</button>' : '')
          + '</div>';
      }
      html += '<div class="card">'
        + '<div class="row"><span class="wid">' + esc(w.id) + '</span><span class="chip ' + esc(state) + '">' + esc(state) + '</span></div>'
        + '<div class="meta">' + esc([repo, w.model, w.age, w.fingerprint].filter(Boolean).join(" · ")) + '</div>'
        + (w.pending
          ? (w.approved
              ? '<div class="meta busyline">approved — it becomes idle on its next beat</div>'
              : (w.code
                  ? '<div class="code">' + esc(w.code.slice(0, 3) + " " + w.code.slice(3)) + '<span class="hint">Compare this with the code on the worker’s screen, then approve.</span></div>'
                  : '<div class="meta busyline">waiting for an orchestrator to offer a code…</div>'))
          : '')
        + (w.activeTaskId ? '<div class="meta busyline">working on ' + esc(w.activeTaskId) + '</div>' : '')
        + (w.cwd ? '<div class="cwd mono">' + esc(w.cwd) + '</div>' : '')
        + foot
        + '</div>';
    });
    $("workers").innerHTML = html;
  }

  function renderTasks() {
    var list = snap.tasks || [];
    $("tcount").textContent = list.length ? String(list.length) : "";
    if (!list.length) {
      $("tasks").innerHTML = '<div class="empty">Nothing dispatched from this phone yet. Tap a worker above.</div>';
      return;
    }
    var html = "";
    list.forEach(function (t) {
      var running = t.state === "queued" || t.state === "sent" || t.state === "running";
      html += '<div class="card task" data-task="' + esc(t.taskId) + '">'
        + '<div class="row"><span class="wid mono" style="font-size:12.5px">' + esc(t.taskId) + '</span><span class="chip ' + esc(t.state) + '">' + esc(t.state) + '</span></div>'
        + '<div class="meta">' + esc(t.to + " · " + t.age + (t.duration ? " · took " + t.duration : "")) + '</div>'
        + (t.detail ? '<div class="detail clamped">' + esc(t.detail) + '</div>' : '')
        + (running ? '<div class="cardfoot"><button class="ghost danger" data-action="cancel" data-task="' + esc(t.taskId) + '">Cancel</button></div>' : '')
        + '</div>';
    });
    $("tasks").innerHTML = html;
  }

  function renderLog() {
    var list = (snap.log || []).slice(-12);
    if (!list.length) { $("log").innerHTML = '<div>quiet</div>'; return; }
    $("log").innerHTML = list.map(function (line) {
      var t = new Date(line.at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
      return '<div class="' + esc(line.level) + '">' + esc(t) + "  " + esc(line.message) + '</div>';
    }).join("");
  }

  function renderSettings() {
    var s = snap.settings || {};
    if (document.activeElement && document.activeElement.tagName === "INPUT") return;
    $("fUrl").value = s.url || "";
    $("fRoom").value = s.room || "";
    $("fName").value = s.name || "";
    $("fUser").value = s.username || "";
    $("fNotify").checked = s.notify === true;
  }

  function render() {
    renderHeader();
    renderWorkers();
    renderTasks();
    renderLog();
    if ($("settings").classList.contains("hidden") === false) renderSettings();
  }

  function openComposer(worker) {
    composerTo = worker;
    $("composerTitle").textContent = "Send to " + worker;
    $("composerHint").textContent = "The worker runs this in its own pi session on its own machine.";
    $("prompt").value = "";
    $("wake").checked = false;
    $("composer").classList.remove("hidden");
    setTimeout(function () { $("prompt").focus(); }, 60);
  }
  function closeComposer() { $("composer").classList.add("hidden"); composerTo = null; }
  function openSettings() { renderSettings(); $("settings").classList.remove("hidden"); }
  function closeSettings() { $("settings").classList.add("hidden"); }

  $("power").addEventListener("click", function () {
    if (snap.status === "online") { pi.post({ type: "disconnect" }); toast("leaving the room"); }
    else { toast("connecting…"); pi.post({ type: "connect" }); }
  });
  $("gear").addEventListener("click", openSettings);
  $("cancelComposer").addEventListener("click", closeComposer);
  $("closeSettings").addEventListener("click", closeSettings);
  $("password").addEventListener("click", function () { pi.post({ type: "password" }); });
  $("save").addEventListener("click", function () {
    pi.post({
      type: "settings",
      settings: {
        url: $("fUrl").value.trim(),
        room: $("fRoom").value.trim(),
        name: $("fName").value.trim(),
        username: $("fUser").value.trim(),
        notify: $("fNotify").checked
      }
    });
    closeSettings();
    toast("saved");
  });
  $("send").addEventListener("click", function () {
    var prompt = $("prompt").value.trim();
    if (!prompt || !composerTo) return;
    pi.post({ type: "dispatch", to: composerTo, prompt: prompt, wake: $("wake").checked });
    toast("sent to " + composerTo);
    closeComposer();
  });
  $("composer").addEventListener("click", function (event) { if (event.target === $("composer")) closeComposer(); });
  $("settings").addEventListener("click", function (event) { if (event.target === $("settings")) closeSettings(); });

  document.addEventListener("click", function (event) {
    var target = event.target;
    var action = target.closest ? target.closest("[data-action]") : null;
    if (action) {
      event.stopPropagation();
      var kind = action.getAttribute("data-action");
      if (kind === "compose") openComposer(action.getAttribute("data-worker"));
      if (kind === "cancel") { pi.post({ type: "cancel", taskId: action.getAttribute("data-task") }); toast("cancel requested"); }
      if (kind === "admit") {
        pi.post({ type: "admit", worker: action.getAttribute("data-worker"), code: action.getAttribute("data-code") });
        toast("approving…");
      }
      if (kind === "revoke") {
        pi.post({ type: "revoke", worker: action.getAttribute("data-worker") });
        toast("revoking…");
      }
      return;
    }
    var card = target.closest ? target.closest("[data-task]") : null;
    if (card) {
      var detail = card.querySelector(".detail");
      if (detail) detail.classList.toggle("clamped");
    }
  });

  pi.onMessage(function (message) {
    if (!message) return;
    if (message.type === "snapshot" && message.snapshot) {
      snap = message.snapshot;
      if (pi.state.set) pi.state.set(snap);
      render();
      return;
    }
    if (message.type === "toast") toast(message.text || "");
  });

  render();
})();
</script>
</body>
</html>`.replace("__INITIAL__", () => initial);
}

async function openApp(): Promise<void> {
	if (!appHandle) {
		appHandle = await api.apps.open("mqtt");
		return;
	}
	try {
		appHandle.show();
	} catch {
		appHandle = await api.apps.open("mqtt");
	}
}

function registerApp(): void {
	api.registerApp({
		name: "mqtt",
		title: "MQTT",
		icon: "antenna.radiowaves.left.and.right",
		description: "Workers on the MQTT room, and a place to send them tasks from the phone",
		render: (app: any) => appHtml(app?.state ?? snapshot()),
		onOpen: (app: any) => {
			appHandle = app;
			if (app?.ui) lastCtx = { ui: app.ui };
			pushNow();
			// Opening the app is a request to use the room — but only join silently if the
			// password is already in the Keychain. A dialog on open would be a surprise.
			void readPassword().then((password) => {
				passwordKnown = Boolean(password);
				pushNow();
				if (!password) return;
				ensureConnected({ ui: app?.ui }).catch((error: Error) => {
					try {
						app?.ui?.notify(`mqtt: ${error.message}`, "error");
					} catch {
						// ignore
					}
				});
			});
		},
		onMessage: (data: any, app: any) => {
			if (app?.ui) lastCtx = { ui: app.ui };
			const kind = data?.type;
			if (kind === "connect") {
				ensureConnected({ ui: app?.ui })
					.then(() => app?.post({ type: "toast", text: "joined " + settings.room }))
					.catch((error: Error) => app?.post({ type: "toast", text: error.message }));
				return;
			}
			if (kind === "disconnect") {
				void disconnectRoom("left the room (from the app)").then(() => app?.post({ type: "toast", text: "disconnected" }));
				return;
			}
			if (kind === "dispatch") {
				void (async () => {
					try {
						await ensureConnected({ ui: app?.ui });
						const outcome = room!.dispatch({
							to: String(data.to),
							prompt: String(data.prompt),
							notify: data.wake ? "wake" : "silent",
						});
						app?.post({ type: "toast", text: outcome.ok ? `sent to ${data.to}` : outcome.error });
					} catch (error) {
						app?.post({ type: "toast", text: (error as Error).message });
					}
				})();
				return;
			}
			if (kind === "cancel") {
				const outcome = room?.cancel(String(data.taskId));
				app?.post({ type: "toast", text: outcome?.ok ? "cancel requested" : outcome?.error ?? "not connected" });
				return;
			}
			if (kind === "settings") {
				void applySettings({
					url: String(data.settings?.url ?? settings.url),
					room: String(data.settings?.room ?? settings.room),
					name: String(data.settings?.name ?? settings.name),
					username: String(data.settings?.username ?? settings.username),
					notify: data.settings?.notify === true,
				}).then(() => app?.post({ type: "toast", text: "saved" }));
				return;
			}
			if (kind === "password") {
				void (async () => {
					const entered = await app?.ui?.input("MQTT room password", "shared by every session in the room");
					if (entered && String(entered).trim()) {
						await storePassword(String(entered));
						app?.post({ type: "toast", text: "password saved" });
						pushNow();
					}
				})();
				return;
			}
			if (kind === "refresh") {
				pushNow();
			}
		},
	});
}
