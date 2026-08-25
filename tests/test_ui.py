from __future__ import annotations

import importlib
import json
import socket
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import pytest


def test_workspace_contains_generic_panels_and_controls():
    template = Path("copilot_hub/templates/terminal.html").read_text(encoding="utf-8")
    for label in (
        "Sessions",
        "Active work",
        "Results",
        "Memory",
        "Handoffs",
        "Usage",
        "Copy selection",
        "Reconnect",
        "Retire worker",
    ):
        assert label in template
    assert "/api/agents/state?compact=true" in template
    assert "/api/agents/results" in template
    assert "/ws/agents/terminal/" in template


def test_terminal_browser_contract_preserves_replay_drafts_and_selection():
    template = Path("copilot_hub/templates/terminal.html").read_text(encoding="utf-8")
    for contract in (
        "client_id",
        "stream_id",
        "offset",
        "terminal_replay_end",
        "input_ack",
        "pending",
        "bracketedPaste",
        "prompt_submit",
        "prompt_cancel",
        "promptKnown",
        "terminalSelectionText",
        "writeClipboardText",
        "scrollToBottom",
        "ResizeObserver",
        "macOptionClickForcesSelection",
    ):
        assert contract in template
    assert "copilot-hub.workspace.v1" in template
    assert "copilot-hub.terminal-debug" in template
    assert "window.__copilotHubTerminalDebug" in template


def test_terminal_drag_click_and_deferred_output_contracts_are_present():
    template = Path("copilot_hub/templates/terminal.html").read_text(encoding="utf-8")
    for contract in (
        "selectionService.shouldForceSelection",
        "event.button === 0",
        "terminalSelectionDragThreshold = 6",
        "movedBeyondThreshold",
        "forwardTerminalClick",
        r"`\x1b[<${button};${column};${row}M`",
        r"`\x1b[<${button};${column};${row}m`",
        "terminalSelectionEdgeActivation",
        "selectionAutoScrollWriting",
        "extendedSelectionFragments",
        "selectionNavigationFragments",
        "restoreExactTerminalSelection",
        "mergeTerminalSelectionText",
        "Promise.race([hold.promise, hold.bypassPromise])",
        "pendingWriteBytes",
        "copyTerminalSelection(state)",
    ):
        assert contract in template


def test_responsive_workspace_keeps_terminal_available():
    style = Path("copilot_hub/static/style.css").read_text(encoding="utf-8")
    assert "grid-template-columns" in style
    assert "@media (max-width: 1200px)" in style
    assert "@media (max-width: 820px)" in style
    assert ".terminal-buffer[hidden]" in style


def load_app(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("COPILOT_HUB_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("COPILOT_HUB_CWD", str(tmp_path / "work"))
    monkeypatch.setenv("COPILOT_HUB_TERMINALS", "0")
    sys.modules.pop("copilot_hub.app", None)
    app_module = importlib.import_module("copilot_hub.app")
    app_module.repository.initialize()
    return app_module


def browser_dependencies():
    try:
        server_module = importlib.import_module("uvicorn")
        browser_api = importlib.import_module("playwright.sync_api")
    except ImportError:
        pytest.skip("UI browser dependencies are not installed")
    return server_module, browser_api


@contextmanager
def serve_app(app, server_module):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = server_module.Server(
        server_module.Config(
            app,
            host="127.0.0.1",
            port=port,
            log_level="error",
            lifespan="off",
        )
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.02)
    if not server.started:
        server.should_exit = True
        thread.join(timeout=2)
        raise RuntimeError("UI test server did not start")
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=5)


def launch_browser(playwright, browser_error):
    for channel in ("chrome", "msedge"):
        try:
            return playwright.chromium.launch(channel=channel, headless=True)
        except browser_error:
            continue
    try:
        return playwright.chromium.launch(headless=True)
    except browser_error:
        pytest.skip("Chrome, Edge, or Playwright Chromium is required")


def install_fake_websocket(page, terminal_buffers):
    script = """
        (() => {
          const encoder = new TextEncoder();
          const configuredBuffers = __TERMINAL_BUFFERS__;
          const serverBuffers = new Map(
            Object.entries(configuredBuffers).map(([workerId, text]) => [
              workerId,
              {
                streamId: `stream-${workerId}`,
                bytes: encoder.encode(text),
              },
            ]),
          );
          const appendBytes = (left, right) => {
            const combined = new Uint8Array(left.length + right.length);
            combined.set(left);
            combined.set(right, left.length);
            return combined;
          };
          window.__hubSocketSends = [];
          window.__hubSockets = [];
          class FakeWebSocket {
            static CONNECTING = 0;
            static OPEN = 1;
            static CLOSING = 2;
            static CLOSED = 3;

            constructor(url) {
              this.url = url;
              this.readyState = FakeWebSocket.CONNECTING;
              this.listeners = new Map();
              const parsed = new URL(url);
              this.workerId = decodeURIComponent(
                parsed.pathname.split("/").at(-1),
              );
              this.requestedStream = parsed.searchParams.get("stream_id");
              this.requestedOffset = Number(
                parsed.searchParams.get("offset") || 0,
              );
              this.sent = [];
              window.__hubSockets.push(this);
              queueMicrotask(() => {
                if (this.readyState !== FakeWebSocket.CONNECTING) return;
                this.readyState = FakeWebSocket.OPEN;
                this.emit("open", {});
                this.replay();
              });
            }

            replay() {
              if (this.readyState !== FakeWebSocket.OPEN) return;
              const buffer = serverBuffers.get(this.workerId);
              const canResume = (
                this.requestedStream === buffer.streamId
                && this.requestedOffset >= 0
                && this.requestedOffset <= buffer.bytes.length
              );
              const offset = canResume ? this.requestedOffset : 0;
              this.emit("message", {
                data: JSON.stringify({
                  type: "terminal_stream",
                  stream_id: buffer.streamId,
                  reset: !canResume,
                  base_offset: 0,
                  offset,
                  replay_end: buffer.bytes.length,
                }),
              });
              if (offset < buffer.bytes.length) {
                this.emit("message", {
                  data: buffer.bytes.slice(offset).buffer,
                });
              }
              this.emit("message", {
                data: JSON.stringify({
                  type: "terminal_replay_end",
                  stream_id: buffer.streamId,
                  offset: buffer.bytes.length,
                }),
              });
            }

            addEventListener(type, callback) {
              const callbacks = this.listeners.get(type) || [];
              callbacks.push(callback);
              this.listeners.set(type, callbacks);
            }

            emit(type, event) {
              for (const callback of this.listeners.get(type) || []) {
                callback.call(this, event);
              }
            }

            send(data) {
              this.sent.push(data);
              window.__hubSocketSends.push({url: this.url, data});
              const payload = JSON.parse(data);
              if (payload.type === "input") {
                queueMicrotask(() => this.emit("message", {
                  data: JSON.stringify({
                    type: "input_ack",
                    worker_id: this.workerId,
                    sequence: payload.sequence,
                  }),
                }));
              }
            }

            close(code = 1000) {
              if (this.readyState >= FakeWebSocket.CLOSING) return;
              this.readyState = FakeWebSocket.CLOSING;
              this.readyState = FakeWebSocket.CLOSED;
              this.emit("close", {code});
            }

            emitLive(text) {
              const buffer = serverBuffers.get(this.workerId);
              const bytes = encoder.encode(text);
              buffer.bytes = appendBytes(buffer.bytes, bytes);
              if (this.readyState === FakeWebSocket.OPEN) {
                this.emit("message", {data: bytes.buffer});
              }
            }
          }
          window.__hubLatestSocket = workerId => (
            Array.from(window.__hubSockets)
              .reverse()
              .find(candidate => candidate.workerId === workerId)
          );
          window.WebSocket = FakeWebSocket;
        })();
    """
    page.add_init_script(
        script.replace("__TERMINAL_BUFFERS__", json.dumps(terminal_buffers))
    )


def terminal_input_messages(page, worker_id):
    return page.evaluate(
        """workerId => window.__hubLatestSocket(workerId).sent
          .map(data => JSON.parse(data))
          .filter(message => message.type === "input")
          .map(message => message.data)""",
        worker_id,
    )


def selection_coordinates(page, text):
    page.wait_for_function(
        """expected => Array.from(document.querySelectorAll(
          ".terminal-buffer:not([hidden]) .xterm-rows > div",
        )).some(row => row.textContent.includes(expected))""",
        arg=text,
    )
    return page.evaluate(
        """expected => {
          const active = document.querySelector(
            ".terminal-buffer:not([hidden])",
          );
          const screen = active?.querySelector(".xterm-screen");
          const row = Array.from(
            active?.querySelectorAll(".xterm-rows > div") || [],
          ).find(candidate => candidate.textContent.includes(expected));
          const snapshot = window.__copilotHubTerminalDebug.activeSnapshot();
          if (!screen || !row || !snapshot?.cols) return null;
          const screenRect = screen.getBoundingClientRect();
          const rowRect = row.getBoundingClientRect();
          const startColumn = row.textContent.indexOf(expected);
          const cellWidth = screenRect.width / snapshot.cols;
          return {
            startX: screenRect.left + (startColumn + .25) * cellWidth,
            endX:
              screenRect.left
              + (startColumn + expected.length - .25) * cellWidth,
            y: rowRect.top + rowRect.height / 2,
            edgeX: screenRect.left + screenRect.width * .65,
            topEdgeY: screenRect.top + 2,
            bottomEdgeY: screenRect.bottom - 2,
          };
        }""",
        text,
    )


def drag_terminal_selection(page, text):
    coordinates = selection_coordinates(page, text)
    assert coordinates is not None
    page.mouse.move(coordinates["startX"], coordinates["y"])
    page.mouse.down()
    page.mouse.move(
        coordinates["endX"],
        coordinates["y"],
        steps=max(4, len(text)),
    )
    page.mouse.up()
    page.wait_for_function(
        """expected => (
          window.__copilotHubTerminalDebug.activeSnapshot()?.selection
            === expected
        )""",
        arg=text,
    )
    return coordinates


def tui_page(label, target="", *, target_row=10):
    lines = [f"{label}-LINE-{index:02d}" for index in range(28)]
    if target:
        lines[target_row] = target
    return "\x1b[2J\x1b[H" + "\r\n".join(lines)


def test_terminal_selection_copy_and_atomic_click(tmp_path, monkeypatch):
    server_module, browser_api = browser_dependencies()
    app_module = load_app(tmp_path, monkeypatch)
    manager = app_module.agent_runtime.ensure_manager()
    mouse_mode = "\x1b[?1002h\x1b[?1006h"
    copy_target = "COPY-THIS-TERMINAL-TEXT"
    initial = f"{mouse_mode}{copy_target}\r\nREADY\r\n❯ existing browser draft"

    with (
        serve_app(app_module.app, server_module) as base_url,
        browser_api.sync_playwright() as playwright,
    ):
        browser = launch_browser(playwright, browser_api.Error)
        try:
            context = browser.new_context(
                viewport={"width": 1400, "height": 850},
                permissions=["clipboard-read", "clipboard-write"],
            )
            page = context.new_page()
            install_fake_websocket(page, {manager["id"]: initial})
            page.goto(
                f"{base_url}/?worker={manager['id']}",
                wait_until="load",
            )
            page.wait_for_function(
                """expected => (
                  window.__copilotHubTerminalDebug.activeText()
                    .includes(expected)
                  && window.__copilotHubTerminalDebug.activeSnapshot()
                    ?.mouseEventsActive
                )""",
                arg=copy_target,
            )

            drag_terminal_selection(page, copy_target)
            assert terminal_input_messages(page, manager["id"]) == []
            deferred_text = "OUTPUT-BUFFERED-WHILE-SELECTED"
            page.evaluate(
                """({workerId, text}) => (
                  window.__hubLatestSocket(workerId).emitLive(`\\r\\n${text}`)
                )""",
                {"workerId": manager["id"], "text": deferred_text},
            )
            page.wait_for_function(
                """() => (
                  window.__copilotHubTerminalDebug.activeSnapshot()
                    ?.pendingWriteBytes > 0
                )"""
            )
            assert deferred_text not in page.evaluate(
                "window.__copilotHubTerminalDebug.activeText()"
            )
            page.keyboard.press("Control+C")
            page.wait_for_function(
                """expected => navigator.clipboard.readText()
                  .then(value => value === expected)""",
                arg=copy_target,
            )
            page.wait_for_function(
                """expected => {
                  const snapshot =
                    window.__copilotHubTerminalDebug.activeSnapshot();
                  return window.__copilotHubTerminalDebug.activeText()
                      .includes(expected)
                    && !snapshot?.selectionHeld
                    && snapshot?.pendingWriteBytes === 0;
                }""",
                arg=deferred_text,
            )

            composer = selection_coordinates(page, "existing browser draft")
            assert composer is not None
            page.keyboard.type("typed draft")
            prior_count = len(terminal_input_messages(page, manager["id"]))
            page.mouse.move(composer["startX"], composer["y"])
            page.mouse.down()
            page.mouse.move(composer["startX"] + 2, composer["y"] + 1)
            page.mouse.up()
            page.wait_for_function(
                """({workerId, priorCount}) => (
                  window.__hubLatestSocket(workerId).sent
                    .map(data => JSON.parse(data))
                    .filter(message => message.type === "input").length
                    === priorCount + 1
                )""",
                arg={"workerId": manager["id"], "priorCount": prior_count},
            )
            click_input = terminal_input_messages(page, manager["id"])[prior_count:]
            assert len(click_input) == 1
            assert click_input[0].startswith("\x1b[<0;")
            assert "M\x1b[<0;" in click_input[0]
            assert click_input[0].endswith("m")
            snapshot = page.evaluate(
                "window.__copilotHubTerminalDebug.activeSnapshot()"
            )
            assert snapshot["promptDirty"] is True
            assert snapshot["promptKnown"] is False
            assert snapshot["hasSelection"] is False

            prior_count = len(terminal_input_messages(page, manager["id"]))
            page.mouse.move(composer["startX"], composer["y"])
            page.mouse.down()
            page.mouse.move(composer["endX"], composer["y"], steps=8)
            page.mouse.move(composer["startX"] + 1, composer["y"], steps=8)
            page.mouse.up()
            page.wait_for_timeout(100)
            assert len(terminal_input_messages(page, manager["id"])) == prior_count
            context.close()
        finally:
            browser.close()


def test_terminal_cross_page_selection_wheel_navigation_and_copy(
    tmp_path,
    monkeypatch,
):
    server_module, browser_api = browser_dependencies()
    app_module = load_app(tmp_path, monkeypatch)
    manager = app_module.agent_runtime.ensure_manager()
    first_target = "FIRST-PAGE-COPY-TARGET"
    second_target = "SECOND-PAGE-COPY-TARGET"
    first_page = tui_page("FIRST", first_target)
    second_page = tui_page("SECOND", second_target, target_row=3)
    initial = "\x1b[?1002h\x1b[?1006h\x1b[?1049h" + first_page

    with (
        serve_app(app_module.app, server_module) as base_url,
        browser_api.sync_playwright() as playwright,
    ):
        browser = launch_browser(playwright, browser_api.Error)
        try:
            context = browser.new_context(
                viewport={"width": 1400, "height": 850},
                permissions=["clipboard-read", "clipboard-write"],
            )
            page = context.new_page()
            install_fake_websocket(page, {manager["id"]: initial})
            page.goto(
                f"{base_url}/?worker={manager['id']}",
                wait_until="load",
            )
            page.wait_for_function(
                "window.__copilotHubTerminalDebug.activeSnapshot()?.mouseEventsActive"
            )
            coordinates = selection_coordinates(page, first_target)
            assert coordinates is not None
            page.mouse.move(coordinates["startX"], coordinates["y"])
            page.mouse.down()
            page.mouse.move(
                coordinates["edgeX"],
                coordinates["bottomEdgeY"],
                steps=10,
            )
            page.wait_for_function(
                """workerId => window.__hubLatestSocket(workerId).sent
                  .map(data => JSON.parse(data))
                  .some(message => (
                    message.type === "input"
                    && message.data.startsWith("\\u001b[<65;")
                  ))""",
                arg=manager["id"],
            )
            page.evaluate(
                """({workerId, text}) => (
                  window.__hubLatestSocket(workerId).emitLive(text)
                )""",
                {"workerId": manager["id"], "text": second_page},
            )
            page.wait_for_function(
                """({first, second}) => {
                  const snapshot =
                    window.__copilotHubTerminalDebug.activeSnapshot();
                  return window.__copilotHubTerminalDebug.activeText()
                      .includes(second)
                    && snapshot?.extendedSelection.includes(first)
                    && snapshot.extendedSelection.includes(second)
                    && snapshot.pendingWriteBytes === 0;
                }""",
                arg={"first": first_target, "second": second_target},
            )
            page.mouse.up()
            page.wait_for_function(
                """() => {
                  const snapshot =
                    window.__copilotHubTerminalDebug.activeSnapshot();
                  return snapshot?.selectionHeld
                    && !snapshot.selectionAutoScrolling;
                }"""
            )
            logical_selection = page.evaluate(
                "window.__copilotHubTerminalDebug.activeSnapshot().extendedSelection"
            )
            assert first_target in logical_selection
            assert second_target in logical_selection

            screen = page.locator(".terminal-buffer:not([hidden]) .xterm-screen")
            bounds = screen.bounding_box()
            assert bounds is not None
            page.mouse.move(
                bounds["x"] + bounds["width"] * 0.6,
                bounds["y"] + bounds["height"] * 0.5,
            )
            page.mouse.wheel(0, -120)
            page.wait_for_function(
                """workerId => window.__hubLatestSocket(workerId).sent
                  .map(data => JSON.parse(data))
                  .some(message => (
                    message.type === "input"
                    && message.data.startsWith("\\u001b[<64;")
                  ))""",
                arg=manager["id"],
            )
            page.evaluate(
                """({workerId, text}) => (
                  window.__hubLatestSocket(workerId).emitLive(text)
                )""",
                {"workerId": manager["id"], "text": first_page},
            )
            page.wait_for_function(
                """expected => {
                  const snapshot =
                    window.__copilotHubTerminalDebug.activeSnapshot();
                  return window.__copilotHubTerminalDebug.activeText()
                      .includes("FIRST-PAGE-COPY-TARGET")
                    && snapshot?.navigationSelection === expected
                    && snapshot.selectionHeld;
                }""",
                arg=logical_selection,
            )
            page.mouse.wheel(0, 120)
            page.wait_for_function(
                """workerId => window.__hubLatestSocket(workerId).sent
                  .map(data => JSON.parse(data))
                  .filter(message => (
                    message.type === "input"
                    && message.data.startsWith("\\u001b[<65;")
                  )).length >= 2""",
                arg=manager["id"],
            )
            page.evaluate(
                """({workerId, text}) => (
                  window.__hubLatestSocket(workerId).emitLive(text)
                )""",
                {"workerId": manager["id"], "text": second_page},
            )
            page.wait_for_function(
                """expected => {
                  const snapshot =
                    window.__copilotHubTerminalDebug.activeSnapshot();
                  return window.__copilotHubTerminalDebug.activeText()
                      .includes("SECOND-PAGE-COPY-TARGET")
                    && snapshot?.navigationSelection === expected
                    && snapshot.selectionHeld;
                }""",
                arg=logical_selection,
            )

            page.locator("#copy-terminal-selection").click()
            page.wait_for_function(
                """({first, second}) => navigator.clipboard.readText()
                  .then(value => (
                    value.includes(first) && value.includes(second)
                  ))""",
                arg={"first": first_target, "second": second_target},
            )
            copied = page.evaluate("navigator.clipboard.readText()")
            assert copied.index(first_target) < copied.index(second_target)
            context.close()
        finally:
            browser.close()


def test_terminal_edge_drag_return_restores_exact_smaller_selection(
    tmp_path,
    monkeypatch,
):
    server_module, browser_api = browser_dependencies()
    app_module = load_app(tmp_path, monkeypatch)
    manager = app_module.agent_runtime.ensure_manager()
    selected_text = "RETURN-TO-ONLY-THIS-TEXT"
    current_page = tui_page("CURRENT", selected_text)
    previous_page = tui_page("PREVIOUS")
    initial = "\x1b[?1002h\x1b[?1006h\x1b[?1049h" + current_page

    with (
        serve_app(app_module.app, server_module) as base_url,
        browser_api.sync_playwright() as playwright,
    ):
        browser = launch_browser(playwright, browser_api.Error)
        try:
            context = browser.new_context(
                viewport={"width": 1400, "height": 850},
                permissions=["clipboard-read", "clipboard-write"],
            )
            page = context.new_page()
            install_fake_websocket(page, {manager["id"]: initial})
            page.goto(
                f"{base_url}/?worker={manager['id']}",
                wait_until="load",
            )
            page.wait_for_function(
                "window.__copilotHubTerminalDebug.activeSnapshot()?.mouseEventsActive"
            )
            coordinates = selection_coordinates(page, selected_text)
            assert coordinates is not None
            page.mouse.move(coordinates["startX"], coordinates["y"])
            page.mouse.down()
            page.mouse.move(
                coordinates["endX"],
                coordinates["y"],
                steps=len(selected_text),
            )
            page.mouse.move(
                coordinates["edgeX"],
                coordinates["topEdgeY"],
                steps=10,
            )
            page.wait_for_function(
                """workerId => window.__hubLatestSocket(workerId).sent
                  .map(data => JSON.parse(data))
                  .some(message => (
                    message.type === "input"
                    && message.data.startsWith("\\u001b[<64;")
                  ))""",
                arg=manager["id"],
            )
            page.evaluate(
                """({workerId, text}) => (
                  window.__hubLatestSocket(workerId).emitLive(text)
                )""",
                {"workerId": manager["id"], "text": previous_page},
            )
            page.wait_for_function(
                "() => window.__copilotHubTerminalDebug.activeText()"
                '.includes("PREVIOUS-LINE-10")'
            )
            page.mouse.move(
                coordinates["edgeX"],
                coordinates["bottomEdgeY"],
                steps=10,
            )
            page.wait_for_function(
                """workerId => window.__hubLatestSocket(workerId).sent
                  .map(data => JSON.parse(data))
                  .some(message => (
                    message.type === "input"
                    && message.data.startsWith("\\u001b[<65;")
                  ))""",
                arg=manager["id"],
            )
            page.evaluate(
                """({workerId, text}) => (
                  window.__hubLatestSocket(workerId).emitLive(text)
                )""",
                {"workerId": manager["id"], "text": current_page},
            )
            page.wait_for_function(
                """expected => window.__copilotHubTerminalDebug.activeText()
                  .includes(expected)""",
                arg=selected_text,
            )
            page.mouse.move(
                coordinates["endX"],
                coordinates["y"],
                steps=10,
            )
            page.wait_for_function(
                """expected => (
                  window.__copilotHubTerminalDebug.activeSnapshot()
                    ?.selection === expected
                )""",
                arg=selected_text,
            )
            page.mouse.up()
            page.wait_for_function(
                """expected => {
                  const snapshot =
                    window.__copilotHubTerminalDebug.activeSnapshot();
                  return snapshot?.selection === expected
                    && snapshot.extendedSelection === expected
                    && snapshot.cachedSelection === expected
                    && snapshot.selectionHeld;
                }""",
                arg=selected_text,
            )
            page.locator("#copy-terminal-selection").click()
            page.wait_for_function(
                """expected => navigator.clipboard.readText()
                  .then(value => value === expected)""",
                arg=selected_text,
            )
            context.close()
        finally:
            browser.close()
