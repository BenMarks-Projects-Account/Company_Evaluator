"""BenTrade Company Evaluator — GUI Launcher (Dashboard)."""

import multiprocessing
import os
import sys
import subprocess
import threading
import time
import tkinter as tk
from tkinter import font as tkfont
from pathlib import Path
import urllib.request
import urllib.error
import atexit
import json

# ── Frozen-exe detection ─────────────────────────────────────
IS_FROZEN = getattr(sys, "frozen", False)

if IS_FROZEN:
    _exe_dir = Path(sys.executable).resolve().parent
    _candidate = _exe_dir
    for _ in range(3):
        if (_candidate / "main.py").exists():
            break
        _candidate = _candidate.parent
    BASE_DIR = _candidate
else:
    BASE_DIR = Path(__file__).resolve().parent

# ── Paths ────────────────────────────────────────────────────
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "company_evaluator.log"
LOCK_FILE = BASE_DIR / ".evaluator.lock"

VENV_PYTHON = BASE_DIR / ".venv" / "Scripts" / "python.exe"
SYSTEM_PYTHON = Path("C:/Python314/python.exe")

if VENV_PYTHON.exists():
    PYTHON = str(VENV_PYTHON)
elif not IS_FROZEN:
    PYTHON = sys.executable
elif SYSTEM_PYTHON.exists():
    PYTHON = str(SYSTEM_PYTHON)
else:
    PYTHON = "python"

HEALTH_URL         = "http://localhost:8100/health"
DASHBOARD_URL      = "http://localhost:8100/api/status/dashboard"
CRAWLER_RUN_URL    = "http://localhost:8100/api/pipeline/run"
CRAWLER_STOP_URL   = "http://localhost:8100/api/pipeline/stop"
CRAWLER_STATUS_URL = "http://localhost:8100/api/pipeline/status"
POLL_INTERVAL_MS   = 5000

SPAWN_LOG = BASE_DIR / ".evaluator_spawns.json"
MAX_SPAWNS = 3
SPAWN_WINDOW_SEC = 10


def _check_spawn_limit():
    now = time.time()
    spawns: list[float] = []
    if SPAWN_LOG.exists():
        try:
            spawns = json.loads(SPAWN_LOG.read_text())
        except (json.JSONDecodeError, OSError):
            spawns = []
    spawns = [t for t in spawns if now - t < SPAWN_WINDOW_SEC]
    if len(spawns) >= MAX_SPAWNS:
        print(f"SAFETY: {len(spawns)} launches in the last {SPAWN_WINDOW_SEC}s. Aborting.")
        sys.exit(1)
    spawns.append(now)
    try:
        SPAWN_LOG.write_text(json.dumps(spawns))
    except OSError:
        pass


def _check_singleton():
    if LOCK_FILE.exists():
        try:
            pid = int(LOCK_FILE.read_text().strip())
            os.kill(pid, 0)
            print(f"Evaluator already running (PID {pid}). Exiting.")
            sys.exit(0)
        except (OSError, ValueError):
            pass
    LOCK_FILE.write_text(str(os.getpid()))
    atexit.register(_remove_lock)


def _remove_lock():
    try:
        LOCK_FILE.unlink(missing_ok=True)
    except OSError:
        pass


def _format_duration(seconds):
    """Format seconds into a human-readable duration."""
    if seconds is None or seconds < 0:
        return "—"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m {seconds % 60}s"
    hours = minutes // 60
    return f"{hours}h {minutes % 60}m"


class LauncherApp(tk.Tk):
    # ── Palette ───────────────────────────────────────────────
    BG          = "#080c14"
    PANEL       = "#0d1321"
    PANEL_LITE  = "#131b2e"
    ACCENT      = "#00e5cc"
    ACCENT_DIM  = "#007a6c"
    CYAN_GLOW   = "#00ffd5"
    BLUE        = "#0af"
    RED         = "#ff4060"
    RED_HOVER   = "#e0304f"
    AMBER       = "#f5a623"
    GREEN       = "#00e676"
    TEXT_PRI    = "#e8ecf1"
    TEXT_SEC    = "#8892a4"
    TEXT_DIM    = "#505a6e"
    PROGRESS_BG = "#1a2440"
    PROGRESS_FG = "#00e5cc"

    def __init__(self):
        super().__init__()
        self.title("BenTrade Company Evaluator")
        self.configure(bg=self.BG)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

        # Size: 520 wide, cap height at 85% of screen
        screen_h = self.winfo_screenheight()
        win_h = min(720, int(screen_h * 0.85))
        self.geometry(f"520x{win_h}")
        self.minsize(420, 480)
        self.resizable(True, True)

        self._process: subprocess.Popen | None = None
        self._stopping = False
        self._backend_online = False
        self._last_dashboard: dict | None = None
        self._last_update_time: float | None = None

        self._build_ui()
        self._start_backend()

    # ── Helpers ───────────────────────────────────────────────
    @staticmethod
    def _hover(btn, enter_bg, leave_bg):
        btn.bind("<Enter>", lambda e: btn.config(bg=enter_bg))
        btn.bind("<Leave>", lambda e: btn.config(bg=leave_bg))

    def _make_btn(self, parent, text, bg, fg, hover_bg, cmd, width=14, state="normal"):
        btn = tk.Button(
            parent, text=text, width=width, bg=bg, fg=fg,
            activebackground=hover_bg, activeforeground=fg,
            font=self._btn_font, relief="flat", cursor="hand2",
            command=cmd, state=state, bd=0, highlightthickness=0,
            pady=5,
        )
        self._hover(btn, hover_bg, bg)
        return btn

    def _make_section_header(self, parent, text):
        lbl = tk.Label(parent, text=text, bg=self.BG, fg=self.ACCENT_DIM,
                       font=self._section_font, anchor="w")
        lbl.pack(fill="x", padx=0, pady=(8, 3))
        return lbl

    def _make_card(self, parent):
        card = tk.Frame(parent, bg=self.PANEL_LITE, bd=0,
                        highlightthickness=1, highlightbackground="#1a2440")
        card.pack(fill="x", pady=(0, 2))
        return card

    def _make_kv_row(self, parent, label, value="—", value_fg=None):
        row = tk.Frame(parent, bg=self.PANEL_LITE)
        row.pack(fill="x", padx=12, pady=1)
        tk.Label(row, text=label, bg=self.PANEL_LITE, fg=self.TEXT_SEC,
                 font=self._data_font, width=14, anchor="w").pack(side="left")
        val_lbl = tk.Label(row, text=value, bg=self.PANEL_LITE,
                           fg=value_fg or self.TEXT_PRI, font=self._data_font,
                           anchor="w")
        val_lbl.pack(side="left", fill="x", expand=True)
        return val_lbl

    # ── UI Build ──────────────────────────────────────────────
    def _build_ui(self):
        # Fonts
        self._brand_font   = tkfont.Font(family="Segoe UI", size=10, weight="bold")
        self._section_font = tkfont.Font(family="Segoe UI", size=9, weight="bold")
        self._data_font    = tkfont.Font(family="Consolas", size=9)
        self._data_sm_font = tkfont.Font(family="Consolas", size=8)
        self._btn_font     = tkfont.Font(family="Segoe UI", size=9, weight="bold")
        self._heading_font = tkfont.Font(family="Segoe UI", size=12, weight="bold")
        self._tiny_font    = tkfont.Font(family="Consolas", size=7)

        # ── Accent bar (top) ─────────────────────────────────
        accent_bar = tk.Canvas(self, height=3, bg=self.BG, highlightthickness=0)
        accent_bar.pack(fill="x")
        accent_bar.update_idletasks()
        w = 520
        for i in range(w):
            ratio = 1.0 - abs(i - w / 2) / (w / 2)
            g = int(229 * ratio)
            b = int(204 * ratio)
            accent_bar.create_line(i, 0, i, 3, fill=f"#00{g:02x}{b:02x}")

        # ── Header ───────────────────────────────────────────
        header = tk.Frame(self, bg=self.PANEL, height=40)
        header.pack(fill="x")
        header.pack_propagate(False)

        badge = tk.Canvas(header, width=32, height=22, bg=self.PANEL, highlightthickness=0)
        badge.pack(side="left", padx=(12, 0), pady=9)
        badge.create_rectangle(0, 0, 32, 22, fill=self.ACCENT, outline="")
        badge.create_text(16, 11, text="CE", fill=self.BG,
                          font=tkfont.Font(family="Segoe UI", size=9, weight="bold"))

        tk.Label(header, text="BENTRADE", bg=self.PANEL, fg=self.TEXT_PRI,
                 font=self._brand_font).pack(side="left", padx=(6, 0))

        self._version_lbl = tk.Label(header, text="Company Evaluator  v1.0",
                                     bg=self.PANEL, fg=self.ACCENT_DIM,
                                     font=tkfont.Font(family="Segoe UI", size=9))
        self._version_lbl.pack(side="right", padx=12)

        tk.Frame(self, bg=self.ACCENT_DIM, height=1).pack(fill="x")

        # ── FIXED BOTTOM: Buttons + error + footer ───────────
        # These are packed side="bottom" FIRST so they always stay visible.

        # Footer accent
        foot_bar = tk.Canvas(self, height=2, bg=self.BG, highlightthickness=0)
        foot_bar.pack(side="bottom", fill="x")
        def _draw_footer_gradient(event=None):
            foot_bar.delete("grad")
            fw = foot_bar.winfo_width()
            if fw < 10:
                fw = 520
            for i in range(fw):
                ratio = 1.0 - abs(i - fw / 2) / (fw / 2)
                g = int(229 * ratio * 0.3)
                b = int(204 * ratio * 0.3)
                foot_bar.create_line(i, 0, i, 2, fill=f"#00{g:02x}{b:02x}", tags="grad")
        foot_bar.bind("<Configure>", _draw_footer_gradient)

        # Error bar
        self._error_bar = tk.Label(self, text="", bg=self.BG, fg=self.RED,
                                   font=self._tiny_font, anchor="w", wraplength=490)
        self._error_bar.pack(side="bottom", fill="x", padx=14, pady=(0, 2))

        # Button rows
        btn_area = tk.Frame(self, bg=self.BG)
        btn_area.pack(side="bottom", fill="x", padx=14, pady=(2, 2))

        row1 = tk.Frame(btn_area, bg=self.BG)
        row1.pack(fill="x", pady=(0, 4))

        self._start_crawler_btn = self._make_btn(
            row1, "\u25b6  Start Crawler", self.ACCENT, self.BG,
            self.CYAN_GLOW, self._start_crawler, width=15, state="disabled",
        )
        self._start_crawler_btn.pack(side="left", padx=(0, 4))

        self._stop_crawler_btn = self._make_btn(
            row1, "\u25a0  Stop Crawler", self.RED, "#fff",
            self.RED_HOVER, self._stop_crawler, width=15, state="disabled",
        )
        self._stop_crawler_btn.pack(side="left", padx=(0, 4))

        self._restart_btn = self._make_btn(
            row1, "\u27f3  Restart", self.PANEL_LITE, self.TEXT_SEC,
            "#1a2440", self._restart_backend, width=12,
        )
        self._restart_btn.pack(side="left")

        row2 = tk.Frame(btn_area, bg=self.BG)
        row2.pack(fill="x")

        self._logs_btn = self._make_btn(
            row2, "Open Logs", self.PANEL_LITE, self.TEXT_SEC,
            "#1a2440", self._open_logs, width=15,
        )
        self._logs_btn.pack(side="left", padx=(0, 4))

        self._db_btn = self._make_btn(
            row2, "Open DB Browser", self.PANEL_LITE, self.TEXT_SEC,
            "#1a2440", self._open_db_browser, width=15,
        )
        self._db_btn.pack(side="left")

        # Separator above buttons
        tk.Frame(self, bg=self.ACCENT_DIM, height=1).pack(side="bottom", fill="x")

        # ── SCROLLABLE BODY (fills remaining space) ──────────
        self._scroll_canvas = tk.Canvas(self, bg=self.BG, highlightthickness=0)
        self._scrollbar = tk.Scrollbar(self, orient="vertical",
                                        command=self._scroll_canvas.yview)
        self._scroll_frame = tk.Frame(self._scroll_canvas, bg=self.BG)

        self._scroll_frame.bind(
            "<Configure>",
            lambda e: self._scroll_canvas.configure(
                scrollregion=self._scroll_canvas.bbox("all"))
        )
        self._scroll_canvas.create_window((0, 0), window=self._scroll_frame,
                                           anchor="nw")
        self._scroll_canvas.configure(yscrollcommand=self._scrollbar.set)

        self._scrollbar.pack(side="right", fill="y")
        self._scroll_canvas.pack(side="left", fill="both", expand=True)

        # Mousewheel scrolling
        def _on_mousewheel(event):
            self._scroll_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        self._scroll_canvas.bind_all("<MouseWheel>", _on_mousewheel)

        # Stretch the inner frame to the canvas width
        def _on_canvas_configure(event):
            self._scroll_canvas.itemconfigure(
                self._scroll_canvas.find_withtag("all")[0],
                width=event.width
            )
        self._scroll_canvas.bind("<Configure>", _on_canvas_configure)

        # Use scroll_frame as the body for all content sections
        body = tk.Frame(self._scroll_frame, bg=self.BG)
        body.pack(expand=True, fill="both", padx=14, pady=(4, 4))

        # ── SERVICE STATUS ───────────────────────────────────
        self._make_section_header(body, "SERVICE STATUS")
        svc_card = self._make_card(body)
        tk.Frame(svc_card, bg=self.PANEL_LITE, height=4).pack()  # top pad

        # Status row with dot
        svc_status_row = tk.Frame(svc_card, bg=self.PANEL_LITE)
        svc_status_row.pack(fill="x", padx=12, pady=1)
        self._svc_dot = tk.Canvas(svc_status_row, width=10, height=10,
                                  bg=self.PANEL_LITE, highlightthickness=0)
        self._svc_dot.pack(side="left", padx=(0, 6), pady=2)
        self._svc_dot_id = self._svc_dot.create_oval(1, 1, 9, 9, fill=self.AMBER, outline="")
        self._svc_status_lbl = tk.Label(svc_status_row, text="Starting…",
                                        bg=self.PANEL_LITE, fg=self.AMBER,
                                        font=self._data_font)
        self._svc_status_lbl.pack(side="left")
        self._svc_port_lbl = tk.Label(svc_status_row, text="Port 8100",
                                      bg=self.PANEL_LITE, fg=self.TEXT_DIM,
                                      font=self._tiny_font)
        self._svc_port_lbl.pack(side="right")

        self._svc_uptime = self._make_kv_row(svc_card, "Uptime", "—")
        self._svc_pid = self._make_kv_row(svc_card, "PID", "—")
        self._svc_mem = self._make_kv_row(svc_card, "Memory", "—")
        tk.Frame(svc_card, bg=self.PANEL_LITE, height=4).pack()  # bottom pad

        # ── CRAWLER ──────────────────────────────────────────
        self._make_section_header(body, "CRAWLER")
        crawl_card = self._make_card(body)
        tk.Frame(crawl_card, bg=self.PANEL_LITE, height=4).pack()

        crawl_status_row = tk.Frame(crawl_card, bg=self.PANEL_LITE)
        crawl_status_row.pack(fill="x", padx=12, pady=1)
        self._crawl_dot = tk.Canvas(crawl_status_row, width=10, height=10,
                                    bg=self.PANEL_LITE, highlightthickness=0)
        self._crawl_dot.pack(side="left", padx=(0, 6), pady=2)
        self._crawl_dot_id = self._crawl_dot.create_oval(1, 1, 9, 9, fill=self.TEXT_DIM, outline="")
        self._crawl_status_lbl = tk.Label(crawl_status_row, text="Idle",
                                          bg=self.PANEL_LITE, fg=self.TEXT_DIM,
                                          font=self._data_font)
        self._crawl_status_lbl.pack(side="left")
        self._crawl_cycle_lbl = tk.Label(crawl_status_row, text="",
                                         bg=self.PANEL_LITE, fg=self.TEXT_DIM,
                                         font=self._tiny_font)
        self._crawl_cycle_lbl.pack(side="right")

        self._crawl_current = self._make_kv_row(crawl_card, "Current", "—")
        self._crawl_speed = self._make_kv_row(crawl_card, "Speed", "—")
        self._crawl_eta = self._make_kv_row(crawl_card, "ETA", "—")

        # Progress bar
        prog_frame = tk.Frame(crawl_card, bg=self.PANEL_LITE)
        prog_frame.pack(fill="x", padx=12, pady=(3, 1))
        self._prog_canvas = tk.Canvas(prog_frame, height=14, bg=self.PROGRESS_BG,
                                      highlightthickness=0)
        self._prog_canvas.pack(fill="x")
        self._prog_bar_id = self._prog_canvas.create_rectangle(0, 0, 0, 14,
                                                                fill=self.PROGRESS_FG, outline="")
        self._prog_text_id = self._prog_canvas.create_text(0, 7, text="0%", fill=self.TEXT_PRI,
                                                            font=self._tiny_font, anchor="w")

        # Cycle stats
        stats_row = tk.Frame(crawl_card, bg=self.PANEL_LITE)
        stats_row.pack(fill="x", padx=12, pady=(3, 1))
        self._crawl_eval_lbl = tk.Label(stats_row, text="Evaluated  —", bg=self.PANEL_LITE,
                                        fg=self.TEXT_SEC, font=self._data_sm_font, anchor="w")
        self._crawl_eval_lbl.pack(side="left", padx=(0, 12))
        self._crawl_fail_lbl = tk.Label(stats_row, text="Failed  —", bg=self.PANEL_LITE,
                                        fg=self.TEXT_SEC, font=self._data_sm_font, anchor="w")
        self._crawl_fail_lbl.pack(side="left")

        stats_row2 = tk.Frame(crawl_card, bg=self.PANEL_LITE)
        stats_row2.pack(fill="x", padx=12, pady=(0, 1))
        self._crawl_remain_lbl = tk.Label(stats_row2, text="Remaining  —", bg=self.PANEL_LITE,
                                          fg=self.TEXT_SEC, font=self._data_sm_font, anchor="w")
        self._crawl_remain_lbl.pack(side="left")

        tk.Frame(crawl_card, bg=self.PANEL_LITE, height=4).pack()

        # ── UNIVERSE ─────────────────────────────────────────
        self._make_section_header(body, "UNIVERSE")
        uni_card = self._make_card(body)
        tk.Frame(uni_card, bg=self.PANEL_LITE, height=4).pack()

        uni_top = tk.Frame(uni_card, bg=self.PANEL_LITE)
        uni_top.pack(fill="x", padx=12, pady=1)
        self._uni_total_lbl = tk.Label(uni_top, text="Total  —   Active  —",
                                       bg=self.PANEL_LITE, fg=self.TEXT_PRI,
                                       font=self._data_font, anchor="w")
        self._uni_total_lbl.pack(side="left")

        # Tier rows (dynamic)
        self._uni_tiers_frame = tk.Frame(uni_card, bg=self.PANEL_LITE)
        self._uni_tiers_frame.pack(fill="x", padx=12, pady=(2, 1))
        self._tier_labels: list[tk.Label] = []

        self._uni_refresh_lbl = tk.Label(uni_card, text="Last refresh  —",
                                         bg=self.PANEL_LITE, fg=self.TEXT_DIM,
                                         font=self._tiny_font, anchor="w")
        self._uni_refresh_lbl.pack(fill="x", padx=12, pady=(1, 1))
        tk.Frame(uni_card, bg=self.PANEL_LITE, height=4).pack()

        # ── RECENT ACTIVITY ──────────────────────────────────
        self._make_section_header(body, "RECENT ACTIVITY")
        activity_card = self._make_card(body)
        tk.Frame(activity_card, bg=self.PANEL_LITE, height=3).pack()
        self._activity_frame = tk.Frame(activity_card, bg=self.PANEL_LITE)
        self._activity_frame.pack(fill="x", padx=8, pady=1)
        self._activity_labels: list[tk.Label] = []
        self._no_activity_lbl = tk.Label(self._activity_frame, text="  No recent activity",
                                         bg=self.PANEL_LITE, fg=self.TEXT_DIM,
                                         font=self._data_sm_font, anchor="w")
        self._no_activity_lbl.pack(fill="x")
        tk.Frame(activity_card, bg=self.PANEL_LITE, height=3).pack()

    # ── Backend process ───────────────────────────────────────
    def _start_backend(self):
        LOG_DIR.mkdir(exist_ok=True)
        log_fh = open(LOG_FILE, "a", encoding="utf-8")
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        self._process = subprocess.Popen(
            [PYTHON, "-m", "uvicorn", "main:app",
             "--host", "0.0.0.0", "--port", "8100",
             "--log-level", "info"],
            cwd=str(BASE_DIR),
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            env=env,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        self._stopping = False
        self._poll_dashboard()

    def _poll_dashboard(self):
        """Poll the backend dashboard every POLL_INTERVAL_MS."""
        if self._stopping:
            return

        def fetch():
            try:
                with urllib.request.urlopen(DASHBOARD_URL, timeout=3) as r:
                    if r.status == 200:
                        return json.loads(r.read().decode())
            except Exception:
                pass
            # Fallback: try basic health check
            try:
                with urllib.request.urlopen(HEALTH_URL, timeout=2) as r:
                    if r.status == 200:
                        return {"_health_only": True}
            except Exception:
                pass
            return None

        data = fetch()

        if data and data.get("_health_only"):
            # Backend is up but dashboard endpoint not available yet
            self._backend_online = True
            self._set_backend_status("running", "Online (initializing)")
            self._enable_buttons(True)
        elif data:
            self._backend_online = True
            self._last_dashboard = data
            self._last_update_time = time.time()
            self._update_display(data)
            self._enable_buttons(True)
        else:
            # Backend down
            if self._process and self._process.poll() is not None:
                self._backend_online = False
                self._set_backend_status("stopped", "Backend Stopped")
                self._enable_buttons(False)
                # Auto-restart after 5 seconds
                self.after(5000, self._auto_restart)
                return
            else:
                self._set_backend_status("starting", "Starting…")

        self.after(POLL_INTERVAL_MS, self._poll_dashboard)

    def _auto_restart(self):
        """Auto-restart the backend if it died unexpectedly."""
        if self._stopping:
            return
        if self._process and self._process.poll() is not None:
            self._set_backend_status("restarting", "Auto-restarting…")
            self.after(2000, self._start_backend)

    def _set_backend_status(self, state, text=None):
        colors = {
            "running": self.ACCENT,
            "starting": self.AMBER,
            "restarting": self.AMBER,
            "stopped": self.RED,
            "error": self.RED,
        }
        color = colors.get(state, self.TEXT_DIM)
        self._svc_status_lbl.config(text=text or state.title(), fg=color)
        self._svc_dot.itemconfig(self._svc_dot_id, fill=color)

    def _enable_buttons(self, backend_online: bool):
        """Enable/disable buttons based on backend state."""
        if backend_online:
            # Crawler buttons depend on crawler state
            crawler = self._last_dashboard.get("crawler", {}) if self._last_dashboard else {}
            running = crawler.get("running", False)
            self._start_crawler_btn.config(state="disabled" if running else "normal")
            self._stop_crawler_btn.config(state="normal" if running else "disabled")
        else:
            self._start_crawler_btn.config(state="disabled")
            self._stop_crawler_btn.config(state="disabled")
        # Restart is always available
        self._restart_btn.config(state="normal")

    # ── Display update ────────────────────────────────────────
    def _update_display(self, data: dict):
        """Update all dashboard panels from the API response."""
        # ── Backend ──────────────────────────────────────────
        be = data.get("backend", {})
        self._set_backend_status("running", "Online")
        self._svc_uptime.config(text=_format_duration(be.get("uptime_seconds")))
        self._svc_pid.config(text=str(be.get("pid", "—")))
        mem = be.get("memory_mb")
        cpu = be.get("cpu_pct")
        mem_text = f"{mem} MB" if mem is not None else "—"
        if cpu is not None:
            mem_text += f"   CPU {cpu:.1f}%"
        self._svc_mem.config(text=mem_text)

        # ── Crawler ──────────────────────────────────────────
        cr = data.get("crawler", {})
        cr_status = cr.get("status", "idle")
        running = cr.get("running", False)
        progress = cr.get("progress", {})
        current = cr.get("current_symbol")
        cycle = cr.get("cycle_number", 0)

        total = progress.get("total", 0)
        evaluated = progress.get("evaluated", 0)
        failed = progress.get("failed", 0)
        remaining = progress.get("remaining", 0)
        pct = progress.get("pct", 0)

        status_colors = {
            "evaluating": self.ACCENT,
            "paused": self.AMBER,
            "running": self.ACCENT,
            "idle": self.TEXT_DIM,
        }
        color = status_colors.get(cr_status, self.TEXT_DIM)

        status_labels = {
            "evaluating": "Evaluating",
            "paused": "Paused",
            "running": "Running",
            "idle": "Idle",
        }
        self._crawl_status_lbl.config(text=status_labels.get(cr_status, cr_status.title()), fg=color)
        self._crawl_dot.itemconfig(self._crawl_dot_id, fill=color)
        self._crawl_cycle_lbl.config(text=f"Cycle {cycle}" if cycle else "")

        if running and current:
            cur_idx = progress.get("current_index", 0)
            self._crawl_current.config(text=f"{current} ({cur_idx}/{total})")
        else:
            self._crawl_current.config(text="—")

        avg_sec = cr.get("avg_seconds_per_symbol")
        self._crawl_speed.config(text=f"~{avg_sec:.1f} sec/symbol" if avg_sec else "—")
        eta_sec = cr.get("eta_seconds")
        self._crawl_eta.config(text=_format_duration(eta_sec) if eta_sec else "—")

        # Progress bar
        self._prog_canvas.update_idletasks()
        bar_w = self._prog_canvas.winfo_width()
        if bar_w < 10:
            bar_w = 460  # fallback
        fill_w = max(1, int(bar_w * pct / 100)) if total > 0 else 0
        self._prog_canvas.coords(self._prog_bar_id, 0, 0, fill_w, 14)
        if total == 0:
            pct_text = "No symbols"
        else:
            pct_text = f" {pct:.0f}%"
        self._prog_canvas.coords(self._prog_text_id, 4, 7)
        self._prog_canvas.itemconfig(self._prog_text_id, text=pct_text)

        # Cycle stats
        self._crawl_eval_lbl.config(text=f"Evaluated  {evaluated}")
        self._crawl_fail_lbl.config(text=f"Failed  {failed}",
                                    fg=self.RED if failed > 0 else self.TEXT_SEC)
        self._crawl_remain_lbl.config(text=f"Remaining  {remaining}")

        # Button states
        self._start_crawler_btn.config(state="disabled" if running else "normal")
        self._stop_crawler_btn.config(state="normal" if running else "disabled")

        # ── Universe ─────────────────────────────────────────
        uni = data.get("universe", {})
        uni_total = uni.get("total", 0)
        uni_active = uni.get("active", 0)
        self._uni_total_lbl.config(text=f"Total  {uni_total}   Active  {uni_active}")

        # Rebuild tier rows
        for lbl in self._tier_labels:
            lbl.destroy()
        self._tier_labels.clear()

        by_tier = uni.get("by_tier", {})
        tier_display = {
            "sp500_top100": "S&P 500 Core",
            "large_cap": "Large Cap",
            "mid_cap": "Mid Cap",
            "small_cap": "Small Cap",
            "penny_stock": "Penny Stock",
            "ipo_discovery": "IPO Discovery",
            "manual": "Manual",
        }
        for source, info in by_tier.items():
            display_name = tier_display.get(source, source)
            t_total = info.get("total", 0)
            t_eval = info.get("evaluated", 0)
            text = f"  {display_name:<18s} {t_total:>5}   Evaluated {t_eval:>5}"
            lbl = tk.Label(self._uni_tiers_frame, text=text, bg=self.PANEL_LITE,
                           fg=self.TEXT_SEC, font=self._data_sm_font, anchor="w")
            lbl.pack(fill="x")
            self._tier_labels.append(lbl)

        last_ref = uni.get("last_refresh", "—")
        if last_ref and last_ref != "—":
            last_ref = last_ref[:10]  # date only
        self._uni_refresh_lbl.config(text=f"Last refresh  {last_ref}")

        # ── Recent Activity ──────────────────────────────────
        activity = data.get("recent_activity", [])
        # Also merge recent_evaluations from DB if no crawler activity
        if not activity:
            activity = []
            for ev in data.get("recent_evaluations", [])[:5]:
                activity.append({
                    "timestamp": ev.get("evaluated_at", ""),
                    "symbol": ev.get("symbol", ""),
                    "score": ev.get("score"),
                    "recommendation": ev.get("recommendation"),
                    "status": "success",
                })

        for lbl in self._activity_labels:
            lbl.destroy()
        self._activity_labels.clear()

        if activity:
            self._no_activity_lbl.pack_forget()
            for entry in activity[:6]:
                ts = entry.get("timestamp", "")
                if len(ts) > 16:
                    ts = ts[11:16]  # HH:MM
                symbol = entry.get("symbol", "?")
                score = entry.get("score")
                rec = entry.get("recommendation", "")
                status = entry.get("status", "")
                error = entry.get("error", "")

                if status == "error":
                    text = f"  {ts}  {symbol:<6s}  Error: {error[:40]}"
                    fg = self.RED
                else:
                    rec_display = rec.replace("_", " ") if rec else ""
                    score_str = f"{score:.1f}" if score is not None else "—"
                    # Colored indicator
                    indicator = "▲" if rec and "BUY" in rec.upper() else "●"
                    if rec and "SELL" in rec.upper():
                        indicator = "▼"
                    text = f"  {ts}  {symbol:<6s}  Score: {score_str}  {indicator} {rec_display}"
                    fg = self.GREEN if "BUY" in (rec or "").upper() else self.TEXT_SEC
                    if "SELL" in (rec or "").upper():
                        fg = self.RED

                lbl = tk.Label(self._activity_frame, text=text, bg=self.PANEL_LITE,
                               fg=fg, font=self._data_sm_font, anchor="w")
                lbl.pack(fill="x")
                self._activity_labels.append(lbl)
        else:
            self._no_activity_lbl.pack(fill="x")

        # ── Last error ───────────────────────────────────────
        last_err = data.get("last_error")
        if last_err:
            ts = last_err.get("timestamp", "")[:16].replace("T", " ")
            sym = last_err.get("symbol", "")
            err = last_err.get("error", "")
            self._error_bar.config(text=f"Last error: {ts} {sym}: {err[:60]}")
        else:
            self._error_bar.config(text="")

    # ── Backend lifecycle ─────────────────────────────────────
    def _stop_backend(self):
        self._stopping = True
        self._set_backend_status("stopped", "Stopping…")

        def do_stop():
            if self._process and self._process.poll() is None:
                self._process.terminate()
                try:
                    self._process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self._process.kill()
            self.after(0, self._on_stopped)

        threading.Thread(target=do_stop, daemon=True).start()

    def _on_stopped(self):
        self._backend_online = False
        self._set_backend_status("stopped", "Offline")
        self._enable_buttons(False)

    def _restart_backend(self):
        """Stop then restart the backend."""
        self._set_backend_status("restarting", "Restarting…")
        self._stopping = True

        def do_restart():
            if self._process and self._process.poll() is None:
                self._process.terminate()
                try:
                    self._process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self._process.kill()
            time.sleep(2)
            self.after(0, self._start_backend)

        threading.Thread(target=do_restart, daemon=True).start()

    # ── Crawler controls ──────────────────────────────────────
    def _start_crawler(self):
        self._start_crawler_btn.config(state="disabled")

        def do_start():
            try:
                body = json.dumps({"full_universe": True}).encode()
                req = urllib.request.Request(
                    CRAWLER_RUN_URL, method="POST", data=body,
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    result = json.loads(resp.read().decode())
                    if result.get("status") == "already_running":
                        self.after(0, lambda: self._show_error("Crawler is already running"))
            except urllib.error.URLError as e:
                self.after(0, lambda: self._show_error(f"Start failed: {e.reason}"))
            except Exception as e:
                self.after(0, lambda: self._show_error(f"Start failed: {e}"))

        threading.Thread(target=do_start, daemon=True).start()

    def _stop_crawler(self):
        self._stop_crawler_btn.config(state="disabled")

        def do_stop():
            try:
                req = urllib.request.Request(CRAWLER_STOP_URL, method="POST", data=b"")
                with urllib.request.urlopen(req, timeout=5):
                    pass
            except urllib.error.URLError as e:
                self.after(0, lambda: self._show_error(f"Stop failed: {e.reason}"))
            except Exception as e:
                self.after(0, lambda: self._show_error(f"Stop failed: {e}"))

        threading.Thread(target=do_stop, daemon=True).start()

    def _show_error(self, msg):
        self._error_bar.config(text=msg)

    # ── Utilities ─────────────────────────────────────────────
    def _open_logs(self):
        if LOG_FILE.exists():
            os.startfile(str(LOG_FILE))
        else:
            LOG_DIR.mkdir(exist_ok=True)
            LOG_FILE.touch()
            os.startfile(str(LOG_FILE))

    def _open_db_browser(self):
        import webbrowser
        webbrowser.open("http://localhost:8100")

    def _on_close(self):
        self._stopping = True
        if self._process and self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
        _remove_lock()
        self.destroy()


if __name__ == "__main__":
    multiprocessing.freeze_support()

    _self = Path(sys.executable).resolve()
    _py = Path(PYTHON).resolve()
    if _self == _py and IS_FROZEN:
        print(f"FATAL: PYTHON ({PYTHON}) points at the launcher exe. Aborting.")
        sys.exit(2)

    _check_spawn_limit()
    _check_singleton()
    app = LauncherApp()
    app.mainloop()
