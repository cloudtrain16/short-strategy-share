#!/usr/bin/env python3
import argparse
import json
import os
import posixpath
import re
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import cgi


HTML_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>short-strategy-share 文件投递</title>
  <style>
    :root { --bg:#f7f8fb; --card:#ffffff; --text:#111827; --muted:#6b7280; --line:#d1d5db; --ok:#0f766e; --btn:#0f172a; --btntext:#ffffff; }
    body { margin:0; font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background:var(--bg); color:var(--text); }
    .wrap { max-width: 860px; margin: 32px auto; padding: 0 16px; }
    .card { background:var(--card); border:1px solid var(--line); border-radius:14px; padding:18px; box-shadow:0 8px 28px rgba(0,0,0,0.04); }
    h1 { margin:0 0 10px; font-size:24px; }
    p { color:var(--muted); margin:4px 0 14px; }
    .drop {
      border: 2px dashed #9ca3af; border-radius: 14px; min-height: 220px; display:flex; align-items:center; justify-content:center;
      background: linear-gradient(180deg,#fbfdff,#f1f5f9); text-align:center; padding:12px; transition:all .15s ease;
    }
    .drop.active { border-color:#0ea5e9; background:#e0f2fe; }
    .small { font-size:13px; color:var(--muted); }
    .row { display:flex; gap:10px; align-items:center; margin-top:12px; flex-wrap:wrap; }
    input[type="text"] { border:1px solid var(--line); border-radius:8px; padding:8px 10px; min-width:280px; }
    button {
      border:0; border-radius:9px; background:var(--btn); color:var(--btntext); padding:9px 14px; cursor:pointer;
      font-weight:600;
    }
    button.secondary { background:#334155; }
    ul { margin:12px 0 0; padding-left:18px; }
    .ok { color:var(--ok); font-weight:600; }
    .mono { font-family: ui-monospace, Menlo, Monaco, Consolas, monospace; font-size:12px; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>文件投递 UI</h1>
      <p>把文件直接拖进来，自动写入仓库目录。不会执行任何代码，只做文件落盘。</p>
      <div id="drop" class="drop">
        <div>
          <div style="font-size:18px;font-weight:700;">拖拽文件到这里</div>
          <div class="small">或点击按钮选择文件（支持多文件）</div>
          <div class="row" style="justify-content:center;">
            <input id="picker" type="file" multiple />
          </div>
        </div>
      </div>
      <div class="row">
        <label>目标子目录：</label>
        <input id="subdir" type="text" value="shared_drop" />
        <button id="uploadBtn">上传</button>
        <button id="refreshBtn" class="secondary">刷新状态</button>
      </div>
      <p class="small">仓库根目录：<span class="mono" id="repoRoot"></span></p>
      <p id="status"></p>
      <ul id="list"></ul>
    </div>
  </div>
  <script>
    const drop = document.getElementById("drop");
    const picker = document.getElementById("picker");
    const subdir = document.getElementById("subdir");
    const statusEl = document.getElementById("status");
    const listEl = document.getElementById("list");
    const repoRootEl = document.getElementById("repoRoot");
    let queued = [];

    function setStatus(msg, ok=false) {
      statusEl.textContent = msg;
      statusEl.className = ok ? "ok" : "";
    }

    async function loadStatus() {
      const res = await fetch("/api/status");
      const data = await res.json();
      repoRootEl.textContent = data.repo_root;
      const items = data.latest_files || [];
      listEl.innerHTML = items.map(x => `<li><span class="mono">${x}</span></li>`).join("");
    }

    function queueFiles(files) {
      queued = [...queued, ...Array.from(files)];
      setStatus(`已选择 ${queued.length} 个文件，点击“上传”开始。`, true);
    }

    ["dragenter","dragover"].forEach(evt => drop.addEventListener(evt, e => {
      e.preventDefault(); e.stopPropagation(); drop.classList.add("active");
    }));
    ["dragleave","drop"].forEach(evt => drop.addEventListener(evt, e => {
      e.preventDefault(); e.stopPropagation(); drop.classList.remove("active");
    }));
    drop.addEventListener("drop", e => queueFiles(e.dataTransfer.files));
    picker.addEventListener("change", e => queueFiles(e.target.files));

    document.getElementById("uploadBtn").addEventListener("click", async () => {
      if (!queued.length) { setStatus("请先选择或拖入文件。"); return; }
      const form = new FormData();
      form.append("subdir", subdir.value.trim());
      for (const f of queued) form.append("files", f, f.name);
      setStatus("上传中...");
      const res = await fetch("/api/upload", { method:"POST", body:form });
      const data = await res.json();
      if (!res.ok) { setStatus(data.error || "上传失败"); return; }
      queued = [];
      picker.value = "";
      setStatus(`上传成功：${data.saved.length} 个文件`, true);
      listEl.innerHTML = data.saved.map(x => `<li><span class="mono">${x}</span></li>`).join("");
    });

    document.getElementById("refreshBtn").addEventListener("click", loadStatus);
    loadStatus();
  </script>
</body>
</html>
"""


def utc_now_str():
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def sanitize_name(name: str) -> str:
    base = Path(name).name
    base = base.strip().replace("\x00", "")
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", base)
    return base or f"file_{utc_now_str()}"


def sanitize_subdir(subdir: str) -> str:
    subdir = (subdir or "").strip().replace("\\", "/")
    subdir = posixpath.normpath("/" + subdir).lstrip("/")
    if subdir in ("", "."):
        return "shared_drop"
    return subdir


class DropHandler(BaseHTTPRequestHandler):
    repo_root = Path(".").resolve()
    default_drop_dir = "shared_drop"

    def _send_json(self, payload, code=200):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_html(self, html, code=200):
        data = html.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            self._send_html(HTML_PAGE)
            return
        if path == "/api/status":
            latest = []
            drop_root = self.repo_root / self.default_drop_dir
            if drop_root.exists():
                files = sorted([p for p in drop_root.rglob("*") if p.is_file()], key=lambda p: p.stat().st_mtime, reverse=True)
                latest = [str(p.relative_to(self.repo_root)) for p in files[:12]]
            self._send_json({"repo_root": str(self.repo_root), "latest_files": latest})
            return
        self._send_json({"error": "not found"}, code=404)

    def do_POST(self):
        path = urlparse(self.path).path
        if path != "/api/upload":
            self._send_json({"error": "not found"}, code=404)
            return

        ctype, _ = cgi.parse_header(self.headers.get("Content-Type", ""))
        if ctype != "multipart/form-data":
            self._send_json({"error": "Content-Type must be multipart/form-data"}, code=400)
            return

        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={"REQUEST_METHOD": "POST", "CONTENT_TYPE": self.headers.get("Content-Type", "")},
        )

        subdir = self.default_drop_dir
        if "subdir" in form:
            value = form["subdir"].value
            subdir = sanitize_subdir(value) or self.default_drop_dir

        target_root = (self.repo_root / subdir).resolve()
        if not str(target_root).startswith(str(self.repo_root)):
            self._send_json({"error": "invalid target subdir"}, code=400)
            return
        target_root.mkdir(parents=True, exist_ok=True)

        files_field = form["files"] if "files" in form else []
        if not isinstance(files_field, list):
            files_field = [files_field]

        saved = []
        for item in files_field:
            if not getattr(item, "filename", ""):
                continue
            safe_name = sanitize_name(item.filename)
            out_path = target_root / safe_name
            if out_path.exists():
                stem = out_path.stem
                suffix = out_path.suffix
                out_path = target_root / f"{stem}_{utc_now_str()}{suffix}"

            with out_path.open("wb") as f:
                data = item.file.read()
                if isinstance(data, str):
                    data = data.encode("utf-8")
                f.write(data)

            saved.append(str(out_path.relative_to(self.repo_root)))

        if not saved:
            self._send_json({"error": "no files uploaded"}, code=400)
            return
        self._send_json({"ok": True, "saved": saved})

    def log_message(self, fmt, *args):
        return


def main():
    parser = argparse.ArgumentParser(description="Drag-and-drop file UI for this repo.")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--drop-dir", default="shared_drop")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    if not repo_root.exists():
        raise RuntimeError(f"repo root not found: {repo_root}")

    DropHandler.repo_root = repo_root
    DropHandler.default_drop_dir = sanitize_subdir(args.drop_dir)

    srv = ThreadingHTTPServer((args.host, args.port), DropHandler)
    print(f"[file-drop-ui] repo={repo_root}")
    print(f"[file-drop-ui] open http://{args.host}:{args.port}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()


if __name__ == "__main__":
    main()
