#!/usr/bin/env python3
import argparse
import cgi
import json
import mimetypes
import os
import posixpath
import shutil
import socket
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse


HTML_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Repo 文件中转站</title>
  <style>
    :root {
      --bg:#f5f7fb; --card:#ffffff; --line:#dbe2ea; --text:#0f172a; --muted:#64748b;
      --btn:#0f172a; --btnText:#fff; --ok:#0f766e; --warn:#92400e;
    }
    * { box-sizing: border-box; }
    body { margin:0; background:var(--bg); color:var(--text); font-family: ui-sans-serif, -apple-system, "Segoe UI", Roboto, sans-serif; }
    .wrap { max-width: 1080px; margin: 20px auto; padding: 0 14px; }
    .card { background:var(--card); border:1px solid var(--line); border-radius:14px; box-shadow:0 10px 28px rgba(0,0,0,.04); }
    .head { padding:14px 16px; border-bottom:1px solid var(--line); display:flex; gap:14px; flex-wrap:wrap; align-items:center; justify-content:space-between; }
    .title { font-size:20px; font-weight:700; }
    .sub { color:var(--muted); font-size:13px; }
    .row { display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
    .main { display:grid; grid-template-columns: 1.5fr 1fr; gap:14px; padding:14px; }
    .panel { border:1px solid var(--line); border-radius:12px; padding:12px; background:#fff; }
    .panel h3 { margin:0 0 10px; font-size:16px; }
    .mono { font-family: ui-monospace, Menlo, Monaco, Consolas, monospace; font-size:12px; }
    .muted { color:var(--muted); }
    .toolbar { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:10px; }
    button, .btn {
      border:0; border-radius:8px; background:var(--btn); color:var(--btnText); padding:8px 12px; cursor:pointer; font-weight:600; text-decoration:none; display:inline-block;
    }
    button.secondary, .btn.secondary { background:#334155; }
    button.light, .btn.light { background:#e2e8f0; color:#0f172a; }
    input[type="text"], input[type="search"] {
      border:1px solid var(--line); border-radius:8px; padding:8px 10px; min-width:240px;
    }
    .pathbar { border:1px solid var(--line); border-radius:8px; background:#f8fafc; padding:8px 10px; margin-bottom:8px; }
    .crumb a { color:#0f172a; text-decoration:none; }
    .crumb a:hover { text-decoration:underline; }
    table { width:100%; border-collapse: collapse; font-size:13px; }
    th, td { border-bottom:1px solid #edf2f7; padding:8px 6px; text-align:left; vertical-align:middle; }
    th { color:#334155; font-size:12px; text-transform:uppercase; letter-spacing:.02em; }
    td.name a { text-decoration:none; color:#0f172a; font-weight:600; }
    td.name a:hover { text-decoration:underline; }
    .tag { font-size:11px; background:#eef2ff; color:#1e3a8a; border-radius:999px; padding:2px 8px; }
    .drop {
      border:2px dashed #9ca3af; border-radius:12px; min-height:180px; display:flex; align-items:center; justify-content:center;
      text-align:center; background:linear-gradient(180deg,#fbfdff,#f1f5f9); transition:all .15s ease; padding:10px;
    }
    .drop.active { border-color:#0ea5e9; background:#e0f2fe; }
    #status.ok { color:var(--ok); font-weight:700; }
    #status.warn { color:var(--warn); font-weight:700; }
    ul.clean { margin:8px 0 0; padding-left:18px; }
    @media (max-width: 900px) {
      .main { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="head">
        <div>
          <div class="title">short-strategy-share 文件中转站</div>
          <div class="sub">可浏览仓库全部文件、下载文件、拖拽上传到任意目录（不会执行文件）</div>
        </div>
        <div class="sub mono">
          Repo: <span id="repoRoot"></span><br/>
          LAN: <span id="lanHint"></span>
        </div>
      </div>

      <div class="main">
        <section class="panel">
          <h3>文件浏览</h3>
          <div class="toolbar">
            <button id="btnRoot" class="light">仓库根目录</button>
            <button id="btnUp" class="secondary">上一级</button>
            <button id="btnRefresh" class="secondary">刷新</button>
            <input id="searchInput" type="search" placeholder="搜索文件名（例如: strategy / .py）" />
            <button id="btnSearch">搜索</button>
          </div>
          <div class="pathbar">
            当前目录: <span class="crumb mono" id="crumb"></span>
          </div>
          <table>
            <thead>
              <tr><th>名称</th><th>类型</th><th>大小</th><th>修改时间</th><th>操作</th></tr>
            </thead>
            <tbody id="fileRows"></tbody>
          </table>
        </section>

        <section class="panel">
          <h3>拖拽上传</h3>
          <p class="muted">目标目录默认是“当前目录”，也可以手动改子目录。</p>
          <div class="row" style="margin-bottom:8px;">
            <label>目标子目录</label>
            <input id="targetDir" type="text" placeholder="留空=当前目录" />
          </div>
          <div id="drop" class="drop">
            <div>
              <div style="font-size:18px;font-weight:700;">拖文件到这里</div>
              <div class="muted">或选择文件（支持多文件）</div>
              <div style="margin-top:8px;"><input id="picker" type="file" multiple /></div>
            </div>
          </div>
          <div class="row" style="margin-top:10px;">
            <button id="btnUpload">上传</button>
            <button id="btnClear" class="secondary">清空队列</button>
          </div>
          <p id="status" class="muted"></p>
          <ul id="latestList" class="clean"></ul>
        </section>
      </div>
    </div>
  </div>

  <script>
    const repoRootEl = document.getElementById("repoRoot");
    const lanHintEl = document.getElementById("lanHint");
    const crumbEl = document.getElementById("crumb");
    const fileRowsEl = document.getElementById("fileRows");
    const latestListEl = document.getElementById("latestList");
    const statusEl = document.getElementById("status");
    const targetDirEl = document.getElementById("targetDir");
    const searchInputEl = document.getElementById("searchInput");
    const dropEl = document.getElementById("drop");
    const pickerEl = document.getElementById("picker");

    let currentPath = "";
    let queued = [];

    function setStatus(msg, level="ok") {
      statusEl.textContent = msg || "";
      statusEl.className = level;
    }

    function fmtSize(n) {
      if (n === null || n === undefined || Number.isNaN(Number(n))) return "-";
      const x = Number(n);
      if (x < 1024) return `${x} B`;
      if (x < 1024*1024) return `${(x/1024).toFixed(1)} KB`;
      if (x < 1024*1024*1024) return `${(x/1024/1024).toFixed(1)} MB`;
      return `${(x/1024/1024/1024).toFixed(2)} GB`;
    }

    function esc(s) {
      return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
    }

    function buildCrumb(path) {
      const parts = path ? path.split("/") : [];
      let html = `<a href="#" data-go="">/</a>`;
      let acc = "";
      for (const part of parts) {
        acc = acc ? `${acc}/${part}` : part;
        html += ` / <a href="#" data-go="${esc(acc)}">${esc(part)}</a>`;
      }
      crumbEl.innerHTML = html;
      crumbEl.querySelectorAll("a[data-go]").forEach(a => {
        a.addEventListener("click", (e) => {
          e.preventDefault();
          openDir(a.getAttribute("data-go") || "");
        });
      });
    }

    async function loadStatus() {
      const res = await fetch("/api/status");
      const data = await res.json();
      repoRootEl.textContent = data.repo_root;
      lanHintEl.textContent = data.lan_hint || "-";
      latestListEl.innerHTML = (data.latest_files || []).map(x => `<li class="mono">${esc(x)}</li>`).join("");
    }

    async function openDir(path) {
      const res = await fetch(`/api/list?path=${encodeURIComponent(path || "")}`);
      const data = await res.json();
      if (!res.ok) {
        setStatus(data.error || "目录读取失败", "warn");
        return;
      }
      currentPath = data.path || "";
      buildCrumb(currentPath);
      targetDirEl.placeholder = `留空=当前目录 (${currentPath || "/"})`;
      renderRows(data.entries || []);
    }

    function renderRows(entries) {
      if (!entries.length) {
        fileRowsEl.innerHTML = `<tr><td colspan="5" class="muted">目录为空</td></tr>`;
        return;
      }
      fileRowsEl.innerHTML = entries.map(e => {
        const name = esc(e.name);
        const path = esc(e.path);
        const typ = e.is_dir ? `<span class="tag">DIR</span>` : "FILE";
        const size = e.is_dir ? "-" : fmtSize(e.size);
        const mtime = esc(e.mtime || "-");
        const nameCell = e.is_dir
          ? `<a href="#" data-open="${path}">${name}</a>`
          : `<span>${name}</span>`;
        const actions = e.is_dir
          ? `<button class="light" data-open="${path}">进入</button>`
          : `<a class="btn light" href="/api/download?path=${encodeURIComponent(e.path)}" target="_blank">下载</a>`;
        return `<tr>
          <td class="name">${nameCell}</td>
          <td>${typ}</td>
          <td>${size}</td>
          <td class="mono">${mtime}</td>
          <td>${actions}</td>
        </tr>`;
      }).join("");

      fileRowsEl.querySelectorAll("[data-open]").forEach(el => {
        el.addEventListener("click", (ev) => {
          ev.preventDefault();
          openDir(el.getAttribute("data-open") || "");
        });
      });
    }

    function queueFiles(files) {
      queued = [...queued, ...Array.from(files)];
      setStatus(`已加入队列 ${queued.length} 个文件`, "ok");
    }

    async function uploadNow() {
      if (!queued.length) {
        setStatus("请先拖入或选择文件", "warn");
        return;
      }
      const manual = (targetDirEl.value || "").trim();
      const target = manual || currentPath;

      const form = new FormData();
      form.append("subdir", target);
      for (const f of queued) form.append("files", f, f.name);

      setStatus("上传中...", "ok");
      const res = await fetch("/api/upload", { method: "POST", body: form });
      const data = await res.json();
      if (!res.ok) {
        setStatus(data.error || "上传失败", "warn");
        return;
      }
      pickerEl.value = "";
      queued = [];
      setStatus(`上传成功: ${data.saved.length} 个文件`, "ok");
      latestListEl.innerHTML = data.saved.map(x => `<li class="mono">${esc(x)}</li>`).join("");
      await openDir(data.target_path || currentPath);
      await loadStatus();
    }

    async function searchFiles() {
      const q = (searchInputEl.value || "").trim();
      if (!q) {
        setStatus("请输入搜索关键词", "warn");
        return;
      }
      const res = await fetch(`/api/search?q=${encodeURIComponent(q)}`);
      const data = await res.json();
      if (!res.ok) {
        setStatus(data.error || "搜索失败", "warn");
        return;
      }
      const entries = (data.matches || []).map(x => ({
        name: x.path.split("/").pop(),
        path: x.path,
        is_dir: false,
        size: x.size,
        mtime: x.mtime
      }));
      setStatus(`搜索结果: ${entries.length} 条`, "ok");
      renderRows(entries);
    }

    ["dragenter","dragover"].forEach(evt => dropEl.addEventListener(evt, e => {
      e.preventDefault(); e.stopPropagation(); dropEl.classList.add("active");
    }));
    ["dragleave","drop"].forEach(evt => dropEl.addEventListener(evt, e => {
      e.preventDefault(); e.stopPropagation(); dropEl.classList.remove("active");
    }));
    dropEl.addEventListener("drop", e => queueFiles(e.dataTransfer.files));
    pickerEl.addEventListener("change", e => queueFiles(e.target.files));

    document.getElementById("btnUpload").addEventListener("click", uploadNow);
    document.getElementById("btnClear").addEventListener("click", () => {
      queued = [];
      pickerEl.value = "";
      setStatus("已清空上传队列", "ok");
    });
    document.getElementById("btnRefresh").addEventListener("click", async () => {
      await openDir(currentPath);
      await loadStatus();
    });
    document.getElementById("btnRoot").addEventListener("click", () => openDir(""));
    document.getElementById("btnUp").addEventListener("click", () => {
      if (!currentPath) return;
      const parts = currentPath.split("/");
      parts.pop();
      openDir(parts.join("/"));
    });
    document.getElementById("btnSearch").addEventListener("click", searchFiles);
    searchInputEl.addEventListener("keydown", (e) => {
      if (e.key === "Enter") searchFiles();
    });

    (async () => {
      await loadStatus();
      await openDir("");
    })();
  </script>
</body>
</html>
"""


def utc_now_str():
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def iso_mtime(path: Path):
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()


def sanitize_name(name: str) -> str:
    base = Path(name).name.replace("\x00", "").strip()
    base = base.replace("/", "_").replace("\\", "_")
    return base or f"file_{utc_now_str()}"


def sanitize_rel_path(path_value: str) -> str:
    p = (path_value or "").strip().replace("\\", "/")
    p = posixpath.normpath("/" + p).lstrip("/")
    if p in ("", "."):
        return ""
    return p


def local_lan_ip() -> str:
    ip = ""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
        sock.close()
    except Exception:
        ip = ""
    return ip


class DropHandler(BaseHTTPRequestHandler):
    repo_root = Path(".").resolve()
    default_drop_dir = "shared_drop"
    host = "127.0.0.1"
    port = 8765

    def _resolve_under_root(self, rel_path: str):
        rel = sanitize_rel_path(rel_path)
        target = (self.repo_root / rel).resolve()
        root = self.repo_root.resolve()
        if target != root and root not in target.parents:
            raise ValueError("path out of repo root")
        return rel, target

    def _send_json(self, payload, code=200):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, body: str):
        data = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_file(self, file_path: Path):
        mime, _ = mimetypes.guess_type(str(file_path))
        mime = mime or "application/octet-stream"
        size = file_path.stat().st_size
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(size))
        self.send_header("Content-Disposition", f'attachment; filename="{quote(file_path.name)}"')
        self.end_headers()
        with file_path.open("rb") as f:
            shutil.copyfileobj(f, self.wfile)

    def _latest_files(self, limit=16):
        drop_root = self.repo_root / self.default_drop_dir
        if not drop_root.exists():
            return []
        files = [p for p in drop_root.rglob("*") if p.is_file()]
        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return [str(p.relative_to(self.repo_root)).replace(os.sep, "/") for p in files[:limit]]

    def _list_dir(self, rel_path: str):
        rel, target = self._resolve_under_root(rel_path)
        if not target.exists():
            raise FileNotFoundError("path not found")
        if not target.is_dir():
            raise NotADirectoryError("path is not directory")

        entries = []
        for p in target.iterdir():
            if p.name == ".git":
                continue
            is_dir = p.is_dir()
            entries.append(
                {
                    "name": p.name,
                    "path": str(p.relative_to(self.repo_root)).replace(os.sep, "/"),
                    "is_dir": is_dir,
                    "size": None if is_dir else p.stat().st_size,
                    "mtime": iso_mtime(p),
                }
            )
        entries.sort(key=lambda x: (not x["is_dir"], x["name"].lower()))
        return rel, entries

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path in ("/", "/index.html"):
            self._send_html(HTML_PAGE)
            return

        if path == "/api/status":
            lan_ip = local_lan_ip()
            lan_hint = f"http://{lan_ip}:{self.port}" if lan_ip else "N/A"
            self._send_json(
                {
                    "repo_root": str(self.repo_root),
                    "latest_files": self._latest_files(),
                    "lan_hint": lan_hint,
                    "bind": f"{self.host}:{self.port}",
                }
            )
            return

        if path == "/api/list":
            req_path = qs.get("path", [""])[0]
            try:
                rel, entries = self._list_dir(req_path)
                self._send_json({"ok": True, "path": rel, "entries": entries})
            except Exception as e:
                self._send_json({"error": str(e)}, code=400)
            return

        if path == "/api/search":
            q = (qs.get("q", [""])[0] or "").strip().lower()
            if not q:
                self._send_json({"matches": []})
                return
            matches = []
            for p in self.repo_root.rglob("*"):
                if p.name == ".git" or ".git" in p.parts:
                    continue
                if not p.is_file():
                    continue
                rel = str(p.relative_to(self.repo_root)).replace(os.sep, "/")
                if q in rel.lower():
                    matches.append({"path": rel, "size": p.stat().st_size, "mtime": iso_mtime(p)})
                if len(matches) >= 300:
                    break
            self._send_json({"matches": matches})
            return

        if path == "/api/download":
            req_path = qs.get("path", [""])[0]
            try:
                _, target = self._resolve_under_root(req_path)
                if not target.exists() or not target.is_file():
                    self._send_json({"error": "file not found"}, code=404)
                    return
                self._send_file(target)
            except Exception as e:
                self._send_json({"error": str(e)}, code=400)
            return

        self._send_json({"error": "not found"}, code=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/api/upload":
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
        if "subdir" in form and getattr(form["subdir"], "value", None) is not None:
            subdir = sanitize_rel_path(form["subdir"].value) or subdir

        try:
            rel, target_root = self._resolve_under_root(subdir)
        except Exception as e:
            self._send_json({"error": f"invalid target path: {e}"}, code=400)
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
                out_path = target_root / f"{out_path.stem}_{utc_now_str()}{out_path.suffix}"

            with out_path.open("wb") as f:
                data = item.file.read()
                if isinstance(data, str):
                    data = data.encode("utf-8")
                f.write(data)
            saved.append(str(out_path.relative_to(self.repo_root)).replace(os.sep, "/"))

        if not saved:
            self._send_json({"error": "no files uploaded"}, code=400)
            return
        self._send_json({"ok": True, "saved": saved, "target_path": rel})

    def log_message(self, fmt, *args):
        return


def main():
    parser = argparse.ArgumentParser(description="Repo file transfer UI (browse + download + drag upload).")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--drop-dir", default="shared_drop")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    if not repo_root.exists():
        raise RuntimeError(f"repo root not found: {repo_root}")

    DropHandler.repo_root = repo_root
    DropHandler.default_drop_dir = sanitize_rel_path(args.drop_dir) or "shared_drop"
    DropHandler.host = args.host
    DropHandler.port = args.port

    srv = ThreadingHTTPServer((args.host, args.port), DropHandler)
    print(f"[file-drop-ui] repo={repo_root}")
    print(f"[file-drop-ui] bind={args.host}:{args.port}")
    lan_ip = local_lan_ip()
    if lan_ip:
        print(f"[file-drop-ui] LAN URL: http://{lan_ip}:{args.port}")
    print(f"[file-drop-ui] open http://127.0.0.1:{args.port}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()


if __name__ == "__main__":
    main()
