#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import tempfile
import time
import zlib
import platform
from datetime import datetime
from pathlib import Path
from typing import Dict, Set, Optional

# ============================================================
# CONFIGURAÇÕES
# ============================================================

IMAGE_EXTS = {
    ".jpg", ".jpeg", ".png", ".tif", ".tiff",
    ".webp", ".heic", ".heif", ".bmp", ".gif"
}
VIDEO_EXTS = {
    ".mp4", ".mov", ".m4v", ".mkv", ".avi",
    ".wmv", ".mts", ".m2ts", ".3gp", ".webm"
}
DEFAULT_EXTS = IMAGE_EXTS | VIDEO_EXTS

DATE_TAGS_PREFERRED = [
    "EXIF:DateTimeOriginal",
    "EXIF:CreateDate",
    "QuickTime:CreateDate",
    "QuickTime:CreationDate",
    "MediaCreateDate",
    "TrackCreateDate",
    "CreateDate",
    "Keys:CreationDate",
    "FileCreateDate",
    "FileModifyDate",
]

EXIFTOOL_DATE_FMT = "%Y-%m-%dT%H:%M:%S%z"
PARTIAL_HASH_BYTES = 64 * 1024
MIN_VALID_YEAR = 1970
MAX_VALID_YEAR = 2100

IS_WINDOWS = platform.system() == "Windows"

# ============================================================
# HASH PARCIAL
# ============================================================

def fast_partial_hash(path: str) -> str:
    crc = 0
    with open(path, "rb") as f:
        crc = zlib.crc32(f.read(PARTIAL_HASH_BYTES), crc)
    return f"{crc:08x}"

# ============================================================
# DATETIME
# ============================================================

def safe_datetime_from_timestamp(ts: float) -> Optional[datetime]:
    try:
        dt = datetime.fromtimestamp(ts)
        if MIN_VALID_YEAR <= dt.year <= MAX_VALID_YEAR:
            return dt
    except Exception:
        pass
    return None

# ============================================================
# VARREDURA
# ============================================================

def fast_walk_entries(root: str, exts: Set[str]):
    stack = [root]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for e in it:
                    try:
                        if e.is_dir(follow_symlinks=False):
                            stack.append(e.path)
                        elif e.is_file(follow_symlinks=False):
                            if Path(e.name).suffix.lower() in exts:
                                st = e.stat(follow_symlinks=False)
                                yield os.path.abspath(e.path), e.name, st.st_mtime
                    except OSError:
                        continue
        except OSError:
            continue

# ============================================================
# EXIFTOOL
# ============================================================

def parse_exif_dt(value: str) -> Optional[datetime]:
    try:
        s = value.strip()
        if s.endswith(":00"):
            s = s[:-3] + s[-2:]
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
    except Exception:
        return None

def exiftool_dates(file_list: list[str]) -> Dict[str, datetime]:
    if not file_list:
        return {}

    try:
        subprocess.check_output(["exiftool", "-ver"], stderr=subprocess.DEVNULL)
    except Exception:
        print("[WARN] exiftool não disponível. Usando mtime.")
        return {}

    tmp = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
    try:
        tmp.write("\n".join(file_list))
        tmp.close()

        args = ["exiftool", "-j", "-d", EXIFTOOL_DATE_FMT]
        for tag in DATE_TAGS_PREFERRED:
            args.append(f"-{tag}")
        args += ["-@", tmp.name]

        proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        if not proc.stdout:
            return {}

        data = json.loads(proc.stdout)
        result = {}
        for item in data:
            src = os.path.abspath(item.get("SourceFile", ""))
            for tag in DATE_TAGS_PREFERRED:
                if tag in item:
                    dt = parse_exif_dt(item[tag])
                    if dt:
                        result[src] = dt
                        break
        return result
    finally:
        os.unlink(tmp.name)

# ============================================================
# INVENTÁRIO
# ============================================================

def ensure_inventory(inv: Path):
    inv.parent.mkdir(parents=True, exist_ok=True)
    inv.touch(exist_ok=True)

def load_inventory(inv: Path) -> Set[str]:
    return {l.strip() for l in inv.read_text().splitlines() if l.strip()}

def rewrite_inventory(inv: Path, keys: Set[str]):
    tmp = inv.with_suffix(".tmp")
    with tmp.open("w") as f:
        for k in sorted(keys):
            f.write(k + "\n")
    tmp.replace(inv)

# ============================================================
# ARMAZENAMENTO
# ============================================================

def unique_target(dest: Path, name: str) -> Path:
    base, ext = Path(name).stem, Path(name).suffix
    p = dest / name
    i = 1
    while p.exists():
        p = dest / f"{base}_{i:03d}{ext}"
        i += 1
    return p

def safe_copy(src: str, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)

def safe_link(src: str, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    if IS_WINDOWS:
        import win32com.client
        shell = win32com.client.Dispatch("WScript.Shell")
        shortcut = shell.CreateShortcut(str(dst.with_suffix(".lnk")))
        shortcut.TargetPath = src
        shortcut.Save()
    else:
        os.symlink(src, dst)

def store_new_file(src: str, main_dir: Path, extra_dir: Optional[Path], name: str):
    main_target = unique_target(main_dir, name)
    safe_copy(src, main_target)
    if extra_dir:
        link_target = unique_target(extra_dir, name)
        safe_link(main_target, link_target)
    return main_target

# ============================================================
# MAIN
# ============================================================

def main():
    ap = argparse.ArgumentParser(
        description="Organiza fotos e vídeos com opção de exclusão da origem após cópia."
    )
    ap.add_argument("source", type=Path)
    ap.add_argument("dest", type=Path)
    ap.add_argument("--inventory-file", type=Path, default=Path("~/inventory_files.txt").expanduser())
    ap.add_argument("--copy-new-to", type=Path)
    ap.add_argument("--delete-source", action="store_true",
                    help="Remove o arquivo da origem após copiar (PERIGOSO)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.delete_source:
        print("\n[WARNING] A opção --delete-source irá APAGAR arquivos da origem.")
        print("Digite DELETE para confirmar e prosseguir:")
        confirm = input("> ").strip()
        if confirm != "DELETE":
            print("[ABORT] Confirmação inválida. Execução cancelada.")
            return

    source = args.source.resolve()
    dest = args.dest.resolve()
    extra = args.copy_new_to.resolve() if args.copy_new_to else None
    inv = args.inventory_file.resolve()

    print("[STATUS] Preparando inventário...")
    ensure_inventory(inv)
    inventory = load_inventory(inv)
    print(f"[STATUS] Inventário carregado ({len(inventory)} entradas)")

    scanned = list(fast_walk_entries(str(source), DEFAULT_EXTS))
    new_files = []

    for p, n, m in scanned:
        key = fast_partial_hash(p)
        if key not in inventory:
            new_files.append((p, n, m, key))

    print(f"[STATUS] Arquivos novos: {len(new_files)}")

    print("[STATUS] Extraindo datas EXIF (apenas novos)...")
    exif_map = exiftool_dates([p for p, _, _, _ in new_files])

    print("[STATUS] Processando...")
    total = len(new_files)
    last_percent = -1

    for idx, (src, name, mtime, key) in enumerate(new_files, 1):
        percent = (idx * 100) // total if total else 100
        if percent != last_percent:
            print(f"[PROGRESS] {percent}% ({idx}/{total})")
            last_percent = percent

        dt = exif_map.get(src) or safe_datetime_from_timestamp(mtime) or datetime(1970, 1, 1)
        target_dir = dest / f"{dt.year:04d}" / f"{dt.month:02d}"
        extra_dir = extra / f"{dt.year:04d}" / f"{dt.month:02d}" if extra else None

        if not args.dry_run:
            target = store_new_file(src, target_dir, extra_dir, name)
            inventory.add(key)

            if args.delete_source:
                try:
                    os.unlink(src)
                except Exception as e:
                    print(f"[ERROR] Falha ao remover {src}: {e}")

    if not args.dry_run:
        rewrite_inventory(inv, inventory)

    print("[DONE] Execução finalizada.")

if __name__ == "__main__":
    main()

