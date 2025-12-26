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
PARTIAL_HASH_BYTES = 64 * 1024  # 64 KB

# Limites defensivos para timestamps (ajuste se quiser)
# (evita crash em NAS com timestamps corrompidos)
MIN_VALID_YEAR = 1970
MAX_VALID_YEAR = 2100


# ============================================================
# HASH PARCIAL (CRC32)
# ============================================================

def fast_partial_hash(path: str) -> str:
    crc = 0
    with open(path, "rb") as f:
        chunk = f.read(PARTIAL_HASH_BYTES)
        crc = zlib.crc32(chunk, crc)
    return f"{crc:08x}"


# ============================================================
# DATETIME FALLBACK SEGURO
# ============================================================

def safe_datetime_from_timestamp(ts: float) -> Optional[datetime]:
    """
    Converte timestamp para datetime, mas sem derrubar o script.
    Retorna None se o timestamp estiver inválido/fora de range.
    """
    try:
        dt = datetime.fromtimestamp(ts)
        if dt.year < MIN_VALID_YEAR or dt.year > MAX_VALID_YEAR:
            return None
        return dt
    except Exception:
        return None


# ============================================================
# VARREDURA OTIMIZADA
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
                            dot = e.name.rfind(".")
                            if dot == -1:
                                continue
                            if e.name[dot:].lower() not in exts:
                                continue
                            st = e.stat(follow_symlinks=False)
                            yield os.path.abspath(e.path), e.name, st.st_mtime
                    except OSError:
                        continue
        except OSError:
            continue


# ============================================================
# EXIFTOOL BATCH (TOLERANTE A ERROS)
# ============================================================

def parse_exif_dt(value: str) -> Optional[datetime]:
    if not value:
        return None
    s = str(value).strip()
    if not s or s == "-":
        return None

    # ExifTool retorna algo como 2020-01-01T12:34:56-0300
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
    except Exception:
        # Alguns casos podem vir com offset -03:00
        if len(s) >= 6 and (s[-6] in "+-") and (s[-3] == ":"):
            s2 = s[:-3] + s[-2:]
            try:
                return datetime.strptime(s2, "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
            except Exception:
                return None
        return None


def exiftool_dates(file_list: list[str]) -> Dict[str, datetime]:
    if not file_list:
        return {}

    try:
        subprocess.check_output(["exiftool", "-ver"], stderr=subprocess.DEVNULL)
    except Exception:
        print("[WARN] exiftool não disponível. Usando mtime como fallback.")
        return {}

    tmp = tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8")
    try:
        tmp.write("\n".join(file_list))
        tmp.close()

        args = [
            "exiftool",
            "-j",
            "-api", "LargeFileSupport=1",
            "-api", "QuickTimeUTC=1",
            "-d", EXIFTOOL_DATE_FMT,
        ]
        for tag in DATE_TAGS_PREFERRED:
            args.append(f"-{tag}")
        args += ["-@", tmp.name]

        proc = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        if proc.returncode != 0:
            print("[WARN] ExifTool retornou erro para alguns arquivos. Usando fallback onde necessário.")

        if not proc.stdout.strip():
            return {}

        try:
            data = json.loads(proc.stdout)
        except Exception:
            print("[WARN] Falha ao interpretar saída do ExifTool. Usando mtime.")
            return {}

        result: Dict[str, datetime] = {}
        for item in data:
            src = os.path.abspath(item.get("SourceFile", ""))
            dt: Optional[datetime] = None
            for tag in DATE_TAGS_PREFERRED:
                if tag in item:
                    dt = parse_exif_dt(item[tag])
                    if dt:
                        break
            if dt:
                # saneamento mínimo também (evita datas absurdas)
                if MIN_VALID_YEAR <= dt.year <= MAX_VALID_YEAR:
                    result[src] = dt
        return result

    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass


# ============================================================
# INVENTÁRIO (HASH ONLY)
# ============================================================

def ensure_inventory(inv: Path) -> None:
    inv.parent.mkdir(parents=True, exist_ok=True)
    if not inv.exists():
        inv.write_text("", encoding="utf-8")


def load_inventory(inv: Path) -> Set[str]:
    return {l.strip() for l in inv.read_text(encoding="utf-8").splitlines() if l.strip()}


def rewrite_inventory(inv: Path, keys: Set[str]) -> None:
    tmp = inv.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for k in sorted(keys):
            f.write(k + "\n")
    tmp.replace(inv)


# ============================================================
# CÓPIA SEGURA
# ============================================================

def unique_target(dest: Path, name: str) -> Path:
    base, ext = Path(name).stem, Path(name).suffix
    p = dest / name
    i = 1
    while p.exists():
        p = dest / f"{base}_{i:03d}{ext}"
        i += 1
    return p


def safe_copy(src: str, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Organiza fotos e vídeos por data real com inventário por hash parcial (robusto)."
    )
    ap.add_argument("source", type=Path)
    ap.add_argument("dest", type=Path)
    ap.add_argument("--inventory-file", type=Path,
                    default=Path("~/inventory_files.txt").expanduser())
    ap.add_argument("--dupe-action", choices=["skip", "to-duplicates"], default="skip")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    source = args.source.resolve()
    dest = args.dest.resolve()
    inv_path = args.inventory_file.expanduser().resolve()

    # -------- INVENTÁRIO STATUS --------
    t0 = time.perf_counter()
    if inv_path.exists():
        print("[STATUS] Arquivo de inventário encontrado. Carregando inventário existente...")
        inventory = load_inventory(inv_path)
        created = False
    else:
        print("[STATUS] Arquivo de inventário não encontrado. Criando novo inventário...")
        if not args.dry_run:
            ensure_inventory(inv_path)
        inventory = set()
        created = True

    elapsed = time.perf_counter() - t0
    if created:
        print(f"[STATUS] Inventário criado e carregado em {elapsed:.2f}s: {inv_path}")
    else:
        print(f"[STATUS] Inventário carregado em {elapsed:.2f}s: {inv_path}")

    print("[STATUS] Iniciando processamento dos arquivos...")

    # -------- ESTATÍSTICAS --------
    total_encontrados = 0
    novos_detectados = 0
    duplicados_detectados = 0
    copiados_destino = 0
    copiados_duplicados = 0
    ignorados = 0
    erros = 0
    mtime_invalidos = 0
    sem_data_exif = 0

    scanned = list(fast_walk_entries(str(source), DEFAULT_EXTS))
    exif_map = exiftool_dates([p for p, _, _ in scanned])

    items = []
    for abs_path, name, mtime in scanned:
        dt = exif_map.get(abs_path)
        if dt is None:
            sem_data_exif += 1
            dt = safe_datetime_from_timestamp(mtime)
            if dt is None:
                mtime_invalidos += 1
                # fallback final: usa uma data "neutra" para não quebrar ordenação
                dt = datetime(1970, 1, 1)
        key = fast_partial_hash(abs_path)
        items.append((dt, abs_path, name, key))

    items.sort(key=lambda x: x[0])

    total = len(items)
    processed = 0
    last_percent = -1

    for dt, abs_path, name, key in items:
        processed += 1
        total_encontrados += 1

        percent = (processed * 100) // total if total else 100
        if percent != last_percent:
            print(f"[PROGRESS] {percent}% ({processed}/{total})")
            last_percent = percent

        try:
            if key in inventory:
                duplicados_detectados += 1
                if args.dupe_action == "skip":
                    ignorados += 1
                    continue
                target_dir = dest / "_duplicates" / f"{dt.year:04d}" / f"{dt.month:02d}"
                copiados_duplicados += 1
            else:
                novos_detectados += 1
                target_dir = dest / f"{dt.year:04d}" / f"{dt.month:02d}"
                copiados_destino += 1

            if not args.dry_run:
                target = unique_target(target_dir, name)
                safe_copy(abs_path, target)
                inventory.add(key)

        except Exception:
            erros += 1
            continue

    if not args.dry_run:
        rewrite_inventory(inv_path, inventory)

    # -------- RESUMO --------
    print("\n========== RESUMO DA EXECUÇÃO ==========")
    print(f"Arquivos encontrados:            {total_encontrados}")
    print(f"Arquivos novos:                  {novos_detectados}")
    print(f"Duplicados detectados:           {duplicados_detectados}")
    print(f"Copiados para destino:           {copiados_destino}")
    print(f"Copiados para _duplicates:       {copiados_duplicados}")
    print(f"Ignorados:                      {ignorados}")
    print(f"Erros:                          {erros}")
    print(f"Sem data EXIF/QuickTime:         {sem_data_exif}")
    print(f"mtime inválido (NAS/corrompido): {mtime_invalidos}")
    if args.dry_run:
        print("Modo DRY-RUN: nenhuma cópia realizada")
    print("=======================================\n")

    print("[DONE] Execução finalizada com sucesso.")


if __name__ == "__main__":
    main()

