"""高性能 PDF 预检脚本 (在原版基础上进行提速)

主要优化点:
1. 并行: 使用 ProcessPoolExecutor 多进程并发检查 (适合 CPU + I/O 混合任务, 规避 GIL)。
2. 样本化字体检查: 默认仅检查前 N 页或按“分布抽样”策略, 大幅减少全量 get_page_fonts 开销。
3. 早停策略: 发现未嵌入字体 / 关键异常时可提前结束后续字体扫描。
4. 条件执行 qpdf: 默认只有在前置检查出现潜在问题时才调用 qpdf (减少外部子进程调用次数)。
5. 缓存: 基于 (文件路径, size, mtime) 的结果缓存, 未变化文件直接跳过。
6. 输出聚合: 子进程不逐行打印, 主进程统一整洁输出, 降低控制台 I/O 开销。

与原脚本兼容:
    原脚本: 0_pre_screening.py (串行 & 全量检查)
    本脚本: 0_pre_screening_fast.py (可配置快速模式)

使用示例:
    python 0_pre_screening_fast.py temp/test \
        --processes 4 \
        --font-scan-pages 5 \
        --font-scan-strategy firstN \
        --conditional-qpdf \
        --cache .precheck_cache.json

参数说明请运行: python 0_pre_screening_fast.py -h
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

import fitz  # PyMuPDF
from pypdf import PdfReader

# ------------------------- 配置与数据结构 ------------------------- #


@dataclass
class PDFCheckConfig:
    enable_qpdf: bool = True
    conditional_qpdf: bool = True  # 若前置全部正常则跳过 qpdf
    font_scan_pages: int = 5  # <=0 表示全量
    font_scan_strategy: str = "firstN"  # firstN | spread
    stop_on_first_unembedded: bool = True
    processes: int = 4
    qpdf_timeout: int = 25
    cache_path: Optional[str] = None
    ignore_dirs: Tuple[str, ...] = tuple()  # 可配置忽略目录前缀
    verbose: bool = True


@dataclass
class PDFCheckResult:
    path: str
    issues: List[str]
    info: List[str]
    size: int
    mtime: float
    duration: float

    def to_cache_entry(self):
        return {
            "size": self.size,
            "mtime": self.mtime,
            "issues": self.issues,
            "info": self.info,
        }


# ------------------------- 工具函数 ------------------------- #


def run_qpdf_check(filepath: str, timeout: int) -> Tuple[bool, List[str], dict]:
    if not shutil.which("qpdf"):
        return False, [], {}
    try:
        proc = subprocess.run(
            ["qpdf", "--check", filepath], capture_output=True, text=True, timeout=timeout
        )
    except Exception as e:  # pragma: no cover - 外部进程异常
        return True, [f"qpdf 执行失败: {e}"], {}
    output = (proc.stdout + proc.stderr).splitlines()
    warnings = [l.strip() for l in output if "WARNING:" in l]
    linearized = None
    for l in output:
        if "File is linearized" in l:
            linearized = True
        elif "File is not linearized" in l:
            linearized = False
    return True, warnings, {"linearized": linearized}


def select_font_pages(total_pages: int, cfg: PDFCheckConfig) -> List[int]:
    if cfg.font_scan_pages <= 0 or cfg.font_scan_pages >= total_pages:
        return list(range(total_pages))
    n = cfg.font_scan_pages
    if cfg.font_scan_strategy == "spread" and total_pages > n:
        # 均匀抽样页索引
        step = total_pages / n
        return sorted({int(i * step) for i in range(n)})
    # 默认 firstN
    return list(range(min(n, total_pages)))


# ------------------------- 核心单文件检查逻辑 (供多进程调用) ------------------------- #


def check_single_pdf(path: str, cfg: PDFCheckConfig) -> PDFCheckResult:
    start = time.time()
    issues: List[str] = []
    info: List[str] = []
    size = os.path.getsize(path)
    mtime = os.path.getmtime(path)

    # 0. 空文件
    if size == 0:
        issues.append("文件大小为 0 字节。")
        return PDFCheckResult(path, issues, info, size, mtime, time.time() - start)

    opened = False
    doc = None
    page_count = 0
    first_page_text_empty = False

    try:
        doc = fitz.open(path)
        opened = True
        page_count = len(doc)
        if page_count == 0:
            issues.append("PyMuPDF: 文件页数为 0。")
        else:
            try:
                page0 = doc.load_page(0)
                page0.get_pixmap(dpi=48)  # 更低 dpi 更快即可验证可渲染
                text = page0.get_text().strip()
                if not text and page_count > 1:
                    first_page_text_empty = True
                    issues.append("警告: 可能是纯图片PDF (首页无可提取文本)。")
            except Exception as e:  # pragma: no cover - 特殊渲染异常
                issues.append(f"PyMuPDF: 第一页处理失败: {e}")
    except Exception as e:
        issues.append(f"PyMuPDF 无法打开: {e}")

    # 1. 回退 pypdf
    if not opened:
        try:
            with open(path, "rb") as f:
                reader = PdfReader(f)
                if getattr(reader, "is_encrypted", False):
                    issues.append("pypdf: 文件已加密。")
                else:
                    pc = len(reader.pages)
                    if pc == 0:
                        issues.append("pypdf: 文件页数为 0。")
                    else:
                        info.append(f"pypdf: 可解析 ({pc} 页)。")
        except Exception as e:
            issues.append(f"pypdf 打开失败: {e}")
    else:
        # 2. 字体抽样检查
        try:
            pages_to_check = select_font_pages(page_count, cfg)
            non_embedded_fonts = []
            for pno in pages_to_check:
                try:
                    fonts = doc.get_page_fonts(pno)
                except Exception as e:  # pragma: no cover - 罕见页级异常
                    issues.append(f"警告: 第 {pno+1} 页字体信息获取失败: {e}")
                    continue
                for font in fonts:
                    embedded = font[3] if len(font) > 3 else None
                    name = font[1] if len(font) > 1 else str(font)
                    if embedded is False and name not in non_embedded_fonts:
                        non_embedded_fonts.append(name)
                        if cfg.stop_on_first_unembedded:
                            break
                if cfg.stop_on_first_unembedded and non_embedded_fonts:
                    break
            if non_embedded_fonts:
                issues.append(
                    "警告: 发现未嵌入字体(抽样): " + ", ".join(non_embedded_fonts)
                )
            elif cfg.font_scan_pages > 0 and page_count > cfg.font_scan_pages:
                info.append(
                    f"字体检查: 抽样 {len(pages_to_check)}/{page_count} 页策略={cfg.font_scan_strategy}"
                )
        except Exception as e:
            issues.append(f"字体检查异常: {e}")

    if doc:
        try:
            doc.close()
        except Exception:  # pragma: no cover
            pass

    # 3. 条件 qpdf
    run_qpdf = cfg.enable_qpdf and (not cfg.conditional_qpdf or issues)
    if run_qpdf:
        qpdf_available, qpdf_warnings, meta = run_qpdf_check(path, cfg.qpdf_timeout)
        if qpdf_available:
            if meta.get("linearized") is True:
                info.append("qpdf: 文件 linearized")
            elif meta.get("linearized") is False:
                info.append("qpdf: 非 linearized")
            if qpdf_warnings:
                linearization_related = any(
                    "shared object" in w or "hint table" in w for w in qpdf_warnings
                )
                if linearization_related:
                    issues.append(
                        "结构警告: linearization hint table/共享对象索引异常 (可能影响流式解析)。"
                    )
                sample = qpdf_warnings[:5]
                issues.append("qpdf WARNING 示例: " + " | ".join(sample))
        else:
            info.append("qpdf 未安装, 跳过。")
    elif cfg.enable_qpdf and cfg.conditional_qpdf and not issues:
        info.append("qpdf: 预检正常 -> 已跳过 (conditional)。")

    return PDFCheckResult(path, issues, info, size, mtime, time.time() - start)


# ------------------------- 缓存处理 ------------------------- #


def load_cache(path: str) -> Dict[str, dict]:
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_cache(path: str, data: Dict[str, dict]):
    if not path:
        return
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def is_cache_valid(entry: dict, size: int, mtime: float) -> bool:
    return entry.get("size") == size and abs(entry.get("mtime", 0) - mtime) < 1e-6


# ------------------------- 主流程 ------------------------- #


def gather_pdfs(root: str, ignore_dirs: Tuple[str, ...]) -> List[str]:
    pdfs = []
    root = os.path.abspath(root)
    for r, dirs, files in os.walk(root):
        # 忽略目录前缀 (简单匹配路径中是否包含)
        skip = False
        for ig in ignore_dirs:
            if ig and ig in r:
                skip = True
                break
        if skip:
            continue
        for fn in files:
            if fn.lower().endswith(".pdf"):
                pdfs.append(os.path.join(r, fn))
    return pdfs


def strict_check_pdfs_fast(directory: str, cfg: PDFCheckConfig):
    start_all = time.time()
    pdf_files = gather_pdfs(directory, cfg.ignore_dirs)
    if not pdf_files:
        print(f"目录 {directory} 下未找到 PDF。")
        return

    cache = load_cache(cfg.cache_path) if cfg.cache_path else {}
    reusable: Dict[str, PDFCheckResult] = {}
    to_process: List[str] = []

    for p in pdf_files:
        size = os.path.getsize(p)
        mtime = os.path.getmtime(p)
        c_entry = cache.get(p)
        if c_entry and is_cache_valid(c_entry, size, mtime):
            reusable[p] = PDFCheckResult(
                p, c_entry.get("issues", []), c_entry.get("info", []), size, mtime, 0.0
            )
        else:
            to_process.append(p)

    if cfg.verbose:
        print(
            f"总文件: {len(pdf_files)} | 使用缓存: {len(reusable)} | 待检查: {len(to_process)} | 并发: {cfg.processes}"
        )

    results: List[PDFCheckResult] = list(reusable.values())

    if to_process:
        with cf.ProcessPoolExecutor(max_workers=cfg.processes) as executor:
            futures = [executor.submit(check_single_pdf, p, cfg) for p in to_process]
            for fut in cf.as_completed(futures):
                res = fut.result()
                results.append(res)

    # 排序输出 (异常优先, 其次路径)
    abnormal = [r for r in results if r.issues]
    normal = [r for r in results if not r.issues]
    abnormal.sort(key=lambda r: r.path)
    normal.sort(key=lambda r: r.path)

    print("\n========== 严格预检(快速版)完成 ==========")
    print(f"耗时: {time.time() - start_all:.2f}s  (含缓存复用)")
    print(f"异常/警告文件: {len(abnormal)} / 总计 {len(results)}\n")

    for r in abnormal:
        print(f"[异常/警告] {r.path}")
        for iss in r.issues:
            print(f"  - {iss}")
        if r.info:
            print("    (信息: " + "; ".join(r.info) + ")")
    if cfg.verbose and normal:
        print("\n[通过] 以下文件未发现显著问题 (省略信息条目)... 共", len(normal))

    # 更新缓存
    if cfg.cache_path:
        new_cache = {r.path: r.to_cache_entry() for r in results}
        save_cache(cfg.cache_path, new_cache)
        if cfg.verbose:
            print(f"缓存已写入: {cfg.cache_path}")
    print("========================================")


# ------------------------- CLI ------------------------- #


def parse_args(argv: List[str]):
    p = argparse.ArgumentParser(description="PDF 严格预检 (快速并行版)")
    p.add_argument("directory", nargs="?", default="temp/test", help="待扫描目录")
    p.add_argument("--no-qpdf", action="store_true", help="完全禁用 qpdf")
    p.add_argument(
        "--no-conditional-qpdf",
        action="store_true",
        help="取消条件执行, 对所有文件都跑 qpdf",
    )
    p.add_argument("--font-scan-pages", type=int, default=5, help="字体检查抽样页数 (<=0 全量)")
    p.add_argument(
        "--font-scan-strategy",
        choices=["firstN", "spread"],
        default="firstN",
        help="字体抽样策略: firstN=前N页, spread=均匀分布",
    )
    p.add_argument(
        "--stop-on-first-unembedded",
        action="store_true",
        help="发现未嵌入字体后立即停止后续页扫描",
    )
    p.add_argument("--processes", type=int, default=4, help="并发进程数")
    p.add_argument("--qpdf-timeout", type=int, default=25, help="qpdf 超时时间秒")
    p.add_argument("--cache", type=str, default=None, help="结果缓存文件路径 (JSON)")
    p.add_argument(
        "--ignore-dirs",
        type=str,
        default="",
        help="以逗号分隔的需忽略目录关键字 (路径中包含即跳过)",
    )
    p.add_argument("--quiet", action="store_true", help="更少输出")
    return p.parse_args(argv)


def main(argv: List[str]):
    args = parse_args(argv)
    if not os.path.isdir(args.directory):
        print(f"错误: 目录 '{args.directory}' 不存在")
        return 2
    cfg = PDFCheckConfig(
        enable_qpdf=not args.no_qpdf,
        conditional_qpdf=not args.no_conditional_qpdf,
        font_scan_pages=args.font_scan_pages,
        font_scan_strategy=args.font_scan_strategy,
        stop_on_first_unembedded=args.stop_on_first_unembedded,
        processes=max(1, args.processes),
        qpdf_timeout=args.qpdf_timeout,
        cache_path=args.cache,
        ignore_dirs=tuple(
            [d.strip() for d in args.ignore_dirs.split(",") if d.strip()] if args.ignore_dirs else []
        ),
        verbose=not args.quiet,
    )
    if cfg.processes == 1:
        # 单进程时仍复用相同逻辑 (少量文件避免进程开销)
        cfg.conditional_qpdf = cfg.conditional_qpdf  # 无变化, 留作说明
    strict_check_pdfs_fast(args.directory, cfg)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI 入口
    sys.exit(main(sys.argv[1:]))
