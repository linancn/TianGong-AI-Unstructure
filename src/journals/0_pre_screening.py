import os
import fitz  # PyMuPDF
from pypdf import PdfReader
from pypdf.errors import PdfReadError

def strict_check_pdfs(directory):
    """
    使用 PyMuPDF 对指定目录中的PDF文件进行更严格的预检。
    如果 PyMuPDF 打开失败，则回退到 pypdf 进行基础检查。
    """
    print(f"开始对目录进行严格预检: {directory}\n")
    abnormal_files = {}

    for root, _, files in os.walk(directory):
        for filename in files:
            if not filename.lower().endswith(".pdf"):
                continue

            filepath = os.path.join(root, filename)
            print(f"--- 正在检查: {filepath} ---")
            
            issues = []

            # 检查1: 文件大小
            if os.path.getsize(filepath) == 0:
                issues.append("文件大小为 0 字节。")
                abnormal_files[filepath] = issues
                print(f"  [异常] {issues[-1]}")
                print("-" * (len(filepath) + 15))
                continue

            doc = None
            try:
                # 主要方案: 尝试用 PyMuPDF (fitz) 打开
                doc = fitz.open(filepath)

                # ... (PyMuPDF 的所有检查) ...
                if len(doc) > 0:
                    try:
                        page = doc.load_page(0)
                        pix = page.get_pixmap(dpi=72) 
                        print("  [正常] PyMuPDF: 第一页渲染成功。")
                    except Exception as e:
                        issues.append(f"PyMuPDF: 页面渲染失败: {e}")
                        print(f"  [异常] {issues[-1]}")

                    text = page.get_text()
                    if not text.strip() and len(doc) > 1:
                        issues.append("警告: PyMuPDF: 可能是纯图片PDF。")
                        print(f"  [警告] {issues[-1]}")
                else:
                    issues.append("PyMuPDF: 文件页数为 0。")
                    print(f"  [异常] {issues[-1]}")

                # 只有在 PyMuPDF 成功时才检查字体 (移到这里)
                non_embedded_fonts = []
                for page_num in range(len(doc)):
                    fonts = doc.get_page_fonts(page_num)
                    for font in fonts:
                        if not font[8] and font[3] not in non_embedded_fonts:
                            non_embedded_fonts.append(font[3])
                if non_embedded_fonts:
                    font_issue = f"警告: 发现未嵌入字体: {', '.join(non_embedded_fonts)}"
                    issues.append(font_issue)
                    print(f"  [警告] {font_issue}")

            except Exception as e:
                # PyMuPDF 打开失败，记录问题并尝试备用方案
                pymupdf_error = f"PyMuPDF 无法打开: {e}"
                print(f"  [警告] {pymupdf_error}")
                issues.append(pymupdf_error)

                # 备用方案: 尝试用 pypdf 打开
                print("  ... 尝试使用 pypdf 进行基础检查 ...")
                try:
                    with open(filepath, "rb") as f:
                        reader = PdfReader(f)
                        if reader.is_encrypted:
                            issues.append("pypdf: 文件已加密。")
                            print(f"  [异常] pypdf: 文件已加密。")
                        elif len(reader.pages) == 0:
                            issues.append("pypdf: 文件页数为 0。")
                            print(f"  [异常] pypdf: 文件页数为 0。")
                        else:
                            # 如果pypdf能成功读取页数，说明文件基本结构是可读的
                            issues.append("pypdf: 文件可基本解析，但与PyMuPDF不兼容。")
                            print(f"  [警告] pypdf: 文件可基本解析 (共 {len(reader.pages)} 页)。")

                except Exception as pypdf_e:
                    issues.append(f"pypdf 也无法打开: {pypdf_e}")
                    print(f"  [异常] pypdf 也无法打开: {pypdf_e}")

            finally:
                if doc:
                    doc.close()

            # 过滤掉仅为不兼容警告的信息，如果pypdf能打开
            final_issues = [iss for iss in issues if not "文件可基本解析" in iss]
            if final_issues:
                abnormal_files[filepath] = final_issues
            else:
                print("  [正常] 文件通过所有严格检查。")
            
            print("-" * (len(filepath) + 15))

    print("\n========== 严格预检完成 ==========")
    if abnormal_files:
        print(f"发现 {len(abnormal_files)} 个存在问题或警告的文件:")
        for file, issues in abnormal_files.items():
            print(f"\n文件: {file}")
            for issue in issues:
                print(f"  - {issue}")
    else:
        print("所有PDF文件均未发现明显异常。")
    print("==================================")


if __name__ == "__main__":
    # 确保 pypdf 已安装: pip install pypdf
    target_directory = "temp/test"
    
    if os.path.isdir(target_directory):
        strict_check_pdfs(target_directory)
    else:
        print(f"错误: 目录 '{target_directory}' 不存在。")