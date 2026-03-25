import os
from pathlib import Path

def list_all_files(root_dir=".", output_file="file_list.txt"):
    root = Path(root_dir).resolve()
    output_path = root / output_file

    with open(output_path, "w", encoding="utf-8") as out:
        for current_dir, dirs, files in os.walk(root):
            if ".git" in dirs:
                dirs.remove(".git")
            if "venv" in dirs:
                dirs.remove("venv")
            for file in files:
                file_path = Path(current_dir) / file
                rel_path = file_path.relative_to(root)
                out.write(str(rel_path) + "\n")

    print(f"ファイル一覧を {output_path} に保存しました。")

if __name__ == "__main__":
    list_all_files()
