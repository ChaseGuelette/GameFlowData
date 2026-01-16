import os


def list_files(startpath):
    # 1. Whitelist: Only allow these directories at the top level
    allowed_root_dirs = {"database", "docs", "notebooks", "src", "tests", ".claude", ".claude-flow", ".session"}

    # 2. Blacklist: Always ignore these (even inside allowed folders)
    ignore_everywhere = {
        "__pycache__",
        ".git",
        ".DS_Store",
        ".venv",
        "venv",
        ".idea",
        ".vscode",
        "node_modules",
        ".pytest_cache",
        ".ruff_cache",
        "htmlcov",
        ".solokit-backup",
        "data",
    }

    for root, dirs, files in os.walk(startpath):
        # Check if we are currently in the root directory
        is_root = os.path.abspath(root) == os.path.abspath(startpath)

        if is_root:
            # In the root, strictly filter to include ONLY the allowed dirs
            # We modify dirs[:] in place so os.walk does not traverse excluded folders
            dirs[:] = [d for d in dirs if d in allowed_root_dirs]
        else:
            # In subfolders, just remove the standard noise
            dirs[:] = [d for d in dirs if d not in ignore_everywhere]

        # Sort lists to make the output deterministic and readable
        dirs.sort()
        files.sort()

        # Formatting and Printing
        level = root.replace(startpath, "").count(os.sep)
        indent = " " * 4 * level
        print(f"{indent}{os.path.basename(root)}/")

        subindent = " " * 4 * (level + 1)
        for f in files:
            # Ignore compiled python files and common system files
            if not f.endswith(".pyc") and f != ".DS_Store":
                print(f"{subindent}{f}")


if __name__ == "__main__":
    list_files(".")
