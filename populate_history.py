from pathlib import Path
import os
import subprocess
import sys


def main() -> int:
    base_dir = Path(__file__).resolve().parent
    frontend_dir = base_dir / "frontend"

    if not frontend_dir.exists():
        print("Could not find the frontend directory next to populate_history.py.")
        return 1

    if os.name == "nt":
        command = ["cmd", "/c", "npm run import:history"]
    else:
        command = ["npm", "run", "import:history"]

    if len(sys.argv) > 1:
        extra_args = " ".join(sys.argv[1:])
        if os.name == "nt":
            command[2] = f"npm run import:history -- {extra_args}"
        else:
            command.extend(["--", *sys.argv[1:]])

    print("Delegating history import to the Node importer in frontend/scripts/import-history.mjs ...")
    completed = subprocess.run(command, cwd=frontend_dir)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
