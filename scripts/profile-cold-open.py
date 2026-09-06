#!/usr/bin/env python3
"""Attribute cold-open time to backend copying and cache work on Linux.

Builds isolated, locked-version diagnostic fixtures. The probe records CPU,
allocation, fault and scheduling observations; it is not a production benchmark.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import tarfile
import tempfile
import tomllib


def run(args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", default="d5dda89fb176d590d03c7812d047ced2712bba94")
    parser.add_argument("--before", default="7a4b4c6f6eb97a548095760c486aa9d214f9a107")
    parser.add_argument("--candidate", help="Git revision; default is the working checkout")
    parser.add_argument("--runs", type=int, default=9)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--prime-get", action="store_true", help="Diagnostic control: clone/drop snapshot before timer; cache stays empty")
    parser.add_argument("--output", type=Path, default=Path("/tmp/waltier-cold-profile.json"))
    args = parser.parse_args()
    if platform.system() != "Linux":
        parser.error("the diagnostic uses Linux CPU and process resource observations")
    if args.runs < 1 or args.warmups < 0:
        parser.error("runs must be positive and warmups nonnegative")
    if args.cpu is not None and args.cpu not in os.sched_getaffinity(0):
        parser.error("CPU is outside the current affinity mask")
    repo = Path(__file__).resolve().parents[1]
    template = (repo / "scripts/fixtures/cold_diagnostic.rs").read_text()
    refs = {name: run(["git", "rev-parse", revision], cwd=repo, capture_output=True).stdout.strip()
            for name, revision in [("baseline", args.baseline), ("before", args.before), ("candidate", args.candidate or "HEAD")]}
    result = {"revisions": refs, "candidate_tracked_changes": not args.candidate and bool(run(["git", "diff", "HEAD", "--", "src"], cwd=repo, capture_output=True).stdout),
              "rustc": run(["rustc", "-V"], capture_output=True).stdout.strip(), "platform": platform.platform(),
              "cpu": args.cpu, "prime_get": args.prime_get, "warmups": args.warmups, "source_hashes": {}, "order": [], "runs": {}, "medians": {}}
    with tempfile.TemporaryDirectory(prefix="waltier-cold-profile-") as temporary:
        root = Path(temporary)
        binaries = {}
        for name, revision in refs.items():
            source = repo
            if name != "candidate" or args.candidate:
                source = root / name
                source.mkdir()
                archive = subprocess.check_output(["git", "archive", revision], cwd=repo)
                with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
                    tar.extractall(source, filter="data")
            result["source_hashes"][name] = {str(path.relative_to(source)): hashlib.sha256(path.read_bytes()).hexdigest() for path in (source / "src").glob("*.rs")}
            original_lock = tomllib.loads((source / "Cargo.lock").read_text())
            libc_version = next(p["version"] for p in original_lock["package"] if p["name"] == "libc")
            fixture = root / f"{name}-fixture"
            (fixture / "src").mkdir(parents=True)
            (fixture / "Cargo.toml").write_text(
                f'[package]\nname="cold-{name}"\nversion="0.0.0"\nedition="2024"\n[dependencies]\n'
                f'waltier={{path={json.dumps(str(source))},default-features=false}}\ntempfile="3"\nlibc="={libc_version}"\n')
            shutil.copyfile(source / "Cargo.lock", fixture / "Cargo.lock")
            # Compatibility forwarding only: all versions retain enabled caches.
            store_source = (source / "src/store.rs").read_text()
            methods = []
            for method, returns in [("cache_namespace", "Option<String>"), ("max_object_bytes", "Option<usize>")]:
                if f"fn {method}(" in store_source:
                    methods.append(f"fn {method}(&self) -> {returns} {{ self.inner.{method}() }}")
            (fixture / "src/main.rs").write_text(template.replace("// NEW_API_METHODS", "\n    ".join(methods)))
            target = root / "target"
            command = ["cargo", "build", "--offline", "--release", "--manifest-path", str(fixture / "Cargo.toml"), "--target-dir", str(target)]
            run(command)
            # The fixture may add a package/edge, never upgrade a locked dependency.
            locked = {(p["name"], p["version"], p.get("checksum")) for p in original_lock["package"]}
            updated = tomllib.loads((fixture / "Cargo.lock").read_text())
            assert all(p["name"] == f"cold-{name}" or (p["name"], p["version"], p.get("checksum")) in locked for p in updated["package"])
            run(command + ["--locked"])
            binaries[name] = target / "release" / f"cold-{name}"

        def measure(name):
            command = [str(binaries[name])] + (["--prime-get"] if args.prime_get else [])
            if args.cpu is not None:
                command = ["taskset", "-c", str(args.cpu), *command]
            data = {}
            for line in run(command, capture_output=True).stdout.splitlines():
                stage, fields = line.split(": ", 1)
                data[stage] = {k: float(v) for k, v in (field.split("=") for field in fields.split())}
            return data

        labels = list(refs)
        for _ in range(args.warmups):
            for name in labels:
                measure(name)
        result["runs"] = {name: [] for name in labels}
        for i in range(args.runs):
            order = labels[i % 3:] + labels[:i % 3]
            if (i // 3) % 2:
                order.reverse()
            result["order"].append(order)
            for name in order:
                result["runs"][name].append(measure(name))
        for name, samples in result["runs"].items():
            result["medians"][name] = {stage: {field: statistics.median(sample[stage][field] for sample in samples)
                                               for field in fields if not field.startswith("cpu_")}
                                        for stage, fields in samples[0].items()}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
