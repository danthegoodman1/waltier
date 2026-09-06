#!/usr/bin/env python3
"""Compare identical release workloads with locked, offline dependencies.

Build all versions first, warm each binary, then alternate measurement order.
Optional Linux CPU affinity limits scheduler migration on hybrid CPUs.
"""
import argparse
import hashlib
import json
import os
import pathlib
import platform
import shutil
import statistics
import subprocess
import tempfile
import tomllib


def run(args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def parse(output):
    cases = {}
    for line in output.splitlines():
        label, fields = line.split(": ", 1)
        cases[label] = {
            key: float(value)
            for key, value in (field.split("=") for field in fields.split())
        }
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", default="d5dda89fb176d590d03c7812d047ced2712bba94")
    parser.add_argument("--before", help="Optional original PR revision for a three-way comparison")
    parser.add_argument("--candidate", help="Git commit/ref; default is the working checkout")
    parser.add_argument("--runs", type=int, default=7)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--cpu", type=int, help="Pin benchmark processes and workers to this Linux CPU")
    parser.add_argument("--benches", nargs="+", choices=["review", "resources", "cache"], default=["review", "resources", "cache"])
    parser.add_argument("--isolate-resources", action="store_true", help="Run snapshot and startup cases in separate processes")
    parser.add_argument("--review-small", action="store_true", help="Only batch64: 120 zero-latency folds and 12 at 1 ms RTT")
    parser.add_argument("--keep-binaries", type=pathlib.Path, help="Copy built binaries here for separate profiling")
    parser.add_argument("--output", type=pathlib.Path, default=pathlib.Path("/tmp/waltier-performance.json"))
    args = parser.parse_args()
    if args.runs < 1 or args.warmups < 0:
        parser.error("runs must be positive and warmups nonnegative")
    if not any(name != "cache" for name in args.benches):
        parser.error("include a shared benchmark: review or resources")
    if args.cpu is not None and (not hasattr(os, "sched_getaffinity") or args.cpu not in os.sched_getaffinity(0)):
        parser.error("CPU must be in the current Linux affinity mask")
    root = pathlib.Path(__file__).resolve().parents[1]

    def revision(ref):
        return run(["git", "-C", str(root), "rev-parse", ref], capture_output=True).stdout.strip()

    refs = {"baseline": revision(args.baseline)}
    if args.before:
        refs["before"] = revision(args.before)
    refs["candidate"] = revision(args.candidate or "HEAD")
    result = {
        "baseline": refs["baseline"],
        "candidate_head": refs["candidate"],
        "candidate_tracked_changes": not args.candidate and bool(run(["git", "-C", str(root), "diff", "HEAD", "--stat"], capture_output=True).stdout),
        "revisions": refs,
        "rustc": run(["rustc", "--version"], capture_output=True).stdout.strip(),
        "platform": platform.platform(),
        "cpu": args.cpu,
        "warmups": args.warmups,
        "resource_cases_isolated": args.isolate_resources,
        "review_small": args.review_small,
        "source_hashes": {},
        "order": [],
        "runs": {label: [] for label in refs},
        "medians": {},
        "ranges": {},
    }
    with tempfile.TemporaryDirectory(prefix="waltier-comparison-") as temporary:
        temp = pathlib.Path(temporary)
        executables = {}
        for label, commit in refs.items():
            source = root
            if label != "candidate" or args.candidate:
                source = temp / label
                source.mkdir()
                archive = temp / f"{label}.tar"
                run(["git", "-C", str(root), "archive", commit, "--output", str(archive)])
                run(["tar", "-xf", str(archive), "-C", str(source)])
            fixture = temp / f"{label}-fixture"
            binaries = fixture / "src" / "bin"
            binaries.mkdir(parents=True)
            (fixture / "Cargo.toml").write_text(
                '[package]\nname="waltier-comparison-' + label + '"\nversion="0.0.0"\nedition="2024"\n'
                '[dependencies]\nwaltier={path=' + json.dumps(str(source)) + '}\ntempfile="3"\n'
            )
            shutil.copyfile(source / "Cargo.lock", fixture / "Cargo.lock")
            original_lock = tomllib.loads((source / "Cargo.lock").read_text())
            result["source_hashes"][label] = {
                str(path.relative_to(source)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in (source / "src").glob("*.rs")
            }
            # The reviewed main predates cache-policy configuration.
            names = [name for name in args.benches if name != "cache" or label != "baseline"]
            for name in names:
                shutil.copyfile(root / "benches" / f"{name}.rs", binaries / f"{name}.rs")
            target = temp / "target"
            command = ["cargo", "build", "--release", "--offline", "--manifest-path", str(fixture / "Cargo.toml"), "--target-dir", str(target)]
            run(command)
            locked = {(p["name"], p["version"], p.get("checksum")) for p in original_lock["package"]}
            updated = tomllib.loads((fixture / "Cargo.lock").read_text())
            assert all(p["name"] == f"waltier-comparison-{label}" or (p["name"], p["version"], p.get("checksum")) in locked for p in updated["package"])
            run(command + ["--locked"])
            executables[label] = []
            for name in names:
                executable = temp / f"{label}-{name}"
                shutil.copyfile(target / "release" / name, executable)
                executable.chmod(0o755)
                if args.keep_binaries:
                    args.keep_binaries.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(executable, args.keep_binaries / executable.name)
                suffixes = [["snapshot"], ["startup"]] if name == "resources" and args.isolate_resources else [[]]
                if name == "review" and args.review_small:
                    suffixes = [["--small"]]
                for suffix in suffixes:
                    command = [str(executable), *suffix]
                    if args.cpu is not None:
                        command = ["taskset", "--cpu-list", str(args.cpu), *command]
                    executables[label].append(command)

        def measure(label):
            sample = {}
            for command in executables[label]:
                sample.update(parse(run(command, capture_output=True).stdout))
            return sample

        labels = list(refs)
        for _ in range(args.warmups):
            for label in labels:
                measure(label)
        for iteration in range(args.runs):
            # Rotate all three positions and reverse pairs on alternate rounds.
            order = labels[iteration % len(labels):] + labels[:iteration % len(labels)]
            if (iteration // len(labels)) % 2:
                order.reverse()
            result["order"].append(order)
            for label in order:
                result["runs"][label].append(measure(label))
            print(f"Completed round {iteration + 1}/{args.runs}: {', '.join(order)}", flush=True)
        for label, samples in result["runs"].items():
            result["medians"][label] = {
                case: {field: statistics.median(sample[case][field] for sample in samples) for field in fields}
                for case, fields in samples[0].items()
            }
            result["ranges"][label] = {
                case: {field: [min(s[case][field] for s in samples), max(s[case][field] for s in samples)] for field in fields}
                for case, fields in samples[0].items()
            }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
