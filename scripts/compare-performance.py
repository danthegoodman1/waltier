#!/usr/bin/env python3
"""Run identical release workloads against a Git baseline and this checkout.

Requires git, tar, Cargo, and already cached dependencies (Cargo runs offline).
Results include every raw run; measurements model MemoryStore/SimStore, not S3.
"""
import argparse
import json
import pathlib
import platform
import shutil
import statistics
import subprocess
import tempfile


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
    parser.add_argument("--candidate", help="Git commit/ref to compare; default is the working checkout")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--output", type=pathlib.Path, default=pathlib.Path("/tmp/waltier-performance.json"))
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    root = pathlib.Path(__file__).resolve().parents[1]
    baseline = run(["git", "-C", str(root), "rev-parse", args.baseline], capture_output=True).stdout.strip()
    head = run(["git", "-C", str(root), "rev-parse", args.candidate or "HEAD"], capture_output=True).stdout.strip()
    result = {
        "baseline": baseline,
        "candidate_head": head,
        "candidate_tracked_changes": not args.candidate and bool(run(["git", "-C", str(root), "diff", "HEAD", "--stat"], capture_output=True).stdout),
        "rustc": run(["rustc", "--version"], capture_output=True).stdout.strip(),
        "platform": platform.platform(),
        "runs": {},
        "medians": {},
    }
    with tempfile.TemporaryDirectory(prefix="waltier-comparison-") as temporary:
        temp = pathlib.Path(temporary)
        archive = temp / "baseline.tar"
        old = temp / "baseline"
        old.mkdir()
        run(["git", "-C", str(root), "archive", baseline, "--output", str(archive)])
        run(["tar", "-xf", str(archive), "-C", str(old)])
        candidate = root
        if args.candidate:
            candidate = temp / "candidate"
            candidate.mkdir()
            run(["git", "-C", str(root), "archive", head, "--output", str(archive)])
            run(["tar", "-xf", str(archive), "-C", str(candidate)])
        for label, source in [("baseline", old), ("candidate", candidate)]:
            fixture = temp / label / "comparison-fixture" if label == "baseline" else temp / "candidate-fixture"
            binaries = fixture / "src" / "bin"
            binaries.mkdir(parents=True)
            (fixture / "Cargo.toml").write_text(
                '[package]\nname="waltier-comparison-' + label + '"\nversion="0.0.0"\nedition="2024"\n'
                '[dependencies]\nwaltier={path=' + json.dumps(str(source)) + '}\ntempfile="3"\n'
            )
            # Cargo adds the fixture package, retaining the repository's locked
            # dependency versions instead of selecting newer cached releases.
            shutil.copyfile(source / "Cargo.lock", fixture / "Cargo.lock")
            names = ["review", "resources"] + (["cache"] if label == "candidate" else [])
            for name in names:
                shutil.copyfile(root / "benches" / f"{name}.rs", binaries / f"{name}.rs")
            target = temp / "target"
            run(["cargo", "build", "--release", "--offline", "--manifest-path", str(fixture / "Cargo.toml"), "--target-dir", str(target)])
            measurements = []
            for _ in range(args.runs):
                sample = {}
                for name in names:
                    sample.update(parse(run([str(target / "release" / name)], capture_output=True).stdout))
                measurements.append(sample)
            result["runs"][label] = measurements
            result["medians"][label] = {
                case: {field: statistics.median(sample[case][field] for sample in measurements) for field in fields}
                for case, fields in measurements[0].items()
            }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
