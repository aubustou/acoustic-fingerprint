from hashlib import sha1
from itertools import product
import json
from pathlib import Path
import subprocess
import re
from typing import Hashable, Optional
from concurrent.futures import ProcessPoolExecutor


FINGERPRINT_PATTERN = re.compile(r"FINGERPRINT=?([0-9,]*)")

SAMPLE_TIME = 500  # in seconds
CORRELATION_SPAN = 150
CORRELATION_STEP = 1
CORRELATION_THRESHOLD = 0.7

recent_checksums = {}
older_checksums = {}

FIRST_PATH = Path()
SECOND_PATH = Path("/nfs/quirinalis/Bagarre/Podcasts/France Inter - RENDEZ-VOUS AVEC X")


def get_checksum(path: Path) -> Optional[list[int]]:
    flp_file = Path(str(path) + ".flp")
    if flp_file.exists():
        return json.load(flp_file.open())

    process = subprocess.run(
        ["fpcalc", "-raw", "-length", str(SAMPLE_TIME), str(path)], capture_output=True
    )
    output = process.stdout
    matches = FINGERPRINT_PATTERN.search(process.stdout.decode())
    fingerprint = list(map(int, matches.groups()[0].split(","))) if matches else None

    json.dump(fingerprint, flp_file.open("w"))
    return fingerprint


def cross_correlation(
    first_fingerprint: list[int], second_fingerprint: list[int], offset: int
) -> float:
    if not (first_fingerprint and second_fingerprint):
        raise RuntimeError("Empty lists cannot be correlated.")

    if offset > 0:
        first_fingerprint = first_fingerprint[offset:]
        second_fingerprint = second_fingerprint[: len(first_fingerprint)]
    elif offset < 0:
        offset = -offset
        second_fingerprint = second_fingerprint[offset:]
        first_fingerprint = first_fingerprint[: len(second_fingerprint)]

    if not (first_fingerprint and second_fingerprint):
        raise RuntimeError("Empty lists cannot be correlated.")
    if len(first_fingerprint) > len(second_fingerprint):
        first_fingerprint = first_fingerprint[: len(second_fingerprint)]
    elif len(first_fingerprint) < len(second_fingerprint):
        second_fingerprint = second_fingerprint[: len(first_fingerprint)]

    covariance = 0
    for index, fingerprint in enumerate(first_fingerprint):
        covariance += 32 - bin(fingerprint ^ second_fingerprint[index]).count("1")
    covariance = covariance / float(len(first_fingerprint))

    return covariance / 32


def correlate(first: list[int], second: list[int]) -> float:
    if not (first and second):
        return 0.0

    corr_xy = [
        cross_correlation(first, second, x)
        for x in range(-CORRELATION_SPAN, CORRELATION_SPAN + 1, CORRELATION_STEP)
    ]
    return max(*corr_xy, 0.0)


def main():
    for path in FIRST_PATH.glob("*.mp3"):
        recent_checksums[str(path)] = get_checksum(path)

    for path in SECOND_PATH.glob("*.mp3"):
        older_checksums[str(path)] = get_checksum(path)

    for first, second in product(recent_checksums.items(), older_checksums.items()):
        score = correlate(first[1], second[1])
        if score > CORRELATION_THRESHOLD:
            print(f"{first[0]} - {second[0]}: {score}")


if __name__ == "__main__":
    main()
