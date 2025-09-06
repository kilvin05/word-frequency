
import argparse
import os
import threading
import re
from collections import Counter
from typing import List, Tuple

WORD_RE = re.compile(r"[A-Za-z0-9']+")

def is_word_char(b: int) -> bool:
    """Return True if the given byte corresponds to an ASCII word character (letter/digit/')."""
    # We only use ASCII classification to decide boundaries; decoding happens after slicing.
    return (ord('A') <= b <= ord('Z')) or (ord('a') <= b <= ord('z')) or (ord('0') <= b <= ord('9')) or (b == ord("'"))

def compute_segments(path: str, n: int) -> List[Tuple[int, int]]:
    """
    Compute n (start, end) byte ranges that cover the file without overlapping words.
    Each [start, end) will begin on a delimiter boundary and end right after a delimiter boundary.
    """
    size = os.path.getsize(path)
    if n <= 0:
        raise ValueError("Number of segments must be > 0")
    if size == 0:
        return [(0, 0)]

    approx = size // n
    segments = []
    with open(path, "rb") as f:
        for i in range(n):
            # initial naive bounds
            start = i * approx
            end = size if i == n - 1 else (i + 1) * approx

            # Adjust start: if not at 0, move forward until we hit a delimiter (non-word char)
            if start > 0:
                f.seek(start - 1)
                prev = f.read(1)
                # If we're in the middle of a word, advance start until delimiter
                while prev and is_word_char(prev[0]):
                    start += 1
                    f.seek(start - 1)
                    prev = f.read(1)
                # Now start is at the first delimiter boundary *after* the split

            # Adjust end: if not at EOF, extend end until we hit a delimiter
            if end < size:
                f.seek(end - 1)
                cur = f.read(1)
                while cur and is_word_char(cur[0]):
                    end += 1
                    if end >= size:
                        end = size
                        break
                    f.seek(end - 1)
                    cur = f.read(1)

            segments.append((start, end))
    # Merge tiny or empty segments if file is very small
    merged = []
    last_end = -1
    for s, e in segments:
        if not merged:
            merged.append([s, e])
            last_end = e
        else:
            if e - s == 0:
                # drop empty; extend previous if needed
                merged[-1][1] = max(merged[-1][1], e)
            else:
                # Keep as-is; ensure monotonic non-overlap
                s = max(s, last_end)
                if s > e:
                    s = e
                merged.append([s, e])
                last_end = e
    return [(s, e) for s, e in merged]

def count_words_bytes(buf: bytes) -> Counter:
    try:
        text = buf.decode("utf-8", errors="ignore").lower()
    except Exception:
        text = buf.decode("latin-1", errors="ignore").lower()
    words = WORD_RE.findall(text)
    return Counter(words)

class SegmentWorker(threading.Thread):
    def __init__(self, path: str, idx: int, start: int, end: int, out_dir: str, results: List[Counter], lock: threading.Lock):
        super().__init__(daemon=True)
        self.path = path
        self.idx = idx
        self.start = start
        self.end = end
        self.out_dir = out_dir
        self.results = results
        self.lock = lock

    def run(self):
        with open(self.path, "rb") as f:
            f.seek(self.start)
            buf = f.read(self.end - self.start)
        counts = count_words_bytes(buf)

        # Save intermediate result per thread
        inter_path = os.path.join(self.out_dir, f"thread_{self.idx}_counts.txt")
        with open(inter_path, "w", encoding="utf-8") as w:
            for word, freq in counts.most_common():
                w.write(f"{word}\t{freq}\n")

        # Store in shared array
        with self.lock:
            self.results[self.idx] = counts

def consolidate(results: List[Counter]) -> Counter:
    total = Counter()
    for c in results:
        if c:
            total.update(c)
    return total

def main():
    parser = argparse.ArgumentParser(description="Threaded word-frequency counter")
    parser.add_argument("--file", required=True, help="Path to the input text file")
    parser.add_argument("--segments", type=int, required=True, help="Number of segments/threads")
    parser.add_argument("--intermediate_dir", default="intermediate", help="Directory to write per-thread outputs")
    args = parser.parse_args()

    if not os.path.exists(args.file):
        raise FileNotFoundError(f"Input file not found: {args.file}")

    os.makedirs(args.intermediate_dir, exist_ok=True)

    # Compute segments
    segs = compute_segments(args.file, args.segments)
    print(f"[INFO] File size: {os.path.getsize(args.file)} bytes")
    print("[INFO] Segments (byte ranges):")
    for i, (s, e) in enumerate(segs):
        print(f"  Thread {i}: [{s}, {e}) length={e-s}")

    # Spawn threads
    lock = threading.Lock()
    results: List[Counter] = [Counter() for _ in range(len(segs))]
    threads = []
    for i, (s, e) in enumerate(segs):
        t = SegmentWorker(args.file, i, s, e, args.intermediate_dir, results, lock)
        t.start()
        threads.append(t)

    # Join
    for t in threads:
        t.join()

    # Consolidate
    final_counts = consolidate(results)

    # Print intermediate paths
    print("\n[INFO] Intermediate per-thread outputs written to:")
    for i in range(len(segs)):
        print(f"  {os.path.join(args.intermediate_dir, f'thread_{i}_counts.txt')}")

    # Print final consolidated counts (sorted by frequency desc, then word asc)
    print("\n[FINAL CONSOLIDATED WORD FREQUENCY]")
    for word, freq in sorted(final_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"{word}\t{freq}")

if __name__ == "__main__":
    main()
