# Multithreaded Word Frequency Counter

This lab implements a multithreaded program that reads a text file, partitions it into **N** segments, and processes each segment concurrently in a **separate thread** to compute per-segment word frequencies. After all threads finish, the main process consolidates the results into a final word-frequency count.

## Features
- Accepts input file and number of segments `N` from the command line.
- Splits the file by **byte ranges** and adjusts boundaries so words are **not split** between segments.
- Each thread outputs its **intermediate** word counts to `./intermediate/thread_<i>_counts.txt`.
- The main process prints a final consolidated word-frequency list to STDOUT.

## Requirements
- Python 3.8+ (no external libraries required).

## How to Run
```bash
# 1) (Optional) Inspect sample input
cat sample.txt

# 2) Run with 4 threads/segments
python wordfreq_threaded.py --file sample.txt --segments 4

# 3) Check intermediate per-thread outputs
ls -l intermediate
cat intermediate/thread_0_counts.txt
```

## Notes
- "Word" is defined by regex: `[A-Za-z0-9']+` (letters, digits, apostrophes), case-insensitive.
- Segmenting by **byte ranges** allows scalable processing of large files.
- We expand each segment's **end** to finish the current word and move the next segment's **start** forward to the next delimiter to avoid double counting or losing split words.
- The final output is printed sorted by frequency (descending), then alphabetically.

## Example
```bash
python wordfreq_threaded.py --file sample.txt --segments 3
```

## Project Layout
```
.
├── intermediate/                  # Per-thread outputs (created at runtime)
├── sample.txt                     # Small sample input
├── wordfreq_threaded.py           # Main program
└── REPORT_double_spaced.txt       # 1–2 page write-up (double-spaced)
```

## Compile/Run Instructions
No compilation needed. Run directly with Python 3.8+ as shown above.
