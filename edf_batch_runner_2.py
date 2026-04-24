"""
%THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%SOFTWARE.
%Author: Navaneethakrishna Makaram email:navaneethakrishna.makaram@outlook.com


EDF Batch Runner — wrapper around edf_discontinuity_csv.py
------------------------------------------------------------
Reads a CSV file containing paths to EDF files and produces:
  - batch_summary.csv               — one row per file overview
  - individual_patient_1.csv        — full detail for file 1
  - individual_patient_2.csv        — full detail for file 2
  - ...

All output files are written to the specified output folder
(defaults to ./edf_batch_output/).

Parallel processing is enabled by default using all available CPU cores.
Use --workers N to set a specific number of workers, or --workers 1 to
disable parallelism.

Input CSV format (one EDF path per row, header optional):
    filepath
    /data/recording1.edf
    /data/recording2.edf
    ...

Usage:
    python edf_batch_runner.py <input_list.csv> [output_folder] [--workers N]

Requirements:
    edf_discontinuity_csv.py must be in the same directory.
"""

import sys
import csv
import os
import concurrent.futures
from pathlib import Path
from datetime import timedelta

# ── Import analyzer functions from the sibling script ─────────────────
sys.path.insert(0, str(Path(__file__).parent))
from edf_discontinuity_csv import (
    load_edf_file,
    seconds_to_hms,
    fmt_time,
)


# ─────────────────────────────────────────────
# Read input CSV of EDF paths
# ─────────────────────────────────────────────

def read_edf_paths(input_csv):
    """
    Accept a CSV where EDF paths appear in any column named
    'filepath', 'path', 'file', or 'filename' (case-insensitive).
    Falls back to the first column if no recognised header is found.
    """
    paths = []
    with open(input_csv, newline='') as f:
        sample = f.read(1024)
        f.seek(0)
        has_header = csv.Sniffer().has_header(sample)
        reader     = csv.reader(f)

        col_index = 0
        if has_header:
            headers = [h.strip().lower() for h in next(reader)]
            for candidate in ('filepath', 'path', 'file', 'filename'):
                if candidate in headers:
                    col_index = headers.index(candidate)
                    break

        for row in reader:
            if not row:
                continue
            val = row[col_index].strip()
            if val:
                paths.append(Path(val))

    return paths


# ─────────────────────────────────────────────
# Individual patient CSV writers
# ─────────────────────────────────────────────

def write_individual_edf_plus(writer, result, file_index):
    """Write full EDF+C / EDF+D detail to an individual CSV."""
    disc        = result['discontinuities']
    time_errors = result['time_errors']
    file_start  = result['file_start']
    start_s     = result['file_start_s']
    end_s       = result['file_end_s']
    rec_dur     = result['record_duration']

    def fmt(s):
        return fmt_time(s, file_start)

    writer.writerow(['=== File Summary ==='])
    writer.writerow(['Filename',              result['filepath'].name])
    writer.writerow(['Full Path',             str(result['filepath'])])
    writer.writerow(['EDF Type',              result['edf_type']])
    writer.writerow(['Patient',               result['header']['patient']])
    writer.writerow(['Recording',             result['header']['recording']])
    writer.writerow(['Start Time',            fmt(start_s)])
    writer.writerow(['End Time',              fmt(end_s)])
    writer.writerow(['Total Duration',        seconds_to_hms(end_s - start_s)])
    writer.writerow(['Num Records',           result['num_records']])
    writer.writerow(['Record Duration (s)',   rec_dur])
    writer.writerow(['Num Signals',           len(result['signals'])])
    writer.writerow(['Discontinuities Found', len(disc)])
    writer.writerow(['Time Errors Found',     len(time_errors)])
    writer.writerow([])

    writer.writerow(['=== Signals ==='])
    writer.writerow(['#', 'Label', 'Transducer', 'Unit', 'Phys Min', 'Phys Max',
                     'Dig Min', 'Dig Max', 'Prefilter', 'Samples/Record', 'Sample Rate (Hz)'])
    for i, s in enumerate(result['signals'], 1):
        sr = round(s['num_samples'] / rec_dur, 4) if rec_dur > 0 else 'N/A'
        writer.writerow([i, s['label'], s['transducer'], s['phys_dim'],
                         s['phys_min'], s['phys_max'], s['dig_min'], s['dig_max'],
                         s['prefilter'], s['num_samples'], sr])
    writer.writerow([])

    writer.writerow(['=== Discontinuities ==='])
    if disc:
        writer.writerow(['Index', 'Record #',
                         'Discontinuity Start',
                         'Discontinuity End',
                         'Gap (s)', 'Gap Direction',
                         'Prev Segment Onset', 'Next Segment Onset', 'Expected Onset'])
        for i, d in enumerate(disc, 1):
            writer.writerow([
                i,
                d['record_index'],
                fmt(d['prev_onset_s'] + rec_dur),
                fmt(d['this_onset_s']),
                f"{d['gap_s']:.6f}",
                'forward' if d['gap_s'] > 0 else 'overlap',
                fmt(d['prev_onset_s']),
                fmt(d['this_onset_s']),
                fmt(d['expected_onset_s']),
            ])
    else:
        writer.writerow(['(none — file is continuous)'])
    writer.writerow([])

    writer.writerow(['=== Time Errors ==='])
    if time_errors:
        writer.writerow(['Index', 'Record #', 'Previous Onset', 'This Onset',
                         'Delta (s)', 'Error'])
        for i, e in enumerate(time_errors, 1):
            writer.writerow([
                i,
                e['record_index'],
                fmt(e['prev_onset_s']),
                fmt(e['this_onset_s']),
                f"{e['delta_s']:.6f}",
                e['error'],
            ])
    else:
        writer.writerow(['(none — timestamps are monotonically increasing)'])


def write_individual_plain_edf(writer, result, file_index):
    """Write full plain EDF detail to an individual CSV."""
    file_start = result['file_start']
    header     = result['header']
    rec_dur    = result['record_duration']
    total_s    = result['total_duration_s']

    start_str = file_start.strftime('%Y-%m-%d %H:%M:%S') if file_start else 'unknown'
    end_str   = (file_start + timedelta(seconds=total_s)).strftime('%Y-%m-%d %H:%M:%S') \
                if (file_start and total_s) else 'unknown'
    dur_str   = seconds_to_hms(total_s) if total_s else 'unknown'

    writer.writerow(['=== File Summary ==='])
    writer.writerow(['Filename',            result['filepath'].name])
    writer.writerow(['Full Path',           str(result['filepath'])])
    writer.writerow(['EDF Type',            result['edf_type']])
    writer.writerow(['Patient',             header['patient']])
    writer.writerow(['Recording',           header['recording']])
    writer.writerow(['Start Time',          start_str])
    writer.writerow(['End Time',            end_str])
    writer.writerow(['Total Duration',      dur_str])
    writer.writerow(['Num Records',         header['num_records']])
    writer.writerow(['Record Duration (s)', rec_dur])
    writer.writerow(['Num Signals',         header['num_signals']])
    writer.writerow([])

    writer.writerow(['=== Signals ==='])
    writer.writerow(['#', 'Label', 'Transducer', 'Unit', 'Phys Min', 'Phys Max',
                     'Dig Min', 'Dig Max', 'Prefilter', 'Samples/Record', 'Sample Rate (Hz)'])
    for i, s in enumerate(result['signals'], 1):
        sr = round(s['num_samples'] / rec_dur, 4) if rec_dur > 0 else 'N/A'
        writer.writerow([i, s['label'], s['transducer'], s['phys_dim'],
                         s['phys_min'], s['phys_max'], s['dig_min'], s['dig_max'],
                         s['prefilter'], s['num_samples'], sr])
    writer.writerow([])
    writer.writerow(['Note', 'Plain EDF — discontinuity analysis not applicable.'])


def write_individual_error(writer, filepath, error_msg):
    """Write an error block when a file fails to process."""
    writer.writerow(['=== File Summary ==='])
    writer.writerow(['Filename',  Path(filepath).name])
    writer.writerow(['Full Path', str(filepath)])
    writer.writerow(['STATUS',    'ERROR'])
    writer.writerow(['Error',     error_msg])


# ─────────────────────────────────────────────
# Per-file worker (runs in parallel)
# ─────────────────────────────────────────────

def process_one(args):
    """
    Load one EDF file, write its individual CSV, and return a summary dict.
    Designed to be called from a process pool — takes a single tuple arg
    so it works cleanly with ProcessPoolExecutor.map().
    """
    idx, edf_path, output_folder = args
    edf_path          = Path(edf_path)
    output_folder     = Path(output_folder)
    individual_filename = f'individual_patient_{idx}.csv'
    individual_path     = output_folder / individual_filename

    try:
        result = load_edf_file(edf_path)

        # Build summary
        if result['edf_type'] in ('EDF+D', 'EDF+C'):
            fs      = result['file_start']
            start_s = result['file_start_s']
            end_s   = result['file_end_s']
            disc    = result['discontinuities']
            terr    = result['time_errors']
            status  = 'ERROR' if terr else ('GAPS' if disc else 'OK')
            summary = {
                'index':           idx,
                'filename':        result['filepath'].name,
                'edf_type':        result['edf_type'],
                'start':           fmt_time(start_s, fs),
                'end':             fmt_time(end_s, fs),
                'duration':        seconds_to_hms(end_s - start_s),
                'discontinuities': len(disc),
                'time_errors':     len(terr),
                'individual_csv':  individual_filename,
                'status':          status,
            }
        else:
            fs    = result['file_start']
            total = result['total_duration_s']
            summary = {
                'index':           idx,
                'filename':        result['filepath'].name,
                'edf_type':        'EDF',
                'start':           fs.strftime('%Y-%m-%d %H:%M:%S') if fs else 'unknown',
                'end':             (fs + timedelta(seconds=total)).strftime('%Y-%m-%d %H:%M:%S')
                                   if (fs and total) else 'unknown',
                'duration':        seconds_to_hms(total) if total else 'unknown',
                'discontinuities': 'N/A',
                'time_errors':     'N/A',
                'individual_csv':  individual_filename,
                'status':          'OK',
            }

        # Write individual CSV
        with open(individual_path, 'w', newline='') as f:
            writer = csv.writer(f)
            if result['edf_type'] in ('EDF+D', 'EDF+C'):
                write_individual_edf_plus(writer, result, idx)
            else:
                write_individual_plain_edf(writer, result, idx)

        return summary

    except Exception as e:
        err_msg = str(e)
        with open(individual_path, 'w', newline='') as f:
            writer = csv.writer(f)
            write_individual_error(writer, edf_path, err_msg)

        return {
            'index':           idx,
            'filename':        edf_path.name,
            'edf_type':        'unknown',
            'start':           '',
            'end':             '',
            'duration':        '',
            'discontinuities': '',
            'time_errors':     '',
            'individual_csv':  individual_filename,
            'status':          f'ERROR: {err_msg}',
        }


# ─────────────────────────────────────────────
# Batch summary CSV
# ─────────────────────────────────────────────

def write_batch_summary_csv(summaries, output_folder):
    """Write batch_summary.csv — one row per file, sorted by index."""
    summary_path = output_folder / 'batch_summary.csv'
    summaries_sorted = sorted(summaries, key=lambda s: s['index'])
    with open(summary_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['#', 'Filename', 'EDF Type', 'Start Time', 'End Time',
                         'Duration', 'Discontinuities', 'Time Errors',
                         'Individual CSV', 'Status'])
        for s in summaries_sorted:
            writer.writerow([
                s['index'],
                s['filename'],
                s['edf_type'],
                s['start'],
                s['end'],
                s['duration'],
                s['discontinuities'],
                s['time_errors'],
                s['individual_csv'],
                s['status'],
            ])
    print(f"  Batch summary  → {summary_path}")
    return summary_path


# ─────────────────────────────────────────────
# Main batch runner
# ─────────────────────────────────────────────

def run_batch(input_csv, output_folder, num_workers=None):
    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    paths = read_edf_paths(input_csv)
    if not paths:
        print("No EDF paths found in input CSV.")
        sys.exit(1)

    # Default: use all available CPUs
    cpu_count   = os.cpu_count() or 1
    num_workers = num_workers or cpu_count
    num_workers = max(1, min(num_workers, len(paths)))  # cap at file count

    print(f"\nBatch processing {len(paths)} file(s)")
    print(f"Output folder  : {output_folder}")
    print(f"Workers        : {num_workers} / {cpu_count} CPUs\n")

    # Build args list — tuples so they serialise cleanly across processes
    work_items = [(idx, str(edf_path), str(output_folder))
                  for idx, edf_path in enumerate(paths, 1)]

    summaries  = []
    completed  = 0

    if num_workers == 1:
        # Sequential fallback — easier to debug
        for item in work_items:
            summary = process_one(item)
            summaries.append(summary)
            completed += 1
            status = summary['status']
            print(f"  [{completed}/{len(paths)}] {summary['filename']}  →  {status}")
    else:
        # Parallel — ProcessPoolExecutor gives true parallelism (bypasses GIL)
        # Results come back out-of-order; we sort by index before writing.
        with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as pool:
            futures = {pool.submit(process_one, item): item for item in work_items}
            for future in concurrent.futures.as_completed(futures):
                summary = future.result()
                summaries.append(summary)
                completed += 1
                status = summary['status']
                print(f"  [{completed}/{len(paths)}] {summary['filename']}  →  {status}")

    # Write batch summary (sorted by original index)
    print()
    write_batch_summary_csv(summaries, output_folder)

    error_count = sum(1 for s in summaries if str(s['status']).startswith('ERROR'))
    print(f"\n  {len(paths)} file(s) processed  |  {error_count} error(s)")
    print(f"  Output folder: {output_folder}\n")


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

def parse_args(argv):
    """Minimal arg parser — no argparse dependency."""
    positional = []
    workers    = None
    i = 1
    while i < len(argv):
        if argv[i] in ('--workers', '-w') and i + 1 < len(argv):
            try:
                workers = int(argv[i + 1])
            except ValueError:
                print(f"Invalid --workers value: {argv[i + 1]}")
                sys.exit(1)
            i += 2
        else:
            positional.append(argv[i])
            i += 1
    return positional, workers


def main():
    positional, workers = parse_args(sys.argv)

    if not positional:
        print("Usage: python edf_batch_runner.py <input_list.csv> [output_folder] [--workers N]")
        print()
        print("  input_list.csv  — CSV with a column of EDF file paths")
        print("                    (header: filepath / path / file / filename)")
        print("                    or just one path per line")
        print("  output_folder   — folder for all output CSVs")
        print("                    (default: ./edf_batch_output/)")
        print("  --workers N     — number of parallel workers")
        print("                    (default: all available CPUs)")
        print("                    (use 1 to disable parallelism)")
        print()
        print("  Output files:")
        print("    <output_folder>/batch_summary.csv")
        print("    <output_folder>/individual_patient_1.csv")
        print("    <output_folder>/individual_patient_2.csv  ...")
        sys.exit(1)

    input_csv     = positional[0]
    output_folder = positional[1] if len(positional) > 1 else 'edf_batch_output'

    run_batch(input_csv, output_folder, num_workers=workers)


if __name__ == '__main__':
    main()
