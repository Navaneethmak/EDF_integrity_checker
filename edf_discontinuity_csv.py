"""
%THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%SOFTWARE.
%Author: Navaneethakrishna Makaram email:navaneethakrishna.makaram@outlook.com

EDF / EDF+C / EDF+D Analyzer — CSV output
-------------------------------------------
Behaviour by file type:
  EDF+D  →  discontinuity analysis  (start time, end time, gaps)
  EDF+C  →  discontinuity analysis  (continuous file, gaps expected to be zero)
  EDF    →  header + signal info exported as CSV  (no annotations channel)

Usage:
    python edf_discontinuity_csv.py <file.edf> [output.csv]

    CSV defaults to <filename>_edf_info.csv if not specified.

Requirements:
    Standard library only (no numpy / matplotlib needed)
"""

import sys
import csv
from pathlib import Path
from datetime import datetime, timedelta


# ─────────────────────────────────────────────
# EDF Header Parsers
# ─────────────────────────────────────────────

def parse_edf_header(f):
    f.seek(0)
    header = {}
    header['version']         = f.read(8).decode('ascii',  errors='replace').strip()
    header['patient']         = f.read(80).decode('ascii', errors='replace').strip()
    header['recording']       = f.read(80).decode('ascii', errors='replace').strip()
    header['startdate']       = f.read(8).decode('ascii',  errors='replace').strip()
    header['starttime']       = f.read(8).decode('ascii',  errors='replace').strip()
    header['header_bytes']    = int(f.read(8).decode('ascii',  errors='replace').strip())
    header['reserved']        = f.read(44).decode('ascii', errors='replace').strip()
    header['num_records']     = int(f.read(8).decode('ascii',  errors='replace').strip())
    header['record_duration'] = float(f.read(8).decode('ascii', errors='replace').strip())
    header['num_signals']     = int(f.read(4).decode('ascii',  errors='replace').strip())
    return header


def parse_edf_signal_headers(f, num_signals):
    def read_n(n):
        return [f.read(n).decode('ascii', errors='replace').strip() for _ in range(num_signals)]

    labels      = read_n(16)
    transducers = read_n(80)
    phys_dim    = read_n(8)
    phys_min    = read_n(8)
    phys_max    = read_n(8)
    dig_min     = read_n(8)
    dig_max     = read_n(8)
    prefilter   = read_n(80)
    num_samples = [int(x) for x in read_n(8)]
    reserved    = read_n(32)

    signals = []
    for i in range(num_signals):
        signals.append({
            'label':       labels[i],
            'transducer':  transducers[i],
            'phys_dim':    phys_dim[i],
            'phys_min':    phys_min[i],
            'phys_max':    phys_max[i],
            'dig_min':     dig_min[i],
            'dig_max':     dig_max[i],
            'prefilter':   prefilter[i],
            'num_samples': num_samples[i],
            'reserved':    reserved[i],
        })
    return signals


def parse_tal(raw_bytes):
    """Parse Time-stamped Annotations List bytes."""
    tals = []
    for rec in raw_bytes.split(b'\x00'):
        if not rec:
            continue
        parts       = rec.split(b'\x14')
        onset_parts = parts[0].split(b'\x15')
        try:
            onset    = float(onset_parts[0].decode('ascii', errors='replace').strip())
            duration = float(onset_parts[1].decode('ascii', errors='replace').strip()) \
                       if len(onset_parts) > 1 and onset_parts[1] else 0.0
        except (ValueError, IndexError):
            continue
        annotations = [p.decode('ascii', errors='replace').strip() for p in parts[1:] if p]
        tals.append((onset, duration, annotations))
    return tals


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def seconds_to_hms(s):
    h   = int(s // 3600)
    m   = int((s % 3600) // 60)
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:06.3f}"


def parse_start_datetime(header):
    sd, st = header['startdate'], header['starttime']
    try:
        day, month, year     = int(sd[0:2]), int(sd[3:5]), int(sd[6:8])
        year += 2000 if year < 85 else 1900
        hour, minute, second = int(st[0:2]), int(st[3:5]), int(st[6:8])
        return datetime(year, month, day, hour, minute, second)
    except Exception:
        return None


def fmt_time(s, file_start):
    if file_start:
        return (file_start + timedelta(seconds=s)).strftime('%Y-%m-%d %H:%M:%S.%f')
    return seconds_to_hms(s)


def detect_edf_type(reserved):
    """Return 'EDF+D', 'EDF+C', or 'EDF' based on the reserved header field."""
    r = reserved.upper()
    if 'EDF+D' in r:
        return 'EDF+D'
    if 'EDF+C' in r:
        return 'EDF+C'
    return 'EDF'


# ─────────────────────────────────────────────
# Loaders
# ─────────────────────────────────────────────

def load_edf_plus(filepath, header, signals):
    """Load EDF+C or EDF+D: extract TAL onsets and detect discontinuities."""
    record_duration = header['record_duration']
    num_records     = header['num_records']
    file_start      = parse_start_datetime(header)
    num_samples     = [s['num_samples'] for s in signals]
    record_sizes    = [ns * 2 for ns in num_samples]
    total_rec_size  = sum(record_sizes)
    data_start      = header['header_bytes']

    ann_index = next((i for i, s in enumerate(signals) if 'EDF Annotations' in s['label']), None)
    if ann_index is None:
        raise ValueError("EDF+C/D file has no 'EDF Annotations' channel.")

    chan_offset = sum(record_sizes[:ann_index])

    record_onsets = []
    with open(filepath, 'rb') as f:
        for rec_i in range(num_records):
            f.seek(data_start + rec_i * total_rec_size + chan_offset)
            raw  = f.read(record_sizes[ann_index])
            tals = parse_tal(raw)
            record_onsets.append(tals[0][0] if tals else None)

    valid_onsets    = [(i, o) for i, o in enumerate(record_onsets) if o is not None]
    discontinuities = []
    time_errors     = []

    for idx in range(1, len(valid_onsets)):
        rec_i,    onset_i    = valid_onsets[idx]
        rec_prev, onset_prev = valid_onsets[idx - 1]
        expected = onset_prev + record_duration
        gap      = onset_i - expected

        # Time reset / non-monotonic: onset goes backwards relative to previous record
        if onset_i < onset_prev:
            time_errors.append({
                'record_index': rec_i,
                'prev_onset_s': onset_prev,
                'this_onset_s': onset_i,
                'delta_s':      onset_i - onset_prev,
                'error':        'TIME_RESET — onset decreased (non-monotonic timestamp)',
            })
        elif abs(gap) > 1e-6:
            discontinuities.append({
                'record_index':     rec_i,
                'prev_onset_s':     onset_prev,
                'this_onset_s':     onset_i,
                'expected_onset_s': expected,
                'gap_s':            gap,
            })

    file_end_s = valid_onsets[-1][1] + record_duration if valid_onsets else None

    return {
        'edf_type':        detect_edf_type(header['reserved']),
        'filepath':        filepath,
        'file_start':      file_start,
        'file_start_s':    valid_onsets[0][1] if valid_onsets else 0,
        'file_end_s':      file_end_s,
        'record_duration': record_duration,
        'num_records':     num_records,
        'signals':         signals,
        'discontinuities': discontinuities,
        'time_errors':     time_errors,
        'header':          header,
    }


def load_plain_edf(filepath, header, signals):
    """Plain EDF: return header + signal metadata only (no TAL)."""
    file_start      = parse_start_datetime(header)
    record_duration = header['record_duration']
    num_records     = header['num_records']
    total_duration_s = record_duration * num_records if record_duration > 0 else None

    return {
        'edf_type':         'EDF',
        'filepath':         filepath,
        'file_start':       file_start,
        'total_duration_s': total_duration_s,
        'record_duration':  record_duration,
        'num_records':      num_records,
        'signals':          signals,
        'header':           header,
    }


def load_edf_file(filepath):
    """Auto-detect EDF type and dispatch to the correct loader."""
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    with open(filepath, 'rb') as f:
        header  = parse_edf_header(f)
        signals = parse_edf_signal_headers(f, header['num_signals'])

    edf_type = detect_edf_type(header['reserved'])
    print(f"  Detected   : {edf_type}  (reserved field: '{header['reserved']}')")

    if edf_type in ('EDF+D', 'EDF+C'):
        return load_edf_plus(filepath, header, signals)
    else:
        return load_plain_edf(filepath, header, signals)


# ─────────────────────────────────────────────
# CSV Export — EDF+C / EDF+D
# ─────────────────────────────────────────────

def export_csv_edf_plus(result, csv_path):
    disc        = result['discontinuities']
    time_errors = result['time_errors']
    file_start  = result['file_start']
    start_s     = result['file_start_s']
    end_s       = result['file_end_s']
    rec_dur     = result['record_duration']

    def fmt(s):
        return fmt_time(s, file_start)

    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)

        writer.writerow(['=== File Summary ==='])
        writer.writerow(['Filename',              result['filepath'].name])
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
                             'Discontinuity Start',    # last sample of previous segment
                             'Discontinuity End',      # first sample of next segment
                             'Gap (s)', 'Gap Direction',
                             'Prev Segment Onset', 'Next Segment Onset', 'Expected Onset'])
            for i, d in enumerate(disc, 1):
                disc_start = fmt(d['prev_onset_s'] + rec_dur)   # end of last good record
                disc_end   = fmt(d['this_onset_s'])              # start of next record
                writer.writerow([
                    i,
                    d['record_index'],
                    disc_start,
                    disc_end,
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

    print(f"  CSV saved  → {csv_path}")


# ─────────────────────────────────────────────
# CSV Export — Plain EDF
# ─────────────────────────────────────────────

def export_csv_plain_edf(result, csv_path):
    file_start = result['file_start']
    header     = result['header']
    rec_dur    = result['record_duration']
    total_s    = result['total_duration_s']

    start_str = file_start.strftime('%Y-%m-%d %H:%M:%S') if file_start else 'unknown'
    end_str   = (file_start + timedelta(seconds=total_s)).strftime('%Y-%m-%d %H:%M:%S') \
                if (file_start and total_s) else 'unknown'
    dur_str   = seconds_to_hms(total_s) if total_s else 'unknown'

    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)

        writer.writerow(['=== File Summary ==='])
        writer.writerow(['Filename',            result['filepath'].name])
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
            writer.writerow([
                i, s['label'], s['transducer'], s['phys_dim'],
                s['phys_min'], s['phys_max'], s['dig_min'], s['dig_max'],
                s['prefilter'], s['num_samples'], sr,
            ])

    print(f"  CSV saved  → {csv_path}")


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Usage: python edf_discontinuity_csv.py <file.edf> [output.csv]")
        print("       CSV defaults to <filename>_edf_info.csv")
        sys.exit(1)

    edf_path = sys.argv[1]
    csv_path = sys.argv[2] if len(sys.argv) > 2 else Path(edf_path).stem + '_edf_info.csv'

    print(f"\nLoading: {edf_path}")
    result = load_edf_file(edf_path)

    if result['edf_type'] in ('EDF+D', 'EDF+C'):
        disc    = result['discontinuities']
        start_s = result['file_start_s']
        end_s   = result['file_end_s']
        fs      = result['file_start']
        print(f"  Start      : {fmt_time(start_s, fs)}")
        print(f"  End        : {fmt_time(end_s, fs)}")
        print(f"  Duration   : {seconds_to_hms(end_s - start_s)}")
        print(f"  Gaps found : {len(disc)}")
        print(f"  Time errors: {len(result['time_errors'])}")
        export_csv_edf_plus(result, csv_path)
    else:
        fs    = result['file_start']
        total = result['total_duration_s']
        print(f"  Start      : {fs.strftime('%Y-%m-%d %H:%M:%S') if fs else 'unknown'}")
        print(f"  Duration   : {seconds_to_hms(total) if total else 'unknown'}")
        print(f"  Signals    : {len(result['signals'])}")
        print(f"  Note       : Plain EDF — exporting header & signal info only.")
        export_csv_plain_edf(result, csv_path)

    print("  Done.\n")


if __name__ == '__main__':
    main()
