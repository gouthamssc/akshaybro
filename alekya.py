import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, List, Dict
import pyarrow as pa
import pyarrow.parquet as pq


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

def read_json(input_path: Path, strict: bool = False):
    """Reads a JSON, JSONL, or single JSON object file and returns a tuple: (list of dicts, skipped_lines)."""
    skipped_lines = 0
    try:
        with input_path.open('r', encoding='utf-8') as f:
            # Read ahead to determine file type
            first_non_ws = ''
            while True:
                pos = f.tell()
                char = f.read(1)
                if not char:
                    break
                if not char.isspace():
                    first_non_ws = char
                    f.seek(pos)
                    break
            if not first_non_ws:
                return [], skipped_lines  # Empty file
            if first_non_ws == '[':
                # Standard JSON array
                data = json.load(f)
                if not isinstance(data, list):
                    raise ValueError('JSON root must be a list')
                return data, skipped_lines
            elif first_non_ws == '{':
                # Could be a single JSON object or JSONL with each line as an object
                try:
                    data = json.load(f)
                    if isinstance(data, dict):
                        return [data], skipped_lines
                    elif isinstance(data, list):
                        return data, skipped_lines
                    else:
                        raise ValueError('JSON root must be a list or object')
                except json.JSONDecodeError:
                    # Not a single JSON object, treat as JSONL
                    f.seek(0)
                    result = []
                    for i, line in enumerate(f, 1):
                        if not line.strip():
                            continue
                        try:
                            result.append(json.loads(line))
                        except Exception as e:
                            msg = f"Malformed JSON on line {i}: {e}\nLine content: {line.strip()}"
                            if strict:
                                logging.error(msg)
                                raise
                            else:
                                logging.warning(msg)
                                skipped_lines += 1
                    return result, skipped_lines
            else:
                raise ValueError('Input file must be a JSON array, a single JSON object, or JSONL (one JSON object per line).')
    except Exception as e:
        logging.error(f"Failed to read JSON: {e}")
        raise

def write_pyarrow(data: List[Dict[str, Any]], output_path: Path, file_format: str = 'arrow'):
    try:
        table = pa.Table.from_pylist(data)
        if file_format == 'arrow':
            with output_path.open('wb') as f:
                with pa.RecordBatchFileWriter(f, table.schema) as writer:
                    writer.write_table(table)
        elif file_format == 'parquet':
            pq.write_table(table, output_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        logging.info(f"Successfully wrote {file_format} file to {output_path}")
    except Exception as e:
        logging.error(f"Failed to write {file_format} file: {e}")
        raise

def normalize_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Ensure all values in each dict are strings, to avoid pyarrow type issues."""
    normalized = []
    for row in data:
        norm_row = {}
        for k, v in row.items():
            if v is None:
                norm_row[k] = None
            else:
                norm_row[k] = str(v)
        normalized.append(norm_row)
    return normalized

def main():
    parser = argparse.ArgumentParser(description="Convert JSON/JSONL to PyArrow format.")
    parser.add_argument('input', type=Path, help='Input JSON or JSONL file')
    parser.add_argument('output', type=Path, help='Output file (.arrow or .parquet)')
    parser.add_argument('--format', choices=['arrow', 'parquet'], default=None, help='Output file format (arrow or parquet)')
    parser.add_argument('--strict', action='store_true', help='Fail on first malformed JSON line (default: skip bad lines)')
    args = parser.parse_args()

    setup_logging()

    if not args.input.exists():
        logging.error(f"Input file {args.input} does not exist.")
        sys.exit(1)

    file_format = args.format
    if not file_format:
        if args.output.suffix == '.parquet':
            file_format = 'parquet'
        else:
            file_format = 'arrow'

    try:
        data, skipped_lines = read_json(args.input, strict=args.strict)
        data = normalize_data(data)
        write_pyarrow(data, args.output, file_format)
        if skipped_lines > 0 and not args.strict:
            logging.info(f"Skipped {skipped_lines} malformed line(s) during conversion.")
    except Exception as e:
        logging.error(f"Conversion failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
