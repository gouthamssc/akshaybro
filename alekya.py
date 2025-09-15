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

def read_json(input_path: Path) -> List[Dict[str, Any]]:
    """Reads a JSON or JSONL file and returns a list of dictionaries."""
    try:
        with input_path.open('r', encoding='utf-8') as f:
            first_char = f.read(1)
            f.seek(0)
            if first_char == '[':
                # Standard JSON array
                data = json.load(f)
                if not isinstance(data, list):
                    raise ValueError('JSON root must be a list')
                return data
            else:
                # JSON Lines (ndjson)
                return [json.loads(line) for line in f if line.strip()]
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

def main():
    parser = argparse.ArgumentParser(description="Convert JSON/JSONL to PyArrow format.")
    parser.add_argument('input', type=Path, help='Input JSON or JSONL file')
    parser.add_argument('output', type=Path, help='Output file (.arrow or .parquet)')
    parser.add_argument('--format', choices=['arrow', 'parquet'], default=None, help='Output file format (arrow or parquet)')
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
        data = read_json(args.input)
        write_pyarrow(data, args.output, file_format)
    except Exception as e:
        logging.error(f"Conversion failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()

