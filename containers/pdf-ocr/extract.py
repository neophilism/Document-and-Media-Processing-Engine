"""Fixed-profile PDF OCR worker. No user-controlled commands or configuration."""
import csv
import json
import os
import re
import subprocess
import sys

LIMIT = 8 * 1024 * 1024


def run(args):
    # Diagnostics go only to the container's bounded stderr, never derivative text.
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL, timeout=15)


def main():
    # File output and RLIMIT_FSIZE keep parser stdout from becoming an unbounded pipe.
    with open('/tmp/info', 'wb') as output:
        subprocess.run(['/usr/bin/pdfinfo', '/input/source.pdf'], check=True,
                       stdout=output, timeout=3)
    with open('/tmp/info', 'rb') as stream:
        info = stream.read(65537)
    match = re.search(rb'^Pages:\s+(\d+)\s*$', info, re.MULTILINE)
    if len(info) > 65536 or not match or not 1 <= int(match[1]) <= 10:
        raise ValueError('unsupported page count')
    pages = []
    total_words = 0
    for number in range(1, int(match[1]) + 1):
        run(['/usr/bin/pdftoppm', '-f', str(number), '-l', str(number), '-singlefile',
             '-scale-to', '2000', '-gray', '/input/source.pdf', '/tmp/page'])
        run(['/usr/bin/tesseract', '/tmp/page.pgm', '/tmp/ocr', '-l', 'eng', '--psm', '3', 'tsv'])
        if os.path.getsize('/tmp/ocr.tsv') > LIMIT:
            raise ValueError('output limit')
        with open('/tmp/ocr.tsv', encoding='utf-8', newline='') as stream:
            rows = csv.DictReader(stream, delimiter='\t')
            words = []
            width = height = None
            for row in rows:
                if row['level'] == '1':
                    width, height = int(row['width']), int(row['height'])
                elif row['level'] == '5' and row['text'].strip():
                    total_words += 1
                    if total_words > 10000 or len(row['text']) > 1000:
                        raise ValueError('word limit')
                    confidence = float(row['conf'])
                    words.append(dict(text=row['text'], left=int(row['left']), top=int(row['top']),
                                      width=int(row['width']), height=int(row['height']),
                                      confidence=None if confidence < 0 else confidence / 100,
                                      block=int(row['block_num']), paragraph=int(row['par_num']),
                                      line=int(row['line_num'])))
        if width is None or height is None:
            raise ValueError('missing page geometry')
        pages.append(dict(page=number, width=width, height=height, words=words))
        os.unlink('/tmp/page.pgm')
        os.unlink('/tmp/ocr.tsv')
    result = json.dumps(dict(version=1, language='eng', pages=pages), ensure_ascii=True, allow_nan=False)
    if len(result.encode('utf-8')) > LIMIT:
        raise ValueError('output limit')
    sys.stdout.write(result)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.stderr.write('OCR worker rejected input or exceeded its fixed profile\n')
        sys.exit(1)
