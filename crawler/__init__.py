import csv
import glob
import logging
import os
from pathlib import Path
import sys
from urllib.parse import urlparse

from crawler.crawler import Crawler
from crawler.downloaders import RequestsDownloader
from crawler.handlers import (
    LocalStoragePDFHandler,
    CSVStatsPDFHandler,
    ProcessHandler,
    get_filename
)


LOG_FORMAT_STR = '[%(asctime)s] %(message)s %(exc_info)s'

logging.basicConfig(
    format=LOG_FORMAT_STR,
    level=logging.INFO,
    stream=sys.stdout,
)
LOGGER = logging.getLogger(__name__)

requests_downloader = RequestsDownloader()


def crawl(url, output_dir, depth=2, method="normal", gecko_path="geckodriver", page_name=None, custom_stats_handler=None, custom_process_handler=None, use_logfile=True):
    head_handlers = {}
    get_handlers = {}

    if use_logfile:
        os.makedirs(output_dir, exist_ok=True)
        logfile_name = os.path.join(output_dir, '_crawl.log')
        fh = logging.FileHandler(logfile_name)
        fh.setFormatter(logging.Formatter(LOG_FORMAT_STR))
        LOGGER.addHandler(fh)

    # get name of page for sub-directories etc. if not custom name given
    if page_name is None:
        page_name = urlparse(url).netloc

    get_handlers['application/pdf'] = LocalStoragePDFHandler(
        directory=output_dir, subdirectory=page_name)

    if custom_stats_handler is None:
        head_handlers['application/pdf'] = CSVStatsPDFHandler(directory=output_dir, name=page_name)
    else:
        for content_type, Handler in custom_stats_handler.items():
            head_handlers[content_type] = Handler

    if custom_process_handler is None:
        process_handler = ProcessHandler()
    else:
        process_handler = custom_process_handler

    if not get_handlers and not head_handlers:
        raise ValueError('You did not specify any output')

    crawler = Crawler(
        downloader=requests_downloader,
        head_handlers=head_handlers,
        get_handlers=get_handlers,
        follow_foreign_hosts=False,
        crawl_method=method,
        gecko_path=gecko_path,
        process_handler=process_handler
    )
    crawler.crawl(url, depth)

def generate_file_layout_script(output_dir, should_copy=True):
    top_symlink_dirname = os.path.join(output_dir, '_readable_symlinks')
    top_copy_dirname = os.path.join('_readable_filenames', output_dir)

    for csv_filename in glob.glob(os.path.join(output_dir, '*.csv')):
        with open(csv_filename, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in reader:
                local_name = row[1]
                canonical_filename = row[0]

                if local_name == 'local_name' and canonical_filename == 'filename':
                    continue # skip header row
                (domain_dir, uuid_filename) = Path(local_name).parts[-2:]
                readable_uniq_fname = canonical_filename.replace('.pdf', '') + '_' + uuid_filename

                if should_copy:
                    copy_dirname = os.path.join(top_copy_dirname, domain_dir)
                    copy_path = os.path.join(copy_dirname, readable_uniq_fname)
                    print(f'mkdir -p "{copy_dirname}" && rsync -cav \\\n  "{local_name}" \\\n  "{copy_path}"')
                else:
                    symlink_dirname = os.path.join(top_symlink_dirname, domain_dir)
                    symlink_path = os.path.join(symlink_dirname, readable_uniq_fname)
                    print(f'mkdir -p "{symlink_dirname}" && ln -s \\\n  "../../../{local_name}" \\\n  "{symlink_path}"')
