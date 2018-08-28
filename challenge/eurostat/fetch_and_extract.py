import requests
from pyunpack import Archive
from tempfile import NamedTemporaryFile
from urllib.parse import urlencode
from challenge.eurostat import InvalidEurostatApiResponse


class EurostatDataFetcher(object):
    def __init__(self, base_url, filename_template, extracted_files_dir):
        """
        :param base_url: BulkDownload url
        :type base_url: str
        :param filename_template: Path to the file in Eurostat file tree with date format injected.
        Example: some_dir/some_sub_dir/some_file_%Y%m.7z
        :type filename_template: str
        :param extracted_files_dir: Directory under which files should be extracted
        :type extracted_files_dir: str
        """
        self.base_url = base_url
        self.filename_template = filename_template
        self.extracted_files_dir = extracted_files_dir

    def _validate_api_response(self, response):
        if response.status_code != 200:
            raise InvalidEurostatApiResponse(status_code=response.status_code)

    def _build_url(self, date):
        params = urlencode(query={'file': date.strftime(self.filename_template)})
        return '{base_url}?{query}'.format(base_url=self.base_url, query=params)

    def fetch_and_extract_to_file(self, date):
        """
        Fetches archive file from Eurostat BulkDownload based on date included in file name.
        Extracts file content into specified directory.
        :type date: datetime.datetime
        """
        fetch_url = self._build_url(date)

        response = requests.get(fetch_url, stream=True)
        self._validate_api_response(response)

        with NamedTemporaryFile(mode='wb') as fd:
            fd.write(response.content)
            Archive(fd.name).extractall(self.extracted_files_dir, auto_create_dir=True)
