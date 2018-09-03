from challenge.eurostat.fetch_and_extract import EurostatDataFetcher, InvalidEurostatApiResponse
from unittest import mock, TestCase
from datetime import datetime
import pytest


class TestEurostatDataFetcher(TestCase):
    def setUp(self):
        self.client = EurostatDataFetcher(
            base_url='base',
            filename_template='file_%Y-%m',
            extracted_files_dir='.'
        )

    @mock.patch('challenge.eurostat.fetch_and_extract.requests')
    def test_should_raise_exception_when_response_status_differ_from_200(self, requests_mock):
        mocked_response = mock.MagicMock()
        mocked_response.status_code = 400
        requests_mock.get().return_value = mocked_response
        with pytest.raises(InvalidEurostatApiResponse):
            # TODO: no idea how to mock/disable retry decorator (tested priv method instead) :(
            self.client._validate_api_response(requests_mock.get())

    @mock.patch('challenge.eurostat.fetch_and_extract.requests')
    def test_should_build_valid_url(self, requests_mock):
        mocked_response = mock.MagicMock()
        mocked_response.status_code = 200
        requests_mock.get().return_value = mocked_response
        expected_url = 'base?file=file_2018-04'

        url = self.client.build_url(datetime(2018, 4, 1))

        self.assertEquals(url, expected_url)