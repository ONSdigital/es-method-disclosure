"""  This Module is for the testing of the disclosure module. """
import os
import os.path
from unittest import TestCase, mock
from src import disclosure


class DisclosureTestCase(TestCase):
    """ This is a class for the disclosure testing. """

    def _get_test_data_fh(self, filename):
        """
        Open a file under the fixtures directory.
        Opens the file in read mode and returns the file object.

        :param input: file url.
        :return: file object.
        """
        filename = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures',
                filename
            )
        )
        return open(filename, 'r')

    @mock.patch('src.disclosure.client')
    def test_successful_disclosure(self, mock_client):
        """
        Mocking the connection to the client.
        :param mock_client:
        :return:
        """
        test_fh = self._get_test_data_fh("testdatadisclosure.csv")
        mock_client.file.return_value.getFile.return_value = test_fh
        result = disclosure.apply(
            {"s3Pointer": "es-algo-poc/luke2.csv"}
        )

        assert "success" in result
        assert result["success"] is True
