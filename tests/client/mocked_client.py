import mock

from chexus import Client


class MockedClient(Client):
    def __init__(self):
        super(Client).__init__()
        self._session = self.mock_session()

    @staticmethod
    def mock_session():
        fake_session = mock.MagicMock()
        fake_session.resource.Bucket.return_value = mock.MagicMock()
        fake_session.resource.Table.return_value = mock.MagicMock()
        return fake_session
