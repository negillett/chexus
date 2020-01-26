import mock

from chexus import Client


class MockedClient(Client):
    def __init__(
        self,
        access_id=None,
        access_key=None,
        session_token=None,
        default_region=None,
    ):
        with mock.patch("boto3.Session") as mocked_sessions:
            mocked_sessions.return_value = mocked_sessions
            Client.__init__(
                self, access_id, access_key, session_token, default_region
            )

    @staticmethod
    def mocked_session():
        fake_session = mock.MagicMock()
        fake_session.resource.Bucket.return_value = mock.MagicMock()
        fake_session.resource.Table.return_value = mock.MagicMock()
        return fake_session
