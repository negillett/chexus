import mock

from chexus import Client
from more_executors import Executors


class MockedClient(Client):
    """A Client with mocked boto3 Session for testing"""

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
        # Only use one retry attempt on tests
        self._executor = Executors.thread_pool(max_workers=4).with_retry(
            max_attempts=1
        )

    @staticmethod
    def mocked_session():
        fake_session = mock.MagicMock()
        fake_session.resource.Bucket.return_value = mock.MagicMock()
        fake_session.resource.Table.return_value = mock.MagicMock()
        return fake_session
