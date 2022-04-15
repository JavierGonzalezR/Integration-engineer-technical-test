import unittest

from app import app


class AppTestCase(unittest.TestCase):

    def test_ping(self):
        tester = app.test_client(self)
        response = tester.get('/ping')
        assert 'PONG' in response.data


if __name__ == '__main__':
    unittest.main()
