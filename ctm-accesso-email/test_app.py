import http.client
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.parse import urlencode
from http.server import ThreadingHTTPServer
from app import Auth, make_handler, OTP_SECONDS, SESSION_SECONDS, READ_PATHS


class AuthTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.now = 10000
        self.sent = []
        self.auth = Auth(Path(self.tmp.name) / 'auth.sqlite3', 's' * 48,
                         ['director@example.org'], lambda e, c: self.sent.append((e, c)),
                         lambda: self.now)

    def tearDown(self):
        self.tmp.cleanup()

    def issue(self):
        challenge = self.auth.issue('Director@example.org', '192.0.2.1')
        return challenge, self.sent[-1][1]

    def test_unknown_address_receives_no_code(self):
        challenge = self.auth.issue('other@example.org', '192.0.2.1')
        self.assertIsNotNone(challenge)
        self.assertEqual(self.sent, [])
        self.assertIsNone(self.auth.verify(challenge, '12345678'))

    def test_code_is_single_use_and_bound_to_challenge(self):
        challenge, code = self.issue()
        self.assertIsNone(self.auth.verify('different-browser', code))
        token = self.auth.verify(challenge, code)
        self.assertEqual(self.auth.identity(token), 'director@example.org')
        self.assertIsNone(self.auth.verify(challenge, code))

    def test_codes_expire(self):
        challenge, code = self.issue()
        self.now += OTP_SECONDS
        self.assertIsNone(self.auth.verify(challenge, code))

    def test_five_failures_lock_code(self):
        challenge, code = self.issue()
        wrong = '00000000' if code != '00000000' else '11111111'
        for _ in range(5):
            self.assertIsNone(self.auth.verify(challenge, wrong))
        self.assertIsNone(self.auth.verify(challenge, code))

    def test_restart_preserves_lockout_and_rate_limits(self):
        challenge, code = self.issue()
        restarted = Auth(self.auth.path, 's' * 48, self.auth.allowed, lambda *_: None, lambda: self.now)
        self.assertIsNone(restarted.issue('director@example.org', '192.0.2.2'))
        self.assertIsNotNone(restarted.verify(challenge, code))

    def test_logout_and_expiry_revoke_access(self):
        challenge, code = self.issue()
        token = self.auth.verify(challenge, code)
        self.auth.logout(token)
        self.assertIsNone(self.auth.identity(token))
        self.now += 61
        challenge, code = self.issue()
        token = self.auth.verify(challenge, code)
        self.now += SESSION_SECONDS
        self.assertIsNone(self.auth.identity(token))

    def test_removed_address_cannot_reuse_session(self):
        challenge, code = self.issue()
        token = self.auth.verify(challenge, code)
        self.auth.allowed = frozenset()
        self.assertIsNone(self.auth.identity(token))

    def test_database_never_contains_plaintext_code_or_tokens(self):
        challenge, code = self.issue()
        with self.auth.db() as db:
            row = str(db.execute('SELECT * FROM otp').fetchall())
        self.assertNotIn(code, row)
        self.assertNotIn(challenge, row)
        token = self.auth.verify(challenge, code)
        with self.auth.db() as db:
            row = str(db.execute('SELECT * FROM sessions').fetchall())
        self.assertNotIn(token, row)

    def test_failed_mail_cannot_leave_valid_code(self):
        def fail(*_):
            raise RuntimeError('smtp secret must not escape')
        self.auth.sender = fail
        with self.assertRaisesRegex(RuntimeError, '^Email delivery unavailable$'):
            self.issue()
        with self.auth.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM otp').fetchone()[0], 0)


class HTTPTests(AuthTests):
    def setUp(self):
        super().setUp()
        self.requests = []
        owner = self
        class Response:
            status = 200
            headers = {'Content-Type': 'application/json'}
            def __init__(self, url): self.url = url
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def read(self, _): return b'{"data":"protected"}'
            def geturl(self): return self.url
        class Opener:
            def open(self, request, timeout):
                owner.requests.append(request)
                return Response(request.full_url)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(
            self.auth, 'https://portal.example.org', 'test-upstream-secret', Opener()))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        super().tearDown()

    def request(self, method, path, fields=None, headers=None):
        c = http.client.HTTPConnection(*self.server.server_address)
        c.request(method, path, urlencode(fields or {}) if fields is not None else None, headers or {})
        r = c.getresponse()
        result = r.status, r.getheaders(), r.read()
        c.close()
        return result

    def post_headers(self):
        return {'Origin': 'https://portal.example.org', 'Content-Type': 'application/x-www-form-urlencoded'}

    def test_all_data_routes_reject_unauthenticated_requests(self):
        for path in READ_PATHS:
            self.assertEqual(self.request('GET', path)[0], 303, path)
        self.assertEqual(self.requests, [])

    def test_full_login_and_secure_cookie(self):
        status, headers, body = self.request('POST', '/auth/request', {'email': 'director@example.org'}, self.post_headers())
        self.assertEqual(status, 200)
        cookie = dict(headers)['Set-Cookie']
        self.assertIn('Secure; HttpOnly; SameSite=Strict', cookie)
        headers = {**self.post_headers(), 'Cookie': cookie.split(';')[0]}
        status, response_headers, _ = self.request('POST', '/auth/verify', {'code': self.sent[-1][1]}, headers)
        self.assertEqual(status, 303)
        session = next(v for k, v in response_headers if k == 'Set-Cookie' and v.startswith('__Host-ctm_session='))
        status, response_headers, body = self.request('GET', '/api/snapshot?day=2026-11-25', headers={'Cookie': session.split(';')[0], 'OAI-Sites-Authorization': 'attacker'})
        self.assertEqual(status, 200)
        self.assertEqual(self.requests[-1].get_header('Oai-sites-authorization'), 'Bearer test-upstream-secret')
        self.assertNotIn(b'test-upstream-secret', body)
        self.assertIn(('Cache-Control', 'private, no-store'), response_headers)

    def test_csrf_rejected_without_sending_email(self):
        for origin in (None, 'https://attacker.example.org'):
            headers = self.post_headers()
            if origin: headers['Origin'] = origin
            else: del headers['Origin']
            self.assertEqual(self.request('POST', '/auth/request', {'email': 'director@example.org'}, headers)[0], 403)
        self.assertEqual(self.sent, [])

    def test_authenticated_user_cannot_write_or_read_owner_pages(self):
        challenge, code = self.issue()
        token = self.auth.verify(challenge, code)
        headers = {'Cookie': '__Host-ctm_session=' + token}
        for path in ('/visite-riservate', '/api/reader-source', '/api/map-bootstrap', '/signin-with-chatgpt'):
            self.assertEqual(self.request('GET', path, headers=headers)[0], 403)
        self.assertEqual(self.request('POST', '/api/schedule', {'anything': 'x'}, {**self.post_headers(), **headers})[0], 403)
        self.assertEqual(self.requests, [])


if __name__ == '__main__':
    unittest.main()
