"""Independent, read-only CTM email OTP gateway. Python 3.11+, no dependencies."""
import hashlib
import hmac
import html
import json
import os
import re
import secrets
import smtplib
import sqlite3
import ssl
import time
from email.message import EmailMessage
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit, urlencode
from urllib.request import Request, build_opener, HTTPRedirectHandler

UPSTREAM = 'https://ctm-servizio-live.energeticflounder1.chatgpt.site'
READ_PATHS = frozenset(('/', '/guida', '/guida.docx', '/fleet-analysis',
    '/api/snapshot', '/api/passages', '/api/headways', '/api/fleet-mission',
    '/api/fleet-line', '/api/stop-history', '/api/stop-history-work',
    '/api/address-history', '/api/address-work', '/api/deviations',
    '/api/line-crossings-work', '/api/operations'))
OTP_SECONDS, SESSION_SECONDS, MAX_ATTEMPTS = 600, 28800, 5
GENERIC = 'Se l’indirizzo è autorizzato, riceverai un codice. Controlla anche la posta indesiderata.'


class Auth:
    def __init__(self, path, secret, allowed, sender, clock=time.time):
        if len(secret) < 32:
            raise ValueError('SESSION_SECRET must contain at least 32 characters')
        self.path, self.secret = str(path), secret.encode()
        self.allowed = frozenset(x.strip().lower() for x in allowed if x.strip())
        self.sender, self.clock = sender, clock
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.db() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS otp (
                    challenge TEXT PRIMARY KEY, email TEXT, digest TEXT,
                    expires REAL, attempts INTEGER DEFAULT 0);
                CREATE TABLE IF NOT EXISTS sessions (
                    digest TEXT PRIMARY KEY, email TEXT, expires REAL);
                CREATE TABLE IF NOT EXISTS rates (
                    key TEXT PRIMARY KEY, start REAL, count INTEGER, last REAL);
            ''')
        os.chmod(self.path, 0o600)

    def db(self):
        return sqlite3.connect(self.path, timeout=10)

    def digest(self, value):
        return hmac.new(self.secret, value.encode(), hashlib.sha256).hexdigest()

    def issue(self, email, ip):
        email, now = email.strip().lower(), self.clock()
        challenge, code = secrets.token_urlsafe(32), f'{secrets.randbelow(100_000_000):08d}'
        # Rate limits survive restarts and apply to permitted and unknown email alike.
        keys = [(self.digest('email:' + email), 10, 60),
                (self.digest('ip:' + ip), 30, 2)]
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('DELETE FROM rates WHERE start < ?', (now - 3600,))
            db.execute('DELETE FROM otp WHERE expires <= ?', (now,))
            db.execute('DELETE FROM sessions WHERE expires <= ?', (now,))
            for key, limit, cooldown in keys:
                row = db.execute('SELECT start,count,last FROM rates WHERE key=?', (key,)).fetchone()
                if row and (row[1] >= limit or now - row[2] < cooldown):
                    return None
            for key, _, _ in keys:
                db.execute('INSERT INTO rates VALUES (?,?,1,?) ON CONFLICT(key) DO UPDATE SET count=count+1,last=excluded.last',
                           (key, now, now))
            if email in self.allowed:
                db.execute('DELETE FROM otp WHERE email=?', (email,))
                db.execute('INSERT INTO otp VALUES (?,?,?,?,0)',
                           (self.digest(challenge), email, self.digest(challenge + ':' + code), now + OTP_SECONDS))
        if email in self.allowed:
            try:
                self.sender(email, code)
            except Exception:
                with self.db() as db:
                    db.execute('DELETE FROM otp WHERE challenge=?', (self.digest(challenge),))
                raise RuntimeError('Email delivery unavailable') from None
        return challenge

    def verify(self, challenge, code):
        now = self.clock()
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            key = self.digest(challenge)
            row = db.execute('SELECT email,digest,expires,attempts FROM otp WHERE challenge=?', (key,)).fetchone()
            if not row:
                return None
            email, digest, expires, attempts = row
            if expires <= now or attempts >= MAX_ATTEMPTS or email not in self.allowed:
                db.execute('DELETE FROM otp WHERE challenge=?', (key,))
                return None
            if not re.fullmatch(r'[0-9]{8}', code) or not hmac.compare_digest(digest, self.digest(challenge + ':' + code)):
                db.execute('UPDATE otp SET attempts=attempts+1 WHERE challenge=?', (key,))
                return None
            db.execute('DELETE FROM otp WHERE challenge=?', (key,))
            db.execute('DELETE FROM sessions WHERE email=?', (email,))
            token = secrets.token_urlsafe(48)
            db.execute('INSERT INTO sessions VALUES (?,?,?)', (self.digest(token), email, now + SESSION_SECONDS))
            return token

    def identity(self, token):
        if not token:
            return None
        with self.db() as db:
            row = db.execute('SELECT email,expires FROM sessions WHERE digest=?', (self.digest(token),)).fetchone()
        return row[0] if row and row[1] > self.clock() and row[0] in self.allowed else None

    def logout(self, token):
        with self.db() as db:
            db.execute('DELETE FROM sessions WHERE digest=?', (self.digest(token),))


def smtp_sender(env):
    def send(email, code):
        message = EmailMessage()
        message['From'], message['To'] = env['SMTP_FROM'], email
        message['Subject'] = 'CTM · Codice di accesso alla dashboard'
        message.set_content(f'Il tuo codice di accesso è: {code}\n\n'
                            'Scade tra 10 minuti e può essere utilizzato una sola volta.\n'
                            'Non condividerlo. Se non hai richiesto l’accesso, ignora questa email.\n')
        host, port = env['SMTP_HOST'], int(env.get('SMTP_PORT', '587'))
        if port == 465:
            connection = smtplib.SMTP_SSL(host, port, timeout=20, context=ssl.create_default_context())
        else:
            connection = smtplib.SMTP(host, port, timeout=20)
        with connection as server:
            if port != 465:
                server.ehlo()
                server.starttls(context=ssl.create_default_context())
                server.ehlo()
            server.login(env['SMTP_USER'], env['SMTP_PASSWORD'])
            server.send_message(message)
    return send


def page(message='', code=False):
    form = '''<form action="/auth/verify" method="post"><label for="code">Codice ricevuto via email</label>
    <input id="code" name="code" inputmode="numeric" autocomplete="one-time-code" pattern="[0-9]{8}" maxlength="8" required>
    <button>Accedi alla dashboard</button></form><p><a href="/accesso">Richiedi un nuovo codice</a></p>''' if code else '''
    <form action="/auth/request" method="post"><label for="email">Email aziendale</label>
    <input id="email" type="email" name="email" autocomplete="email" maxlength="254" required>
    <button>Ricevi il codice via email</button></form>'''
    return f'''<!doctype html><html lang="it"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
    <title>CTM · Accesso riservato</title><style>*{{box-sizing:border-box}}body{{margin:0;background:#edf4f3;color:#16343f;font:17px/1.6 system-ui;display:grid;min-height:100vh;place-items:center;padding:24px}}
    main{{background:white;max-width:480px;width:100%;padding:36px;border-radius:18px;border-top:7px solid #087c78}}small{{color:#087c78;font-weight:700}}h1{{line-height:1.2}}label{{display:block;font-weight:600}}input,button{{font:inherit;width:100%;padding:13px;margin:8px 0 16px;border:1px solid #8aa4ab;border-radius:8px}}button{{background:#087c78;color:white;border:0;font-weight:700;cursor:pointer}}a{{color:#087c78}}.message{{background:#edf4f3;padding:14px;border-radius:8px}}</style></head>
    <body><main><small>CTM SpA · OSSERVATORIO SERVIZIO</small><h1>Accesso riservato</h1><p>Consulta la dashboard con la tua email e un codice temporaneo.</p>
    {('<p class="message" role="status">' + html.escape(message) + '</p>') if message else ''}{form}
    <p>Accesso personale consentito esclusivamente agli indirizzi autorizzati.</p></main></body></html>'''.encode()


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def make_handler(auth, origin, upstream_token, opener=None):
    if not re.fullmatch(r'https://[a-zA-Z0-9.-]+', origin):
        raise ValueError('PUBLIC_ORIGIN must be a bare HTTPS origin')
    opener = opener or build_opener(NoRedirect())

    class Handler(BaseHTTPRequestHandler):
        # Never log query parameters, email addresses, session cookies or OTP codes.
        def log_message(self, *_):
            pass

        def respond(self, status, body=b'', content_type='text/html; charset=utf-8', headers=()):
            self.send_response(status)
            for key, value in [('Content-Type', content_type), ('Cache-Control', 'private, no-store'),
                               ('X-Content-Type-Options', 'nosniff'), ('Referrer-Policy', 'no-referrer'),
                               ('X-Frame-Options', 'DENY'), ('Content-Security-Policy', "frame-ancestors 'none'; base-uri 'self'; form-action 'self'"),
                               ('Strict-Transport-Security', 'max-age=31536000'), *headers]:
                self.send_header(key, value)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            if self.command != 'HEAD':
                self.wfile.write(body)

        def cookie(self, name):
            cookie = SimpleCookie()
            try:
                cookie.load(self.headers.get('Cookie', ''))
                return cookie[name].value if name in cookie else ''
            except Exception:
                return ''

        def set_cookie(self, name, token, seconds):
            return ('Set-Cookie', f'{name}={token}; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age={seconds}')

        def do_HEAD(self):
            self.do_GET()

        def do_GET(self):
            path = urlsplit(self.path).path
            if path == '/health':
                return self.respond(200, b'{"status":"ok"}', 'application/json')
            if path == '/accesso':
                return self.respond(200, page())
            if path == '/esci':
                return self.respond(200, b'<form action="/auth/logout" method="post"><button>Esci dalla dashboard</button></form>')
            if not auth.identity(self.cookie('__Host-ctm_session')):
                return self.respond(303, headers=[('Location', '/accesso')])
            if path not in READ_PATHS:
                return self.respond(403, b'Operazione non consentita')
            # Fixed origin, exact path allowlist, GET only, no caller-provided headers.
            query = urlsplit(self.path).query
            target = UPSTREAM + path + ('?' + query if query else '')
            request = Request(target, headers={'OAI-Sites-Authorization': 'Bearer ' + upstream_token,
                                              'Accept-Encoding': 'identity'}, method='GET')
            try:
                with opener.open(request, timeout=45) as response:
                    data = response.read(32 * 1024 * 1024 + 1)
                    if len(data) > 32 * 1024 * 1024:
                        return self.respond(502, b'Risposta troppo grande')
                    if response.geturl().split('?')[0] != target.split('?')[0]:
                        return self.respond(502, b'Accesso alla fonte non disponibile')
                    mime = response.headers.get('Content-Type', 'application/octet-stream')
                    extra = []
                    disposition = response.headers.get('Content-Disposition')
                    if disposition:
                        extra.append(('Content-Disposition', disposition))
                    return self.respond(response.status, data, mime, extra)
            except HTTPError as error:
                return self.respond(error.code if error.code in (404, 429, 503) else 502,
                                    b'Dato temporaneamente non disponibile')
            except (URLError, TimeoutError, OSError):
                return self.respond(502, b'Fonte temporaneamente non disponibile')

        def do_POST(self):
            # Strict origin validation prevents login and logout CSRF.
            if self.headers.get('Origin') != origin:
                return self.respond(403, b'Richiesta non consentita')
            if self.headers.get('Content-Type', '').split(';')[0] != 'application/x-www-form-urlencoded':
                return self.respond(415, b'Formato non consentito')
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 2048:
                    return self.respond(413, b'Richiesta non valida')
                form = parse_qs(self.rfile.read(length).decode('utf-8'), max_num_fields=4)
            except (ValueError, UnicodeError):
                return self.respond(400, b'Richiesta non valida')
            path = urlsplit(self.path).path
            if path == '/auth/request':
                email = form.get('email', [''])[0]
                if len(email) > 254 or not re.fullmatch(r'[^\s@]+@[^\s@]+\.[^\s@]+', email):
                    return self.respond(400, page('Inserisci un indirizzo email valido.'))
                # On Render, use the last appended forwarded IP. Rate limits also apply per email.
                ip = self.headers.get('X-Forwarded-For', self.client_address[0]).split(',')[-1].strip()
                try:
                    challenge = auth.issue(email, ip)
                except RuntimeError:
                    return self.respond(503, page('Invio email non disponibile. Riprova più tardi.'))
                if not challenge:
                    return self.respond(429, page('Attendi un minuto prima di richiedere un nuovo codice.'))
                return self.respond(200, page(GENERIC, code=True), headers=[
                    self.set_cookie('__Host-ctm_challenge', challenge, OTP_SECONDS)])
            if path == '/auth/verify':
                token = auth.verify(self.cookie('__Host-ctm_challenge'), form.get('code', [''])[0])
                if not token:
                    return self.respond(401, page('Codice non valido o scaduto.', code=True))
                return self.respond(303, headers=[('Location', '/'),
                    self.set_cookie('__Host-ctm_session', token, SESSION_SECONDS),
                    self.set_cookie('__Host-ctm_challenge', '', 0)])
            if path == '/auth/logout':
                auth.logout(self.cookie('__Host-ctm_session'))
                return self.respond(303, headers=[('Location', '/accesso'),
                    self.set_cookie('__Host-ctm_session', '', 0)])
            return self.respond(403, b'Operazione non consentita')

    return Handler


def main():
    env = os.environ
    required = ['PUBLIC_ORIGIN', 'SESSION_SECRET', 'ALLOWED_EMAILS', 'UPSTREAM_TOKEN',
                'SMTP_HOST', 'SMTP_FROM', 'SMTP_USER', 'SMTP_PASSWORD']
    missing = [key for key in required if not env.get(key)]
    if missing:
        raise SystemExit('Missing configuration keys: ' + ', '.join(missing))
    auth = Auth(env.get('AUTH_DATABASE', '/var/data/auth.sqlite3'), env['SESSION_SECRET'],
                env['ALLOWED_EMAILS'].split(','), smtp_sender(env))
    server = ThreadingHTTPServer(('0.0.0.0', int(env.get('PORT', '10000'))),
                                make_handler(auth, env['PUBLIC_ORIGIN'], env['UPSTREAM_TOKEN']))
    server.serve_forever()


if __name__ == '__main__':
    main()
