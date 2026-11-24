#!/usr/bin/env python3
"""Create a fresh synthetic space and ephemeral lab credentials before building."""
import argparse
import json
from pathlib import Path
import secrets
from urllib.parse import urlsplit
import uuid


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--url', required=True, help='Trusted HTTPS origin of the disposable server')
    p.add_argument('--state', type=Path, required=True)
    p.add_argument('--team', help='Optional local Apple development team for signing')
    a = p.parse_args()
    u = urlsplit(a.url)
    if u.scheme != 'https' or not u.hostname or u.username is not None or u.path not in ('', '/') or u.query or u.fragment:
        p.error('--url must be an HTTPS origin')
    if a.team and not a.team.isalnum():
        p.error('team must be alphanumeric')
    a.state.mkdir(parents=True, exist_ok=True)
    config_path = a.state / 'config.json'
    if config_path.exists():
        p.error('state already configured; choose a new directory to preserve evidence')
    cfg = {'url':a.url.rstrip('/'), 'token':secrets.token_hex(20),
           'password':secrets.token_hex(24), 'space':str(uuid.uuid4()),
           'actors':{r:str(uuid.uuid4()) for r in ('pixel','iphone','desktop')}}
    data = json.dumps(cfg, indent=2)
    config_path.write_text(data)
    config_path.chmod(0o600)
    app = Path(__file__).resolve().parent / 'app'
    assets = app / 'resource/assets'
    assets.mkdir(parents=True, exist_ok=True)
    (assets / 'lab.json').write_text(data)
    if a.team:
        (app / 'platform/ios/DayApp.local.xcconfig').write_text(
            f'DEVELOPMENT_TEAM = {a.team}\nCODE_SIGN_STYLE = Automatic\n')
    print(f'Configured fresh space {cfg["space"]}; build both devices before starting campaign.')


if __name__ == '__main__':
    main()
