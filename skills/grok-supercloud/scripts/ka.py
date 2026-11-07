#!/usr/bin/env python3
"""Bounded task registry for a Grok foreground waiting agent (stdlib only)."""
import argparse
import contextlib
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
import uuid

ROOT = Path(os.environ.get('KA_STATE_DIR', str(Path.home() / '.grok-supercloud'))).expanduser()
MAX_WAIT = 720
LEASE_SECONDS = 900
HEARTBEAT_SECONDS = 60


def setup():
    ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(ROOT, 0o700)
    (ROOT / 'jobs').mkdir(mode=0o700, exist_ok=True)


@contextlib.contextmanager
def locked(name, nonblocking=False):
    setup()
    fd = os.open(ROOT / (name + '.lock'), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | (fcntl.LOCK_NB if nonblocking else 0))
        yield
    finally:
        os.close(fd)


def load():
    try:
        return json.loads((ROOT / 'state.json').read_text())
    except FileNotFoundError:
        return {'jobs': [], 'lease': None}


def save(state):
    temp = ROOT / ('state.' + uuid.uuid4().hex + '.tmp')
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as out:
        json.dump(state, out, ensure_ascii=False)
        out.flush()
        os.fsync(out.fileno())
    os.replace(temp, ROOT / 'state.json')


def deadline(value):
    now = dt.datetime.now().astimezone()
    v = value.strip().lower()
    if v.endswith(('m', 'h', 'd')) and v[:-1].isdigit():
        duration = int(v[:-1]) * {'m': 60, 'h': 3600, 'd': 86400}[v[-1]]
        target = now + dt.timedelta(seconds=duration)
    elif len(v) == 5 and v[2] == ':':
        parsed = dt.time.fromisoformat(v)
        target = now.replace(hour=parsed.hour, minute=parsed.minute, second=0, microsecond=0)
        if target <= now:
            target += dt.timedelta(days=1)
    else:
        target = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
        if target.tzinfo is None:
            target = target.astimezone()
    if target.timestamp() <= now.timestamp():
        raise ValueError('截止时间必须晚于现在')
    return target.timestamp()


def identity(pid):
    try:
        raw = Path(f'/proc/{pid}/stat').read_text()
        return raw[raw.rfind(')') + 2:].split()[19]
    except (OSError, IndexError):
        return None


def pid_alive(pid, expected):
    if not pid or int(pid) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:
        proc = Path(f'/proc/{int(pid)}/stat').read_text()
        if proc[proc.rfind(')') + 2:].split()[0] == 'Z':
            return False
    except (OSError, IndexError):
        pass
    current = identity(pid)
    return not (expected and current and expected != current)


def refresh(state, now):
    for job in state['jobs']:
        if job['status'] != 'running':
            continue
        result_file = ROOT / 'jobs' / (job['id'] + '.result')
        if result_file.exists():
            try:
                job['exit_code'] = int(result_file.read_text().strip())
                job['status'] = 'completed' if job['exit_code'] == 0 else 'failed'
            except (OSError, ValueError):
                job['status'] = 'unknown'
        elif job['deadline'] <= now:
            job['status'] = 'expired'
        elif job['kind'] == 'pid' and not pid_alive(job.get('pid'), job.get('identity')):
            job['status'] = 'ended'
        elif job['kind'] == 'run' and job.get('pid') and not pid_alive(job['pid'], job.get('identity')):
            job['status'] = 'unknown'


def start(args):
    until = deadline(args.until)
    if args.pid is not None and not pid_alive(args.pid, None):
        raise ValueError('指定的进程不存在或无法访问')
    jobid = uuid.uuid4().hex[:12]
    kind = 'run' if args.run else ('pid' if args.pid else 'hold')
    job = {'id': jobid, 'label': args.task, 'kind': kind, 'deadline': until, 'status': 'running',
           'created': time.time(), 'pid': args.pid, 'identity': identity(args.pid) if args.pid else None}
    if args.run:
        job['command'] = args.run
        job['cwd'] = str(Path(args.cwd).expanduser().resolve())
        job['log'] = str(ROOT / 'jobs' / (jobid + '.log'))
    with locked('state'):
        state = load()
        refresh(state, time.time())
        state['jobs'].append(job)
        lease = state.get('lease')
        created = not lease or lease.get('stopped', False) or lease['expires'] < time.time()
        if created:
            lease = {'token': secrets.token_urlsafe(18), 'expires': time.time() + LEASE_SECONDS, 'jobs': []}
            state['lease'] = lease
        lease.setdefault('jobs', []).append(jobid)
        save(state)
        if args.run:
            try:
                p = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '_worker', jobid],
                                     start_new_session=True, stdin=subprocess.DEVNULL,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True)
            except OSError:
                job['status'] = 'failed'
                save(state)
                raise
            job['pid'] = p.pid
            job['identity'] = identity(p.pid)
            save(state)
    print(json.dumps({'result': 'START' if created else 'JOIN', 'job': jobid,
                      'deadline': dt.datetime.fromtimestamp(until).astimezone().isoformat(),
                      **({'lease': lease['token']} if created else {})}, ensure_ascii=False))


def worker(jobid):
    with locked('state'):
        job = next(j for j in load()['jobs'] if j['id'] == jobid)
    result_file = ROOT / 'jobs' / (jobid + '.result')
    log_fd = os.open(job['log'], os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        with os.fdopen(log_fd, 'ab') as log:
            result = subprocess.run(['/bin/sh', '-c', job['command']], cwd=job['cwd'],
                                    stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
            code = result.returncode
    except Exception:
        code = 125
    fd = os.open(result_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as out:
        out.write(str(code))


def wait(args):
    try:
        with locked('waiter', nonblocking=True):
            end = time.monotonic() + args.seconds
            while True:
                with locked('state'):
                    state = load()
                    lease = state.get('lease')
                    if not lease or not secrets.compare_digest(lease['token'], args.lease):
                        print('STALE'); return
                    if lease.get('stopped', False):
                        print('STOPPED'); return
                    now = time.time()
                    if lease['expires'] < now:
                        print('STALE'); return
                    before = [(j['status'], j.get('exit_code')) for j in state['jobs']]
                    refresh(state, now)
                    live = [j for j in state['jobs'] if j['status'] == 'running']
                    if not live:
                        result = 'EXPIRED' if any(j['status'] == 'expired' and j['id'] in lease.get('jobs', []) for j in state['jobs']) else 'DONE'
                        state['lease'] = None
                        save(state)
                        print(result); return
                    changed = before != [(j['status'], j.get('exit_code')) for j in state['jobs']]
                    renew = lease['expires'] - now <= LEASE_SECONDS - HEARTBEAT_SECONDS
                    if renew:
                        lease['expires'] = now + LEASE_SECONDS
                    if changed or renew:
                        save(state)
                    next_deadline = min(j['deadline'] for j in live)
                remaining = end - time.monotonic()
                if remaining <= 0:
                    print('WAIT'); return
                time.sleep(max(0.05, min(5, remaining, max(0.05, next_deadline-time.time()))))
    except BlockingIOError:
        print('BUSY')


def report(args):
    with locked('state'):
        state = load(); refresh(state, time.time()); save(state)
    rows = []
    for job in state['jobs']:
        if args.job and job['id'] != args.job:
            continue
        row = {k: job[k] for k in ('id', 'label', 'kind', 'status', 'exit_code') if k in job}
        row['deadline'] = dt.datetime.fromtimestamp(job['deadline']).astimezone().isoformat()
        if job.get('log'):
            try:
                with open(job['log'], 'rb') as f:
                    f.seek(0, 2); f.seek(max(0, f.tell()-8192)); row['log_tail'] = f.read().decode('utf-8', 'replace').splitlines()[-8:]
            except OSError:
                row['log_tail'] = []
        rows.append(row)
    lease = state.get('lease')
    print(json.dumps({'jobs': rows, 'watcher': bool(lease and not lease.get('stopped', False) and lease['expires'] > time.time())}, ensure_ascii=False))


def stop(args):
    with locked('state'):
        state = load()
        for job in state['jobs']:
            if job['status'] == 'running' and (not args.job or job['id'] == args.job):
                job['status'] = 'stopped'
        if not any(j['status'] == 'running' for j in state['jobs']):
            if state.get('lease'):
                state['lease']['stopped'] = True
        save(state)
    print('STOPPED')


def main():
    p = argparse.ArgumentParser(description='Grok Supercloud keepalive')
    sub = p.add_subparsers(dest='action', required=True)
    a = sub.add_parser('start'); a.add_argument('--task', required=True); a.add_argument('--until', required=True)
    group = a.add_mutually_exclusive_group(); group.add_argument('--pid', type=int); group.add_argument('--run')
    a.add_argument('--cwd', default='.')
    w = sub.add_parser('wait'); w.add_argument('--lease', required=True); w.add_argument('--seconds', type=int, default=MAX_WAIT, choices=range(1, MAX_WAIT+1), metavar='1..720')
    r = sub.add_parser('report'); r.add_argument('--job')
    s = sub.add_parser('stop'); s.add_argument('--job')
    worker_p = sub.add_parser('_worker'); worker_p.add_argument('job')
    args = p.parse_args()
    try:
        {'start': start, 'wait': wait, 'report': report, 'stop': stop, '_worker': lambda x: worker(x.job)}[args.action](args)
    except (ValueError, OSError, KeyError) as e:
        if args.action == 'wait': print('ERROR')
        else: print(str(e), file=sys.stderr)
        raise SystemExit(1)

if __name__ == '__main__':
    main()
