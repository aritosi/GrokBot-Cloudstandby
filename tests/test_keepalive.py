import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / 'skills/grokbot-cloudstandby/scripts'

class KeepaliveTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = {**os.environ, 'KA_STATE_DIR': self.tmp.name}

    def tearDown(self): self.tmp.cleanup()

    def run_cmd(self, cmd, *args):
        return subprocess.run([str(SCRIPTS / cmd), *args], env=self.env, text=True,
                              capture_output=True, timeout=12, check=True).stdout.strip()

    def test_multiple_jobs_share_lease_and_only_one_waiter(self):
        a = json.loads(self.run_cmd('ka-start', '--task', 'one', '--until', '1h'))
        b = json.loads(self.run_cmd('ka-start', '--task', 'two', '--until', '1h'))
        self.assertEqual((a['result'], b['result']), ('START', 'JOIN'))
        self.assertNotIn('lease', b)
        p = subprocess.Popen([str(SCRIPTS/'ka-wait'), '--lease', a['lease'], '--seconds', '3'],
                             env=self.env, text=True, stdout=subprocess.PIPE)
        try:
            time.sleep(.3)
            self.assertEqual(self.run_cmd('ka-wait', '--lease', a['lease'], '--seconds', '1'), 'BUSY')
            self.assertEqual(p.communicate(timeout=5)[0].strip(), 'WAIT')
        finally:
            if p.poll() is None: p.terminate(); p.wait()

    def test_simultaneous_starts_grant_only_one_lease(self):
        commands = [[str(SCRIPTS / 'ka-start'), '--task', f'task-{i}', '--until', '1h'] for i in range(8)]
        processes = [subprocess.Popen(c, env=self.env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) for c in commands]
        results = []
        for process in processes:
            stdout, stderr = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0, stderr)
            results.append(json.loads(stdout)['result'])
        self.assertEqual(results.count('START'), 1)
        self.assertEqual(results.count('JOIN'), 7)

    def test_deadline_stops_wait_but_not_task(self):
        until = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=2)).isoformat()
        a = json.loads(self.run_cmd('ka-start', '--task', 'short', '--until', until))
        self.assertEqual(self.run_cmd('ka-wait', '--lease', a['lease'], '--seconds', '5'), 'EXPIRED')
        state = json.loads(self.run_cmd('ka-report'))
        self.assertEqual(state['jobs'][0]['status'], 'expired')
        self.assertFalse(state['watcher'])

    def test_command_success_and_failure_are_distinct(self):
        a = json.loads(self.run_cmd('ka-start', '--task', 'fail', '--until', '1m', '--run', 'printf test; exit 7'))
        self.assertEqual(self.run_cmd('ka-wait', '--lease', a['lease'], '--seconds', '5'), 'DONE')
        state = json.loads(self.run_cmd('ka-report'))
        self.assertEqual(state['jobs'][0]['status'], 'failed')
        self.assertEqual(state['jobs'][0]['exit_code'], 7)
        self.assertEqual(state['jobs'][0]['log_tail'], ['test'])

    def test_stop_does_not_terminate_registered_pid(self):
        p = subprocess.Popen(['sleep','10'])
        try:
            a = json.loads(self.run_cmd('ka-start', '--task', 'external', '--until', '1m', '--pid', str(p.pid)))
            self.run_cmd('ka-stop', '--job', a['job'])
            self.assertIsNone(p.poll())
            self.assertEqual(self.run_cmd('ka-wait', '--lease', a['lease'], '--seconds', '1'), 'STOPPED')
            new = json.loads(self.run_cmd('ka-start', '--task', 'restart', '--until', '1m'))
            self.assertEqual(new['result'], 'START')
            self.assertNotEqual(a['lease'], new['lease'])
        finally:
            p.terminate(); p.wait()

    def test_wait_checks_jobs_without_rewriting_unchanged_state(self):
        a = json.loads(self.run_cmd('ka-start', '--task', 'quiet', '--until', '1m'))
        state_path = Path(self.tmp.name) / 'state.json'
        before = state_path.stat().st_mtime_ns
        self.assertEqual(self.run_cmd('ka-wait', '--lease', a['lease'], '--seconds', '6'), 'WAIT')
        self.assertEqual(state_path.stat().st_mtime_ns, before)

    def test_running_waiter_recognizes_a_user_stop(self):
        a = json.loads(self.run_cmd('ka-start', '--task', 'manual', '--until', '1m'))
        p = subprocess.Popen([str(SCRIPTS/'ka-wait'), '--lease', a['lease'], '--seconds', '8'],
                             env=self.env, text=True, stdout=subprocess.PIPE)
        try:
            time.sleep(.3)
            self.assertEqual(self.run_cmd('ka-stop'), 'STOPPED')
            self.assertEqual(p.communicate(timeout=7)[0].strip(), 'STOPPED')
        finally:
            if p.poll() is None: p.terminate(); p.wait()

if __name__ == '__main__': unittest.main()
