#!/usr/bin/env python3
"""Real local OS-process RF3 acceptance; keeps data and raw evidence on failure."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import sys
import time
import urllib.request
import uuid



BUCKET_COUNT = 256


def bucket_for(key):
    """Independent SHA256 bucket computation (route version Sha256Bucket256V1)."""
    raw = key.encode() if isinstance(key, str) else bytes(key)
    digest = hashlib.sha256(b'multiraft-slot/key/sha256-v1\0' + len(raw).to_bytes(8, 'big') + raw).digest()
    return int.from_bytes(digest[:8], 'big') % BUCKET_COUNT


def slot_for(key, layout):
    """Route through the bucket table read from the layout artifact; no embedded table."""
    buckets = layout['buckets']
    assert layout['bucket_count'] == BUCKET_COUNT and len(buckets) == BUCKET_COUNT, 'layout bucket table must have 256 entries'
    return buckets[bucket_for(key)]


def free_ports(count):
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind(('127.0.0.1', 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


class Run:
    def __init__(self, binary, output, layout_tool=None, slot_count=4):
        self.binary = str(Path(binary).resolve())
        self.layout_tool = str(Path(layout_tool).resolve() if layout_tool else Path(self.binary).with_name('slot-layout'))
        self.slot_count = slot_count
        self.output = output.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.processes = {}
        self.records = []
        self.cases = []
        self.stop_signal = None
        self.cleaning = False
        self.fsm_storage = 'image'
        self.extra_args = []
        self.data_tag = None
        ports = free_ports(9)
        self.admin = dict(zip(range(1, 4), ports[6:]))
        self.placement = dict(cluster_id='slot-acceptance-' + uuid.uuid4().hex, slot_count=slot_count, group_id_base=100,
                              nodes=[dict(node_id=i, raft=f'127.0.0.1:{ports[i-1]}', application_rpc=f'127.0.0.1:{ports[i+2]}', weight=1) for i in range(1, 4)])
        self.layout_path = self.output / 'layout.json'
        self.requests = (self.output / 'requests.jsonl').open('a', buffering=1)
        self._layout = None
        self._keys = None

    def ensure_layout(self):
        """Generate the v2 artifact with the Rust placement tool on first use."""
        if self._layout is None:
            placement_path = self.output / 'placement-input.json'
            placement_path.write_text(json.dumps(self.placement, indent=2))
            generated = subprocess.run([self.layout_tool, '--input', str(placement_path), '--output', str(self.layout_path)],
                                       check=True, capture_output=True, text=True)
            self.layout_digests = json.loads(generated.stdout)
            self._layout = json.loads(self.layout_path.read_text())
            assert self._layout['format_version'] == 2 and self._layout['slot_count'] == self.slot_count, self._layout
        return self._layout

    @property
    def layout(self):
        return self.ensure_layout()

    @property
    def keys(self):
        if self._keys is None:
            keys = {}
            for n in range(100_000):
                key = f'key-{n}'
                keys.setdefault(slot_for(key, self.layout), key)
                if len(keys) == self.slot_count:
                    break
            assert len(keys) == self.slot_count, keys
            self._keys = keys
        return self._keys

    def record_signal(self, signum, _frame):
        # Signal handlers only publish intent. They never throw across ownership
        # registration, cleanup, receipt IO, or handler installation/restoration.
        if self.stop_signal is None:
            self.stop_signal = signum

    def checkpoint(self):
        if not self.cleaning and self.stop_signal is not None:
            raise InterruptedBySignal(self.stop_signal)

    def api(self, node, path, data=None, timeout=10):
        self.checkpoint()
        started = time.monotonic()
        record = dict(node=node, path=path, request=data)
        try:
            body = None if data is None else json.dumps(data).encode()
            request = urllib.request.Request(f'http://127.0.0.1:{self.admin[node]}{path}', data=body, headers={'Content-Type': 'application/json'})
            with urllib.request.urlopen(request, timeout=min(timeout, 10)) as response:
                result = json.load(response)
            record['response'] = result
            self.checkpoint()
            return result
        except Exception as exc:
            record['transport_error'] = repr(exc)
            raise
        finally:
            record['elapsed_seconds'] = time.monotonic() - started
            self.requests.write(json.dumps(record) + '\n')

    def poll(self, label, predicate, seconds=30):
        until = time.monotonic() + seconds
        last = None
        while time.monotonic() < until:
            self.checkpoint()
            try:
                last = predicate()
                self.checkpoint()
                if last:
                    return last
            except (OSError, ValueError, KeyError):
                pass
            time.sleep(.1)
        raise AssertionError(f'{label} timed out; last={last!r}')

    def data_dir(self, node):
        tag = self.data_tag or self.fsm_storage
        return self.output / (f'data-{node}' if tag == 'image' else f'{tag}-data-{node}')

    def start(self, node, layout=None, env=None):
        self.checkpoint()
        assert node not in self.processes
        log_path = self.output / f'node-{node}-{time.time_ns()}.log'
        log = log_path.open('wb')
        command = [self.binary, '--layout', str(layout or self.layout_path), '--node-id', str(node), '--data-dir', str(self.data_dir(node)), '--admin', f'127.0.0.1:{self.admin[node]}', '--test-controls', '--fsm-storage', self.fsm_storage] + self.extra_args
        try:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                                       env=dict(os.environ, **env) if env else None)
            self.processes[node] = process
            self.records.append(dict(node=node, pid=process.pid, command=command, log=str(log_path), env=env))
        finally:
            log.close()
        self.checkpoint()
        return process

    def disconnect_gated_write(self, node, key, command_id, delta):
        """Send a real HTTP request, confirm apply entry, discard its response."""
        self.checkpoint()
        request = dict(key=key, command_id=command_id, delta=delta, timeout_ms=30_000)
        body = json.dumps(request).encode()
        raw = (f'POST /write HTTP/1.1\r\nHost: 127.0.0.1:{self.admin[node]}\r\n'
               f'Content-Type: application/json\r\nContent-Length: {len(body)}\r\n'
               'Connection: close\r\n\r\n').encode() + body
        connection = socket.create_connection(('127.0.0.1', self.admin[node]), timeout=10)
        entered = None
        try:
            connection.sendall(raw)
            self.requests.write(json.dumps(dict(node=node, path='/write', request=request, event='raw_http_sent', response_received=False)) + '\n')
            entered = self.poll('socket command gate entered', lambda: (state if (state := self.api(node, '/status'))['gate']['entered'] and state['gate']['command_id'] == command_id else None))
            self.requests.write(json.dumps(dict(node=node, command_id=command_id, event='gate_entered_before_disconnect', gate=entered['gate'])) + '\n')
            # Linux and macOS both use two native ints for struct linger.
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        finally:
            connection.close()
            self.requests.write(json.dumps(dict(node=node, command_id=command_id, event='raw_http_socket_closed', gate_entered=entered is not None, response_received=False, socket_closed=connection.fileno() == -1)) + '\n')
        self.checkpoint()
        return dict(gate=entered['gate'], socket_closed=True, response_received=False)

    def ready(self, nodes):
        for node in nodes:
            self.poll(f'node {node} ready', lambda: self.api(node, '/status'))

    def stop(self, node, kill=False):
        self.checkpoint()
        process = self.processes[node]
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGKILL if kill else signal.SIGTERM)
            except ProcessLookupError:
                pass  # Exited between poll and signal; still wait/reap below.
        try:
            code = process.wait(timeout=35)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            code = process.wait(timeout=5)
            self.processes.pop(node)
            raise AssertionError(f'node {node} shutdown required kill ({code})')
        self.processes.pop(node)
        for record in self.records:
            if record['pid'] == process.pid:
                record.update(exit_code=code, deliberate_kill=kill)
        if not kill:
            assert code == 0, (node, code)
        self.checkpoint()
        return code

    def leader(self, slot, nodes=(1, 2, 3)):
        def observe():
            leaders = [self.api(n, '/status')['slots'][slot]['group']['leader'] for n in nodes]
            return leaders[0] if leaders[0] in nodes and len(set(leaders)) == 1 else None
        return self.poll(f'slot {slot} leader in {nodes}', observe)

    def write(self, node, key, cid, delta=1, timeout_ms=5000):
        return self.api(node, '/write', dict(key=key, command_id=cid, delta=delta, timeout_ms=timeout_ms))

    def read(self, node, key, expected, local=False, applied_at_least=None):
        response = self.api(node, '/read', dict(key=key, local=local))
        assert response.get('ok') and response['result']['value'] == expected, response
        if not local:
            # A linearizable read carries the applied index its value reflects.
            applied = response.get('applied')
            assert isinstance(applied, dict) and isinstance(applied.get('index'), int), response
            assert applied_at_least is None or applied['index'] >= applied_at_least, (applied_at_least, response)
        return response['result']

    def passed(self, name, **evidence):
        self.cases.append(dict(name=name, passed=True, **evidence))
        print('PASS', name, flush=True)

    def normal(self):
        self.ensure_layout()
        for node in (1, 2, 3):
            self.start(node)
        self.ready((1, 2, 3))
        statuses = [self.api(n, '/status') for n in (1, 2, 3)]
        assert len({json.dumps(s['digest']) for s in statuses}) == 1
        assert len({p.pid for p in self.processes.values()}) == 3
        for state in statuses:
            assert all(s['storage']['backend'] == 'Some(RocksDb)' and s['storage']['mode'] == 'NativeDurable' for s in state['slots']), state
        expected = {}
        for slot, key in self.keys.items():
            leader = self.leader(slot)
            follower = next(n for n in (1, 2, 3) if n != leader)
            result = self.write(follower, key, f'first-{slot}', 3)
            effect = dict(key=key, command_id=f'first-{slot}', value=3, accepted=True)
            assert result.get('ok') and result['effects'] == effect and result['slot'] == slot, result
            retry = self.write(follower, key, f'first-{slot}', 3)
            assert retry.get('ok') and retry['effects'] == effect, retry
            changed = self.write(follower, key, f'first-{slot}', 99)
            assert changed.get('ok') and changed['effects'] == dict(effect, accepted=False), changed
            self.read(follower, key, 3, applied_at_least=changed['index'])
            expected[key] = 3
        self.passed('three_os_processes_durable_multislot_forward_exact_effects_and_idempotency', pids=[p.pid for p in self.processes.values()])
        # Gate only the actual leader; observe entry before relying on timeout.
        slot, key = next(iter(self.keys.items()))
        leader = self.leader(slot)
        self.api(leader, '/gate', dict(command_id='gate-write'))
        before = self.read(leader, key, 3)['apply_calls']
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(self.write, leader, key, 'gate-write', 7, 700)
            entered = self.poll('gate entered', lambda: (s if (s := self.api(leader, '/status'))['gate']['entered'] else None))
            unknown = pending.result(timeout=5)
        assert unknown.get('error') == 'Unknown', unknown
        inflight = self.api(leader, '/status')['inflight']
        assert inflight['slots'][slot] == 1, inflight
        busy = self.write(leader, key, 'busy-write')
        assert busy.get('error') == 'Busy', busy
        cold_slot = next(s for s in self.keys if s != slot)
        cold = self.keys[cold_slot]
        cold_result = self.write(leader, cold, 'cold-while-hot', 2)
        assert cold_result.get('ok') and cold_result['effects']['value'] == 5, cold_result
        expected[cold] = 5
        self.api(leader, '/gate', dict(release=True))
        self.poll('gate released inflight', lambda: self.api(leader, '/status')['inflight']['global'] == 0)
        value = self.read(leader, key, 10)
        assert value['apply_calls'] == before + 1, value
        assert 'busy-write' not in value['commands']
        retry = self.write(leader, key, 'gate-write', 7)
        assert retry.get('ok') and retry['effects']['value'] == 10, retry
        expected[key] = 10
        self.passed('unknown_retains_slot_budget_without_auto_resubmit_and_cold_slot_progress', entered=entered['gate'], unknown=unknown, busy=busy)
        # A dropped public HTTP connection is a distinct caller outcome from
        # receiving Unknown: no response is read, and retained work must finish.
        before = self.read(leader, key, expected[key])['apply_calls']
        self.api(leader, '/gate', dict(command_id='socket-lost-write'))
        disconnected = self.disconnect_gated_write(leader, key, 'socket-lost-write', 6)
        retained = self.api(leader, '/status')['inflight']
        assert retained['slots'][slot] == 1, retained
        busy = self.write(leader, key, 'busy-after-disconnect')
        assert busy.get('error') == 'Busy', busy
        self.api(leader, '/gate', dict(release=True))
        self.poll('disconnected write finished', lambda: self.api(leader, '/status')['inflight']['slots'][slot] == 0)
        expected[key] += 6
        value = self.read(leader, key, expected[key])
        assert value['apply_calls'] == before + 1 and 'busy-after-disconnect' not in value['commands'], value
        effect = dict(key=key, command_id='socket-lost-write', value=expected[key], accepted=True)
        retry = self.write(leader, key, 'socket-lost-write', 6)
        assert retry.get('ok') and retry['effects'] == effect, retry
        self.read(leader, key, expected[key])
        self.passed('public_http_disconnect_after_gate_entry_retains_work_and_exact_retry', disconnected=disconnected, retained=retained, explicit_retry=retry)
        self.stop(leader, kill=True)
        survivors = tuple(n for n in (1, 2, 3) if n != leader)
        new_leader = self.leader(slot, survivors)
        result = self.write(survivors[0], key, 'after-leader-kill', 1)
        expected[key] += 1
        assert result.get('ok') and result['effects']['value'] == expected[key], result
        self.start(leader)
        self.ready((leader,))
        self.poll('restarted follower recovered', lambda: self.api(leader, '/read', dict(key=key, local=True)).get('result', {}).get('value') == expected[key])
        self.passed('kill_leader_quorum_progress_same_data_restart', killed=leader, elected=new_leader)
        self.recovery(expected)

    def recovery(self, expected=None):
        self.ensure_layout()
        if expected is None:
            for n in (1, 2, 3):
                self.start(n)
            self.ready((1, 2, 3))
            expected = {}
            for slot, key in self.keys.items():
                leader = self.leader(slot)
                result = self.write(leader, key, f"first-{slot}", 3)
                assert result.get("ok"), result
                expected[key] = 3
        # Submit is not completion. Poll every local durable provider AND purge facts.
        for n in (1, 2, 3):
            for s in self.keys:
                result = self.api(n, '/compact', dict(slot=s))
                assert result.get('ok'), result
                self.poll(f'node {n} slot {s} compaction complete', lambda: (state if (state := self.api(n, '/status'))['slots'][s]['storage']['progress'] == 'CompletedObserved' else None), 45)
        def compacted():
            all_status = [self.api(n, '/status') for n in (1, 2, 3)]
            return all_status if all(s['storage']['snapshot'] is not None and s['storage']['purged'] != 'None' and s['storage']['progress'] == 'CompletedObserved' for state in all_status for s in state['slots']) else None
        facts = self.poll('durable snapshots and purge observed', compacted, 45)
        for s, k in self.keys.items():
            result = self.write(1, k, f'suffix-{s}', 4)
            expected[k] += 4
            assert result.get('ok') and result['effects']['value'] == expected[k], result
        for n in (1, 2, 3):
            self.stop(n)
        for n in (1, 2, 3):
            self.start(n)
        self.ready((1, 2, 3))
        for s, k in self.keys.items():
            self.leader(s)
            for n in (1, 2, 3):
                self.read(n, k, expected[k])
            retry = self.write(2, k, f'first-{s}', 3)
            assert retry.get('ok') and retry['effects']['value'] == 3, retry
            self.read(1, k, expected[k])
        self.passed('snapshot_purge_committed_suffix_all_restart_and_dedup_restore', storage_facts=facts)
        for n in (1, 2, 3):
            self.stop(n)
        altered = dict(self.layout, cluster_id=self.layout['cluster_id'] + '-mismatch')
        path = self.output / 'mismatch-layout.json'
        path.write_text(json.dumps(altered))
        original_files = {str(p.relative_to(self.output / 'data-1')): hashlib.sha256(p.read_bytes()).hexdigest() for p in (self.output / 'data-1').rglob('*') if p.is_file()}
        process = self.start(1, path)
        mismatch_exit = process.wait(timeout=30)
        assert mismatch_exit != 0, 'manifest mismatch accepted'
        self.records[-1]['exit_code'] = mismatch_exit
        self.processes.pop(1)
        after_files = {str(p.relative_to(self.output / 'data-1')): hashlib.sha256(p.read_bytes()).hexdigest() for p in (self.output / 'data-1').rglob('*') if p.is_file()}
        assert original_files == after_files, 'manifest rejection modified original data'
        self.start(1)
        self.ready((1,))
        self.stop(1)
        self.passed('manifest_mismatch_fails_closed_original_directory_reopens')

    def fingerprint(self):
        self.ensure_layout()
        # New data/cluster. Two A and one B have identical addresses but distinct digests.
        self.layout['cluster_id'] += '-fingerprint'
        self.layout_path.write_text(json.dumps(self.layout, indent=2))
        for n in (1, 2, 3):
            # Preserve normal-case data; no deletion or reuse with changed identity.
            (self.output / f'data-{n}').rename(self.output / f'normal-data-{n}')
        altered = dict(self.layout, cluster_id=self.layout['cluster_id'] + '-B')
        path = self.output / 'fingerprint-B.json'
        path.write_text(json.dumps(altered))
        self.start(1)
        self.start(2)
        self.start(3, path)
        self.ready((1, 2, 3))
        key = self.keys[0]
        leader = self.leader(0, (1, 2))
        result = self.write(leader, key, 'A-quorum', 1)
        assert result.get('ok'), result
        other = 1 if leader == 2 else 2
        self.stop(other, kill=True)
        outcome = self.write(leader, key, 'ambiguous-A-B', 5, 1200)
        assert not outcome.get('ok'), outcome
        isolated = self.api(3, '/read', dict(key=key, local=True))
        assert isolated.get('ok') and isolated['result']['value'] == 0, isolated
        self.start(other)
        self.ready((other,))
        self.leader(0, (1, 2))
        self.poll('retained A write completes after quorum restoration', lambda: all(self.api(n, '/status')['inflight']['slots'][0] == 0 for n in (1, 2)))
        # The earlier Unknown may commit late. Explicit same-id retry disambiguates.
        resolved = self.write(1, key, 'ambiguous-A-B', 5)
        assert resolved.get('ok') and resolved['effects']['value'] == 6, resolved
        self.read(2, key, 6)
        self.passed('native_fingerprint_RF3_AA_quorum_AB_no_applied_unknown_disambiguated', no_quorum_result=outcome, isolated_B=isolated, resolved=resolved)

    def local_view(self, node, key):
        response = self.api(node, '/read', dict(key=key, local=True))
        if not response.get('ok'):
            # Transient (e.g. reads gated during a snapshot install); poll retries ValueError.
            raise ValueError(f'local read refused: {response}')
        assert response['result']['storage'] == 'disk', response
        return dict(response['result'], applied=response['applied'])

    def disk_converged(self, label, expected, calls):
        """Every node: expected value and apply count, FSM-stored applied index equal
        to the bridge's applied index, and that index equal across nodes."""
        def observe():
            views = {n: {k: self.local_view(n, k) for k in self.keys.values()} for n in (1, 2, 3)}
            for k in self.keys.values():
                if any(views[n][k]['applied'] is None for n in (1, 2, 3)):
                    return None  # A replica that has applied nothing yet.
                indices = {views[n][k]['applied']['index'] for n in (1, 2, 3)}
                if len(indices) != 1 or any(views[n][k]['value'] != expected[k] or views[n][k]['apply_calls'] != calls[k]
                                            or views[n][k]['applied_index'] != views[n][k]['applied']['index'] for n in (1, 2, 3)):
                    return None
            return views
        return self.poll(label, observe, 45)

    def disk(self):
        """Disk-backed FSM mode: SIGKILL and cold restarts recover state and applied index from RocksDB."""
        self.ensure_layout()
        self.fsm_storage = 'disk'
        for n in (1, 2, 3):
            self.start(n)
        self.ready((1, 2, 3))
        for state in [self.api(n, '/status') for n in (1, 2, 3)]:
            assert state['fsm_storage'] == 'disk', state
            assert all(s['storage']['backend'] == 'Some(RocksDb)' and s['storage']['snapshot'] is None for s in state['slots']), state
        expected, calls = {}, {}
        for slot, key in self.keys.items():
            leader = self.leader(slot)
            follower = next(n for n in (1, 2, 3) if n != leader)
            effect = dict(key=key, command_id=f'disk-{slot}', value=3, accepted=True)
            result = self.write(follower, key, f'disk-{slot}', 3)
            assert result.get('ok') and result['effects'] == effect and result['slot'] == slot, result
            retry = self.write(follower, key, f'disk-{slot}', 3)
            assert retry.get('ok') and retry['effects'] == effect, retry
            changed = self.write(follower, key, f'disk-{slot}', 99)
            assert changed.get('ok') and changed['effects'] == dict(effect, accepted=False), changed
            expected[key], calls[key] = 3, 3
        self.disk_converged('disk replicas converge', expected, calls)
        # Compaction publishes a RocksDB checkpoint (never a whole image) and purges the log.
        compacted = self.compact_disk(1, 0)
        self.passed('disk_fsm_RF3_forward_exact_effects_idempotency_and_checkpoint_compaction', compacted=compacted)
        slot, key = next(iter(self.keys.items()))
        leader = self.leader(slot)
        self.stop(leader, kill=True)
        survivors = tuple(n for n in (1, 2, 3) if n != leader)
        new_leader = self.leader(slot, survivors)
        result = self.write(survivors[0], key, 'disk-after-kill', 1)
        expected[key] += 1
        calls[key] += 1
        assert result.get('ok') and result['effects']['value'] == expected[key], result
        self.start(leader)
        self.ready((leader,))
        recovered = self.poll('killed node recovered from RocksDB', lambda: (v if (v := self.local_view(leader, key))['value'] == expected[key] else None))
        assert recovered['opened_index'] is not None and recovered['opened_index'] > 0, recovered
        self.disk_converged('disk replicas converge after leader kill', expected, calls)
        self.passed('disk_fsm_sigkill_leader_restart_opens_rocksdb_state', killed=leader, elected=new_leader, recovered=recovered)
        # SIGKILL every node at once, then cold-start all from the same data.
        before = {n: {k: self.local_view(n, k)['applied_index'] for k in self.keys.values()} for n in (1, 2, 3)}
        for n in (1, 2, 3):
            self.stop(n, kill=True)
        for n in (1, 2, 3):
            self.start(n)
        self.ready((1, 2, 3))
        for n in (1, 2, 3):
            for k in self.keys.values():
                opened = self.local_view(n, k)['opened_index']
                assert opened is not None and opened >= before[n][k], (n, k, opened, before[n][k])
        views = self.disk_converged('disk replicas converge after all-node SIGKILL', expected, calls)
        self.passed('disk_fsm_sigkill_all_cold_restart_state_and_applied_index_agree', opened_at_least=before,
                    applied={n: {k: v['applied']['index'] for k, v in views[n].items()} for n in views})
        # Clean shutdown and cold restart, then a committed suffix and an exact retry.
        for n in (1, 2, 3):
            self.stop(n)
        for n in (1, 2, 3):
            self.start(n)
        self.ready((1, 2, 3))
        for s, k in self.keys.items():
            self.leader(s)
            result = self.write(1, k, f'disk-suffix-{s}', 4)
            expected[k] += 4
            calls[k] += 1
            assert result.get('ok') and result['effects']['value'] == expected[k], result
            retry = self.write(2, k, f'disk-{s}', 3)
            calls[k] += 1
            assert retry.get('ok') and retry['effects'] == dict(key=k, command_id=f'disk-{s}', value=3, accepted=True), retry
        self.disk_converged('disk replicas converge after clean cold restart', expected, calls)
        for n in (1, 2, 3):
            self.stop(n)
        self.passed('disk_fsm_clean_cold_restart_suffix_and_original_dedup_effects')

    def compact_disk(self, node, slot):
        # A replica still applying a backlog holds its FSM lock; the reservation
        # can miss its deadline (Unavailable) or find a build running (Busy).
        result = self.poll(f'node {node} slot {slot} compaction accepted',
                           lambda: (r if (r := self.api(node, '/compact', dict(slot=slot))).get('ok') or r.get('error') not in ('Unavailable', 'Busy') else None), 120)
        assert result.get('ok'), result
        def done():
            storage = self.api(node, '/status')['slots'][slot]['storage']
            if storage is None:
                return None  # The status call's own 1 s budget expired on a busy node.
            snapshot = storage['snapshot']
            if storage['progress'] == 'CompletedObserved' and snapshot and storage['purged'] != 'None':
                assert snapshot['id'].startswith('disk-v1-'), storage
                return storage
            return None
        return self.poll(f'node {node} slot {slot} disk compaction complete', done, 120)

    def blob_view(self, node, key):
        view = self.local_view(node, key)
        return dict(blobs=view['blobs'], applied=view['applied']['index'] if view['applied'] else None, opened=view['opened_index'])

    def bulk(self, node, key, total_bytes, seed, blob=4 * 1024 * 1024):
        """Start the demo's background blob writer, then wait on the FSM's byte counter."""
        start = self.blob_view(node, key)['blobs']['bytes']
        began = time.monotonic()
        count = -(-total_bytes // blob)
        result = self.api(node, '/bulk', dict(key=key, count=count, bytes=blob, concurrency=8, seed=seed))
        assert result.get('ok'), result
        done = self.poll('bulk writes applied', lambda: (b if (b := self.blob_view(node, key)['blobs']['bytes']) - start >= count * blob else None), 1800)
        return dict(bytes=done - start, seconds=round(time.monotonic() - began, 1))

    def pid_node(self, pid):
        return next(n for n, p in self.processes.items() if p.pid == pid)

    def rss_kib(self, pids):
        out = subprocess.run(['ps', '-o', 'pid=,rss=', '-p', ','.join(str(p) for p in pids)], capture_output=True, text=True).stdout
        return {int(pid): int(rss) for pid, rss in (line.split() for line in out.strip().splitlines())}

    def sample_rss(self, nodes, until, seconds):
        """Poll `until()` while sampling every node's RSS every 50 ms; return (result, peaks MiB, samples)."""
        peaks, samples, deadline = {}, 0, time.monotonic() + seconds
        while True:
            pids = {self.processes[n].pid: n for n in nodes if n in self.processes}
            for pid, kib in self.rss_kib(pids).items():
                peaks[pids[pid]] = max(peaks.get(pids[pid], 0), kib)
            samples += 1
            if samples % 4 == 0:
                try:
                    result = until()
                    if result:
                        return result, {n: round(k / 1024, 1) for n, k in sorted(peaks.items())}, samples
                except (OSError, ValueError, KeyError):
                    pass
            assert time.monotonic() < deadline, f'catch-up timed out; peaks={peaks}'
            time.sleep(.05)

    def workspace(self, node, group):
        return self.data_dir(node) / 'fsm-rocksdb' / f'group-{group}.snapshots'

    def disk_snapshot(self, snapshot_bytes=1024 ** 3):
        """Streaming disk snapshot: a replica down while GB-scale data is written and the
        log compacted catches up by snapshot within bounded memory; a SIGKILL during the
        transfer and an abort between the install renames both leave its old state."""
        self.ensure_layout()
        self.fsm_storage, self.data_tag = 'disk', 'disk-snapshot'
        self.extra_args = ['--per-slot-requests', '32', '--global-requests', '64']
        for n in (1, 2, 3):
            self.start(n)
        self.ready((1, 2, 3))
        slot, key = next(iter(self.keys.items()))
        group = self.layout['group_id_base'] + slot
        leader = self.leader(slot)
        victim = next(n for n in (1, 2, 3) if n != leader)
        others = tuple(n for n in (1, 2, 3) if n != victim)
        ws = self.workspace(victim, group)
        result = self.write(leader, key, 'snap-0', 5)
        assert result.get('ok'), result
        self.poll('victim applied the first write', lambda: self.local_view(victim, key)['value'] == 5)

        def lag(total_bytes, seed):
            """Stop the victim, write to the Slot, compact both live replicas."""
            before = self.blob_view(victim, key)
            self.stop(victim)
            written = self.bulk(leader, key, total_bytes, seed)
            self.poll('live replicas applied the writes', lambda: len({json.dumps(self.blob_view(n, key)['blobs']) for n in others}) == 1, 300)
            compactions = {n: self.compact_disk(n, slot) for n in others}
            return before, written, compactions, self.blob_view(leader, key)

        def caught(target):
            return lambda: (v if (v := self.blob_view(victim, key))['blobs'] == target['blobs'] and v['applied'] >= target['applied'] else None)

        # (1)+(2) Full catch-up by streamed snapshot; sample every node's RSS throughout.
        before, written, compactions, target = lag(snapshot_bytes, seed=1)
        size = compactions[leader]['snapshot']['bytes']
        assert size >= snapshot_bytes, compactions
        baseline = {n: round(k / 1024, 1) for n, k in ((self.pid_node(p), k) for p, k in self.rss_kib([self.processes[n].pid for n in others]).items())}
        started = time.monotonic()
        self.start(victim)
        view, peaks, samples = self.sample_rss((1, 2, 3), caught(target), 900)
        seconds = round(time.monotonic() - started, 1)
        assert (ws / 'installed').exists(), 'caught up by snapshot install'
        assert before['applied'] <= view['opened'] < target['applied'], (before, view, target)
        sender = self.leader(slot, others)
        assert all(peak <= 256 for peak in peaks.values()), peaks
        self.passed('disk_snapshot_lagging_replica_catches_up_by_streamed_checkpoint', written=written,
                    snapshot_bytes=size, catch_up_seconds=seconds, rss_peak_mib=peaks, rss_before_mib=baseline,
                    sender=sender, receiver=victim,
                    rss_samples=samples, view=view)
        self.stop(victim)
        self.start(victim)
        self.ready((victim,))
        self.poll('restart after catch-up recovers the installed store', caught(target), 60)
        self.passed('disk_snapshot_restart_after_install_recovers_installed_state')

        # (3a) Receiver SIGKILLed mid-transfer: it reopens its old store, then catches up.
        before, _, compactions, target = lag(snapshot_bytes // 4, seed=2)
        threshold = min(64 * 1024 * 1024, compactions[leader]['snapshot']['bytes'] // 8)
        self.start(victim)
        def staged():
            return sum(f.stat().st_size for d in ws.glob('staging-*') for f in d.iterdir())
        partial = self.poll('transfer in progress', lambda: (b if (b := staged()) > threshold else None), 300)
        self.stop(victim, kill=True)
        self.start(victim)
        self.ready((victim,))
        reopened = self.blob_view(victim, key)
        assert before['applied'] <= reopened['opened'] < target['applied'], (before, reopened, target)
        self.poll('catch-up after the killed transfer', caught(target), 600)
        self.passed('disk_snapshot_receiver_killed_mid_transfer_reopens_old_state_then_catches_up',
                    partial_staged_bytes=partial, reopened_at=reopened['opened'], old_applied=before['applied'])

        # (3b) Abort between the two install renames: restart rolls back to the old store.
        before, _, _, target = lag(min(64 * 1024 * 1024, snapshot_bytes // 8), seed=3)
        process = self.start(victim, env={'MULTIRAFT_TEST_DISK_INSTALL_FAULT': 'abort_after_retire'})
        code = self.poll('install aborted between renames', lambda: process.poll(), 600)
        assert code != 0, code
        self.processes.pop(victim)
        live = self.data_dir(victim) / 'fsm-rocksdb' / f'group-{group}'
        retired = [p.name for p in ws.glob('retired-*')]
        assert not live.exists() and retired, (live, retired)
        self.start(victim)
        self.ready((victim,))
        rolled_back = self.blob_view(victim, key)
        assert before['applied'] <= rolled_back['opened'] < target['applied'], (before, rolled_back, target)
        assert not list(ws.glob('retired-*')), 'rollback consumed the retired store'
        self.poll('catch-up after rollback', caught(target), 600)
        self.passed('disk_snapshot_abort_mid_swap_rolls_back_then_catches_up', exit_code=code, retired=retired,
                    reopened_at=rolled_back['opened'], old_applied=before['applied'])
        for n in (1, 2, 3):
            self.stop(n)

    def finish(self, error=None):
        self.cleaning = True
        try:
            cleanup = []
            for node in list(self.processes):
                try:
                    cleanup.append(dict(node=node, exit=self.stop(node)))
                except Exception as exc:
                    cleanup.append(dict(node=node, error=repr(exc)))
            if self.stop_signal is not None:
                error = error or repr(InterruptedBySignal(self.stop_signal))
            receipt = dict(layout_digests=getattr(self, 'layout_digests', None), passed=error is None and all('error' not in c for c in cleanup), error=error,
                           cases=self.cases, processes=self.records, cleanup=cleanup,
                           evidence_dir=str(self.output), scope='local real OS-process RF3 correctness, not capacity or production Admit/TiKV')
            (self.output / 'receipt.json').write_text(json.dumps(receipt, indent=2))
            return receipt['passed']
        finally:
            self.requests.close()


class InterruptedBySignal(BaseException):
    def __init__(self, signum):
        self.signum = signum
        super().__init__(f"received {signal.Signals(signum).name}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', default='.tmp/slot-foundation/target/debug/multiraft-slot-demo')
    parser.add_argument('--layout-tool', help='slot-layout generator; defaults to the sibling of --binary')
    parser.add_argument('--slot-count', type=int, default=4)
    parser.add_argument('--output-dir', type=Path)
    parser.add_argument('--case', choices=('all', 'normal', 'recovery', 'fingerprint', 'disk', 'disk-snapshot'), default='all')
    parser.add_argument('--snapshot-mib', type=int, default=1024, help='data written to one Slot for --case disk-snapshot')
    args = parser.parse_args(argv)
    output = args.output_dir or Path('.tmp/slot-foundation') / ('acceptance-' + uuid.uuid4().hex)
    run = Run(args.binary, output, args.layout_tool, args.slot_count)
    error = None
    exit_code = 1
    signals = (signal.SIGINT, signal.SIGTERM)
    previous_handlers = {sig: signal.getsignal(sig) for sig in signals}
    try:
        for sig in signals:
            signal.signal(sig, run.record_signal)
        run.checkpoint()
        if args.case in ('all', 'normal'):
            run.normal()
            run.checkpoint()
        if args.case == 'recovery':
            run.recovery()
            run.checkpoint()
        if args.case == 'fingerprint':
            # fingerprint() preserves any prior case directories without deleting them.
            for n in (1, 2, 3):
                (run.output / f'data-{n}').mkdir()
        if args.case in ('all', 'fingerprint'):
            run.fingerprint()
            run.checkpoint()
        if args.case == 'disk':
            run.disk()
            run.checkpoint()
        if args.case == 'disk-snapshot':
            run.disk_snapshot(args.snapshot_mib * 1024 * 1024)
            run.checkpoint()
    except BaseException as exc:
        import traceback
        traceback.print_exc()
        error = repr(exc)
        if isinstance(exc, InterruptedBySignal):
            exit_code = 128 + exc.signum
        elif isinstance(exc, KeyboardInterrupt):
            exit_code = 130
    finally:
        # The same non-throwing handlers stay installed throughout cleanup.
        # No per-signal ignore transition can abandon child ownership.
        try:
            passed = run.finish(error)
            if run.stop_signal is not None:
                exit_code = 128 + run.stop_signal
                if passed:
                    # Also cover the first signal arriving during receipt IO.
                    passed = False
                    receipt_path = output / 'receipt.json'
                    receipt = json.loads(receipt_path.read_text())
                    receipt.update(passed=False, error=repr(InterruptedBySignal(run.stop_signal)))
                    receipt_path.write_text(json.dumps(receipt, indent=2))
        finally:
            # No new children are started beyond this point. Block both signals
            # together only while restoring their original handlers; pending
            # signals then resume the caller's semantics after receipt commit.
            previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, signals)
            try:
                for sig, handler in previous_handlers.items():
                    signal.signal(sig, handler)
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
    print(json.dumps(dict(passed=passed, receipt=str(output / 'receipt.json'))))
    return 0 if passed else exit_code


if __name__ == '__main__':
    sys.exit(main())
