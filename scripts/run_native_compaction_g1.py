#!/usr/bin/env python3
"""Run only the reviewed native-compaction G1 matrix on its dedicated host."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import signal
import socket
import subprocess
import time


def checked(args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--expected-sha', required=True)
    parser.add_argument('--labels', type=Path, required=True)
    args = parser.parse_args()
    assert socket.gethostname() == 'iZk1ah3k883dc7jcznm36hZ'
    assert pwd.getpwuid(os.getuid()).pw_name == 'ecs-user'
    root = args.root.resolve(strict=True)
    assert root.is_relative_to('/srv/tornado-message-data')
    repo = Path(__file__).resolve().parents[1]
    assert repo.is_relative_to(root)
    labels = json.loads(args.labels.read_text())
    node = labels[0] if isinstance(labels, list) else labels
    assert node['spec']['hostname'] == socket.gethostname()
    assert node['metadata']['labels']['project'] == 'tornado-message'
    assert node['metadata']['labels']['purpose'] == 'dedicated-lab'
    sha = checked(['git', 'rev-parse', 'HEAD'], cwd=repo)
    assert sha == args.expected_sha
    assert not checked(['git', 'status', '--porcelain'], cwd=repo)
    toolchain = Path('/home/ecs-user/.rustup/toolchains/1.97.0-x86_64-unknown-linux-gnu/bin')
    cargo = toolchain / 'cargo'
    env = os.environ.copy()
    for key, folder in [('CARGO_HOME', 'cargo-home'), ('CARGO_TARGET_DIR', 'target'),
                        ('RUSTUP_HOME', 'rustup-home'), ('TMPDIR', 'tmp'), ('TMP', 'tmp'), ('TEMP', 'tmp')]:
        path = root / folder
        path.mkdir(exist_ok=True)
        env[key] = str(path)
    env['RUSTC'] = str(toolchain / 'rustc')
    env['RUSTDOC'] = str(toolchain / 'rustdoc')
    env['PATH'] = str(toolchain) + ':' + env['PATH']
    for key in ['RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'LD_PRELOAD', 'NATIVE_ENOSPC_ROOT']:
        env.pop(key, None)
    run_id = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + sha[:12]
    run = root / 'runs' / run_id
    (run / 'logs').mkdir(parents=True)
    (run / 'fixtures').mkdir()
    env['NATIVE_EVIDENCE_ROOT'] = str(run / 'fixtures')
    paths = [root, repo, run] + [Path(env[key]) for key in ['CARGO_HOME', 'CARGO_TARGET_DIR', 'RUSTUP_HOME', 'TMPDIR']]
    mounts = {}
    for path in paths:
        canonical = path.resolve(strict=True)
        assert canonical.is_relative_to('/srv/tornado-message-data')
        mount = checked(['findmnt', '-n', '-o', 'SOURCE,TARGET,FSTYPE,OPTIONS', '-T', str(canonical)])
        assert mount.split()[:2] == ['/dev/vdb1', '/srv/tornado-message-data']
        mounts[str(canonical)] = mount
    assert os.statvfs(root).f_bavail * os.statvfs(root).f_frsize > 5 * 1024**3
    manifest = {
        'run_id': run_id, 'backend_sha': sha,
        'parent': checked(['git', 'rev-parse', 'HEAD^'], cwd=repo),
        'remote': checked(['git', 'remote', 'get-url', 'origin'], cwd=repo),
        'lock_sha256': digest(repo / 'Cargo.lock'),
        'native_lock_blocks': [block.strip() for block in (repo / 'Cargo.lock').read_text().split('[[package]]') if re.search(r'name = \"openraft[^\"]*\"', block)],
        'consumer_baseline_sha': 'f608b63a0f21aebd00f25abf1c9eef49ba2ba522',
        'consumer_baseline_lock_sha256': 'af60b09ae5bedd68d17545b59c2083cfc34d4ae37d8e121ad260a668bc2a820c',
        'host': socket.gethostname(), 'login': pwd.getpwuid(os.getuid()).pw_name,
        'labels': node, 'mounts': mounts,
        'cargo': checked([str(cargo), '--version'], env=env),
        'rustc': checked([env['RUSTC'], '--version'], env=env),
        'toolchain': 'Pinned direct 1.97 binaries; sysroot is read-only and rustup is not invoked.',
        'environment': {key: env[key] for key in ['CARGO_HOME', 'CARGO_TARGET_DIR', 'RUSTUP_HOME', 'TMPDIR', 'TMP', 'TEMP', 'RUSTC', 'RUSTDOC', 'NATIVE_EVIDENCE_ROOT']},
        'sync_grade': 'Data or All as specified by each exact case',
        'positive_repetitions': 5, 'test_threads': 1,
        'limits': 'Process-crash and injected ENOSPC only; no physical power-loss or capacity claim.',
        'publication_cuts': ['data_synced', 'metadata_synced', 'generation_synced', 'generation_published', 'manifest_synced', 'manifest_renamed', 'active_synced', 'obsolete_removed'],
        'install_cuts': ['staged', 'application_restored', 'activated', 'bridge_updated'],
        'purge_cuts': ['purge_marker_synced', 'rewrite_data_synced', 'rewrite_renamed', 'rewrite_directory_synced'],
    }
    (run / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    receipts = []

    def run_logged(command, log, timeout):
        begin = time.time()
        with log.open('wb') as output:
            process = subprocess.Popen(command, cwd=repo, env=env, stdout=output,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            timed_out = False
            try:
                code = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                os.killpg(process.pid, signal.SIGKILL)
                code = process.wait()
        return code, timed_out, begin, time.time()

    def fingerprint(path):
        path = Path(path).resolve(strict=True)
        assert path.is_relative_to(root / 'target')
        stat = path.stat()
        return {'path': str(path), 'size': stat.st_size, 'mtime_ns': stat.st_mtime_ns,
                'inode': stat.st_ino, 'sha256': digest(path)}

    def execute(name, command, timeout=300, require_tests=True):
        log = run / 'logs' / (name + '.log')
        timing = run / 'logs' / (name + '.time.txt')
        build_command = None
        binaries_before = []
        if require_tests:
            build_command = command[:command.index('--')] + ['--no-run', '--message-format=json']
            build_log = run / 'logs' / (name + '.build.jsonl')
            build_code, build_timeout, _, _ = run_logged(build_command, build_log, 1800)
            assert build_code == 0 and not build_timeout, 'case prebuild failed: ' + str(build_log)
            executables = set()
            for line in build_log.read_text(errors='replace').splitlines():
                try:
                    artifact = json.loads(line)
                except ValueError:
                    continue
                if artifact.get('reason') == 'compiler-artifact' and artifact.get('executable'):
                    executables.add(artifact['executable'])
            assert executables, 'case prebuild selected no executable'
            binaries_before = [fingerprint(path) for path in sorted(executables)]
        actual = ['/usr/bin/time', '-v', '-o', str(timing)] + command
        code, timed_out, begin, end = run_logged(actual, log, timeout)
        binaries_after = [fingerprint(item['path']) for item in binaries_before]
        binaries_unchanged = all(a['sha256'] == b['sha256'] for a, b in zip(binaries_before, binaries_after))
        content = log.read_text(errors='replace')
        selected = not require_tests or bool(re.search(r'test result: ok\. [1-9][0-9]* passed', content))
        node_logs = []
        fixture_roots = re.findall(r'NATIVE_EVIDENCE case=\S+ root=(\S+)', content)
        for fixture in fixture_roots:
            fixture = Path(fixture).resolve(strict=True)
            assert fixture.is_relative_to(run)
            for node_log in sorted(fixture.glob('node-*-epoch-*.log')):
                node_logs.append({'path': str(node_log), 'sha256': digest(node_log)})
        scope = {'topology': 'Exact test fixture; RF3 cases use three OS processes and one Group', 'sync_grade': 'Data/All per exact fixture; NativeDurable never uses Os', 'oracle': 'Direct owner assertions in the named test, with RF3 per-victim/every-voter NATIVE_ORACLE records; no readiness substitution', 'recovery_channel': 'Named RF3 case separates isolated local snapshot+suffix, peer native gRPC install, and receiver local restart; unit cases retain their narrower source scope', 'seed': 'not-exposed', 'test_threads': 1}
        if name.startswith('isolated_local_post_purge'):
            scope.update(topology='RF3 Group 7: three processes before stop, only node 2 after restart', oracle='Direct node 2 FSM value 17 with purged prefix and durable checkpoint', recovery_channel='Local durable native snapshot plus retained committed suffix; no live peers')
        elif name.startswith('every_voter_cold'):
            scope.update(topology='RF3 Group 7, nodes 1/2/3, all stopped after each has purged', oracle='Every voter value 17 after cold reopen, then proposal and ReadIndex-backed value 20', recovery_channel='Each local checkpoint plus suffix; quorum explicitly restored for subsequent writes/read')
        elif name.startswith('native_peer_snapshot_receiver'):
            scope.update(topology='RF3 Group 7, node 3 stopped behind both purged survivors', oracle='Node 3 durable snapshot beyond old applied prefix, value 13, then value 20 after isolated receiver restart', recovery_channel='Native gRPC install followed by local snapshot+suffix recovery with no live peers')
        elif name.startswith('enospc-'):
            scope.update(topology='One Data-grade Group 7 process plus fresh reopen', oracle='Injected ENOSPC marker, Unconfirmed operation, no purge/checkpoint, all 10 acknowledged increments restored', recovery_channel='Retained local log; this does not claim physical disk exhaustion or power-loss behavior')
        elif name.startswith('native_snapshot_catalog'):
            scope.update(topology='Catalog-owned Group 42 fixtures', oracle='Full metadata/byte integrity and eight enumerated publication crash cuts', recovery_channel='Validated active generation only; no fallback candidate selection')
        elif name.startswith('native_post_purge_restart'):
            scope.update(topology='One Data-grade native Raft Group 7 plus process-crash children', oracle='Value 17 from actual removed prefix plus suffix; missing checkpoint/middle entry rejects; all declared install/purge cuts', recovery_channel='OpenRaft startup install and committed-suffix replay')
        receipt = {'case': name, 'scope': scope, 'command': command, 'timeout_seconds': timeout, 'exit_code': code,
                   'timed_out': timed_out, 'nonzero_intended_tests': selected, 'elapsed_seconds': end - begin,
                   'utc_start': datetime.datetime.fromtimestamp(begin, datetime.timezone.utc).isoformat(),
                   'utc_end': datetime.datetime.fromtimestamp(end, datetime.timezone.utc).isoformat(),
                   'prebuild_command': build_command, 'binaries_before': binaries_before, 'binaries_after': binaries_after,
                   'binary_unchanged': binaries_unchanged, 'node_log_count': len(node_logs), 'node_logs': node_logs,
                   'log': str(log), 'log_sha256': digest(log), 'timing': str(timing),
                   'pass': code == 0 and selected and binaries_unchanged}
        receipts.append(receipt)
        (run / 'receipts.json').write_text(json.dumps(receipts, indent=2) + '\n')
        print(json.dumps(receipt), flush=True)
        return receipt['pass']

    prefix = [str(cargo), 'test', '--locked']
    # Compile only on the validated data disk; all laboratory-only tests remain ignored here.
    if not execute('compile', prefix + ['--workspace', '--no-run'], timeout=1800, require_tests=False):
        return 1
    suites = [
        ('multiraft-store', 'durable_committed'),
        ('multiraft-store', 'native_snapshot_catalog'),
        ('multiraft-store', 'native_snapshot_bridge'),
        ('multiraft-store', 'native_post_purge_restart'),
        ('multiraft-net', 'native_compaction_config'),
        ('multiraft-net', 'native_compaction_operations'),
        ('multiraft-net', 'native_compaction_cancellation'),
        ('multiraft-net', 'native_compaction_repeated'),
        ('multiraft-net', 'native_snapshot_grpc'),
        ('multiraft-net', 'native_snapshot_inprocess'),
        ('multiraft-net', 'disabled_snapshot_policy'),
        ('multiraft-net', 'snapshot_recovery'),
    ]
    rf3 = ['isolated_local_post_purge_recovery_has_no_live_peer_source',
           'every_voter_cold_reopens_after_purge_then_proposal_and_readindex_succeed',
           'native_peer_snapshot_receiver_later_reopens_without_any_live_peer']
    for repetition in range(1, 6):
        for package, target in suites:
            execute(f'{target}-r{repetition}', prefix + ['-p', package, '--test', target, '--', '--test-threads=1', '--nocapture'])
        for case in rf3:
            execute(f'{case}-r{repetition}', prefix + ['-p', 'multiraft-net', '--test', 'native_rf3_recovery', case, '--', '--ignored', '--exact', '--nocapture'], timeout=300)
        case = 'enospc_snapshot_write_preserves_all_acknowledged_retained_state'
        execute(f'enospc-r{repetition}', prefix + ['-p', 'multiraft-net', '--test', 'native_enospc', case, '--', '--ignored', '--exact', '--nocapture'])
        execute(f'wire-bounds-r{repetition}', prefix + ['-p', 'multiraft-net', '--lib', 'snapshot_bound_tests', '--', '--nocapture'])
        execute(f'rpc-cancel-r{repetition}', prefix + ['-p', 'multiraft-net', '--lib', 'cancelled_service_waiter', '--', '--nocapture'])
    assert checked(['git', 'rev-parse', 'HEAD'], cwd=repo) == sha
    assert not checked(['git', 'status', '--porcelain'], cwd=repo)
    assert digest(repo / 'Cargo.lock') == manifest['lock_sha256']
    summary = {'run': str(run), 'backend_sha': sha, 'passed': sum(r['pass'] for r in receipts),
               'total': len(receipts), 'all_required_passed': all(r['pass'] for r in receipts),
               'memory_note': 'GNU time records process-tree maximum RSS, not summed concurrent node RSS.'}
    (run / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    sums = []
    for path in sorted(run.rglob('*')):
        if path.is_file() and path.name != 'SHA256SUMS':
            sums.append(digest(path) + '  ' + str(path.relative_to(run)))
    (run / 'SHA256SUMS').write_text('\n'.join(sums) + '\n')
    print(json.dumps(summary), flush=True)
    return 0 if summary['all_required_passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
