"""Pure harness helper checks; no Raft processes or external resources."""
import contextlib
import importlib.util
import io
import json
import signal
import tempfile
from pathlib import Path
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('slot_acceptance', Path(__file__).with_name('slot_acceptance.py'))
acceptance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(acceptance)


FIXTURE = Path(__file__).resolve().parent.parent / 'crates/multiraft-slot-demo/layouts/rf3-12slot.json'


class RoutingTests(unittest.TestCase):
    def test_known_hash_vector_and_slot_coverage(self):
        # Buckets are computed here; the table is read from the committed artifact.
        layout = json.loads(FIXTURE.read_text())
        self.assertEqual(acceptance.bucket_for(''), 164)
        self.assertEqual(acceptance.bucket_for('channel-a'), 79)
        self.assertEqual(acceptance.slot_for('', layout), 7)
        self.assertEqual(acceptance.slot_for('channel-a', layout), 3)
        self.assertEqual(acceptance.slot_for('key-0', layout), 11)
        self.assertEqual({acceptance.slot_for(f'key-{i}', layout) for i in range(1000)}, set(range(12)))


class DisconnectTests(unittest.TestCase):
    def test_gate_entry_precedes_socket_close_without_response_read(self):
        Path('.tmp').mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir='.tmp') as directory:
            run = acceptance.Run('unused', Path(directory) / 'disconnect')
            connection = mock.Mock()
            connection.fileno.return_value = -1
            state = {'gate': {'entered': True, 'command_id': 'lost'}}
            with mock.patch.object(acceptance.socket, 'create_connection', return_value=connection), mock.patch.object(run, 'api', return_value=state):
                result = run.disconnect_gated_write(1, 'key', 'lost', 6)
            self.assertTrue(result['socket_closed'])
            self.assertFalse(result['response_received'])
            connection.recv.assert_not_called()
            connection.close.assert_called_once()
            sent = connection.sendall.call_args.args[0]
            self.assertTrue(sent.startswith(b'POST /write HTTP/1.1\r\n'))
            self.assertEqual(json.loads(sent.split(b'\r\n\r\n', 1)[1])['command_id'], 'lost')
            events = [json.loads(line) for line in (run.output / 'requests.jsonl').read_text().splitlines()]
            self.assertEqual([event['event'] for event in events], ['raw_http_sent', 'gate_entered_before_disconnect', 'raw_http_socket_closed'])
            self.assertTrue(events[-1]['gate_entered'])
            self.assertFalse(events[-1]['response_received'])
            run.finish()


class InterruptionTests(unittest.TestCase):
    def check_interruption(self, trigger, expected_exit):
        Path('.tmp').mkdir(exist_ok=True)
        old_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
        with tempfile.TemporaryDirectory(dir='.tmp') as directory:
            output = Path(directory) / 'interrupted'
            actual_finish = acceptance.Run.finish
            calls = []

            def finish(run, error):
                calls.append(error)
                # Repeated signals only record intent; they cannot interrupt cleanup.
                signal.raise_signal(signal.SIGINT)
                signal.raise_signal(signal.SIGTERM)
                return actual_finish(run, error)

            with mock.patch.object(acceptance.Run, 'normal', side_effect=trigger), mock.patch.object(acceptance.Run, 'finish', new=finish), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                code = acceptance.main(['--case', 'normal', '--output-dir', str(output)])
            self.assertEqual(code, expected_exit)
            self.assertEqual(len(calls), 1)
            self.assertIsNotNone(calls[0])
            receipt = json.loads((output / 'receipt.json').read_text())
            self.assertFalse(receipt['passed'])
            self.assertTrue(receipt['error'])
            self.assertEqual(receipt['processes'], [])
        for sig, handler in old_handlers.items():
            self.assertEqual(signal.getsignal(sig), handler)

    def test_signal_during_spawn_registers_child_before_cleanup(self):
        Path('.tmp').mkdir(exist_ok=True)
        process = mock.Mock(pid=987654)
        process.poll.return_value = None
        process.wait.return_value = 0

        def spawn(*_args, **_kwargs):
            signal.raise_signal(signal.SIGTERM)
            return process

        with tempfile.TemporaryDirectory(dir='.tmp') as directory:
            output = Path(directory) / 'spawn-interrupted'
            with mock.patch.object(acceptance.Run, 'normal', new=lambda run: run.start(1)), mock.patch.object(acceptance.subprocess, 'Popen', side_effect=spawn), mock.patch.object(acceptance.os, 'killpg') as kill, contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                code = acceptance.main(['--case', 'normal', '--output-dir', str(output)])
            self.assertEqual(code, 143)
            kill.assert_called_once_with(process.pid, signal.SIGTERM)
            process.wait.assert_called_once_with(timeout=35)
            receipt = json.loads((output / 'receipt.json').read_text())
            self.assertFalse(receipt['passed'])
            self.assertEqual(receipt['processes'][0]['pid'], process.pid)
            self.assertEqual(receipt['cleanup'], [{'node': 1, 'exit': 0}])

    def test_former_cleanup_handler_switch_window_cannot_skip_finish(self):
        Path('.tmp').mkdir(exist_ok=True)
        real_signal = signal.signal
        actual_finish = acceptance.Run.finish
        finishes = []
        switches = []

        def switch(sig, handler):
            switches.append((sig, handler))
            # This is the exact former failure window: after SIGINT changed,
            # before SIGTERM changed. The installed SIGTERM handler must not
            # throw even if delivered while another handler is being changed.
            result = real_signal(sig, handler)
            if sig == signal.SIGINT:
                term_handler = signal.getsignal(signal.SIGTERM)
                if callable(term_handler):
                    term_handler(signal.SIGTERM, None)
            return result

        def finish(run, error):
            finishes.append(error)
            return actual_finish(run, error)

        with tempfile.TemporaryDirectory(dir='.tmp') as directory:
            output = Path(directory) / 'window'
            with mock.patch.object(acceptance.signal, 'signal', side_effect=switch), mock.patch.object(acceptance.Run, 'normal', side_effect=lambda: signal.raise_signal(signal.SIGTERM)), mock.patch.object(acceptance.Run, 'finish', new=finish), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                code = acceptance.main(['--case', 'normal', '--output-dir', str(output)])
            self.assertEqual(code, 143)
            self.assertEqual(len(finishes), 1)
            self.assertFalse(json.loads((output / 'receipt.json').read_text())['passed'])
            self.assertFalse(any(handler == signal.SIG_IGN for _, handler in switches))

    def test_signal_at_phase_end_prevents_next_phase(self):
        Path('.tmp').mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir='.tmp') as directory:
            output = Path(directory) / 'phase'
            with mock.patch.object(acceptance.Run, 'normal', side_effect=lambda: signal.raise_signal(signal.SIGTERM)), mock.patch.object(acceptance.Run, 'fingerprint') as next_phase, contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                code = acceptance.main(['--case', 'all', '--output-dir', str(output)])
            self.assertEqual(code, 143)
            next_phase.assert_not_called()
            self.assertFalse(json.loads((output / 'receipt.json').read_text())['passed'])

    def test_http_response_signal_is_observed_and_recorded(self):
        Path('.tmp').mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir='.tmp') as directory:
            run = acceptance.Run('unused', Path(directory) / 'http')
            response = mock.MagicMock()
            response.__enter__.return_value = response
            response.read.side_effect = lambda: (run.record_signal(signal.SIGTERM, None) or b'{"ok":true}')
            with mock.patch.object(acceptance.urllib.request, 'urlopen', return_value=response) as request:
                with self.assertRaises(acceptance.InterruptedBySignal):
                    run.api(1, '/status', timeout=100)
                self.assertEqual(request.call_args.kwargs['timeout'], 10)
            self.assertFalse(run.finish())
            self.assertTrue((run.output / 'requests.jsonl').read_text())

    def test_poll_observes_signal_even_when_predicate_completes(self):
        Path('.tmp').mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir='.tmp') as directory:
            run = acceptance.Run('unused', Path(directory) / 'poll')
            def predicate():
                run.record_signal(signal.SIGTERM, None)
                return True
            with self.assertRaises(acceptance.InterruptedBySignal):
                run.poll('signal', predicate)
            self.assertFalse(run.finish())

    def test_first_signal_during_cleanup_produces_failed_receipt(self):
        Path('.tmp').mkdir(exist_ok=True)
        def normal(run):
            run.processes[1] = object()
        def stop(run, node, kill=False):
            self.assertTrue(run.cleaning)
            signal.raise_signal(signal.SIGTERM)
            run.processes.pop(node)
            return 0
        with tempfile.TemporaryDirectory(dir='.tmp') as directory:
            output = Path(directory) / 'cleanup-first'
            with mock.patch.object(acceptance.Run, 'normal', new=normal), mock.patch.object(acceptance.Run, 'stop', new=stop), contextlib.redirect_stdout(io.StringIO()):
                code = acceptance.main(['--case', 'normal', '--output-dir', str(output)])
            self.assertEqual(code, 143)
            receipt = json.loads((output / 'receipt.json').read_text())
            self.assertFalse(receipt['passed'])
            self.assertEqual(receipt['cleanup'], [{'node': 1, 'exit': 0}])

    def test_missing_process_group_still_waits_and_reaps(self):
        Path('.tmp').mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir='.tmp') as directory:
            run = acceptance.Run('unused', Path(directory) / 'race')
            process = mock.Mock(pid=987654)
            process.poll.return_value = None
            process.wait.return_value = 0
            run.processes[1] = process
            with mock.patch.object(acceptance.os, 'killpg', side_effect=ProcessLookupError):
                self.assertEqual(run.stop(1), 0)
            process.wait.assert_called_once_with(timeout=35)
            self.assertEqual(run.processes, {})
            run.finish()

    def test_receipt_write_failure_closes_log_and_restores_handlers(self):
        Path('.tmp').mkdir(exist_ok=True)
        old_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
        original_write = Path.write_text
        runs = []
        def write(path, *args, **kwargs):
            if path.name == 'receipt.json':
                raise OSError('injected receipt write failure')
            return original_write(path, *args, **kwargs)
        with tempfile.TemporaryDirectory(dir='.tmp') as directory:
            with mock.patch.object(acceptance.Run, 'normal', new=lambda run: runs.append(run)), mock.patch.object(Path, 'write_text', new=write):
                with self.assertRaisesRegex(OSError, 'injected'):
                    acceptance.main(['--case', 'normal', '--output-dir', str(Path(directory) / 'write-fail')])
            self.assertTrue(runs[0].requests.closed)
        for sig, handler in old_handlers.items():
            self.assertEqual(signal.getsignal(sig), handler)

    def test_first_signal_during_receipt_write_marks_receipt_failed(self):
        Path('.tmp').mkdir(exist_ok=True)
        original_write = Path.write_text
        signaled = []
        def write(path, *args, **kwargs):
            result = original_write(path, *args, **kwargs)
            if path.name == 'receipt.json' and not signaled:
                signaled.append(True)
                signal.raise_signal(signal.SIGTERM)
            return result
        with tempfile.TemporaryDirectory(dir='.tmp') as directory:
            output = Path(directory) / 'receipt-signal'
            with mock.patch.object(acceptance.Run, 'normal', return_value=None), mock.patch.object(Path, 'write_text', new=write), contextlib.redirect_stdout(io.StringIO()):
                code = acceptance.main(['--case', 'normal', '--output-dir', str(output)])
            self.assertEqual(code, 143)
            self.assertFalse(json.loads((output / 'receipt.json').read_text())['passed'])

    def test_pending_signal_during_restore_observes_both_old_handlers(self):
        Path('.tmp').mkdir(exist_ok=True)
        real_signal = signal.signal
        original = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
        delivered = []
        runs = []
        old_int = lambda *_args: None
        def old_term(*_args):
            self.assertIs(signal.getsignal(signal.SIGINT), old_int)
            self.assertIs(signal.getsignal(signal.SIGTERM), old_term)
            self.assertTrue(runs[0].requests.closed)
            self.assertTrue((runs[0].output / 'receipt.json').exists())
            delivered.append('after-both-restored')
        real_signal(signal.SIGINT, old_int)
        real_signal(signal.SIGTERM, old_term)
        def switch(sig, handler):
            result = real_signal(sig, handler)
            if sig == signal.SIGINT and handler is old_int:
                signal.raise_signal(signal.SIGTERM)
                self.assertEqual(delivered, [])
            return result
        try:
            with tempfile.TemporaryDirectory(dir='.tmp') as directory:
                with mock.patch.object(acceptance.Run, 'normal', new=lambda run: runs.append(run)), mock.patch.object(acceptance.signal, 'signal', side_effect=switch), contextlib.redirect_stdout(io.StringIO()):
                    code = acceptance.main(['--case', 'normal', '--output-dir', str(Path(directory) / 'restore')])
                self.assertEqual(code, 0)
                self.assertEqual(delivered, ['after-both-restored'])
        finally:
            for sig, handler in original.items():
                real_signal(sig, handler)

    def test_keyboard_interrupt_finishes_with_failure_receipt(self):
        self.check_interruption(KeyboardInterrupt(), 130)

    def test_sigterm_finishes_with_failure_receipt(self):
        self.check_interruption(lambda: signal.raise_signal(signal.SIGTERM), 143)

    def test_sigint_finishes_with_failure_receipt(self):
        self.check_interruption(lambda: signal.raise_signal(signal.SIGINT), 130)


if __name__ == '__main__':
    unittest.main()
