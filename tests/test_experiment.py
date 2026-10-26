import json
import socket
import struct
import tempfile
from pathlib import Path
import unittest

from experiment import (make_cookie, parameter_message, percentile, read_servers,
                        recv_json, recv_state, send_json, summarize, tcp_info)


class ProtocolTest(unittest.TestCase):
    def test_cookie_is_exactly_37_bytes(self):
        cookie = make_cookie()
        self.assertEqual(len(cookie), 37)
        self.assertEqual(cookie[-1], 0)

    def test_length_prefixed_json_round_trip(self):
        left, right = socket.socketpair()
        try:
            send_json(left, {'tcp': True, 'time': 10})
            self.assertEqual(recv_json(right), {'tcp': True, 'time': 10})
        finally:
            left.close(); right.close()

    def test_signed_state(self):
        left, right = socket.socketpair()
        try:
            left.sendall(struct.pack('b', 9))
            self.assertEqual(recv_state(right), 9)
            left.sendall(struct.pack('b', -1))
            with self.assertRaises(ConnectionRefusedError):
                recv_state(right)
        finally:
            left.close(); right.close()

    def test_parameter_message(self):
        value = parameter_message(10, 131072, 'cubic')
        self.assertTrue(value['tcp'])
        self.assertEqual(value['parallel'], 1)
        self.assertEqual(value['congestion'], 'cubic')

    def test_server_file_and_deterministic_port_range(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'servers.json'
            path.write_text(json.dumps({'servers': [
                {'ip': 'example.test', 'port': '5201-5210', 'location': 'test'}]}))
            first = read_servers(path, 422)
            second = read_servers(path, 422)
            self.assertEqual(first, second)
            self.assertTrue(5201 <= first[0]['port'] <= 5210)

    def test_percentile_and_summary(self):
        self.assertEqual(percentile([0, 10, 20], .95), 19)
        rows = [dict(destination='a', port=1, algorithm='cubic', goodput_bps=x)
                for x in (1e6, 3e6)]
        result = summarize(rows)[0]
        self.assertEqual(result['mean_Mbps'], 2)
        self.assertEqual(result['median_Mbps'], 2)

    @unittest.skipUnless(Path('/proc/sys/net/ipv4/tcp_available_congestion_control').exists(),
                         'Linux TCP_INFO test')
    def test_linux_tcp_info_layout(self):
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0)); listener.listen()
        client = socket.create_connection(listener.getsockname())
        server, _ = listener.accept()
        try:
            info = tcp_info(client)
            self.assertGreater(info['snd_mss'], 0)
            self.assertGreaterEqual(info['snd_cwnd_bytes'], info['snd_mss'])
        finally:
            client.close(); server.close(); listener.close()


if __name__ == '__main__':
    unittest.main()
