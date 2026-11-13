"""Unit tests for PeerLink peer discovery (UDP Broadcast and Multicast)."""

import json
import socket
import time
import unittest

from src.discovery.broadcast import (
    BROADCAST_MAGIC,
    UDPBroadcastDiscovery,
)
from src.discovery.multicast import (
    MULTICAST_MAGIC,
    UDPMulticastDiscovery,
    join_multicast_group,
    leave_multicast_group,
)
from src.discovery.rendezvous_server import (
    MSG_CONNECT_NOTIFY,
    MSG_REGISTER_ACK,
    RENDEZVOUS_MAGIC,
    RendezvousClient,
    RendezvousServer,
    get_peer_address,
    register_peer,
)
from src.discovery.hole_punching import (
    PUNCH_MAGIC,
    TYPE_ACK,
    TYPE_SYN,
    UDPHolePuncher,
    initiate_hole_punching,
)


class TestUDPBroadcastDiscovery(unittest.TestCase):
    """Test suite for UDPBroadcastDiscovery."""

    def test_build_beacon_payload(self):
        """Verify beacon payload structure and attributes."""
        discovery = UDPBroadcastDiscovery(
            peer_id="test-peer-1",
            username="Mubashir",
            tcp_port=5000,
            udp_port=5001,
        )
        payload = discovery.build_beacon_payload()

        self.assertEqual(payload["magic"], BROADCAST_MAGIC)
        self.assertEqual(payload["peer_id"], "test-peer-1")
        self.assertEqual(payload["username"], "Mubashir")
        self.assertEqual(payload["tcp_port"], 5000)
        self.assertEqual(payload["udp_port"], 5001)
        self.assertIn("timestamp", payload)
        self.assertIsInstance(payload["timestamp"], float)

    def test_self_packet_ignored(self):
        """Ensure peer ignores discovery packets sent by itself."""
        discovery = UDPBroadcastDiscovery(peer_id="self-id")
        self_payload = {
            "magic": BROADCAST_MAGIC,
            "peer_id": "self-id",
            "username": "Self",
            "tcp_port": 5000,
            "udp_port": 5001,
            "timestamp": time.time(),
        }
        data = json.dumps(self_payload).encode("utf-8")
        discovery._handle_incoming_packet(data, ("127.0.0.1", 5002))

        self.assertEqual(discovery.get_peers(), {})

    def test_invalid_packet_ignored(self):
        """Ensure malformed or non-PeerLink packets are ignored."""
        discovery = UDPBroadcastDiscovery(peer_id="receiver")

        # Corrupt JSON
        discovery._handle_incoming_packet(b"not valid json", ("127.0.0.1", 5002))
        self.assertEqual(discovery.get_peers(), {})

        # Wrong magic header
        wrong_magic = {
            "magic": "OTHER_APP",
            "peer_id": "other-id",
            "username": "Other",
        }
        discovery._handle_incoming_packet(
            json.dumps(wrong_magic).encode("utf-8"), ("127.0.0.1", 5002)
        )
        self.assertEqual(discovery.get_peers(), {})

    def test_handle_incoming_valid_packet(self):
        """Ensure valid beacons from remote peers are added to the peer registry."""
        found_events = []

        def callback(peer):
            found_events.append(peer)

        discovery = UDPBroadcastDiscovery(
            peer_id="local-node",
            on_peer_found=callback,
        )

        remote_beacon = {
            "magic": BROADCAST_MAGIC,
            "peer_id": "remote-node-1",
            "username": "Hamza",
            "tcp_port": 5000,
            "udp_port": 5001,
            "timestamp": time.time(),
        }
        data = json.dumps(remote_beacon).encode("utf-8")
        discovery._handle_incoming_packet(data, ("192.168.1.100", 5002))

        peers = discovery.get_peers()
        self.assertIn("remote-node-1", peers)
        self.assertEqual(peers["remote-node-1"]["username"], "Hamza")
        self.assertEqual(peers["remote-node-1"]["ip"], "192.168.1.100")
        self.assertEqual(len(found_events), 1)
        self.assertEqual(found_events[0]["peer_id"], "remote-node-1")

    def test_prune_stale_peers(self):
        """Ensure inactive peers are pruned when timeout expires."""
        lost_events = []

        def on_lost(peer_id):
            lost_events.append(peer_id)

        discovery = UDPBroadcastDiscovery(
            peer_id="local-node",
            peer_timeout=0.2,
            on_peer_lost=on_lost,
        )

        remote_beacon = {
            "magic": BROADCAST_MAGIC,
            "peer_id": "stale-peer",
            "username": "Hamid",
            "tcp_port": 5000,
            "udp_port": 5001,
            "timestamp": time.time(),
        }
        discovery._handle_incoming_packet(
            json.dumps(remote_beacon).encode("utf-8"), ("192.168.1.105", 5002)
        )
        self.assertIn("stale-peer", discovery.get_peers())

        time.sleep(0.3)
        discovery.prune_stale_peers()

        self.assertEqual(discovery.get_peers(), {})
        self.assertEqual(lost_events, ["stale-peer"])

    def test_live_peer_discovery_over_network(self):
        """Verify two discovery instances on the same host can discover each other."""
        test_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        test_sock.bind(("", 0))
        test_port = test_sock.getsockname()[1]
        test_sock.close()

        peer_a = UDPBroadcastDiscovery(
            peer_id="peer-A",
            username="Alice",
            broadcast_port=test_port,
            broadcast_interval=0.2,
        )
        peer_b = UDPBroadcastDiscovery(
            peer_id="peer-B",
            username="Bob",
            broadcast_port=test_port,
            broadcast_interval=0.2,
        )

        try:
            peer_a.start()
            peer_b.start()

            peer_a.broadcast_beacon(target_ip="127.0.0.1")
            peer_b.broadcast_beacon(target_ip="127.0.0.1")

            time.sleep(0.8)

            peers_of_a = peer_a.get_peers()
            peers_of_b = peer_b.get_peers()

            self.assertIn("peer-B", peers_of_a)
            self.assertEqual(peers_of_a["peer-B"]["username"], "Bob")

            self.assertIn("peer-A", peers_of_b)
            self.assertEqual(peers_of_b["peer-A"]["username"], "Alice")
        finally:
            peer_a.stop()
            peer_b.stop()


class TestUDPMulticastDiscovery(unittest.TestCase):
    """Test suite for UDPMulticastDiscovery."""

    def test_multicast_build_beacon_payload(self):
        """Verify multicast beacon payload format."""
        discovery = UDPMulticastDiscovery(
            peer_id="mcast-peer-1",
            username="Mubashir",
            tcp_port=5000,
            udp_port=5001,
        )
        payload = discovery.build_beacon_payload()

        self.assertEqual(payload["magic"], MULTICAST_MAGIC)
        self.assertEqual(payload["peer_id"], "mcast-peer-1")
        self.assertEqual(payload["username"], "Mubashir")
        self.assertEqual(payload["tcp_port"], 5000)
        self.assertEqual(payload["udp_port"], 5001)
        self.assertIn("timestamp", payload)

    def test_multicast_self_packet_ignored(self):
        """Ensure peer ignores its own multicast packets."""
        discovery = UDPMulticastDiscovery(peer_id="self-mcast")
        payload = {
            "magic": MULTICAST_MAGIC,
            "peer_id": "self-mcast",
            "username": "Self",
            "tcp_port": 5000,
            "udp_port": 5001,
            "timestamp": time.time(),
        }
        discovery._handle_incoming_packet(
            json.dumps(payload).encode("utf-8"), ("127.0.0.1", 5003)
        )
        self.assertEqual(discovery.get_peers(), {})

    def test_multicast_invalid_packet_ignored(self):
        """Ensure malformed multicast packets are discarded."""
        discovery = UDPMulticastDiscovery(peer_id="mcast-receiver")

        discovery._handle_incoming_packet(b"not json", ("127.0.0.1", 5003))
        self.assertEqual(discovery.get_peers(), {})

        wrong_magic = {
            "magic": "WRONG_MAGIC",
            "peer_id": "other",
            "username": "Other",
        }
        discovery._handle_incoming_packet(
            json.dumps(wrong_magic).encode("utf-8"), ("127.0.0.1", 5003)
        )
        self.assertEqual(discovery.get_peers(), {})

    def test_multicast_handle_incoming_valid_packet(self):
        """Verify peer registration and callbacks from multicast beacon."""
        discovered = []

        def callback(peer):
            discovered.append(peer)

        discovery = UDPMulticastDiscovery(
            peer_id="mcast-local",
            on_peer_found=callback,
        )

        remote_beacon = {
            "magic": MULTICAST_MAGIC,
            "peer_id": "mcast-remote",
            "username": "Taha",
            "tcp_port": 5000,
            "udp_port": 5001,
            "timestamp": time.time(),
        }
        discovery._handle_incoming_packet(
            json.dumps(remote_beacon).encode("utf-8"), ("192.168.1.110", 5003)
        )

        peers = discovery.get_peers()
        self.assertIn("mcast-remote", peers)
        self.assertEqual(peers["mcast-remote"]["username"], "Taha")
        self.assertEqual(len(discovered), 1)

    def test_multicast_prune_stale_peers(self):
        """Verify inactive multicast peers are pruned on timeout."""
        lost = []

        discovery = UDPMulticastDiscovery(
            peer_id="mcast-local",
            peer_timeout=0.2,
            on_peer_lost=lambda pid: lost.append(pid),
        )

        remote_beacon = {
            "magic": MULTICAST_MAGIC,
            "peer_id": "stale-mcast-peer",
            "username": "Ibrahim",
            "tcp_port": 5000,
            "udp_port": 5001,
            "timestamp": time.time(),
        }
        discovery._handle_incoming_packet(
            json.dumps(remote_beacon).encode("utf-8"), ("192.168.1.112", 5003)
        )
        self.assertIn("stale-mcast-peer", discovery.get_peers())

        time.sleep(0.3)
        discovery.prune_stale_peers()

        self.assertEqual(discovery.get_peers(), {})
        self.assertEqual(lost, ["stale-mcast-peer"])

    def test_multicast_group_join_leave(self):
        """Test join_multicast_group and leave_multicast_group socket functions."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", 0))
        try:
            join_multicast_group(sock, "239.255.0.1")
            leave_multicast_group(sock, "239.255.0.1")
        finally:
            sock.close()

    def test_live_multicast_discovery_over_network(self):
        """Verify two multicast instances discover each other on the multicast group."""
        test_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        test_sock.bind(("", 0))
        test_port = test_sock.getsockname()[1]
        test_sock.close()

        peer_a = UDPMulticastDiscovery(
            peer_id="mcast-peer-A",
            username="Alice",
            multicast_group="239.255.0.1",
            multicast_port=test_port,
            beacon_interval=0.2,
        )
        peer_b = UDPMulticastDiscovery(
            peer_id="mcast-peer-B",
            username="Bob",
            multicast_group="239.255.0.1",
            multicast_port=test_port,
            beacon_interval=0.2,
        )

        try:
            peer_a.start()
            peer_b.start()

            peer_a.send_beacon()
            peer_b.send_beacon()

            time.sleep(0.8)

            peers_of_a = peer_a.get_peers()
            peers_of_b = peer_b.get_peers()

            self.assertIn("mcast-peer-B", peers_of_a)
            self.assertEqual(peers_of_a["mcast-peer-B"]["username"], "Bob")

            self.assertIn("mcast-peer-A", peers_of_b)
            self.assertEqual(peers_of_b["mcast-peer-A"]["username"], "Alice")
        finally:
            peer_a.stop()
            peer_b.stop()


class TestRendezvousServer(unittest.TestCase):
    """Test suite for RendezvousServer and RendezvousClient."""

    def setUp(self):
        test_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        test_sock.bind(("", 0))
        self.server_port = test_sock.getsockname()[1]
        test_sock.close()

        self.server = RendezvousServer(
            host="127.0.0.1", port=self.server_port, peer_timeout=15.0
        )
        self.server.start()
        time.sleep(0.05)

    def tearDown(self):
        self.server.stop()

    def test_client_register_and_ack(self):
        client = RendezvousClient(
            peer_id="client-1",
            username="Mubashir",
            server_host="127.0.0.1",
            server_port=self.server_port,
        )
        try:
            ack = client.register()
            self.assertIsNotNone(ack)
            self.assertEqual(ack["type"], MSG_REGISTER_ACK)
            self.assertEqual(ack["peer_id"], "client-1")
            self.assertEqual(client.mapped_ip, "127.0.0.1")
            self.assertIsNotNone(client.mapped_port)

            registered = self.server.get_registered_peers()
            self.assertIn("client-1", registered)
            self.assertEqual(registered["client-1"]["username"], "Mubashir")
        finally:
            client.deregister()
            client.close()

    def test_list_and_get_peers(self):
        client_a = RendezvousClient(
            peer_id="client-A",
            username="Alice",
            server_host="127.0.0.1",
            server_port=self.server_port,
        )
        client_b = RendezvousClient(
            peer_id="client-B",
            username="Bob",
            server_host="127.0.0.1",
            server_port=self.server_port,
        )
        try:
            client_a.register()
            client_b.register()

            peers = client_a.list_peers()
            peer_ids = [p["peer_id"] for p in peers]
            self.assertIn("client-B", peer_ids)
            self.assertNotIn("client-A", peer_ids)

            info = client_a.get_peer("client-B")
            self.assertIsNotNone(info)
            self.assertEqual(info["username"], "Bob")
        finally:
            client_a.deregister()
            client_b.deregister()
            client_a.close()
            client_b.close()

    def test_heartbeat(self):
        client = RendezvousClient(
            peer_id="client-hb",
            username="HeartbeatTest",
            server_host="127.0.0.1",
            server_port=self.server_port,
        )
        try:
            client.register()
            success = client.send_heartbeat()
            self.assertTrue(success)
        finally:
            client.deregister()
            client.close()

    def test_prune_stale_peers(self):
        short_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        short_sock.bind(("", 0))
        short_port = short_sock.getsockname()[1]
        short_sock.close()

        server = RendezvousServer(host="127.0.0.1", port=short_port, peer_timeout=0.3)
        server.start()
        client = RendezvousClient(
            peer_id="stale-client",
            server_host="127.0.0.1",
            server_port=short_port,
        )
        try:
            client.register()
            self.assertIn("stale-client", server.get_registered_peers())
            time.sleep(0.5)
            server.prune_stale_peers()
            self.assertNotIn("stale-client", server.get_registered_peers())
        finally:
            client.close()
            server.stop()

    def test_connect_request_mediation(self):
        client_x = RendezvousClient(
            peer_id="peer-X",
            server_host="127.0.0.1",
            server_port=self.server_port,
        )
        client_y = RendezvousClient(
            peer_id="peer-Y",
            server_host="127.0.0.1",
            server_port=self.server_port,
        )
        try:
            client_x.register()
            client_y.register()

            resp = client_x.request_connection("peer-Y")
            self.assertIsNotNone(resp)
            self.assertEqual(resp["type"], MSG_CONNECT_NOTIFY)
            self.assertEqual(resp["role"], "initiator")
            self.assertEqual(resp["remote_peer"]["peer_id"], "peer-Y")
        finally:
            client_x.deregister()
            client_y.deregister()
            client_x.close()
            client_y.close()

    def test_global_memory_registry(self):
        register_peer("peer-mem", ("10.0.0.1", 6000))
        addr = get_peer_address("peer-mem")
        self.assertEqual(addr, ("10.0.0.1", 6000))


class TestUDPHolePuncher(unittest.TestCase):
    """Test suite for UDPHolePuncher."""

    def test_build_packet(self):
        puncher = UDPHolePuncher(peer_id="puncher-test")
        try:
            data = puncher._build_packet(TYPE_SYN)
            msg = json.loads(data.decode("utf-8"))
            self.assertEqual(msg["magic"], PUNCH_MAGIC)
            self.assertEqual(msg["type"], TYPE_SYN)
            self.assertEqual(msg["peer_id"], "puncher-test")
            self.assertIn("timestamp", msg)
        finally:
            puncher.close()

    def test_direct_loopback_hole_punch(self):
        import threading
        puncher_a = UDPHolePuncher(peer_id="peer-A")
        puncher_b = UDPHolePuncher(peer_id="peer-B")

        result_a = [False]
        result_b = [False]

        def run_a():
            result_a[0] = puncher_a.punch("127.0.0.1", puncher_b.local_port, timeout=2.0)

        def run_b():
            result_b[0] = puncher_b.punch("127.0.0.1", puncher_a.local_port, timeout=2.0)

        t_a = threading.Thread(target=run_a)
        t_b = threading.Thread(target=run_b)

        try:
            t_a.start()
            t_b.start()
            t_a.join(timeout=3.0)
            t_b.join(timeout=3.0)

            self.assertTrue(result_a[0])
            self.assertTrue(result_b[0])
        finally:
            puncher_a.close()
            puncher_b.close()

    def test_hole_punch_timeout(self):
        puncher = UDPHolePuncher(peer_id="lonely-peer")
        try:
            success = puncher.punch("127.0.0.1", 59999, attempts=3, interval=0.05, timeout=0.3)
            self.assertFalse(success)
        finally:
            puncher.close()

    def test_coordinate_and_punch_with_rendezvous(self):
        import threading
        test_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        test_sock.bind(("", 0))
        server_port = test_sock.getsockname()[1]
        test_sock.close()

        server = RendezvousServer(host="127.0.0.1", port=server_port, peer_timeout=2.0)
        server.start()

        client_a = RendezvousClient(
            peer_id="peer-A",
            username="Alice",
            server_host="127.0.0.1",
            server_port=server_port,
        )
        client_b = RendezvousClient(
            peer_id="peer-B",
            username="Bob",
            server_host="127.0.0.1",
            server_port=server_port,
        )

        puncher_a = UDPHolePuncher(peer_id="peer-A", sock=client_a.sock)
        puncher_b = UDPHolePuncher(peer_id="peer-B", sock=client_b.sock)

        result_a = [None]
        result_b = [False]

        def run_punch_a():
            result_a[0] = puncher_a.coordinate_and_punch(client_a, "peer-B", timeout=2.0)

        def run_punch_b():
            result_b[0] = puncher_b.punch("127.0.0.1", client_a.mapped_port, timeout=2.0)

        try:
            client_a.register()
            client_b.register()

            t_a = threading.Thread(target=run_punch_a)
            t_b = threading.Thread(target=run_punch_b)

            t_a.start()
            t_b.start()

            t_a.join(timeout=3.0)
            t_b.join(timeout=3.0)

            self.assertIsNotNone(result_a[0])
            self.assertTrue(result_b[0])
        finally:
            puncher_a.close()
            puncher_b.close()
            client_a.close()
            client_b.close()
            server.stop()


if __name__ == "__main__":
    unittest.main()
