"""UDP multicast-based peer discovery module for PeerLink."""

import json
import logging
import socket
import struct
import threading
import time
import uuid
from typing import Callable, Dict, Optional

logger = logging.getLogger(__name__)

DEFAULT_MULTICAST_GROUP = "239.255.0.1"
DEFAULT_MULTICAST_PORT = 5003
DEFAULT_BEACON_INTERVAL = 2.0
DEFAULT_PEER_TIMEOUT = 6.0
DEFAULT_MULTICAST_TTL = 2
MULTICAST_MAGIC = "PEERLINK_MULTICAST"


def join_multicast_group(
    sock: socket.socket, multicast_group: str, interface_ip: str = "0.0.0.0"
):
    """Join an IPv4 multicast group on the specified interface."""
    mreq = struct.pack(
        "=4s4s",
        socket.inet_aton(multicast_group),
        socket.inet_aton(interface_ip),
    )
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)


def leave_multicast_group(
    sock: socket.socket, multicast_group: str, interface_ip: str = "0.0.0.0"
):
    """Leave an IPv4 multicast group."""
    try:
        mreq = struct.pack(
            "=4s4s",
            socket.inet_aton(multicast_group),
            socket.inet_aton(interface_ip),
        )
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
    except (socket.error, OSError) as e:
        logger.debug(f"Failed to leave multicast group: {e}")


class UDPMulticastDiscovery:
    """Handles automatic peer discovery across the local subnet using UDP multicast."""

    def __init__(
        self,
        peer_id: Optional[str] = None,
        username: str = "Anonymous",
        multicast_group: str = DEFAULT_MULTICAST_GROUP,
        multicast_port: int = DEFAULT_MULTICAST_PORT,
        tcp_port: int = 5000,
        udp_port: int = 5001,
        beacon_interval: float = DEFAULT_BEACON_INTERVAL,
        peer_timeout: float = DEFAULT_PEER_TIMEOUT,
        ttl: int = DEFAULT_MULTICAST_TTL,
        on_peer_found: Optional[Callable[[Dict], None]] = None,
        on_peer_lost: Optional[Callable[[str], None]] = None,
    ):
        self.peer_id = peer_id or str(uuid.uuid4())[:8]
        self.username = username
        self.multicast_group = multicast_group
        self.multicast_port = multicast_port
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.beacon_interval = beacon_interval
        self.peer_timeout = peer_timeout
        self.ttl = ttl

        self.on_peer_found = on_peer_found
        self.on_peer_lost = on_peer_lost

        self._peers: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._running = False

        self._send_sock: Optional[socket.socket] = None
        self._recv_sock: Optional[socket.socket] = None

        self._listener_thread: Optional[threading.Thread] = None
        self._broadcaster_thread: Optional[threading.Thread] = None
        self._pruner_thread: Optional[threading.Thread] = None

    def build_beacon_payload(self) -> dict:
        """Create the JSON multicast beacon payload for this peer."""
        return {
            "magic": MULTICAST_MAGIC,
            "peer_id": self.peer_id,
            "username": self.username,
            "tcp_port": self.tcp_port,
            "udp_port": self.udp_port,
            "timestamp": time.time(),
        }

    def _init_sockets(self):
        """Initialize multicast sender and receiver sockets."""
        # Sender socket
        self._send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self._send_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, self.ttl)
        # Enable loopback so peers running on the same host can discover each other
        self._send_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)

        # Receiver socket
        self._recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self._recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._recv_sock.bind(("", self.multicast_port))
        join_multicast_group(self._recv_sock, self.multicast_group)
        self._recv_sock.settimeout(1.0)

    def start(self):
        """Start multicast discovery services."""
        if self._running:
            return

        self._init_sockets()
        self._running = True

        self._listener_thread = threading.Thread(
            target=self._listen_loop, daemon=True, name="UDPMulticastListener"
        )
        self._broadcaster_thread = threading.Thread(
            target=self._broadcast_loop, daemon=True, name="UDPMulticastBroadcaster"
        )
        self._pruner_thread = threading.Thread(
            target=self._prune_loop, daemon=True, name="UDPMulticastPruner"
        )

        self._listener_thread.start()
        self._broadcaster_thread.start()
        self._pruner_thread.start()
        logger.info(
            f"Multicast discovery started for '{self.username}' ({self.peer_id}) "
            f"on {self.multicast_group}:{self.multicast_port}"
        )

    def stop(self):
        """Stop multicast discovery and cleanly release sockets."""
        if not self._running:
            return

        self._running = False

        if self._recv_sock:
            try:
                leave_multicast_group(self._recv_sock, self.multicast_group)
            except Exception:
                pass
            try:
                self._recv_sock.close()
            except Exception:
                pass

        if self._send_sock:
            try:
                self._send_sock.close()
            except Exception:
                pass

        if self._listener_thread and self._listener_thread.is_alive():
            self._listener_thread.join(timeout=1.5)
        if self._broadcaster_thread and self._broadcaster_thread.is_alive():
            self._broadcaster_thread.join(timeout=1.5)
        if self._pruner_thread and self._pruner_thread.is_alive():
            self._pruner_thread.join(timeout=1.5)

        logger.info(f"Multicast discovery stopped for '{self.username}' ({self.peer_id})")

    def send_beacon(self):
        """Send a single multicast beacon packet to the multicast group."""
        if not self._send_sock:
            return
        payload = json.dumps(self.build_beacon_payload()).encode("utf-8")
        try:
            self._send_sock.sendto(payload, (self.multicast_group, self.multicast_port))
        except (socket.error, OSError) as e:
            logger.debug(f"Multicast send error: {e}")

    def _broadcast_loop(self):
        """Periodically broadcast multicast beacons."""
        while self._running:
            self.send_beacon()
            slept = 0.0
            while self._running and slept < self.beacon_interval:
                time.sleep(0.1)
                slept += 0.1

    def _listen_loop(self):
        """Continuously receive and handle incoming multicast packets."""
        while self._running:
            try:
                data, addr = self._recv_sock.recvfrom(4096)
                self._handle_incoming_packet(data, addr)
            except socket.timeout:
                continue
            except (socket.error, OSError):
                break

    def _handle_incoming_packet(self, data: bytes, addr: tuple):
        """Process incoming multicast packet and update peer registry."""
        try:
            message = json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return

        if message.get("magic") != MULTICAST_MAGIC:
            return

        sender_id = message.get("peer_id")
        if not sender_id or sender_id == self.peer_id:
            return

        ip = addr[0]
        peer_info = {
            "peer_id": sender_id,
            "username": message.get("username", "Unknown"),
            "ip": ip,
            "tcp_port": message.get("tcp_port", self.tcp_port),
            "udp_port": message.get("udp_port", self.udp_port),
            "last_seen": time.time(),
        }

        with self._lock:
            is_new = sender_id not in self._peers
            self._peers[sender_id] = peer_info

        if is_new and self.on_peer_found:
            try:
                self.on_peer_found(peer_info)
            except Exception as e:
                logger.error(f"Error in on_peer_found callback: {e}")

    def _prune_loop(self):
        """Periodically check and remove inactive peers."""
        while self._running:
            self.prune_stale_peers()
            time.sleep(1.0)

    def prune_stale_peers(self):
        """Prune peers whose beacons have timed out."""
        now = time.time()
        lost_peers = []

        with self._lock:
            for peer_id, info in list(self._peers.items()):
                if now - info["last_seen"] > self.peer_timeout:
                    lost_peers.append(peer_id)
                    del self._peers[peer_id]

        for peer_id in lost_peers:
            logger.info(f"Multicast peer '{peer_id}' timed out and was removed.")
            if self.on_peer_lost:
                try:
                    self.on_peer_lost(peer_id)
                except Exception as e:
                    logger.error(f"Error in on_peer_lost callback: {e}")

    def get_peers(self) -> Dict[str, Dict]:
        """Return a copy of currently active discovered multicast peers."""
        with self._lock:
            return {k: v.copy() for k, v in self._peers.items()}


def discover_peers(
    timeout: float = 3.0,
    multicast_group: str = DEFAULT_MULTICAST_GROUP,
    multicast_port: int = DEFAULT_MULTICAST_PORT,
) -> Dict[str, Dict]:
    """Helper function to perform one-off multicast peer discovery for a given duration."""
    discovery = UDPMulticastDiscovery(
        username="MulticastProbe",
        multicast_group=multicast_group,
        multicast_port=multicast_port,
        beacon_interval=1.0,
        peer_timeout=timeout + 2.0,
    )
    discovery.start()
    try:
        time.sleep(timeout)
        return discovery.get_peers()
    finally:
        discovery.stop()
