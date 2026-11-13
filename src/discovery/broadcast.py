"""UDP broadcast-based LAN peer discovery module for PeerLink."""

import json
import logging
import socket
import threading
import time
import uuid
from typing import Callable, Dict, Optional

logger = logging.getLogger(__name__)

DEFAULT_BROADCAST_PORT = 5002
DEFAULT_BEACON_INTERVAL = 2.0
DEFAULT_PEER_TIMEOUT = 6.0
BROADCAST_MAGIC = "PEERLINK_DISCOVERY"


class UDPBroadcastDiscovery:
    """Handles automatic peer discovery across the local area network using UDP broadcast."""

    def __init__(
        self,
        peer_id: Optional[str] = None,
        username: str = "Anonymous",
        broadcast_port: int = DEFAULT_BROADCAST_PORT,
        tcp_port: int = 5000,
        udp_port: int = 5001,
        broadcast_interval: float = DEFAULT_BEACON_INTERVAL,
        peer_timeout: float = DEFAULT_PEER_TIMEOUT,
        on_peer_found: Optional[Callable[[Dict], None]] = None,
        on_peer_lost: Optional[Callable[[str], None]] = None,
    ):
        self.peer_id = peer_id or str(uuid.uuid4())[:8]
        self.username = username
        self.broadcast_port = broadcast_port
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.broadcast_interval = broadcast_interval
        self.peer_timeout = peer_timeout

        self.on_peer_found = on_peer_found
        self.on_peer_lost = on_peer_lost

        self._peers: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._running = False

        self._broadcast_sock: Optional[socket.socket] = None
        self._listen_sock: Optional[socket.socket] = None

        self._listener_thread: Optional[threading.Thread] = None
        self._broadcaster_thread: Optional[threading.Thread] = None
        self._pruner_thread: Optional[threading.Thread] = None

    def build_beacon_payload(self) -> dict:
        """Create the JSON beacon payload for this peer."""
        return {
            "magic": BROADCAST_MAGIC,
            "peer_id": self.peer_id,
            "username": self.username,
            "tcp_port": self.tcp_port,
            "udp_port": self.udp_port,
            "timestamp": time.time(),
        }

    def _init_sockets(self):
        """Initialize UDP broadcast and listening sockets."""
        # Socket for sending broadcasts
        self._broadcast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._broadcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        # Socket for receiving broadcasts
        self._listen_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._listen_sock.bind(("", self.broadcast_port))
        self._listen_sock.settimeout(1.0)

    def start(self):
        """Start the discovery service (listener, broadcaster, and pruner threads)."""
        if self._running:
            return

        self._init_sockets()
        self._running = True

        self._listener_thread = threading.Thread(
            target=self._listen_loop, daemon=True, name="UDPDiscoveryListener"
        )
        self._broadcaster_thread = threading.Thread(
            target=self._broadcast_loop, daemon=True, name="UDPDiscoveryBroadcaster"
        )
        self._pruner_thread = threading.Thread(
            target=self._prune_loop, daemon=True, name="UDPDiscoveryPruner"
        )

        self._listener_thread.start()
        self._broadcaster_thread.start()
        self._pruner_thread.start()
        logger.info(f"Peer discovery started for '{self.username}' ({self.peer_id}) on port {self.broadcast_port}")

    def stop(self):
        """Stop peer discovery and clean up sockets and threads."""
        if not self._running:
            return

        self._running = False

        if self._listen_sock:
            try:
                self._listen_sock.close()
            except Exception:
                pass

        if self._broadcast_sock:
            try:
                self._broadcast_sock.close()
            except Exception:
                pass

        if self._listener_thread and self._listener_thread.is_alive():
            self._listener_thread.join(timeout=1.5)
        if self._broadcaster_thread and self._broadcaster_thread.is_alive():
            self._broadcaster_thread.join(timeout=1.5)
        if self._pruner_thread and self._pruner_thread.is_alive():
            self._pruner_thread.join(timeout=1.5)

        logger.info(f"Peer discovery stopped for '{self.username}' ({self.peer_id})")

    def broadcast_beacon(self, target_ip: str = "<broadcast>"):
        """Send a single discovery beacon to target_ip (default: <broadcast>)."""
        if not self._broadcast_sock:
            return
        payload = json.dumps(self.build_beacon_payload()).encode("utf-8")
        try:
            self._broadcast_sock.sendto(payload, (target_ip, self.broadcast_port))
        except (socket.error, OSError) as e:
            logger.debug(f"Broadcast send error: {e}")

    def _broadcast_loop(self):
        """Periodically broadcast discovery beacons."""
        while self._running:
            self.broadcast_beacon()
            # Sleep in small increments to allow rapid shutdown
            slept = 0.0
            while self._running and slept < self.broadcast_interval:
                time.sleep(0.1)
                slept += 0.1

    def _listen_loop(self):
        """Continuously listen for incoming UDP discovery packets."""
        while self._running:
            try:
                data, addr = self._listen_sock.recvfrom(4096)
                self._handle_incoming_packet(data, addr)
            except socket.timeout:
                continue
            except (socket.error, OSError):
                break

    def _handle_incoming_packet(self, data: bytes, addr: tuple):
        """Parse incoming beacon and update peer registry."""
        try:
            message = json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return

        if message.get("magic") != BROADCAST_MAGIC:
            return

        sender_id = message.get("peer_id")
        # Ignore packets originating from this peer
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
        """Remove peers that have not sent a beacon within peer_timeout."""
        now = time.time()
        lost_peers = []

        with self._lock:
            for peer_id, info in list(self._peers.items()):
                if now - info["last_seen"] > self.peer_timeout:
                    lost_peers.append(peer_id)
                    del self._peers[peer_id]

        for peer_id in lost_peers:
            logger.info(f"Peer '{peer_id}' timed out and was removed.")
            if self.on_peer_lost:
                try:
                    self.on_peer_lost(peer_id)
                except Exception as e:
                    logger.error(f"Error in on_peer_lost callback: {e}")

    def get_peers(self) -> Dict[str, Dict]:
        """Return a copy of all currently active discovered peers."""
        with self._lock:
            return {k: v.copy() for k, v in self._peers.items()}


def discover_peers(timeout: float = 3.0, broadcast_port: int = DEFAULT_BROADCAST_PORT) -> Dict[str, Dict]:
    """Helper function to perform one-off peer discovery for a given duration."""
    discovery = UDPBroadcastDiscovery(
        username="DiscoveryProbe",
        broadcast_port=broadcast_port,
        broadcast_interval=1.0,
        peer_timeout=timeout + 2.0,
    )
    discovery.start()
    try:
        time.sleep(timeout)
        return discovery.get_peers()
    finally:
        discovery.stop()
