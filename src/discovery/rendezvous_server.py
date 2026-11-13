"""Peer rendezvous and signaling server for WAN peer registration and NAT traversal."""

import json
import logging
import socket
import threading
import time
import uuid
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_RENDEZVOUS_PORT = 5004
DEFAULT_PEER_EXPIRY = 30.0
RENDEZVOUS_MAGIC = "PEERLINK_RENDEZVOUS"

# Message Types
MSG_REGISTER = "REGISTER"
MSG_REGISTER_ACK = "REGISTER_ACK"
MSG_LIST_PEERS = "LIST_PEERS"
MSG_PEER_LIST = "PEER_LIST"
MSG_GET_PEER = "GET_PEER"
MSG_PEER_INFO = "PEER_INFO"
MSG_HEARTBEAT = "HEARTBEAT"
MSG_HEARTBEAT_ACK = "HEARTBEAT_ACK"
MSG_DEREGISTER = "DEREGISTER"
MSG_DEREGISTER_ACK = "DEREGISTER_ACK"
MSG_CONNECT_REQUEST = "CONNECT_REQUEST"
MSG_CONNECT_NOTIFY = "CONNECT_NOTIFY"
MSG_ERROR = "ERROR"


class RendezvousServer:
    """UDP-based rendezvous server for peer registration, discovery, and NAT traversal signaling."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = DEFAULT_RENDEZVOUS_PORT,
        peer_timeout: float = DEFAULT_PEER_EXPIRY,
    ):
        self.host = host
        self.port = port
        self.peer_timeout = peer_timeout

        self._peers: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._running = False

        self._sock: Optional[socket.socket] = None
        self._listen_thread: Optional[threading.Thread] = None
        self._prune_thread: Optional[threading.Thread] = None

    def start(self):
        """Start the rendezvous server."""
        if self._running:
            return

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.settimeout(1.0)
        self._running = True

        self._listen_thread = threading.Thread(
            target=self._listen_loop, daemon=True, name="RendezvousListener"
        )
        self._prune_thread = threading.Thread(
            target=self._prune_loop, daemon=True, name="RendezvousPruner"
        )

        self._listen_thread.start()
        self._prune_thread.start()
        logger.info(f"Rendezvous server listening on {self.host}:{self.port}")

    def stop(self):
        """Stop the rendezvous server and close sockets."""
        if not self._running:
            return

        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass

        if self._listen_thread and self._listen_thread.is_alive():
            self._listen_thread.join(timeout=1.5)
        if self._prune_thread and self._prune_thread.is_alive():
            self._prune_thread.join(timeout=1.5)

        logger.info("Rendezvous server stopped.")

    def _listen_loop(self):
        """Main listening loop receiving UDP control messages."""
        while self._running:
            try:
                data, addr = self._sock.recvfrom(4096)
                self._handle_message(data, addr)
            except socket.timeout:
                continue
            except (socket.error, OSError):
                break

    def _handle_message(self, data: bytes, addr: Tuple[str, int]):
        """Parse and route incoming request packets."""
        try:
            msg = json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return

        if msg.get("magic") != RENDEZVOUS_MAGIC:
            return

        msg_type = msg.get("type")
        if msg_type == MSG_REGISTER:
            self._handle_register(msg, addr)
        elif msg_type == MSG_HEARTBEAT:
            self._handle_heartbeat(msg, addr)
        elif msg_type == MSG_LIST_PEERS:
            self._handle_list_peers(msg, addr)
        elif msg_type == MSG_GET_PEER:
            self._handle_get_peer(msg, addr)
        elif msg_type == MSG_DEREGISTER:
            self._handle_deregister(msg, addr)
        elif msg_type == MSG_CONNECT_REQUEST:
            self._handle_connect_request(msg, addr)

    def _send_response(self, payload: dict, addr: Tuple[str, int]):
        """Send JSON response to client address."""
        if not self._sock:
            return
        payload["magic"] = RENDEZVOUS_MAGIC
        try:
            raw = json.dumps(payload).encode("utf-8")
            self._sock.sendto(raw, addr)
        except (socket.error, OSError) as e:
            logger.debug(f"Error sending rendezvous response to {addr}: {e}")

    def _handle_register(self, msg: dict, addr: Tuple[str, int]):
        """Handle peer registration and echo public endpoint."""
        peer_id = msg.get("peer_id")
        if not peer_id:
            self._send_response({"type": MSG_ERROR, "error": "Missing peer_id"}, addr)
            return

        peer_info = {
            "peer_id": peer_id,
            "username": msg.get("username", "Anonymous"),
            "public_ip": addr[0],
            "public_port": addr[1],
            "tcp_port": msg.get("tcp_port", 5000),
            "udp_port": msg.get("udp_port", 5001),
            "private_ip": msg.get("private_ip", addr[0]),
            "private_port": msg.get("private_port", addr[1]),
            "last_seen": time.time(),
        }

        with self._lock:
            self._peers[peer_id] = peer_info

        logger.info(f"Registered peer '{peer_id}' from {addr[0]}:{addr[1]}")
        response = {
            "type": MSG_REGISTER_ACK,
            "peer_id": peer_id,
            "mapped_ip": addr[0],
            "mapped_port": addr[1],
        }
        self._send_response(response, addr)

    def _handle_heartbeat(self, msg: dict, addr: Tuple[str, int]):
        """Handle keepalive message from a registered peer."""
        peer_id = msg.get("peer_id")
        with self._lock:
            if peer_id in self._peers:
                self._peers[peer_id]["last_seen"] = time.time()
                self._peers[peer_id]["public_ip"] = addr[0]
                self._peers[peer_id]["public_port"] = addr[1]
                self._send_response({"type": MSG_HEARTBEAT_ACK, "peer_id": peer_id}, addr)
            else:
                self._send_response({"type": MSG_ERROR, "error": "Peer not registered"}, addr)

    def _handle_list_peers(self, msg: dict, addr: Tuple[str, int]):
        """Return list of active peers, excluding requester if specified."""
        requester_id = msg.get("peer_id")
        with self._lock:
            peer_list = [
                info.copy()
                for pid, info in self._peers.items()
                if pid != requester_id
            ]

        self._send_response({"type": MSG_PEER_LIST, "peers": peer_list}, addr)

    def _handle_get_peer(self, msg: dict, addr: Tuple[str, int]):
        """Return details for a single target peer."""
        target_id = msg.get("target_peer_id")
        with self._lock:
            peer_info = self._peers.get(target_id)
            if peer_info:
                response = {"type": MSG_PEER_INFO, "peer": peer_info.copy()}
            else:
                response = {"type": MSG_ERROR, "error": f"Peer '{target_id}' not found"}

        self._send_response(response, addr)

    def _handle_deregister(self, msg: dict, addr: Tuple[str, int]):
        """Handle voluntary peer deregistration."""
        peer_id = msg.get("peer_id")
        with self._lock:
            if peer_id in self._peers:
                del self._peers[peer_id]
                logger.info(f"Deregistered peer '{peer_id}'")

        self._send_response({"type": MSG_DEREGISTER_ACK, "peer_id": peer_id}, addr)

    def _handle_connect_request(self, msg: dict, addr: Tuple[str, int]):
        """Mediate hole punching between two peers."""
        source_id = msg.get("source_peer_id") or msg.get("peer_id")
        target_id = msg.get("target_peer_id")

        with self._lock:
            source_info = self._peers.get(source_id)
            target_info = self._peers.get(target_id)

        if not source_info:
            self._send_response(
                {"type": MSG_ERROR, "error": "Source peer not registered"}, addr
            )
            return

        if not target_info:
            self._send_response(
                {"type": MSG_ERROR, "error": f"Target peer '{target_id}' not registered"}, addr
            )
            return

        # Notify Source of Target's endpoint
        self._send_response(
            {
                "type": MSG_CONNECT_NOTIFY,
                "role": "initiator",
                "remote_peer": target_info.copy(),
            },
            (source_info["public_ip"], source_info["public_port"]),
        )

        # Notify Target of Source's endpoint
        self._send_response(
            {
                "type": MSG_CONNECT_NOTIFY,
                "role": "responder",
                "remote_peer": source_info.copy(),
            },
            (target_info["public_ip"], target_info["public_port"]),
        )

    def _prune_loop(self):
        """Periodically clean up inactive peers."""
        while self._running:
            self.prune_stale_peers()
            time.sleep(2.0)

    def prune_stale_peers(self):
        """Remove peers that haven't sent a heartbeat within peer_timeout."""
        now = time.time()
        with self._lock:
            for peer_id, info in list(self._peers.items()):
                if now - info["last_seen"] > self.peer_timeout:
                    logger.info(f"Rendezvous peer '{peer_id}' expired and was removed.")
                    del self._peers[peer_id]

    def get_registered_peers(self) -> Dict[str, Dict]:
        """Return a copy of all currently registered peers."""
        with self._lock:
            return {k: v.copy() for k, v in self._peers.items()}


class RendezvousClient:
    """Client utility for communicating with a PeerLink Rendezvous Server."""

    def __init__(
        self,
        peer_id: Optional[str] = None,
        username: str = "Anonymous",
        server_host: str = "127.0.0.1",
        server_port: int = DEFAULT_RENDEZVOUS_PORT,
        tcp_port: int = 5000,
        udp_port: int = 5001,
        sock: Optional[socket.socket] = None,
    ):
        self.peer_id = peer_id or str(uuid.uuid4())[:8]
        self.username = username
        self.server_host = server_host
        self.server_port = server_port
        self.tcp_port = tcp_port
        self.udp_port = udp_port

        self._external_sock = sock is not None
        if sock:
            self.sock = sock
        else:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.bind(("", 0))
        self.sock.settimeout(2.0)
        self.mapped_ip: Optional[str] = None
        self.mapped_port: Optional[int] = None

    def _send_and_recv(self, payload: dict, timeout: float = 2.0) -> Optional[dict]:
        """Send message to rendezvous server and wait for response."""
        payload["magic"] = RENDEZVOUS_MAGIC
        data = json.dumps(payload).encode("utf-8")
        prev_timeout = self.sock.gettimeout()

        start_time = time.time()
        try:
            self.sock.sendto(data, (self.server_host, self.server_port))
            while time.time() - start_time < timeout:
                remaining = max(0.05, timeout - (time.time() - start_time))
                self.sock.settimeout(remaining)
                try:
                    resp_data, _ = self.sock.recvfrom(4096)
                    resp = json.loads(resp_data.decode("utf-8"))
                    if resp.get("magic") == RENDEZVOUS_MAGIC:
                        return resp
                except socket.timeout:
                    break
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
        except (socket.error, OSError) as e:
            logger.debug(f"Rendezvous communication failed: {e}")
        finally:
            self.sock.settimeout(prev_timeout)
        return None

    def register(self, timeout: float = 2.0) -> Optional[dict]:
        """Register with rendezvous server and obtain public mapped endpoint."""
        payload = {
            "type": MSG_REGISTER,
            "peer_id": self.peer_id,
            "username": self.username,
            "tcp_port": self.tcp_port,
            "udp_port": self.udp_port,
        }
        resp = self._send_and_recv(payload, timeout=timeout)
        if resp and resp.get("type") == MSG_REGISTER_ACK:
            self.mapped_ip = resp.get("mapped_ip")
            self.mapped_port = resp.get("mapped_port")
            return resp
        return None

    def list_peers(self, timeout: float = 2.0) -> List[Dict]:
        """Retrieve list of registered peers from the server."""
        payload = {
            "type": MSG_LIST_PEERS,
            "peer_id": self.peer_id,
        }
        resp = self._send_and_recv(payload, timeout=timeout)
        if resp and resp.get("type") == MSG_PEER_LIST:
            return resp.get("peers", [])
        return []

    def get_peer(self, target_peer_id: str, timeout: float = 2.0) -> Optional[Dict]:
        """Retrieve details of a specific target peer."""
        payload = {
            "type": MSG_GET_PEER,
            "peer_id": self.peer_id,
            "target_peer_id": target_peer_id,
        }
        resp = self._send_and_recv(payload, timeout=timeout)
        if resp and resp.get("type") == MSG_PEER_INFO:
            return resp.get("peer")
        return None

    def send_heartbeat(self, timeout: float = 2.0) -> bool:
        """Send keepalive heartbeat to server."""
        payload = {
            "type": MSG_HEARTBEAT,
            "peer_id": self.peer_id,
        }
        resp = self._send_and_recv(payload, timeout=timeout)
        return resp is not None and resp.get("type") == MSG_HEARTBEAT_ACK

    def request_connection(self, target_peer_id: str, timeout: float = 2.0) -> Optional[Dict]:
        """Request rendezvous server to coordinate hole punch with target peer."""
        payload = {
            "type": MSG_CONNECT_REQUEST,
            "peer_id": self.peer_id,
            "source_peer_id": self.peer_id,
            "target_peer_id": target_peer_id,
        }
        return self._send_and_recv(payload, timeout=timeout)

    def deregister(self, timeout: float = 2.0) -> bool:
        """Deregister peer from server."""
        payload = {
            "type": MSG_DEREGISTER,
            "peer_id": self.peer_id,
        }
        resp = self._send_and_recv(payload, timeout=timeout)
        return resp is not None and resp.get("type") == MSG_DEREGISTER_ACK

    def close(self):
        """Close socket if created internally."""
        if not self._external_sock and self.sock:
            try:
                self.sock.close()
            except Exception:
                pass


# Backward-compatible global in-memory registry functions
_global_registry: Dict[str, Tuple[str, int]] = {}
_registry_lock = threading.Lock()


def register_peer(peer_id: str, address: Tuple[str, int]):
    """Register peer address in local memory table."""
    with _registry_lock:
        _global_registry[peer_id] = address


def get_peer_address(peer_id: str) -> Optional[Tuple[str, int]]:
    """Retrieve peer address from local memory table."""
    with _registry_lock:
        return _global_registry.get(peer_id)
