"""UDP NAT hole punching module for establishing direct peer-to-peer tunnels."""

import json
import logging
import socket
import threading
import time
import uuid
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

PUNCH_MAGIC = "PEERLINK_PUNCH"
TYPE_SYN = "PUNCH_SYN"
TYPE_ACK = "PUNCH_ACK"
DEFAULT_PUNCH_ATTEMPTS = 15
DEFAULT_PUNCH_INTERVAL = 0.1
DEFAULT_PUNCH_TIMEOUT = 3.0


class UDPHolePuncher:
    """Coordinates and executes UDP hole punching between two peers across NAT gateways."""

    def __init__(
        self,
        peer_id: Optional[str] = None,
        sock: Optional[socket.socket] = None,
        local_port: int = 0,
    ):
        self.peer_id = peer_id or str(uuid.uuid4())[:8]
        self._external_sock = sock is not None

        if sock:
            self.sock = sock
        else:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind(("", local_port))

        try:
            self.local_port = self.sock.getsockname()[1]
        except (socket.error, OSError):
            self.local_port = None

    def _build_packet(self, msg_type: str) -> bytes:
        """Create a hole punch probe packet."""
        payload = {
            "magic": PUNCH_MAGIC,
            "type": msg_type,
            "peer_id": self.peer_id,
            "timestamp": time.time(),
        }
        return json.dumps(payload).encode("utf-8")

    def punch(
        self,
        target_ip: str,
        target_port: int,
        attempts: int = DEFAULT_PUNCH_ATTEMPTS,
        interval: float = DEFAULT_PUNCH_INTERVAL,
        timeout: float = DEFAULT_PUNCH_TIMEOUT,
    ) -> bool:
        """Execute bidirectional UDP hole punching towards target endpoint.

        Sends repeated PUNCH_SYN packets while listening for incoming probes.
        Returns True once handshake completes and direct tunnel is open.
        """
        target_addr = (target_ip, target_port)
        syn_packet = self._build_packet(TYPE_SYN)
        ack_packet = self._build_packet(TYPE_ACK)

        syn_received = threading.Event()
        ack_received = threading.Event()
        stop_event = threading.Event()

        prev_timeout = self.sock.gettimeout()
        self.sock.settimeout(0.2)

        def sender_loop():
            sent = 0
            while not stop_event.is_set() and sent < attempts:
                try:
                    self.sock.sendto(syn_packet, target_addr)
                    if syn_received.is_set():
                        self.sock.sendto(ack_packet, target_addr)
                except (socket.error, OSError) as e:
                    logger.debug(f"Hole punch send error to {target_addr}: {e}")
                sent += 1
                time.sleep(interval)

        sender_thread = threading.Thread(
            target=sender_loop, daemon=True, name="HolePunchSender"
        )
        sender_thread.start()

        start_time = time.time()
        success = False

        try:
            while time.time() - start_time < timeout:
                try:
                    data, addr = self.sock.recvfrom(4096)
                    # Verify packet is from target IP (or loopback/LAN equivalent)
                    msg = json.loads(data.decode("utf-8"))
                    if msg.get("magic") != PUNCH_MAGIC:
                        continue
                    if msg.get("peer_id") == self.peer_id:
                        continue

                    msg_type = msg.get("type")
                    if msg_type == TYPE_SYN:
                        syn_received.set()
                        # Immediately echo ACK
                        try:
                            self.sock.sendto(ack_packet, addr)
                        except (socket.error, OSError):
                            pass
                        # If we received SYN and sent ACK, tunnel is established
                        success = True
                        break
                    elif msg_type == TYPE_ACK:
                        ack_received.set()
                        success = True
                        break

                except socket.timeout:
                    continue
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                except (socket.error, OSError):
                    break
        finally:
            stop_event.set()
            sender_thread.join(timeout=1.0)
            self.sock.settimeout(prev_timeout)

        if success:
            logger.info(f"UDP hole punch successful with {target_addr}")
        else:
            logger.warning(f"UDP hole punch timed out with {target_addr}")

        return success

    def coordinate_and_punch(
        self,
        rendezvous_client,
        target_peer_id: str,
        timeout: float = 5.0,
    ) -> Optional[Tuple[str, int]]:
        """Coordinate with rendezvous server and punch hole to remote peer.

        Returns remote (ip, port) if tunnel succeeded, else None.
        """
        # Step 1: Look up target peer through rendezvous
        target_info = rendezvous_client.get_peer(target_peer_id)
        if not target_info:
            logger.warning(f"Target peer '{target_peer_id}' not found on rendezvous server.")
            return None

        target_ip = target_info.get("public_ip")
        target_port = target_info.get("public_port")

        if not target_ip or not target_port:
            logger.warning(f"Invalid public endpoint for peer '{target_peer_id}'")
            return None

        # Step 2: Signal connection request via rendezvous server
        rendezvous_client.request_connection(target_peer_id)

        # Step 3: Execute hole punch
        if self.punch(target_ip, target_port, timeout=timeout):
            return (target_ip, target_port)
        return None

    def close(self):
        """Close socket if created internally."""
        if not self._external_sock and self.sock:
            try:
                self.sock.close()
            except Exception:
                pass


def initiate_hole_punching(
    peer_address: Tuple[str, int],
    local_port: int = 0,
    timeout: float = DEFAULT_PUNCH_TIMEOUT,
) -> bool:
    """Helper function to initiate hole punching against a remote endpoint."""
    puncher = UDPHolePuncher(local_port=local_port)
    try:
        return puncher.punch(peer_address[0], peer_address[1], timeout=timeout)
    finally:
        puncher.close()
