# PeerLink — Changes by Mubashir Ali

**Team Member:** Mubashir Ali (23K-6077)  
**Role:** Peer Discovery and NAT Traversal  
**Assigned Feature Branch:** `feature/peer-discovery`  
**Last Updated:** November 14, 2026  

---

## 1. Overview of Responsibilities
As defined in `docs/team-roles.md`, Mubashir is responsible for:
- [x] Study and implement UDP discovery mechanisms.
- [x] UDP broadcast-based LAN peer discovery.
- [x] UDP multicast-based peer discovery.
- [x] Rendezvous server for public peer registration and exchange.
- [x] UDP hole punching for NAT traversal.
- [x] Unit and integration tests for discovery components.
- [x] System architecture documentation (`docs/architecture.md`).
- [x] Testing on Wi-Fi and mobile hotspot; documenting NAT limitations.

---

## 2. Completed Changes

### A. UDP Broadcast Peer Discovery (`src/discovery/broadcast.py`)
- **Status:** Completed & Tested
- **Key Features Implemented:**
  - Implemented `UDPBroadcastDiscovery` class using standard Python `socket` and `threading`.
  - Configured sockets with `SO_BROADCAST` and `SO_REUSEADDR` for clean broadcast communication across LAN and multiple local instances.
  - Defined discovery packet structure (`PEERLINK_DISCOVERY` magic, `peer_id`, `username`, `tcp_port`, `udp_port`, and `timestamp`).
  - Added concurrent background listener thread to ingest incoming beacons and update the peer table.
  - Implemented self-packet filtering to prevent a node from discovering itself.
  - Added periodic beacon broadcast thread (default interval: 2.0s).
  - Implemented automatic pruning thread for stale peers exceeding timeout (default: 6.0s).
  - Added callback triggers (`on_peer_found`, `on_peer_lost`) for integration with other modules.
  - Added thread-safe locking (`threading.Lock`) for concurrent peer dictionary reads and updates.
  - Added `discover_peers()` helper function for one-shot synchronous peer discovery probes.

### B. UDP Multicast Peer Discovery (`src/discovery/multicast.py`)
- **Status:** Completed & Tested
- **Key Features Implemented:**
  - Implemented `UDPMulticastDiscovery` class utilizing RFC 2365 administratively scoped multicast group `239.255.0.1` on port `5003`.
  - Added `join_multicast_group()` and `leave_multicast_group()` utility functions using `socket.IP_ADD_MEMBERSHIP` and `socket.IP_DROP_MEMBERSHIP`.
  - Configured multicast sender socket with `IP_MULTICAST_TTL` and `IP_MULTICAST_LOOP` to allow multi-instance testing on the same host.
  - Defined multicast beacon format with `PEERLINK_MULTICAST` magic, `peer_id`, `username`, `tcp_port`, `udp_port`, and `timestamp`.
  - Concurrently listens on multicast socket, validates packets, discards self-sent packets, and updates active peer registry.
  - Thread-safe peer dictionary updates and automatic pruning thread for timed-out peers.
  - Callback triggers for `on_peer_found` and `on_peer_lost`.
  - Added `discover_peers()` helper function for one-shot multicast probes.

### C. Rendezvous Server for WAN Discovery & Signaling (`src/discovery/rendezvous_server.py`)
- **Status:** Completed & Tested
- **Key Features Implemented:**
  - Implemented `RendezvousServer` listening on `0.0.0.0:5004` to coordinate peer discovery across WAN/NAT boundaries.
  - Detects public mapped reflexive endpoint (`mapped_ip`, `mapped_port`) from incoming UDP datagrams and returns them in `REGISTER_ACK`.
  - Handles message protocols: `REGISTER`, `HEARTBEAT`, `LIST_PEERS`, `GET_PEER`, `DEREGISTER`, and `CONNECT_REQUEST`.
  - Implemented NAT traversal connection mediation (`CONNECT_REQUEST` / `CONNECT_NOTIFY` dispatch to both peers).
  - Implemented background pruner thread to purge expired peers.
  - Implemented `RendezvousClient` helper providing a synchronous client API (`register()`, `list_peers()`, `get_peer()`, `send_heartbeat()`, `request_connection()`, `deregister()`).
  - Preserved backward-compatible `register_peer()` and `get_peer_address()` module functions.

### D. UDP Hole Punching for Direct NAT Traversal (`src/discovery/hole_punching.py`)
- **Status:** Completed & Tested
- **Key Features Implemented:**
  - Implemented `UDPHolePuncher` class for direct peer-to-peer UDP tunnel establishment across NAT routers.
  - Supports reusing existing sockets from `RendezvousClient` to maintain identical external NAT translation port bindings.
  - Packet schema with `PEERLINK_PUNCH` magic, `PUNCH_SYN`, and `PUNCH_ACK` message types.
  - `punch()`: Sends rapid punch probes while listening for incoming probes and replies with `PUNCH_ACK` upon reception.
  - `coordinate_and_punch()`: End-to-end coordinated flow combining Rendezvous signaling and simultaneous punch exchange.
  - Implemented clean socket timeout handling and graceful teardown.
  - Preserved backward-compatible `initiate_hole_punching()` module function.

### E. Discovery Unit Test Suite (`tests/test_discovery.py`)
- **Status:** Completed & Verified (23/23 tests passing)
- **Broadcast Tests (6 tests):**
  - Schema, self-filtering, corrupt packet resilience, peer registration, pruning, and live dual-instance loopback broadcast.
- **Multicast Tests (7 tests):**
  - Multicast formatting, self-filtering, corrupt packet handling, callback execution, pruning, IGMP socket group join/leave, and live dual-instance loopback multicast.
- **Rendezvous Tests (6 tests):**
  - Registration/mapped endpoint resolution, peer listing, heartbeat, pruning, connection mediation, and memory registry.
- **Hole Punching Tests (4 tests):**
  - `test_build_packet`: Validates punch probe payload formatting.
  - `test_direct_loopback_hole_punch`: Validates simultaneous bidirectional hole punching between two endpoints.
  - `test_hole_punch_timeout`: Validates timeout handling when destination is unreachable.
  - `test_coordinate_and_punch_with_rendezvous`: Full integration test from registration through signaling to successful direct hole punching.

### F. System Architecture Documentation (`docs/architecture.md`)
- **Status:** Completed
- **Key Sections Documented:**
  - Multi-tier system architecture diagram (Application, Discovery, Transport, OS Sockets).
  - Comparative analysis of LAN Broadcast vs Subnet Multicast vs WAN Rendezvous discovery.
  - In-depth NAT analysis (Full Cone, Restricted Cone, Port-Restricted Cone, Symmetric NAT).
  - Complete Mermaid sequence diagram illustrating the 4-phase UDP hole punching protocol flow.
  - Empirical testing results across LAN Wi-Fi, Cross-Subnet Wi-Fi to Mobile Hotspot, and Mobile Hotspot with CGNAT limitations.
  - Master directory of all network ports, protocols, and module ownership.

---

## 3. Summary of Deliverables
All assigned responsibilities for **Mubashir Ali** on the `feature/peer-discovery` branch are now **100% complete**:
- [x] Broadcast Discovery (`src/discovery/broadcast.py`)
- [x] Multicast Discovery (`src/discovery/multicast.py`)
- [x] Rendezvous Server (`src/discovery/rendezvous_server.py`)
- [x] UDP Hole Punching (`src/discovery/hole_punching.py`)
- [x] Unit Test Suite (`tests/test_discovery.py` — 23 tests pass)
- [x] Architecture & NAT Documentation (`docs/architecture.md`)
- [x] Changes Log (`changesbymubashir.md`)

---

## 4. Change History Log

| Date | Files Modified | Description | Status |
|------|----------------|-------------|--------|
| 2026-11-14 | `src/discovery/broadcast.py` | Implemented full UDP broadcast discovery engine | Completed |
| 2026-11-14 | `tests/test_discovery.py` | Added comprehensive unit tests for broadcast discovery | Completed (6/6 Pass) |
| 2026-11-14 | `changesbymubashir.md` | Created change tracker and remaining tasks roadmap | Completed |
| 2026-11-14 | `src/discovery/multicast.py` | Implemented full UDP multicast discovery with IGMP group membership | Completed |
| 2026-11-14 | `tests/test_discovery.py` | Added 7 unit tests for multicast discovery | Completed (13/13 Pass) |
| 2026-11-14 | `src/discovery/rendezvous_server.py` | Implemented Rendezvous Server & Client for WAN discovery and hole punching signaling | Completed |
| 2026-11-14 | `tests/test_discovery.py` | Added 6 unit tests for Rendezvous Server and Client | Completed (19/19 Pass) |
| 2026-11-14 | `src/discovery/hole_punching.py` | Implemented UDP Hole Punching engine and coordinated NAT traversal | Completed |
| 2026-11-14 | `tests/test_discovery.py` | Added 4 unit tests for UDP Hole Punching | Completed (23/23 Pass) |
| 2026-11-14 | `docs/architecture.md` | Authored comprehensive system architecture, NAT analysis, sequence diagrams, and port allocations | Completed |
| 2026-11-14 | `changesbymubashir.md` | Finalized change tracker; all Mubashir modules 100% complete | Completed |
