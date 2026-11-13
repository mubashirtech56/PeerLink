# PeerLink — Team Roles & Responsibilities

**Project:** Peer-to-Peer Chat and File Transfer System with TCP/UDP Transport and Link Utilization Analysis  
**Course:** Computer Networks Lab  
**Class:** BS Computer Science, BCS-5A  
**University:** FAST-NUCES Peshawar  

---

## Team Overview

| Member | Role | Primary Branch |
|--------|------|----------------|
| Muhammad Hamza | Protocol Design & Reliable UDP | `feature/protocol-design` / `feature/reliable-udp` |
| Hamid Khan | TCP Chat & File Transfer | `feature/tcp-transfer` |
| Mubashir Ali | Peer Discovery & NAT Traversal | `feature/peer-discovery` |
| Muhammad Ibrahim Khalil | Network Experiments & Wireshark | `feature/network-experiments` |
| Muhammad Taha Rizwan | Performance Analysis & Final Report | `feature/performance-analysis` |

---

## Member 1: Muhammad Hamza

**Role:** Protocol Design and Reliable UDP

**Responsibilities:**
- Define the UDP header format and packet structure.
- Specify sequence numbers, ACK rules, and state machine.
- Implement reliable UDP variants:
  - Stop-and-Wait ARQ
  - Go-Back-N ARQ
  - Selective Repeat ARQ
- Implement receiver window management.
- Implement adaptive timeout using RFC 6298.
- Write unit tests for UDP and reliability.
- Share the protocol specification with all members before parallel development starts.

**Files Owned:**
- `docs/protocol-specification.md`
- `src/transport/udp/`
- `tests/test_udp.py`
- `tests/test_reliability.py`

**Branches:**
- `feature/protocol-design`
- `feature/reliable-udp` (after protocol design is merged)

---

## Member 2: Hamid Khan

**Role:** TCP Chat and File Transfer

**Responsibilities:**
- Set up TCP server and client.
- Implement peer-to-peer connection handling.
- Implement length-prefixed framing.
- Implement chat message exchange.
- Implement chunked file transfer with integrity checks.
- Run Nagle algorithm experiments.
- Test with multiple peers.

**Files Owned:**
- `src/transport/tcp/`
- `src/application/chat.py`
- `src/application/file_transfer.py`
- `src/application/message.py`
- `tests/test_tcp.py`
- `tests/test_file_transfer.py`

**Branch:** `feature/tcp-transfer`

---

## Member 3: Mubashir Ali

**Role:** Peer Discovery and NAT Traversal

**Responsibilities:**
- Study UDP discovery mechanisms.
- Implement UDP broadcast-based LAN peer discovery.
- Implement UDP multicast-based discovery.
- Create a rendezvous server for peer registration.
- Exchange public IP and port information.
- Implement UDP hole punching.
- Test on Wi-Fi and mobile hotspot.
- Document NAT limitations and traversal results.

**Files Owned:**
- `src/discovery/broadcast.py`
- `src/discovery/multicast.py`
- `src/discovery/rendezvous_server.py`
- `src/discovery/hole_punching.py`
- `tests/test_discovery.py`
- `docs/architecture.md`

**Branch:** `feature/peer-discovery`

---

## Member 4: Muhammad Ibrahim Khalil

**Role:** Network Experiments and Wireshark Analysis

**Responsibilities:**
- Prepare the testing environment.
- Run LAN baseline experiments.
- Configure `tc netem` for network emulation.
- Execute typical internet, poor mobile, and severe network scenarios.
- Run `iperf3` and `ping` measurements.
- Capture traffic with Wireshark.
- Analyze retransmissions and packet behavior.
- Save results as CSV and PCAP files.
- Capture and organize Wireshark screenshots.

**Files Owned:**
- `experiments/baseline/`
- `experiments/typical_internet/`
- `experiments/poor_mobile/`
- `experiments/severe/`
- `experiments/results/`
- `screenshots/wireshark/`

**Branch:** `feature/network-experiments`

---

## Member 5: Muhammad Taha Rizwan

**Role:** Performance Analysis and Final Report

**Responsibilities:**
- Collect experimental data from CSV measurements.
- Calculate RTT and jitter.
- Calculate throughput and goodput.
- Calculate measured link utilization.
- Calculate theoretical utilization.
- Compare measured vs theoretical results.
- Generate Matplotlib graphs.
- Prepare the final report.
- Prepare the final demonstration.

**Files Owned:**
- `analysis/utilization.py`
- `analysis/throughput.py`
- `analysis/comparison.py`
- `analysis/graphs.py`
- `src/monitoring/`
- `docs/`

**Branch:** `feature/performance-analysis`

---

## Shared / Integration Branches

| Branch | Owner / Purpose |
|--------|-----------------|
| `main` | Final stable project |
| `develop` | Integration of everyone's work |

---

## Integration Rules

1. Never push unfinished code directly to `main`.
2. Work only in your assigned feature branch.
3. Commit regularly with meaningful messages.
4. Test your module before opening a Pull Request.
5. Never modify another member's module without discussing it first.
6. Review integration changes before merging into `develop`.
7. Keep experimental results and screenshots in the repository.
8. Update documentation whenever you change a module's interface.

---

## Development Order

| Phase | Owner(s) | Deliverable |
|-------|----------|-------------|
| 1. Foundation | Hamza | Protocol specification, packet format, interfaces |
| 2. Parallel Build | Hamid, Hamza, Mubashir | TCP transport, reliable UDP, peer discovery |
| 3. Integration | All | Working PeerLink application on `develop` |
| 4. Experimentation | Ibrahim | CSV results and PCAP captures |
| 5. Analysis & Reporting | Taha | Graphs, utilization comparison, final report |

---

## Important Note

The **protocol specification must be completed first**. Hamza should share the packet format and function interfaces with all four other members before parallel development begins. This ensures all modules integrate cleanly into `develop`.