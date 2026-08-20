#!/usr/bin/env python3
import argparse
import csv
import re
import subprocess
from pathlib import Path
from typing import TypedDict

FRAME_LENGTH = re.compile(r"ethertype IPv4 \(0x0800\), length (\d+):")
PAYLOAD_LENGTH = re.compile(r"length (\d+)$")
TCP_SEQUENCE = re.compile(
    r"IP ([^ ]+) > ([^:]+): Flags \[[^]]+\].* seq (\d+):(\d+),.*length (\d+)$"
)
FIELDS = (
    "capture",
    "protocol",
    "packet_count",
    "frame_bytes",
    "transport_payload_bytes",
    "link_network_transport_overhead_bytes",
    "repeated_tcp_segment_count",
    "repeated_tcp_payload_bytes",
)


class PacketSummary(TypedDict):
    capture: str
    protocol: str
    packet_count: int
    frame_bytes: int
    transport_payload_bytes: int
    link_network_transport_overhead_bytes: int
    repeated_tcp_segment_count: int
    repeated_tcp_payload_bytes: int


def _read_packets(path: Path) -> list[str]:
    completed = subprocess.run(
        ["tcpdump", "-tt", "-nn", "-S", "-e", "-r", str(path)],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.splitlines()


def _protocol(line: str) -> str | None:
    if " UDP," in line and (".5353 " in line or ".5353:" in line):
        return "mdns_udp_5353"
    if " UDP," in line and (".9000 " in line or ".9000:" in line):
        return "gossip_udp_9000"
    if " Flags [" in line and (".8000 " in line or ".8000:" in line):
        return "node_http_tcp_8000"
    return None


def _summarize(path: Path) -> list[PacketSummary]:
    summaries: dict[str, PacketSummary] = {
        protocol: {
            "capture": str(path),
            "protocol": protocol,
            "packet_count": 0,
            "frame_bytes": 0,
            "transport_payload_bytes": 0,
            "link_network_transport_overhead_bytes": 0,
            "repeated_tcp_segment_count": 0,
            "repeated_tcp_payload_bytes": 0,
        }
        for protocol in ("mdns_udp_5353", "gossip_udp_9000", "node_http_tcp_8000")
    }
    seen_tcp_segments = set()
    for line in _read_packets(path):
        protocol = _protocol(line)
        if protocol is None:
            continue
        frame_match = FRAME_LENGTH.search(line)
        payload_match = PAYLOAD_LENGTH.search(line)
        if frame_match is None or payload_match is None:
            continue
        frame_bytes = int(frame_match.group(1))
        payload_bytes = int(payload_match.group(1))
        summary = summaries[protocol]
        summary["packet_count"] += 1
        summary["frame_bytes"] += frame_bytes
        summary["transport_payload_bytes"] += payload_bytes
        summary["link_network_transport_overhead_bytes"] += max(
            frame_bytes - payload_bytes, 0
        )

        if protocol != "node_http_tcp_8000" or payload_bytes == 0:
            continue
        sequence_match = TCP_SEQUENCE.search(line)
        if sequence_match is None:
            continue
        source, destination, start, end, _length = sequence_match.groups()
        segment = (source, destination, start, end)
        if segment in seen_tcp_segments:
            summary["repeated_tcp_segment_count"] += 1
            summary["repeated_tcp_payload_bytes"] += payload_bytes
        else:
            seen_tcp_segments.add(segment)
    return list(summaries.values())


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarize reviewer packet captures using tcpdump."
    )
    parser.add_argument("captures", nargs="+", type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    rows = []
    for capture in args.captures:
        rows.extend(_summarize(capture))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
