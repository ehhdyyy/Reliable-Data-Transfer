"""
PA2 -- Reliable Data Transfer Simulation
run_sim.py -- GIVEN, do not modify, EXCEPT for the STUDENT_NAME line below.

CLI harness that wires a protocol's Sender/Receiver together with the
Channels and Simulator, runs a scenario, and prints (or saves) the trace.
The fixed scenario scripts live right below (SCENARIOS), so this is the
only file you need to open to see what any given scenario does.

Usage:
    python run_sim.py --protocol gbn --scenario 5
    python run_sim.py --protocol gbn --scenario 8 --save mytrace.txt
    python run_sim.py --protocol gbn --scenario 5 --solution   (TA only)

To generate the single zip file you actually submit (every required
scenario for BOTH protocols, run against your own gbn.py and sr.py, and
bundled together into one pa2_<your_name>_submission.zip):
    python run_sim.py --make-submission
"""

import argparse
import importlib
import hashlib
import json
import os
import platform
import re
import sys
import zipfile
from datetime import datetime, timezone

from engine import TraceLogger, Channel, Simulator, Timer

# ---------------------------------------------------------------------------
# Fill in your name below before running --make-submission. This is the
# ONE line in this file you are expected to edit -- everything else in
# run_sim.py stays as given. Your name is embedded in the submission
# manifest and used to name your submission zip, so the TA can tell whose
# submission she's looking at without opening it.
# ---------------------------------------------------------------------------
STUDENT_NAME = "YOUR_NAME_HERE"

# ---------------------------------------------------------------------------
# Scenarios: each is a fixed, deterministic script describing what happens
# to specific packets in transit. Scripts are keyed by "the Nth packet sent
# on this channel", 1-indexed, so behavior doesn't depend on protocol
# internals -- only on how many packets that protocol happens to put on
# the wire.
#
# NUM_SEQNUMS is the modulus for sequence numbers (0..NUM_SEQNUMS-1). It
# matters most for Selective Repeat, where the usable window must be at
# most NUM_SEQNUMS / 2 to avoid ambiguity -- see scenario 7.
# ---------------------------------------------------------------------------

NUM_SEQNUMS = 8

SCENARIOS = {
    1: {
        "name": "Clean channel",
        "description": "No loss, no corruption. Baseline sanity check.",
        "data_script": {},
        "ack_script": {},
        "messages": ["m0", "m1", "m2", "m3"],
    },
    2: {
        "name": "Single data packet dropped",
        "description": "The 2nd DATA packet sent is dropped; sender must time out and resend.",
        "data_script": {2: "drop"},
        "ack_script": {},
        "messages": ["m0", "m1", "m2", "m3"],
    },
    3: {
        "name": "Single ACK dropped",
        "description": "The 2nd ACK sent is dropped; sender must time out even though the "
                        "receiver got the data (watch for duplicate delivery at the receiver).",
        "data_script": {},
        "ack_script": {2: "drop"},
        "messages": ["m0", "m1", "m2", "m3"],
    },
    4: {
        "name": "Single ACK corrupted",
        "description": "The 2nd ACK sent is corrupted in transit.",
        "data_script": {},
        "ack_script": {2: "corrupt"},
        "messages": ["m0", "m1", "m2", "m3"],
    },
    5: {
        "name": "Multi-packet-outstanding single drop",
        "description": "With several packets outstanding in the window, the 3rd DATA packet "
                        "is dropped. Exposes whether timeout() correctly resends the WHOLE "
                        "window (GBN) rather than just the earliest packet.",
        "data_script": {3: "drop"},
        "ack_script": {},
        "messages": ["m0", "m1", "m2", "m3", "m4", "m5"],
    },
    6: {
        "name": "Burst loss near window boundary",
        "description": "Two consecutive DATA packets near a window-size boundary are dropped.",
        "data_script": {4: "drop", 5: "drop"},
        "ack_script": {},
        "messages": ["m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7"],
    },
    7: {
        "name": "SR half-window-boundary probe",
        "description": "Designed for Selective Repeat with NUM_SEQNUMS=8: sends enough "
                        "packets to wrap the sequence space. If SR's window size is not "
                        "kept at <= NUM_SEQNUMS/2, an old retransmission and a new packet "
                        "can carry the same sequence number and be confused. The 1st DATA "
                        "packet is dropped to force a retransmission after wraparound begins.",
        "data_script": {1: "drop"},
        "ack_script": {},
        "messages": ["m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9"],
    },
    8: {
        "name": "Fast retransmit probe (GBN only)",
        "description": "The 1st DATA packet (seq0) is dropped while a full window's worth "
                        "of later packets (seq1..seq3) still arrive at the receiver. Each "
                        "one arrives out of order and should trigger a duplicate ACK for "
                        "seq -1 (the last in-order packet). Three duplicate ACKs arrive well "
                        "before the base packet's normal timeout, so a correct fast-retransmit "
                        "implementation resends seq0 early. Without fast retransmit, the base "
                        "packet only gets resent once the ordinary timer fires.",
        "data_script": {1: "drop"},
        "ack_script": {},
        "messages": ["m0", "m1", "m2", "m3", "m4", "m5"],
    },
    9: {
        "name": "SR out-of-order buffering probe",
        "description": "The 1st DATA packet (seq0) is dropped while seq1..seq3 still arrive "
                        "and get ACKed individually -- the defining SR behavior. A correct "
                        "receiver buffers seq1-3 and delivers everything once seq0 is "
                        "resent. If out-of-order packets are discarded instead of buffered, "
                        "those messages are gone for good even though the sender was told "
                        "they arrived: delivered output will be missing messages entirely, "
                        "not just delayed.",
        "data_script": {1: "drop"},
        "ack_script": {},
        "messages": ["m0", "m1", "m2", "m3"],
    },
}

REQUIRED_SCENARIOS = {
    "gbn": [1, 5, 8, 9],
    "sr": [1, 9],
}


def load_protocol(name, use_solution):
    module_name = f"{name}_SOLUTION" if use_solution else name
    return importlib.import_module(module_name)


def run_scenario(protocol, scenario_id, use_solution=False):
    """
    Runs one scenario for one protocol and returns
    (logger, delivered, scenario_dict) -- everything needed to print,
    save, or bundle the result. Shared by the normal CLI path and
    --make-submission so there's exactly one place this wiring happens.
    """
    scenario = SCENARIOS[scenario_id]
    module = load_protocol(protocol, use_solution)

    logger = TraceLogger()
    data_channel = Channel("data", scenario["data_script"], logger)
    ack_channel = Channel("ack", scenario["ack_script"], logger)
    delivered = []

    # Forward-reference pattern: sender and receiver each need the other's
    # rdt_rcv as a callback before both objects exist, so we close over a
    # mutable box and fill it in after construction.
    box = {"sender": None, "receiver": None}

    def to_receiver(packet, current_tick):
        box["receiver"].rdt_rcv(packet, current_tick)

    def to_sender(packet, current_tick):
        box["sender"].rdt_rcv(packet, current_tick)

    SenderClass = getattr(module, f"{protocol.upper()}Sender") if protocol != "rdt3" \
        else getattr(module, "RDT3Sender")
    ReceiverClass = getattr(module, f"{protocol.upper()}Receiver") if protocol != "rdt3" \
        else getattr(module, "RDT3Receiver")

    timer = Timer()
    sender = SenderClass(timer, data_channel, to_receiver)
    receiver = ReceiverClass(ack_channel, to_sender)
    receiver._init_delivery(delivered, logger)
    if hasattr(sender, "_attach_logger"):
        sender._attach_logger(logger)
    box["sender"] = sender
    box["receiver"] = receiver

    sim = Simulator(logger)
    sim.run(sender, receiver, scenario["messages"], data_channel, ack_channel, delivered)

    return logger, delivered, scenario


def _run_protocol_scenarios(protocol):
    """Runs every required scenario for one protocol against the student's
    own module and returns a list of per-scenario result dicts."""
    results = []
    for scenario_id in REQUIRED_SCENARIOS[protocol]:
        print(f"Running {protocol} scenario {scenario_id}...")
        logger, delivered, scenario = run_scenario(protocol, scenario_id, use_solution=False)
        results.append({
            "protocol": protocol,
            "scenario": scenario_id,
            "name": scenario["name"],
            "trace_text": logger.render(),
            "sent": scenario["messages"],
            "delivered": delivered,
            "match": delivered == scenario["messages"],
            "summary_counts": logger.summary_counts(),
        })
    return results


def _sanitize_name(name):
    """Turns a free-typed name into something safe to put in a filename:
    spaces become underscores, anything that isn't alphanumeric, underscore
    or hyphen is dropped."""
    name = name.strip().replace(" ", "_")
    return re.sub(r"[^A-Za-z0-9_-]", "", name)


def make_submission():
    """
    Runs every required scenario for BOTH protocols against the student's
    own gbn.py and sr.py, and bundles both source files plus every
    resulting trace into one pa2_<student_name>_submission.zip.

    The manifest's bundle_sha256 covers ONLY the trace text (not the
    source code) -- source files sit in the bundle for the TA to read
    alongside your written narratives, but are never part of the
    automated correctness check, so editing a comment in your code after
    generating the zip does not require regenerating it. If you change
    your actual protocol logic, re-run this command to produce a fresh
    zip with matching traces.
    """
    if not STUDENT_NAME or not STUDENT_NAME.strip() or STUDENT_NAME.strip() == "YOUR_NAME_HERE":
        print("ERROR: STUDENT_NAME is not set. Open run_sim.py, find STUDENT_NAME near the")
        print("       top of the file, and fill in your name before generating a submission.")
        sys.exit(1)

    sanitized_name = _sanitize_name(STUDENT_NAME)
    if not sanitized_name:
        print(f"ERROR: STUDENT_NAME ({STUDENT_NAME!r}) doesn't contain any usable characters "
              f"for a filename. Use your normal name, e.g. \"Jane Doe\".")
        sys.exit(1)

    sources = {}
    for protocol in ("gbn", "sr"):
        source_path = f"{protocol}.py"
        if not os.path.exists(source_path):
            print(f"ERROR: {source_path} not found in the current directory.")
            sys.exit(1)
        with open(source_path, "rb") as f:
            sources[protocol] = f.read()

    all_results = []
    for protocol in ("gbn", "sr"):
        all_results.extend(_run_protocol_scenarios(protocol))

    # Hash covers trace text only, in a fixed (protocol, scenario) order --
    # NOT the source code. See docstring above for why.
    bundle_hasher = hashlib.sha256()
    for r in all_results:
        bundle_hasher.update(f"{r['protocol']}:{r['scenario']}".encode())
        bundle_hasher.update(r["trace_text"].encode())
    bundle_hash = bundle_hasher.hexdigest()

    manifest = {
        "student_name": STUDENT_NAME.strip(),
        "bundle_sha256": bundle_hash,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": platform.python_version(),
        "protocols_included": ["gbn", "sr"],
        "results": [
            {k: r[k] for k in ("protocol", "scenario", "name", "sent", "delivered", "match", "summary_counts")}
            for r in all_results
        ],
    }

    out_path = f"pa2_{sanitized_name}_submission.zip"
    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest, indent=2))
        for protocol, source_bytes in sources.items():
            zf.writestr(f"source/{protocol}.py", source_bytes)
        for r in all_results:
            zf.writestr(f"traces/{r['protocol']}_scenario_{r['scenario']}.txt", r["trace_text"] + "\n")

    print(f"\nWrote {out_path}")
    all_match = all(r["match"] for r in all_results)
    print(f"All scenarios delivered correctly: {all_match}")
    if not all_match:
        print("NOTE: at least one scenario did not fully deliver -- that's fine to submit")
        print("      if you're still debugging, but make sure it's not the version you")
        print("      intend as your final answer.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--protocol", choices=["rdt3", "gbn", "sr"])
    parser.add_argument("--scenario", type=int, choices=sorted(SCENARIOS.keys()))
    parser.add_argument("--solution", action="store_true", help="TA only: use the *_SOLUTION module")
    parser.add_argument("--save", default=None, help="path to save the rendered trace")
    parser.add_argument("--make-submission", action="store_true",
                         help="build pa2_<student_name>_submission.zip (both protocols) instead of a single run")
    args = parser.parse_args()

    if args.make_submission:
        make_submission()
        return

    if not args.protocol or args.scenario is None:
        parser.error("--protocol and --scenario are required unless using --make-submission")

    scenario = SCENARIOS[args.scenario]
    print(f"Protocol : {args.protocol}{' (SOLUTION)' if args.solution else ''}")
    print(f"Scenario : {args.scenario} -- {scenario['name']}")
    print(f"           {scenario['description']}")
    print()

    logger, delivered, scenario = run_scenario(args.protocol, args.scenario, use_solution=args.solution)

    print(logger.render())
    print()
    print(f"Sent messages     : {scenario['messages']}")
    print(f"Delivered messages: {delivered}")
    print(f"Match             : {delivered == scenario['messages']}")
    print(f"Event summary     : {logger.summary_counts()}")

    if args.save:
        logger.save(args.save)
        print(f"\nTrace saved to {args.save}")


if __name__ == "__main__":
    main()
