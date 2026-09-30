"""
PA2 -- Reliable Data Transfer Simulation
engine.py -- GIVEN, do not modify.

Everything the simulation runs on, in one file so you only have to look
in one place: Packet, TraceLogger, Channel, Timer, Simulator.

  Packet       -- a DATA or ACK packet with a checksum for corruption checks
  TraceLogger  -- records every event with a line number and logical tick
  Channel      -- a deterministic, scripted unreliable link (drop/corrupt)
  Timer        -- a countdown timer keyed to the simulator's logical clock
  Simulator    -- the main discrete-event loop

Read protocols/base.py for the Sender/Receiver interface these pieces
expect. You won't need to edit anything in this file.
"""


# ---------------------------------------------------------------------------
# Packet
# ---------------------------------------------------------------------------

class Packet:
    def __init__(self, seqnum, data="", is_ack=False, acknum=None):
        self.seqnum = seqnum
        self.acknum = acknum if acknum is not None else seqnum
        self.data = data
        self.is_ack = is_ack
        self.checksum = self._compute_checksum()

    def _compute_checksum(self):
        s = f"{self.seqnum}{self.acknum}{self.data}{self.is_ack}"
        return sum(ord(c) for c in s) % 65536

    def is_corrupt(self):
        return self.checksum != self._compute_checksum()

    def corrupt(self):
        """Used only by Channel to simulate corruption in transit."""
        self.data = self.data + "\x00CORRUPT"

    def copy(self):
        """Return an identical packet (used for unmodified resends)."""
        p = Packet(self.seqnum, self.data, self.is_ack, self.acknum)
        p.checksum = self.checksum
        return p

    def __repr__(self):
        kind = "ACK" if self.is_ack else "DATA"
        return f"Packet({kind} seq={self.seqnum} ack={self.acknum} data={self.data!r})"


# ---------------------------------------------------------------------------
# TraceLogger
# ---------------------------------------------------------------------------

class TraceLogger:
    """
    Records every protocol event with a line number and a logical tick, so
    traces are deterministic and diffable. Cite line numbers from render()
    output in your write-up.
    """

    def __init__(self):
        self.tick_count = 0
        self.entries = []  # list of (line_no, tick, event_type, actor, detail)

    def tick(self):
        self.tick_count += 1
        return self.tick_count

    def log(self, event_type, actor, detail=""):
        line_no = len(self.entries) + 1
        self.entries.append((line_no, self.tick_count, event_type, actor, detail))
        return line_no

    def render(self):
        lines = []
        for line_no, t, event_type, actor, detail in self.entries:
            lines.append(f"[{line_no}] t={t} {event_type} ({actor}) {detail}")
        return "\n".join(lines)

    def save(self, path):
        with open(path, "w") as f:
            f.write(self.render() + "\n")

    def summary_counts(self):
        counts = {}
        for _, _, event_type, _, _ in self.entries:
            counts[event_type] = counts.get(event_type, 0) + 1
        return counts


# ---------------------------------------------------------------------------
# Channel
# ---------------------------------------------------------------------------

PROP_DELAY = 2  # ticks a packet takes to arrive after being sent, if not dropped


class Channel:
    """
    A deterministic, scripted unreliable link. Instead of a random seed,
    each channel is given a fixed script saying what happens to the Nth
    packet sent on THIS channel: "drop", "corrupt", or nothing. Every run
    is byte-for-byte reproducible, which is what makes trace-diffing
    possible.
    """

    # Which endpoint is acting for a send/receive on this channel. The data
    # channel runs sender->receiver; the ack channel runs receiver->sender.
    _SEND_ACTOR = {"data": "sender", "ack": "receiver"}
    _RECEIVE_ACTOR = {"data": "receiver", "ack": "sender"}

    def __init__(self, name, script, logger):
        self.name = name
        self.script = script or {}
        self.logger = logger
        self.sent_count = 0
        self.in_flight = []  # list of (arrival_tick, packet, deliver_to)

    def send(self, packet, current_tick, deliver_to):
        """
        Send packet onto the channel. deliver_to is called later as
        deliver_to(packet, arrival_tick) once the packet arrives, unless
        it was dropped.
        """
        self.sent_count += 1
        action = self.script.get(self.sent_count)

        if action == "drop":
            self.logger.log("DROP", "sim", f"{packet} (send #{self.sent_count})")
            return

        if action == "corrupt":
            packet = packet.copy()
            packet.corrupt()
            self.logger.log("CORRUPT", "sim", f"{packet} (send #{self.sent_count})")

        arrival_tick = current_tick + PROP_DELAY
        self.logger.log("TRANSPORT_SEND", self._SEND_ACTOR[self.name], f"{packet} arrives t={arrival_tick}")
        self.in_flight.append((arrival_tick, packet, deliver_to))

    def deliver_ready(self, current_tick):
        """Called once per tick by the Simulator to deliver arrived packets."""
        still_in_flight = []
        for arrival_tick, packet, deliver_to in self.in_flight:
            if arrival_tick <= current_tick:
                self.logger.log("TRANSPORT_RECEIVE", self._RECEIVE_ACTOR[self.name], f"{packet}")
                deliver_to(packet, current_tick)
            else:
                still_in_flight.append((arrival_tick, packet, deliver_to))
        self.in_flight = still_in_flight


# ---------------------------------------------------------------------------
# Timer + Simulator
# ---------------------------------------------------------------------------

TIMEOUT_DURATION = 10
MAX_TICKS = 500  # safety cap so a broken protocol can't loop forever


class Timer:
    def __init__(self, duration=TIMEOUT_DURATION):
        self.duration = duration
        self.deadline = None  # None = not running

    def start(self, current_tick):
        self.deadline = current_tick + self.duration

    def stop(self):
        self.deadline = None

    def running(self):
        return self.deadline is not None

    def due(self, current_tick):
        return self.deadline is not None and current_tick >= self.deadline


class Simulator:
    """
    The main discrete-event loop. Each tick it:
      1. Feeds the next message to the sender (if it has room)
      2. Delivers any packets whose propagation delay has elapsed
      3. Checks every one of the sender's timers for expiry and fires
         timeout() (or timeout_for()) accordingly
    """

    def __init__(self, logger):
        self.logger = logger

    def run(self, sender, receiver, messages, data_channel, ack_channel, delivered_sink):
        """
        messages : list of str, application-layer data to send in order
        Returns nothing; results live in the logger's trace and in
        delivered_sink (list of data delivered by the receiver, in the
        order it happened -- compare against `messages` for correctness).
        """
        pending = list(messages)
        current_tick = 0

        while current_tick < MAX_TICKS:
            current_tick = self.logger.tick()

            # 1. App layer feeds sender while there's room and data left
            while pending and sender.has_room():
                data = pending.pop(0)
                self.logger.log("APP_SEND", "app", f"data={data!r}")
                sender.rdt_send(data, current_tick)

            # 2. Deliver anything that has arrived on either channel
            data_channel.deliver_ready(current_tick)
            ack_channel.deliver_ready(current_tick)

            # 3. Check all of the sender's timers
            timers = sender.get_timers()
            for key, timer in timers.items():
                if timer.due(current_tick):
                    if key is None:
                        self.logger.log("TIMEOUT", "sender", f"t={current_tick}")
                        sender.timeout(current_tick)
                    else:
                        self.logger.log("TIMEOUT", "sender", f"key={key} t={current_tick}")
                        sender.timeout_for(key, current_tick)

            # Stop condition: nothing left to send, no packets in flight,
            # no timers running
            no_more_work = (
                not pending
                and not data_channel.in_flight
                and not ack_channel.in_flight
                and not any(t.running() for t in sender.get_timers().values())
            )
            if no_more_work:
                break

        self.logger.log("DONE", "sim", f"delivered={len(delivered_sink)} ticks={current_tick}")
