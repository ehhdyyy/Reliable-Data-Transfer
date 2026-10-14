"""
PA2 -- Reliable Data Transfer Simulation
gbn.py -- STUDENT FILE

Go-Back-N sender and receiver. This file is incomplete in two places:
one bug for you to find and fix, and one feature left for you to
implement (fast retransmit, marked with a TODO below). See the
assignment brief for what's expected at each step.

WINDOW is the sender's window size. NUM_SEQNUMS (from run_sim.py) is
the sequence number modulus.
"""

from protocols.base import DeliverMixin
from run_sim import NUM_SEQNUMS

WINDOW = 4
DUP_ACK_THRESHOLD = 3


def _in_window(seq, base, count):
    """True if seq is one of the next `count` sequence numbers starting at base (mod N)."""
    return (seq - base) % NUM_SEQNUMS < count


class GBNSender:
    def __init__(self, timer, data_channel, receiver_callback):
        self.timer = timer
        self.data_channel = data_channel
        self.receiver_callback = receiver_callback

        self.base = 0
        self.nextseqnum = 0
        self.sent_packets = {}  # seqnum -> Packet, for resending

        # --- fast retransmit state ---
        # Initialized to base-1 (mod N) so the first duplicate ACK the
        # receiver could ever send is still recognized correctly.
        self.last_ack_seen = (self.base - 1) % NUM_SEQNUMS
        self.dup_ack_count = 0

        self.logger = None  # set by run_sim via _attach_logger

    def _attach_logger(self, logger):
        self.logger = logger

    def has_room(self):
        return (self.nextseqnum - self.base) % NUM_SEQNUMS < WINDOW

    def rdt_send(self, data, current_tick):
        from engine import Packet
        packet = Packet(self.nextseqnum, data=data, is_ack=False)
        self.sent_packets[self.nextseqnum] = packet
        self.data_channel.send(packet, current_tick, self.receiver_callback)

        if self.base == self.nextseqnum:
            self.timer.start(current_tick)

        self.nextseqnum = (self.nextseqnum + 1) % NUM_SEQNUMS

    def timeout(self, current_tick):
        packet = self.sent_packets.get(self.base)
        if packet is not None:
            for(seqnum, packet) in self.sent_packets.items():
                self.logger.log("DEBUG", "sender", f"resending seq={seqnum}")
                self.data_channel.send(packet.copy(), current_tick, self.receiver_callback)
        self.timer.start(current_tick)

    def rdt_rcv(self, packet, current_tick):
        if packet.is_corrupt():
            return  # corrupt ACKs are silently ignored, same as a lost ACK

        acknum = packet.acknum

        if _advances_base(acknum, self.base, self.nextseqnum):
            # New cumulative progress: slide the window forward.
            self.base = (acknum + 1) % NUM_SEQNUMS
            self.last_ack_seen = acknum
            self.dup_ack_count = 0

            for seq in list(self.sent_packets.keys()):
                if not _in_window(seq, self.base, (self.nextseqnum - self.base) % NUM_SEQNUMS):
                    del self.sent_packets[seq]

            if self.base == self.nextseqnum:
                self.timer.stop()
            else:
                self.timer.start(current_tick)
        else:
            # Duplicate ACK -- same cumulative ack as before.
            if acknum == self.last_ack_seen:
                self.dup_ack_count += 1

                # TODO (fast retransmit): implement this using
                # self.dup_ack_count, self.last_ack_seen, and
                # self._log_fast_retransmit(). Reset self.dup_ack_count
                # to 0 once you trigger it. See the assignment brief.
                if self.dup_ack_count >= DUP_ACK_THRESHOLD:
                    retransmit_packet = self.sent_packets.get(self.base)
                    if retransmit_packet is not None:
                        self._log_fast_retransmit(self.base)
                        self.data_channel.send(retransmit_packet.copy(), current_tick, self.receiver_callback)
                    self.dup_ack_count = 0

    def get_timers(self):
        return {None: self.timer}

    def _log_fast_retransmit(self, seqnum):
        if self.logger is not None:
            self.logger.log("FAST_RETRANS", "sender", f"resending seq={seqnum}")


def _advances_base(acknum, base, nextseqnum):
    """True if acknum is >= base and < nextseqnum in modular window order,
    i.e. it acknowledges at least one currently-outstanding packet."""
    outstanding = (nextseqnum - base) % NUM_SEQNUMS
    return _in_window(acknum, base, outstanding)


class GBNReceiver(DeliverMixin):
    def __init__(self, ack_channel, sender_callback):
        self.ack_channel = ack_channel
        self.sender_callback = sender_callback
        self.expected_seqnum = 0

    def rdt_rcv(self, packet, current_tick):
        from engine import Packet

        if (not packet.is_corrupt()) and packet.seqnum == self.expected_seqnum:
            self.deliver(packet.data)
            ack = Packet(self.expected_seqnum, is_ack=True, acknum=self.expected_seqnum)
            self.expected_seqnum = (self.expected_seqnum + 1) % NUM_SEQNUMS
            self.ack_channel.send(ack, current_tick, self.sender_callback)
        else:
            # Corrupt, or out of order: re-ACK the last correctly received packet.
            last_ok = (self.expected_seqnum - 1) % NUM_SEQNUMS
            ack = Packet(last_ok, is_ack=True, acknum=last_ok)
            self.ack_channel.send(ack, current_tick, self.sender_callback)
