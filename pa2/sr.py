"""
PA2 -- Reliable Data Transfer Simulation
sr.py -- STUDENT FILE

Selective Repeat sender and receiver. This file is incomplete in one
place -- see the assignment brief for what's expected.

WINDOW is intentionally NUM_SEQNUMS // 2, not NUM_SEQNUMS. See the
assignment brief's reasoning question if you want to think through why.
"""

from protocols.base import DeliverMixin
from run_sim import NUM_SEQNUMS
from engine import Packet, Timer

WINDOW = NUM_SEQNUMS // 2


def _in_window(seq, base, count):
    return (seq - base) % NUM_SEQNUMS < count


class SRSender:
    def __init__(self, timer, data_channel, receiver_callback):
        # `timer` (a single Timer) is passed for interface consistency with
        # GBN/rdt3, but SR needs one timer PER outstanding packet, so it's
        # unused here -- self.timers below is what actually matters.
        self.data_channel = data_channel
        self.receiver_callback = receiver_callback

        self.base = 0
        self.nextseqnum = 0
        self.sent_packets = {}   # seqnum -> Packet
        self.timers = {}         # seqnum -> Timer, only while outstanding
        self.logger = None

    def _attach_logger(self, logger):
        self.logger = logger

    def has_room(self):
        return (self.nextseqnum - self.base) % NUM_SEQNUMS < WINDOW

    def rdt_send(self, data, current_tick):
        seq = self.nextseqnum
        packet = Packet(seq, data=data, is_ack=False)
        self.sent_packets[seq] = packet
        self.timers[seq] = Timer()
        self.timers[seq].start(current_tick)
        self.data_channel.send(packet, current_tick, self.receiver_callback)
        self.nextseqnum = (self.nextseqnum + 1) % NUM_SEQNUMS

    def rdt_rcv(self, packet, current_tick):
        if packet.is_corrupt():
            return

        acknum = packet.acknum
        if acknum not in self.timers:
            return  # already acked once, or outside our outstanding set -- ignore

        self.timers[acknum].stop()
        del self.timers[acknum]
        del self.sent_packets[acknum]

        # Slide base forward past any run of already-acked sequence numbers.
        while self.base != self.nextseqnum and self.base not in self.sent_packets:
            self.base = (self.base + 1) % NUM_SEQNUMS

    def timeout_for(self, seqnum, current_tick):
        # Unlike GBN, only the ONE packet whose timer fired gets resent --
        # every other outstanding packet keeps its own timer running.
        packet = self.sent_packets.get(seqnum)
        if packet is not None:
            self.data_channel.send(packet.copy(), current_tick, self.receiver_callback)
            self.timers[seqnum].start(current_tick)

    def get_timers(self):
        return self.timers


class SRReceiver(DeliverMixin):
    def __init__(self, ack_channel, sender_callback):
        self.ack_channel = ack_channel
        self.sender_callback = sender_callback
        self.base = 0
        self.buffer = {}  # seqnum -> data, for packets received out of order

    def rdt_rcv(self, packet, current_tick):
        if packet.is_corrupt():
            return  # SR sends no ACK for corrupt packets, same as a drop

        seq = packet.seqnum
        if not _in_window(seq, self.base, WINDOW):
            return  # outside our receive window -- not something we can use

        # SR always ACKs individually, in or out of order.
        ack = Packet(seq, is_ack=True, acknum=seq)
        self.ack_channel.send(ack, current_tick, self.sender_callback)
        self.buffer[seq] = packet.data
        self._logger.log("DEBUG", "receiver", f"buffer= {self.buffer}")
        if seq == self.base:
            self.deliver(self.buffer.pop(self.base))
            self._logger.log("DEBUG", "receiver", f"buffer= {self.buffer}")
            self.base = (self.base + 1) % NUM_SEQNUMS
            while self.base in self.buffer:
                self.deliver(self.buffer.pop(self.base))
                self._logger.log("DEBUG", "receiver", f"buffer= {self.buffer}")
                self.base = (self.base + 1) % NUM_SEQNUMS
        else:
            pass
