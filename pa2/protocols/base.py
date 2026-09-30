"""
PA2 -- Reliable Data Transfer Simulation
protocols/base.py -- GIVEN, do not modify.

This file documents the interface every Sender/Receiver must implement
(not enforced via ABC -- just follow it), and provides DeliverMixin, a
small real mixin that Receivers inherit from to hand data up to the
application layer and log the delivery.

SENDER CONTRACT
---------------
__init__(self, timer, data_channel, receiver_callback)
    timer              : a Timer instance (only meaningful for single-timer
                          protocols; per-packet-timer protocols such as SR
                          manage their own dict of Timers instead and may
                          ignore this argument)
    data_channel       : Channel to send DATA packets on
    receiver_callback  : function(packet, current_tick) -> None, used to
                          hand a packet directly to the receiver's rdt_rcv
                          (the data_channel actually does the delivery;
                          this callback is what the channel calls)

has_room(self) -> bool
    Whether the sender's window has space for another rdt_send() call.

rdt_send(self, data, current_tick) -> None
    Called by the simulator when the application layer has data to send.
    Only called when has_room() is True.

rdt_rcv(self, packet, current_tick) -> None
    Called when an ACK packet arrives at the sender.

timeout(self, current_tick) -> None
    Called by the simulator when a single shared timer (key=None from
    get_timers()) fires.

timeout_for(self, key, current_tick) -> None
    Called by the simulator when a per-key timer (key=seqnum) fires.
    Only needed for protocols with per-packet timers (e.g. SR).

get_timers(self) -> dict
    Return {key: Timer}. Use {None: self.timer} for a single shared timer
    (rdt3.0, GBN). Use {seqnum: timer, ...} for per-packet timers (SR).

RECEIVER CONTRACT
-----------------
__init__(self, ack_channel, sender_callback)
    ack_channel      : Channel to send ACK packets on
    sender_callback  : function(packet, current_tick) -> None, used to hand
                        an ACK to the sender's rdt_rcv via the ack_channel

rdt_rcv(self, packet, current_tick) -> None
    Called when a DATA packet arrives at the receiver.

Receivers inherit from DeliverMixin below to deliver in-order data to the
application layer (delivered_sink) and log an APP_RECEIVE trace event.
"""


class DeliverMixin:
    def _init_delivery(self, delivered_sink, logger):
        self._delivered_sink = delivered_sink
        self._logger = logger

    def deliver(self, data):
        self._delivered_sink.append(data)
        self._logger.log("APP_RECEIVE", "app", f"data={data!r}")
