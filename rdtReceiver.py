# Name: <your full name> (<your student number>), <partner> (<their number>)
"""The receiving half of the protocol: Go-Back-N and Selective Repeat.

Go-Back-N keeps a receive window of one and buffers nothing. Selective Repeat
buffers out-of-order segments and acknowledges each one individually, and must
acknowledge again any segment it has already delivered.

Everything below the GIVEN line is written for you, including the argument
parser, the registration datagram and the writing of the output file.
Implement the class above it.
"""

import argparse
import socket
import sys
import threading
import time

import config
import segment
from segment import RDTSegment, RdtBuffer


class Receiver:
    """One direction of a connection, from the channel into the application.

    Two threads. The network thread you start in start() owns the socket and
    decides what to do with each arriving segment; the application thread calls
    receive() and blocks until there is something in order to hand it.
    """

    def __init__(self, sock, channelAddr, protocol):
        """Set up the receive buffer.

        One segment.RdtBuffer of config.RCV_BUF slots holds everything. A
        segment enters it at the index its sequence number names and leaves it
        when the application reads that index, so in-order, exactly-once
        delivery comes out of the buffer rather than out of code you write, and
        the space left in it is what rcvWin advertises.

        Two indices matter and they are not the same:

            buf.base          the next index the APPLICATION has not read
            buf.firstEmpty()  the next index YOU have not received  (rcvBase)

        A slow application leaves segments sitting between them. Compare an
        arriving segment against firstEmpty(), never against base.

        Selective Repeat also needs the width of its receive window, which is
        config.WINDOW until task 5 and segment.checkWindow(protocol) afterward.
        Go-Back-N never needs it: its receive window is one segment.
        """
        self.sock = sock
        self.channelAddr = channelAddr
        self.protocol = protocol
        self.buffer = segment.RdtBuffer(config.RCV_BUF)
        self.running = False
        self.network_thread = None
        self.lock = threading.Lock()
        self.finReceived = threading.Event()

    def start(self):
        """Start the network thread.

        One loop: read a datagram, validate() it, and decide. Compare seqNum
        against firstEmpty() and branch on the protocol -- a receive window of
        one for Go-Back-N, of N for Selective Repeat. Every
        branch either acknowledges or deliberately says nothing, and which is
        which is the protocol; see each task for the rules.

        Task 5 adds one hazard worth knowing about now. When a gap is filled,
        several buffered segments become deliverable at once, and putSeqNum can
        block if the application has not kept up -- on this thread, which then
        stops reading the socket. That is not a bug to fix here; it is what
        flow control in task 3 exists to prevent. Keep the socket call and the
        delivery outside the buffer's Condition and it stays survivable.
        """
        
        def sendAck(ackNum, is_fin = False):
            # calculate how many empty slots are available in receiver's buffer
            rcvWin = self.buffer.freeSlots()
            flags = segment.FLAG_ACK | (segment.FLAG_FIN if is_fin else 0)
            # create ack segment
            ack = RDTSegment(ackNum=ackNum, flags = flags, rcvWin=rcvWin)
            # send ack to the sender
            self.sock.sendto(ack.pack(), self.channelAddr)
            # retrieve next internal sequence index that the receiver expects to receive
            rcvBase = self.buffer.firstEmpty()
            segment.traceSend(ack, self.buffer.base, rcvBase, rcvWin)

        def network():
            # receiver continuously listens for incoming packets
            while True:
                # acquires lock before accessing shared variables
                with self.lock:
                    # Checks whether sender has been stopped
                    if not self.running:
                        break
                # attempt - receive UDP datagram - max 4096 bytes - returns raw data and address of sender
                try:
                    raw, addr = self.sock.recvfrom(4096)
                # no packet arrived before socket timeout
                except TimeoutError:
                    continue
                # socket-related operating system error
                except OSError:
                    break
                # if packet is empty - ignore and start listening again
                if not raw:
                    continue
                # if packet is not empty - validate packet
                error = RDTSegment.validate(raw)
                # error is found - invalid packet
                if error is not None:
                    # initialze packet id to -1
                    ident = -1
                    # if len of packet is at least as long as expected header
                    # prevents trying to unpack a packet that is too short
                    if len(raw) >= segment.HEADER_LEN:
                        # attempt extract packet fields
                        bad = RDTSegment.unpack(raw)
                        if(bad.isAck):
                            ident = bad.ackNum
                        else:
                            ident = bad.seqNum
                    if error == "checksum":
                        segment.traceCorrupt(ident)
                    else: 
                        segment.traceDrop(ident, error)
                    continue
                # no error found - unpack packet
                pkt = RDTSegment.unpack(raw)
                # firstEmpty(): retrieves next buffer position that has not yet received a segment
                # freeSlots(): calculate how many empty buffer slots are available
                rcvBase = self.buffer.firstEmpty()
                rcvWin = self.buffer.freeSlots()
                segment.traceRecv(pkt, self.buffer.base, rcvBase, rcvWin)

                # convert internal rcv index into seq num
                expectedSeq = segment.mask(rcvBase)
                # check incoming packet has expected seq num
                # check if the receive buffer is full
                if (pkt.seqNum == expectedSeq) and self.buffer.freeSlots() > 0:
                    # insert segment into rcv buffer at appropriate seq index
                    # send ack for successfull accept segment
                    if (self.buffer.putNext(pkt)):
                        if pkt.isFin:
                            self.finReceived.set()
                        sendAck(pkt.seqNum, is_fin=pkt.isFin)
                # handling unexpected seq numbers
                else:
                    segment.traceDrop(pkt.seqNum, "window")
                    # check if receiver already received at least one segment - prevent calculating acknowledgement for negative internal sequence index
                    # send ack for previuos sequence number
                    if rcvBase > 0:
                        sendAck(segment.mask(rcvBase-1), is_fin = self.finReceived.is_set())
        self.sock.settimeout(0.05)
        self.running = True
        self.network_thread = threading.Thread(target=network, daemon=True)
        self.network_thread.start()
                        
    def receive(self):
        """Return one segment's payload, in order, blocking until there is one.

        Return None once the sender has closed the connection and everything
        has been handed over. Each segment must be returned exactly once, ever,
        however many times it arrives.
        """
        # retrieve next segment from buffer in correct order
        seg = self.buffer.getNext()
        # if none: no segment available to return
        if seg is None:
            return None
        if seg.isFin:
            self.buffer.close()
            return None
        segment.traceDeliver(seg.seqNum, seg.length)
        return seg.payload

    def waitClosed(self):
        """Block until the connection has been taken down, and report whether
        it was taken down cleanly.

        After acknowledging a FIN, do not exit immediately: if that
        acknowledgement is lost the sender will ask again, and there is nobody
        left to answer. Wait a bounded time, answer any further FIN, and only
        then return.
        """
        # check for aborted / incomplete transfer -> no FIN received
        # distinguish a normal end of stream from a stalled transfer where the sender crashed and idle exit woke up-> has a FIN arrived
        if not self.finReceived.is_set():
            with self.lock: 
                self.running = False
            if self.network_thread is not None:
                self.network_thread.join(timeout=0.2)
            return False

        linger = time.monotonic() + min(2.0, max(0.5, 4.0 * config.RTO_INIT))
        while time.monotonic() < linger:
            time.sleep(0.01)

        with self.lock: 
            self.running = False
        if self.network_thread is not None:
            self.network_thread.join(timeout=0.2)
        return True


# ============================================================================
# GIVEN - do not modify anything below this line.
#
# Argument parsing, socket setup, registration with the channel and the writing
# of the output file, including the --slow pacing that fills the receive buffer.
# The grading script invokes exactly these options, and none of this is what the
# project is teaching, so it is handed to you rather than assigned.
# ============================================================================

def parseArgs(argv):
    ap = argparse.ArgumentParser(description="CMPT 371 Project 3 - RDT receiver")
    ap.add_argument("--channel", type=int, required=True,
                    help="the port channel.py printed for the receiver side")
    ap.add_argument("--out", required=True, help="write the received file here")
    ap.add_argument("--protocol", default="gbn", choices=("gbn", "sr"))
    ap.add_argument("--trace", default=None, help="write the event log here")
    ap.add_argument("--slow", type=int, default=0,
                    help="milliseconds the application spends on each segment")
    return ap.parse_args(argv)


def openSocket(channelPort):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((config.HOST, 0))
    # A generous receive buffer: a burst that arrives while this program is
    # busy elsewhere is otherwise dropped by the operating system, and looks
    # exactly like loss the channel never injected.
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
    # One empty datagram announces this end to the channel, which has no other
    # way to learn where to forward. Empty datagrams are never forwarded.
    sock.sendto(b"", (config.HOST, channelPort))
    return sock


IDLE_EXIT = 5.0  # seconds without a delivered segment before giving up


def application(receiver, outPath, slowMs, progress):
    """Drain delivered payloads into the output file, optionally slowly."""
    with open(outPath, "wb") as out:
        while True:
            payload = receiver.receive()
            if payload is None:
                return
            out.write(payload)
            out.flush()
            progress[0] = time.monotonic()
            if slowMs:
                time.sleep(slowMs / 1000.0)


def main(argv=None):
    args = parseArgs(argv if argv is not None else sys.argv[1:])
    if (not isinstance(segment.SEED, int) or isinstance(segment.SEED, bool)
            or segment.SEED <= 0):
        sys.exit("set SEED at the top of segment.py to your Canvas group number")
    segment.traceOpen(args.trace)

    sock = openSocket(args.channel)
    receiver = Receiver(sock, (config.HOST, args.channel), args.protocol)
    receiver.start()

    # The application runs on its own thread so that this one can notice a
    # stream that has stopped arriving. Graceful teardown is task 4; until it is
    # written, nothing ever makes receive() return None and this program would
    # otherwise hang after the last segment, failing the earlier tasks as well.
    # Whatever was delivered has already been written, and the grading script
    # compares the file, so exiting here loses nothing that was received.
    progress = [time.monotonic()]
    worker = threading.Thread(target=application,
                              args=(receiver, args.out, args.slow, progress),
                              daemon=True)
    worker.start()
    while worker.is_alive():
        worker.join(timeout=0.25)
        if time.monotonic() - progress[0] > IDLE_EXIT:
            break
    ok = receiver.waitClosed() if not worker.is_alive() else True
    sock.close()
    if not ok:
        print("transfer failed: the sender stopped before closing", file=sys.stderr)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
