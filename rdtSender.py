# Name: <your full name> (<your student number>), <partner> (<their number>)
"""The sending half of the protocol: Go-Back-N and Selective Repeat.

Stop-and-wait is Go-Back-N with a window of one, so build task 1 in a way that
task 2 can widen rather than replace.

Everything below the GIVEN line is written for you, including the argument
parser and the reading of the input file. Implement the class above it.
"""

import argparse
import socket
import sys
import threading
import time

import config
import segment
from segment import RDTSegment, RdtBuffer


class Sender:
    """One direction of a connection, from the application into the channel.

    Two threads. The application thread calls send() and close(); the network
    thread you start in start() owns the socket, receives acknowledgements and
    runs the retransmission timers. They share the window, so decide which lock
    protects which variable and write it down here -- you will be asked.
    """

    def __init__(self, sock, channelAddr, protocol):
        """Set up the window and the timers.

        The window is a segment.RdtBuffer. Size it with config.WINDOW for now.
        In task 5 you replace that with segment.checkWindow(protocol), which
        returns the same number but refuses to run when the configured window is
        too wide for the sequence space; leave it as config.WINDOW until you
        have written windowLimit, because checkWindow asks windowLimit for the
        answer and cannot work before it exists.

        The buffer owns base, next and the segments themselves, so this class
        never touches slots directly: it calls putNext to admit a segment, get
        to fetch one for retransmission, markAcked when an ACK arrives, and
        setLimit when flow control or congestion control narrows the window.

        Beside it you need the receiver's advertised window and your timer
        deadlines, and one threading.Lock of your own to protect them. That lock
        is yours and is not the buffer's: RdtBuffer has its own Condition
        guarding slots, base and next, and never needs yours. Two primitives,
        two jobs -- a Condition where a thread has to wait for the buffer to
        change, a plain Lock where it only has to read and write your own
        fields without another thread doing so at the same time.

        The indices the buffer keeps never wrap; reduce them into the sequence
        space only when a segment is built, so exactly one place in your code
        has to think about wrapping.

        Timers are deadlines, not threads. Keep the time by which an ACK must
        arrive, and None when no timer is running: one deadline for the whole
        window under Go-Back-N, one per segment under Selective Repeat --
        RDTSegment already has a `deadline` attribute for exactly that.
        Cancelling a timer is then an assignment. The value is config.RTO_INIT
        and it does not change; measuring the path is the bonus.
        """
        self.sock = sock
        self.channelAddr = channelAddr
        self.protocol = protocol
        self.window = segment.RdtBuffer(config.WINDOW)
        self.lock = threading.Lock()
        self.deadline = None
        self.running = False
        self.nextIdx = 0
        self.network_thread = None

    def start(self):
        """Start the network thread.

        That thread is one loop and nothing else. Give the socket a short
        timeout -- a few milliseconds -- so it wakes whether or not a datagram
        arrived, and each time round:

            * if a datagram arrived, validate it and act on it: an ACK marks
              its segment, may advance base, and clears that segment's
              deadline. Task 3 also reads rcvWin off it and calls setLimit.
            * then compare time.monotonic() against the deadlines you kept and
              retransmit whatever has expired, emitting traceTimeout first.

        Do the socket call outside the buffer's Condition, never inside it.
        """
        # setting socket timeout to 5ms
        self.sock.settimeout(0.005)
        # allows network thread to continue executing
        self.running = True

        def network():
            # continuously check for incoming packets and expired timers
            while (True):
                # acquires lock before accessing shared variables
                with self.lock:
                    # Checks whether sender has been stopped
                    if not self.running:
                        break
                try: 
                    raw, addr = self.sock.recvfrom(4096)
                # timeout error
                except TimeoutError:
                    raw = None
                # for BlockingIOErrors, Socket closed, ConnectionResetErrors, MSG too long, sys interrupts, etc...
                except OSError:
                    break

                # only if something was actually received
                if (raw is not None):
                    # checks and validates received packet -> checksum, lengths, flags
                    error = RDTSegment.validate(raw)
                    # error found
                    if (error is not None):
                        # packet identifier set to -1
                        ident = -1
                        # received packet atleast as large as expected header
                        if (len(raw) >= segment.HEADER_LEN):
                            # attempst unpack bad packet information
                            bad = RDTSegment.unpack(raw)
                            # determine which number to use for identifying invalid packet
                            if(bad.isAck):
                                ident = bad.ackNum
                            else:
                                ident = bad.seqNum
                        if (error == "checksum"):
                            # debuffing trace function for corruptions and etc...
                            segment.traceCorrupt(ident)
                        else:
                            segment.traceDrop(ident, error)
                    else:
                        # if no error is found
                        # convert raw bytes in RDTSegment
                        # access fields isAck, ackNum, seqNum, rcvWin
                        pkt = RDTSegment.unpack(raw)
                        # if received packet is an acknowledgement
                        # sender only needs to process Acks in this section
                        if (pkt.isAck):
                            # read current window state
                            # acquire buffer condition lock
                            # Retrieves base: oldest unacknowledged segment
                            # nxt: next segment index
                            with self.window.cv:
                                base = self.window.base
                                nxt = self.window.next
                            
                            segment.traceRecv(pkt, base, nxt, config.WINDOW)
                            if (nxt > base and pkt.ackNum == segment.mask(base)):
                                self.window.getNext()
                                with self.lock:
                                    self.deadline = None
                # check expired timers
                timedOut = False
                with self.lock:
                    # Checks: A timer is currently active
                    # current time has reached or passed its deadline
                    if (self.deadline is not None and time.monotonic() >= self.deadline):
                        # restart timer by setting new deadline
                        self.deadline = time.monotonic() + config.RTO_INIT
                        # indicate retransmission is required
                        timedOut = True
                # if retransmission is required
                if(timedOut):
                    # read current sending window
                    with self.window.cv:
                        base = self.window.base
                        nxt = self.window.next
                    # checks for outstanding segments
                    if (base < nxt):
                        # retrieve segment at specified internal index
                        seg = self.window.get(base)
                        if (seg is not None):
                             # record timeout occurance: passes seq number of oldest outstanding segment, internal base index, next index
                            segment.traceTimeout(segment.mask(base), base, nxt)
                            # retransmit segment
                            self.sock.sendto(seg.pack(), self.channelAddr)
                            segment.traceSend(seg, base, nxt, config.WINDOW)
        # create and start new thread: 
        # daemon = True: will not prevent program from exiting if the meain thread finishes
        self.network_thread = threading.Thread(target=network, daemon=True)
        self.network_thread.start()
        
                            
    def send(self, payload):
        """Hand one segment's worth of data to the protocol.

        Blocks while the window is full, and returns True once the segment has
        been accepted for transmission, or False if the transfer has failed.

        One exception to blocking: if the receiver has advertised a window of
        zero and nothing is outstanding, no acknowledgement will ever arrive to
        reopen it, because there is nothing left to acknowledge. After
        config.PERSIST seconds, transmit anyway.
        """
        # create new segment
        # segment.mask(self.nextIdx): convert internal index into seq number
        seg = RDTSegment(seqNum=segment.mask(self.nextIdx), payload=payload)
        self.nextIdx += 1

        # attempt insert segmentinto sending window -> window can be full = fail
        if not (self.window.putNext(seg)):
            return False
        with self.window.cv:
            base = self.window.base
            nxt = self.window.next
        with self.lock:
            # set retransmission deadline
            self.deadline = time.monotonic() + config.RTO_INIT
            # send segment to receiver
            self.sock.sendto(seg.pack(), self.channelAddr)
            segment.traceSend(seg, base, nxt, config.WINDOW)
        return True

    def close(self):
        """Wait for every segment to be acknowledged, then take the connection
        down: send a FIN, retransmit it if it is lost, and return True once it
        has been acknowledged. Return False if the connection could not be
        closed. Must return within a bounded time in either case.
        """
        raise NotImplementedError


# ============================================================================
# GIVEN - do not modify anything below this line.
#
# Argument parsing, socket setup and the reading of the input file. The grading
# script invokes exactly these options, and none of this is what the project is
# teaching, so it is handed to you rather than assigned.
# ============================================================================

def parseArgs(argv):
    ap = argparse.ArgumentParser(description="CMPT 371 Project 3 - RDT sender")
    ap.add_argument("--channel", type=int, required=True,
                    help="the port channel.py printed for the sender side")
    ap.add_argument("--in", dest="inPath", required=True, help="file to send")
    ap.add_argument("--protocol", default="gbn", choices=("gbn", "sr"))
    ap.add_argument("--trace", default=None, help="write the event log here")
    return ap.parse_args(argv)


def openSocket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((config.HOST, 0))
    # A generous receive buffer: a burst of acknowledgements that arrives while
    # this program is busy elsewhere is otherwise dropped by the operating
    # system, and looks exactly like loss the channel never injected.
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
    return sock


def main(argv=None):
    args = parseArgs(argv if argv is not None else sys.argv[1:])
    if (not isinstance(segment.SEED, int) or isinstance(segment.SEED, bool)
            or segment.SEED <= 0):
        sys.exit("set SEED at the top of segment.py to your Canvas group number")
    segment.traceOpen(args.trace)

    data = open(args.inPath, "rb").read()
    sock = openSocket()
    sender = Sender(sock, (config.HOST, args.channel), args.protocol)
    sender.start()
    for i in range(0, len(data), config.MSS):
        if not sender.send(data[i:i + config.MSS]):
            break
    try:
        ok = sender.close()
    except NotImplementedError:
        # Graceful teardown is task 4. Until it is written, give the last window
        # time to be acknowledged and then exit cleanly, so that the tasks
        # before it are still graded on the bytes that arrived rather than on
        # how the connection ended. This costs two seconds and nothing else.
        time.sleep(2.0)
        ok = True
    sock.close()
    if not ok:
        print("transfer failed: %s" % (getattr(sender, "failed", None) or "unknown"),
              file=sys.stderr)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
