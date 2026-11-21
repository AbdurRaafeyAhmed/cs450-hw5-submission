
"""
Where solution code to HW5 should be written.  No other files should
be modified.
"""

import socket
import io
import time
import typing
import struct
import homework5
import homework5.logging


def send(sock: socket.socket, data: bytes):
    """
    Implementation of the sending logic for sending data over a slow,
    lossy, constrained network.

    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.
        data -- A bytes object, containing the data to send over the network.
    """

    # Naive implementation where we chunk the data to be sent into
    # packets as large as the network will allow, and then send them
    # over the network, pausing half a second between sends to let the
    # network "rest" :)
    
    # Leave 5 bytes 
    chunk_size = homework5.MAX_PACKET - 5
    seq = 0
    estimated_rtt = 0.2
    dev_rtt = 0.1
    timeout = 0.6

    #one chunk at a time
    for start in range(0, len(data), chunk_size):
        number = struct.pack('!I', seq)
        packet = b'D' + number + data[start:start + chunk_size]
        ack = b'A' + number
        retried = False

        # Keep sending until we get the correct ACK
        while True:
            sock.send(packet)
            sent_at = time.monotonic()
            sock.settimeout(timeout)
            try:
                while sock.recv(homework5.MAX_PACKET) != ack:
                    pass

                # Update the timeout using the round trip time
                if not retried:
                    rtt = time.monotonic() - sent_at
                    dev_rtt = 0.75 * dev_rtt + 0.25 * abs(rtt - estimated_rtt)
                    estimated_rtt = 0.875 * estimated_rtt + 0.125 * rtt
                    timeout = max(0.25, estimated_rtt + 4 * dev_rtt)
                break
            except socket.timeout:
                # No ACK came back so we need to try again
                retried = True

        # Move to the next packet after getting the ACK
        seq += 1

    # Let the receiver know finished sending data.
    number = struct.pack('!I', seq)
    sock.settimeout(max(0.5, timeout))
    while True:
        sock.send(b'F' + number)
        try:
            while sock.recv(homework5.MAX_PACKET) != b'K' + number:
                pass
            break
        except socket.timeout:
            pass


def recv(sock: socket.socket, dest: io.BufferedIOBase) -> int:
    """
    Implementation of the receiving logic for receiving data over a slow,
    lossy, constrained network.

    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.

    Return:
        The number of bytes written to the destination.
    """
    # logger = homework5.logging.get_logger("hw5-receiver")
    # Naive solution, where we continually read data off the socket
    # until we don't receive any more data, and then return.
    
    expected = 0
    total = 0

    # Keep listening for packets from the sender
    while True:
        packet = sock.recv(homework5.MAX_PACKET)

        # Skip packets that are too small to have a header
        if len(packet) < 5:
            continue

        kind = packet[:1]
        number = packet[1:5]
        seq = struct.unpack('!I', number)[0]

        if kind == b'D':
            # Only write the packet if it's the one we're expecting
            if seq == expected:
                chunk = packet[5:]
                dest.write(chunk)
                total += len(chunk)
                expected += 1

            # Send an ACK even if we already got this packet
            if seq < expected:
                sock.send(b'A' + number)

        elif kind == b'F' and seq == expected:
            # All the data arrived, so confirm that we're done
            sock.send(b'K' + number)
            break

    dest.flush()

    # Stay around briefly in case the final ACK gets lost
    sock.settimeout(6.0)
    while True:
        try:
            packet = sock.recv(homework5.MAX_PACKET)
            if packet == b'F' + struct.pack('!I', expected):
                sock.send(b'K' + struct.pack('!I', expected))
        except socket.timeout:
            break

    return total
