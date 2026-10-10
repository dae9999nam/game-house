#!/usr/bin/env python3
"""Single-threaded TCP game-house client for POSIX terminals and pipes."""

import os
import select
import socket
import sys


MAX_LINE_BYTES = 4096
CONNECT_TIMEOUT = 5.0
AUTHENTICATING = "AUTHENTICATING"
IN_HALL = "IN_HALL"
WAITING = "WAITING"
PLAYING = "PLAYING"

AUTH_OK = "1001 Authentication successful"
AUTH_FAILED = "1002 Authentication failed"
WAIT = "3011 Wait"
START = "3012 Game started. Please guess true or false"
FULL = "3013 The room is full"
RESULTS = {
    "3021 You are the winner",
    "3022 You lost this game",
    "3023 The result is a tie",
}
BYE = "4001 Bye bye"
UNRECOGNIZED = "4002 Unrecognized message"


class ServerDisconnected(Exception):
    """The server closed the connection before a normal /exit reply."""


class ProtocolError(Exception):
    """The server sent an invalid or out-of-state response."""


def enable_keepalive(connection):
    """Let TCP detect silent broken connections without new protocol messages."""
    connection.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    idle_option = getattr(socket, "TCP_KEEPIDLE", None)
    if idle_option is None:  # macOS uses a different name for the idle period.
        idle_option = getattr(socket, "TCP_KEEPALIVE", None)
    options = ((idle_option, 15), (getattr(socket, "TCP_KEEPINTVL", None), 5),
               (getattr(socket, "TCP_KEEPCNT", None), 3))
    for option, value in options:
        if option is not None:
            try:
                connection.setsockopt(socket.IPPROTO_TCP, option, value)
            except OSError:
                pass  # Unsupported tuning retains the system's TCP defaults.


class GameClient:
    def __init__(self, connection):
        self.connection = connection
        self.stdin_fd = sys.stdin.fileno()
        self.incoming = bytearray()
        self.keyboard = bytearray()
        self.keyboard_eof = False
        self.discarding_input = False
        self.state = AUTHENTICATING
        self.auth_field = "username"
        self.username = ""
        self.awaiting_reply = False
        self.valid_guess_sent = False
        self.finished = False

    def prompt(self):
        if self.state == AUTHENTICATING and not self.awaiting_reply:
            if self.auth_field == "username":
                print("Please input your user name:", flush=True)
            else:
                print("Please input your password:", flush=True)

    def needs_input(self):
        return self.state != WAITING and not self.awaiting_reply

    def send_line(self, message):
        encoded = message.encode("utf-8")
        if len(encoded) > MAX_LINE_BYTES:
            print("Input is too long.", flush=True)
            return False
        self.connection.sendall(encoded + b"\n")
        self.awaiting_reply = True
        return True

    def accept_input(self, line):
        if self.state == AUTHENTICATING:
            if self.auth_field == "username":
                self.username = line
                self.auth_field = "password"
                self.prompt()
            else:
                if not self.send_line("/login " + self.username + " " + line):
                    # The combined command can be too long even when each
                    # field fits. Let the user replace either credential.
                    self.username = ""
                    self.auth_field = "username"
                    self.prompt()
        elif self.state == IN_HALL:
            self.send_line(line)
        elif self.state == PLAYING:
            # The written instructions use a bare Boolean; the appendix shows
            # /guess commands. Both terminal forms use the same wire protocol.
            tokens = line.split()
            message = line if tokens[:1] == ["/guess"] else "/guess " + line
            if self.send_line(message):
                self.valid_guess_sent = message.split() in (
                    ["/guess", "true"], ["/guess", "false"])

    @staticmethod
    def is_room_list(line):
        fields = line.split(" ")
        if len(fields) < 3 or fields[0] != "3001":
            return False
        if not all(field.isascii() and field.isdigit() for field in fields[1:]):
            return False
        count = int(fields[1])
        return count > 0 and len(fields) == count + 2 and all(
            int(value) in (0, 1, 2) for value in fields[2:]
        )

    def accept_response(self, line):
        """Validate replies before updating the local input/receive state."""
        if self.state == AUTHENTICATING and self.awaiting_reply:
            if line == AUTH_OK:
                self.state = IN_HALL
                self.username = ""
            elif line in (AUTH_FAILED, UNRECOGNIZED):
                self.auth_field = "username"
                self.username = ""
            else:
                raise ProtocolError()
            self.awaiting_reply = False
        elif line == UNRECOGNIZED and self.state != AUTHENTICATING:
            # A guess can reach the server just AFTER an opponent disconnect
            # settles the game. Its rejection then follows the result, possibly
            # crossing our next hall command. 4002 rejects a command; it never
            # changes the player's state. Keep accepting ordered hall replies
            # even if an older rejection has already enabled keyboard input.
            # Likewise, an older error must not enable a second valid guess.
            if (self.state == IN_HALL
                    or (self.state == PLAYING and not self.valid_guess_sent)):
                self.awaiting_reply = False
        elif self.state == IN_HALL:
            if line == WAIT:
                self.state = WAITING
            elif line == START:
                self.state = PLAYING
                self.valid_guess_sent = False
            elif line == BYE:
                self.finished = True
            elif line != FULL and not self.is_room_list(line):
                raise ProtocolError()
            self.awaiting_reply = False
        elif self.state == WAITING:
            if line != START:
                raise ProtocolError()
            self.state = PLAYING
            self.valid_guess_sent = False
            self.awaiting_reply = False
        elif self.state == PLAYING:
            # The opponent can disappear while we are still reading a guess.
            if line in RESULTS:
                self.state = IN_HALL
                self.valid_guess_sent = False
                self.awaiting_reply = False
            else:
                raise ProtocolError()
        else:
            raise ProtocolError()
        print(line, flush=True)
        if self.finished:
            print("Client ends", flush=True)
        else:
            self.prompt()

    def receive(self):
        data = self.connection.recv(4096)
        if not data:
            raise ServerDisconnected()
        self.incoming.extend(data)
        while b"\n" in self.incoming:
            raw, _, remaining = self.incoming.partition(b"\n")
            self.incoming = bytearray(remaining)
            if raw.endswith(b"\r"):
                raw = raw[:-1]
            if len(raw) > MAX_LINE_BYTES:
                raise ProtocolError()
            try:
                line = raw.decode("utf-8")
            except UnicodeDecodeError as error:
                raise ProtocolError() from error
            self.accept_response(line)
            if self.finished:
                return
        # Allow one extra byte for a possible CR before the next LF.
        if len(self.incoming) > MAX_LINE_BYTES + 1:
            raise ProtocolError()

    def read_keyboard(self):
        data = os.read(self.stdin_fd, 4096)
        if data:
            self.keyboard.extend(data)
        else:
            self.keyboard_eof = True

    def consume_keyboard(self):
        """Consume one input line, retaining pipe data for future client states."""
        newline = self.keyboard.find(b"\n")
        if newline < 0 and not self.keyboard_eof:
            if len(self.keyboard) > MAX_LINE_BYTES + 1:
                self.discarding_input = True
                self.keyboard.clear()
            return False
        if newline >= 0:
            raw = bytes(self.keyboard[:newline])
            del self.keyboard[:newline + 1]
        elif self.keyboard:
            raw = bytes(self.keyboard)
            self.keyboard.clear()
        else:
            if self.discarding_input:
                self.discarding_input = False
                print("Input is too long.", flush=True)
            print("Input closed. Client ends", flush=True)
            self.finished = True
            return True
        if raw.endswith(b"\r"):
            raw = raw[:-1]
        if self.discarding_input or len(raw) > MAX_LINE_BYTES:
            self.discarding_input = False
            print("Input is too long.", flush=True)
            self.prompt()
            return True
        try:
            line = raw.decode("utf-8")
        except UnicodeDecodeError:
            print("Input is not valid UTF-8.", flush=True)
            self.prompt()
            return True
        self.accept_input(line)
        return True

    def run(self):
        self.prompt()
        while not self.finished:
            # Check TCP first, even when several keyboard lines were read at
            # once. Never block in input(): server EOF must interrupt any prompt.
            ready, _, _ = select.select([self.connection], [], [], 0)
            if ready:
                self.receive()
                continue
            if self.needs_input() and self.consume_keyboard():
                continue
            readers = [self.connection]
            if self.needs_input() and not self.keyboard_eof:
                readers.append(self.stdin_fd)
            ready, _, _ = select.select(readers, [], [])
            if self.connection in ready:
                self.receive()
            elif self.stdin_fd in ready:
                self.read_keyboard()


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 2:
        print("Usage: python3 GameClient.py <server_hostname_or_IP> <server_port>",
              file=sys.stderr)
        return 1
    try:
        port = int(args[1])
        if not 1 <= port <= 65535:
            raise ValueError()
    except ValueError:
        print("Error: server_port must be an integer from 1 to 65535.", file=sys.stderr)
        return 1
    if os.name != "posix":
        print("Error: this client requires a POSIX terminal (Linux or macOS).",
              file=sys.stderr)
        return 1
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
            connection.settimeout(CONNECT_TIMEOUT)
            connection.connect((args[0], port))
            enable_keepalive(connection)
            GameClient(connection).run()
        return 0
    except KeyboardInterrupt:
        print("\nClient ends", flush=True)
        return 0
    except ServerDisconnected:
        print("Server disconnected unexpectedly.", file=sys.stderr)
    except ProtocolError:
        print("Invalid response from server. Client ends", file=sys.stderr)
    except (OSError, ValueError) as error:
        print("Connection error: {}".format(error), file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
