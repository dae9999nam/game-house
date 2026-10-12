#!/usr/bin/env python3
"""COMP3234 / ELEC3443: a threaded TCP game house (standard library only)."""

import queue
import random
import socket
import sys
import threading
import traceback
from dataclasses import dataclass, field
from enum import Enum, auto


NUM_ROOMS = 10
MAX_LINE_BYTES = 4096
SOCKET_TIMEOUT = 0.2

AUTH_OK = "1001 Authentication successful"
AUTH_FAILED = "1002 Authentication failed"
WAIT = "3011 Wait"
START = "3012 Game started. Please guess true or false"
FULL = "3013 The room is full"
WIN = "3021 You are the winner"
LOSE = "3022 You lost this game"
TIE = "3023 The result is a tie"
BYE = "4001 Bye bye"
UNRECOGNIZED = "4002 Unrecognized message"


class State(Enum):
    AUTHENTICATING = auto()
    IN_HALL = auto()
    WAITING = auto()
    PLAYING = auto()
    DISCONNECTED = auto()


@dataclass(eq=False)
class Session:
    sock: socket.socket
    address: tuple
    username: str = None
    state: State = State.AUTHENTICATING
    room_id: int = None             # Internal zero-based index; protocol is 1-based.
    guess: str = None
    closing: bool = False
    outgoing: queue.Queue = field(default_factory=queue.Queue)
    thread: threading.Thread = None


class CommandReader:
    """Split TCP bytes into UTF-8 lines, bounding even unterminated bad input.

    None represents one malformed line. An oversized line produces one error,
    then is discarded through its newline so the next command can recover.
    """

    def __init__(self):
        self.buffer = bytearray()
        self.discarding = False

    def feed(self, data):
        parts = data.split(b"\n")
        for index, part in enumerate(parts):
            if not self.discarding:
                self.buffer.extend(part)
                # CRLF is one delimiter, so its CR must not make a valid
                # maximum-length command oversized, even across recv calls.
                trailing_cr = (len(self.buffer) == MAX_LINE_BYTES + 1
                               and self.buffer.endswith(b"\r"))
                if len(self.buffer) > MAX_LINE_BYTES and not trailing_cr:
                    self.buffer.clear()
                    self.discarding = True
                    yield None
            if index < len(parts) - 1:
                if not self.discarding:
                    try:
                        yield self.buffer.decode("utf-8").rstrip("\r")
                    except UnicodeDecodeError:
                        yield None
                self.buffer.clear()
                self.discarding = False


def enable_keepalive(sock):
    """Ask TCP to detect silent failures without adding protocol messages."""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    idle_option = getattr(socket, "TCP_KEEPIDLE",
                          getattr(socket, "TCP_KEEPALIVE", None))
    for option, value in ((idle_option, 15),
                          (getattr(socket, "TCP_KEEPINTVL", None), 5),
                          (getattr(socket, "TCP_KEEPCNT", None), 3)):
        if option is not None:
            try:
                sock.setsockopt(socket.IPPROTO_TCP, option, value)
            except OSError:
                pass  # Some systems expose constants but do not support tuning.


class GameHouse:
    def __init__(self, credentials):
        self.credentials = credentials
        self.rooms = [[] for _ in range(NUM_ROOMS)]
        self.sessions = set()
        self.lock = threading.Lock()
        self.stopping = threading.Event()

    # This lock protects ALL session/game state and room membership, including
    # /list snapshots. Replies are queued under the same lock to preserve their
    # logical order (Wait, Start, Result). Only a socket's owning handler sends
    # its queued messages. No send, recv, close or join holds the room lock;
    # therefore a slow/broken peer cannot block unrelated rooms or interleave
    # writes from different threads. There is no second application lock to nest.

    def handle_command(self, player, message):
        tokens = message.split() if message is not None else []
        with self.lock:
            if player.state is State.DISCONNECTED or player.closing:
                return
            reply = UNRECOGNIZED
            if player.state is State.AUTHENTICATING:
                if len(tokens) == 3 and tokens[0] == "/login":
                    username, password = tokens[1:]
                    if self.credentials.get(username) == password:
                        player.username = username
                        player.state = State.IN_HALL
                        reply = AUTH_OK
                    else:
                        reply = AUTH_FAILED
            elif player.state is State.IN_HALL:
                if tokens == ["/list"]:
                    counts = " ".join(str(len(room)) for room in self.rooms)
                    reply = "3001 {} {}".format(NUM_ROOMS, counts)
                elif tokens == ["/exit"]:
                    player.closing = True
                    reply = BYE
                elif len(tokens) == 2 and tokens[0] == "/enter":
                    try:
                        digits = tokens[1]
                        if digits[:1] in ("+", "-"):
                            digits = digits[1:]
                        if not digits.isascii() or not digits.isdecimal():
                            raise ValueError("Room number must be an integer")
                        room_id = int(tokens[1]) - 1
                    except ValueError:
                        room_id = -1
                    if 0 <= room_id < NUM_ROOMS:
                        self.enter_room(player, room_id)
                        return
            elif player.state is State.PLAYING:
                if (len(tokens) == 2 and tokens[0] == "/guess"
                        and tokens[1] in ("true", "false")
                        and player.guess is None):
                    player.guess = tokens[1]
                    room = self.rooms[player.room_id]
                    if all(member.guess is not None for member in room):
                        self.finish_game(room)
                    return  # A valid first guess has no acknowledgement.
            player.outgoing.put(reply)

    def enter_room(self, player, room_id):
        """Called only with self.lock held and the player in the hall."""
        room = self.rooms[room_id]
        if len(room) == 2:
            player.outgoing.put(FULL)
            return
        room.append(player)
        player.room_id = room_id
        player.guess = None
        if len(room) == 1:
            player.state = State.WAITING
            player.outgoing.put(WAIT)
        else:
            for member in room:
                member.state = State.PLAYING
                member.outgoing.put(START)

    @staticmethod
    def return_to_hall(player):
        """Called with the room lock held for a connected player."""
        player.state = State.IN_HALL
        player.room_id = None
        player.guess = None

    def finish_game(self, room):
        """Resolve once, atomically, while self.lock is held."""
        if room[0].guess == room[1].guess:
            replies = [TIE, TIE]
        else:
            answer = random.choice(("true", "false"))
            replies = [WIN if member.guess == answer else LOSE for member in room]
        for member, reply in zip(room, replies):
            self.return_to_hall(member)
            member.outgoing.put(reply)
        room.clear()

    def cleanup_client(self, player):
        """Idempotent logical cleanup; the owning handler closes the socket."""
        with self.lock:
            if player.state is State.DISCONNECTED:
                return
            if player.room_id is not None:
                room = self.rooms[player.room_id]
                # Clearing the whole room also settles a playing disconnect.
                # A simultaneous second cleanup sees no room and cannot settle
                # this game again. Failed result delivery likewise cannot reopen it.
                for opponent in room:
                    if opponent is not player:
                        self.return_to_hall(opponent)
                        opponent.outgoing.put(WIN)
                room.clear()
            player.state = State.DISCONNECTED
            player.closing = True
            player.room_id = None
            player.guess = None
            self.sessions.discard(player)

    @staticmethod
    def send_pending(player):
        """Only the owning handler calls this; errors reach its finally cleanup."""
        while True:
            try:
                message = player.outgoing.get_nowait()
            except queue.Empty:
                return
            player.sock.sendall((message + "\n").encode("utf-8"))

    def client_handler(self, player):
        reader = CommandReader()
        try:
            player.sock.settimeout(SOCKET_TIMEOUT)
            enable_keepalive(player.sock)
            while not self.stopping.is_set():
                self.send_pending(player)
                if player.closing:
                    break  # /exit's 4001 has been sent before closing.
                try:
                    data = player.sock.recv(4096)
                except socket.timeout:
                    continue  # Also gives queued opponent notifications a turn.
                if not data:
                    break
                for command in reader.feed(data):
                    self.handle_command(player, command)
                    self.send_pending(player)
                    if player.closing:
                        break
        except OSError as error:
            if not self.stopping.is_set():
                print("Client {} disconnected: {}".format(player.address, error),
                      file=sys.stderr)
        except Exception:
            print("Unexpected client error for {}".format(player.address),
                  file=sys.stderr)
            traceback.print_exc()
        finally:
            self.cleanup_client(player)
            try:
                player.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass  # Already closed/reset peers need no additional action.
            player.sock.close()

    def serve(self, port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("", port))
            listener.listen()
            try:
                while True:
                    client_socket, address = listener.accept()
                    player = Session(client_socket, address)
                    player.thread = threading.Thread(
                        target=self.client_handler, args=(player,), daemon=True)
                    with self.lock:
                        self.sessions.add(player)
                    try:
                        player.thread.start()
                    except Exception:
                        self.cleanup_client(player)
                        client_socket.close()
                        raise
            finally:
                self.stopping.set()
                with self.lock:
                    players = list(self.sessions)
                for player in players:
                    try:
                        player.sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                for player in players:
                    if player.thread.ident is not None:
                        player.thread.join(timeout=2)


def load_credentials(path):
    credentials = {}
    with open(path, encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            line = line.strip()
            if not line:
                continue
            username, separator, password = line.partition(":")
            if (not separator or not username or not password
                    or any(char.isspace() for char in username + password)
                    or username in credentials):
                raise ValueError("Invalid or duplicate account on line {}".format(
                    line_number))
            credentials[username] = password
    return credentials


def main():
    if len(sys.argv) != 3:
        print("Usage: python3 GameServer.py <server_port> <path_to_UserInfo.txt>",
              file=sys.stderr)
        return 1
    try:
        port = int(sys.argv[1])
        if not 1 <= port <= 65535:
            raise ValueError("Port must be between 1 and 65535")
        credentials = load_credentials(sys.argv[2])
        GameHouse(credentials).serve(port)
    except (OSError, ValueError, UnicodeError) as error:
        print("Server error: {}".format(error), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
