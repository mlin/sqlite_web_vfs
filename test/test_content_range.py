# Content-Range tests using a local raw-socket SQLite origin.
# Except for /badprobe.db, probes are valid; faults affect extents only.
#
# /good.db: correct responses.
# /wrong.db: correct body, range endpoints shifted by 4096 bytes.
# /nocr.db: missing Content-Range.
# /wrongtotal.db: correct endpoints, total increased by 4096.
# /badprobe.db: shifted probe endpoints; correct extent responses.
# /interim.db: correct Content-Range in 103; none in final 206.
# /interimok.db: wrong Content-Range in 103; correct one in final 206.
# /trailer.db: correct Content-Range only in chunked trailers.
import os
import re
import socket
import sqlite3
import threading
import urllib.parse

import pytest

HERE = os.path.dirname(__file__)
BUILD = os.path.abspath(os.path.join(HERE, "..", "build"))
ROWS = 400


def _make_db_bytes():
    path = os.path.join(HERE, "_content_range_test.db")
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE t (x TEXT)")
    con.executemany("INSERT INTO t VALUES (?)", [("x" * 100 + str(i),) for i in range(ROWS)])
    con.commit()
    con.close()
    with open(path, "rb") as f:
        data = f.read()
    os.unlink(path)
    return data


DB = _make_db_bytes()


def _respond(conn, status, headers, body):
    lines = [f"HTTP/1.1 {status}"]
    lines += [f"{k}: {v}" for k, v in headers]
    lines.append(f"Content-Length: {len(body)}")
    lines.append("")
    lines.append("")
    conn.sendall("\r\n".join(lines).encode() + body)


def _serve_range(conn, path, first, last):
    body = DB[first : last + 1]
    total = len(DB)
    correct_cr = f"bytes {first}-{last}/{total}"
    probe = (first, last) == (0, 99)
    if path == "/badprobe.db" and probe:
        shifted = f"bytes {first + 4096}-{last + 4096}/{total}"
        _respond(conn, "206 Partial Content", [("Content-Range", shifted)], body)
    elif probe or path in ("/good.db", "/badprobe.db"):
        _respond(conn, "206 Partial Content", [("Content-Range", correct_cr)], body)
    elif path == "/wrong.db":
        shifted = f"bytes {first + 4096}-{last + 4096}/{total}"
        _respond(conn, "206 Partial Content", [("Content-Range", shifted)], body)
    elif path == "/nocr.db":
        _respond(conn, "206 Partial Content", [], body)
    elif path == "/wrongtotal.db":
        wrong_total = f"bytes {first}-{last}/{total + 4096}"
        _respond(conn, "206 Partial Content", [("Content-Range", wrong_total)], body)
    elif path == "/interim.db":
        interim = f"HTTP/1.1 103 Early Hints\r\nContent-Range: {correct_cr}\r\n\r\n"
        conn.sendall(interim.encode())
        _respond(conn, "206 Partial Content", [], body)
    elif path == "/interimok.db":
        interim = (
            "HTTP/1.1 103 Early Hints\r\n"
            f"Content-Range: bytes {first + 4096}-{last + 4096}/{total}\r\n"
            "\r\n"
        )
        conn.sendall(interim.encode())
        _respond(conn, "206 Partial Content", [("Content-Range", correct_cr)], body)
    elif path == "/trailer.db":
        chunked = (
            (
                "HTTP/1.1 206 Partial Content\r\n"
                "Transfer-Encoding: chunked\r\n"
                "\r\n"
                f"{len(body):x}\r\n"
            ).encode()
            + body
            + (f"\r\n0\r\nContent-Range: {correct_cr}\r\n\r\n").encode()
        )
        conn.sendall(chunked)
    else:
        _respond(conn, "404 Not Found", [], b"")


def _handle_conn(conn):
    buf = b""
    try:
        while True:
            while b"\r\n\r\n" not in buf:
                chunk = conn.recv(65536)
                if not chunk:
                    return
                buf += chunk
            request, buf = buf.split(b"\r\n\r\n", 1)
            lines = request.decode("latin1").split("\r\n")
            path = lines[0].split(" ")[1]
            headers = {}
            for line in lines[1:]:
                if ": " in line:
                    k, v = line.split(": ", 1)
                    headers[k.lower()] = v
            m = re.fullmatch(r"bytes=(\d+)-(\d+)", headers.get("range", ""))
            if m:
                _serve_range(conn, path, int(m.group(1)), int(m.group(2)))
            elif path.endswith(".db"):
                _respond(conn, "200 OK", [], DB)
            else:
                _respond(conn, "404 Not Found", [], b"")
    except (ConnectionResetError, BrokenPipeError):
        return
    finally:
        conn.close()


@pytest.fixture(scope="module")
def origin():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(50)
    port = srv.getsockname()[1]

    def accept_loop():
        while True:
            try:
                conn, _ = srv.accept()
            except OSError:
                return
            threading.Thread(target=_handle_conn, args=(conn,), daemon=True).start()

    threading.Thread(target=accept_loop, daemon=True).start()
    yield f"http://127.0.0.1:{port}"
    srv.close()


def _count_rows(origin, path):
    con = sqlite3.connect(":memory:")
    con.enable_load_extension(True)
    con.load_extension(os.path.join(BUILD, "web_vfs"))
    url = urllib.parse.quote(f"{origin}{path}", safe="")
    con = sqlite3.connect(
        f"file:/__web__?vfs=web&mode=ro&immutable=1&web_url={url}&web_nodbi=1&web_small_KiB=4",
        uri=True,
    )
    try:
        return con.execute("SELECT count(*) FROM t").fetchone()[0]
    finally:
        con.close()


def test_good_origin(origin):
    assert _count_rows(origin, "/good.db") == ROWS


def test_wrong_content_range_rejected(origin):
    with pytest.raises(sqlite3.Error):
        _count_rows(origin, "/wrong.db")


def test_missing_content_range_rejected(origin):
    with pytest.raises(sqlite3.Error):
        _count_rows(origin, "/nocr.db")


def test_wrong_total_rejected(origin):
    with pytest.raises(sqlite3.Error):
        _count_rows(origin, "/wrongtotal.db")


def test_bad_probe_rejected(origin):
    with pytest.raises(sqlite3.Error):
        _count_rows(origin, "/badprobe.db")


def test_interim_headers_do_not_leak(origin):
    with pytest.raises(sqlite3.Error):
        _count_rows(origin, "/interim.db")


def test_final_response_headers_win(origin):
    assert _count_rows(origin, "/interimok.db") == ROWS


def test_trailer_content_range_rejected(origin):
    with pytest.raises(sqlite3.Error):
        _count_rows(origin, "/trailer.db")
