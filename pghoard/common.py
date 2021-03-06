"""
pghoard - common utility functions

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""

import fcntl
import logging
import os

try:
    from backports import lzma  # pylint: disable=import-error, unused-import
except ImportError:
    import lzma  # pylint: disable=import-error, unused-import


try:
    from urllib.parse import urlparse, parse_qs  # pylint: disable=no-name-in-module, import-error
except ImportError:
    from urlparse import urlparse, parse_qs  # pylint: disable=no-name-in-module, import-error


if hasattr(lzma, "open"):
    lzma_open = lzma.open  # pylint: disable=no-member, maybe-no-member
    lzma_open_read = lzma.open  # pylint: disable=no-member, maybe-no-member
    lzma_compressor = lzma.LZMACompressor  # pylint: disable=no-member
    lzma_decompressor = lzma.LZMADecompressor  # pylint: disable=no-member
elif not hasattr(lzma, "options"):
    def lzma_open(filepath, mode, preset):
        return lzma.LZMAFile(filepath, mode=mode, preset=preset)

    def lzma_open_read(filepath, mode):
        return lzma.LZMAFile(filepath, mode=mode)

    def lzma_compressor(preset):
        return lzma.LZMACompressor(preset=preset)  # pylint: disable=no-member

    def lzma_decompressor():
        return lzma.LZMADecompressor()  # pylint: disable=no-member
else:
    def lzma_open(filepath, mode, preset):
        return lzma.LZMAFile(filepath, mode=mode, options={"level": preset})  # pylint: disable=unexpected-keyword-arg

    def lzma_open_read(filepath, mode):
        return lzma.LZMAFile(filepath, mode=mode)

    def lzma_compressor(preset):
        return lzma.LZMACompressor(options={"level": preset})  # pylint: disable=no-member

    def lzma_decompressor():
        return lzma.LZMADecompressor()  # pylint: disable=no-member

try:
    from Queue import Queue, Empty  # pylint: disable=import-error, unused-import
except ImportError:
    from queue import Queue, Empty  # pylint: disable=import-error, unused-import


default_log_format_str = "%(asctime)s\t%(name)s\t%(threadName)s\t%(levelname)s\t%(message)s"
syslog_format_str = '%(name)s %(levelname)s: %(message)s'


def create_connection_string(connection_info):
    return " ".join("{}='{}'".format(k, str(v).replace("'", "\\'"))
                    for k, v in sorted(connection_info.items()))


def get_connection_info(info):
    """turn a connection info object into a dict or return it if it was a
    dict already.  supports both the traditional libpq format and the new
    url format"""
    if isinstance(info, dict):
        return info.copy()
    elif info.startswith("postgres://") or info.startswith("postgresql://"):
        return parse_connection_string_url(info)
    else:
        return parse_connection_string_libpq(info)


def parse_connection_string_url(url):
    p = urlparse(url)
    fields = {}
    if p.hostname:
        fields["host"] = p.hostname
    if p.port:
        fields["port"] = str(p.port)
    if p.username:
        fields["user"] = p.username
    if p.password is not None:
        fields["password"] = p.password
    if p.path and p.path != "/":
        fields["dbname"] = p.path[1:]
    for k, v in parse_qs(p.query).items():
        fields[k] = v[-1]
    return fields


def parse_connection_string_libpq(connection_string):
    """parse a postgresql connection string as defined in
    http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING"""
    fields = {}
    while True:
        connection_string = connection_string.strip()
        if not connection_string:
            break
        if "=" not in connection_string:
            raise ValueError("expecting key=value format in connection_string fragment {!r}".format(connection_string))
        key, rem = connection_string.split("=", 1)
        if rem.startswith("'"):
            asis, value = False, ""
            for i in range(1, len(rem)):
                if asis:
                    value += rem[i]
                    asis = False
                elif rem[i] == "'":
                    break  # end of entry
                elif rem[i] == "\\":
                    asis = True
                else:
                    value += rem[i]
            else:
                raise ValueError("invalid connection_string fragment {!r}".format(rem))
            connection_string = rem[i + 1:]  # pylint: disable=undefined-loop-variable
        else:
            res = rem.split(None, 1)
            if len(res) > 1:
                value, connection_string = res
            else:
                value, connection_string = rem, ""
        fields[key] = value
    return fields


def create_pgpass_file(log, connection_string_or_info):
    """Look up password from the given object which can be a dict or a
    string and write a possible password in a pgpass file;
    returns a connection_string without a password in it"""
    info = get_connection_info(connection_string_or_info)
    if "password" not in info:
        return create_connection_string(info)
    content = "{host}:{port}:{dbname}:{user}:{password}\n".format(
        host=info.get("host", ""), port=info.get("port", 5432),
        user=info.get("user", ""), password=info.pop("password"),
        dbname=info.get("dbname", "*"))
    pgpass_path = os.path.join(os.environ.get("HOME"), ".pgpass")
    if os.path.exists(pgpass_path):
        with open(pgpass_path, "r") as fp:
            pgpass_data = fp.read()
    else:
        pgpass_data = ""
    if content in pgpass_data:
        log.debug("Not adding authentication data to: %s since it's already there", pgpass_path)
    else:
        with open(pgpass_path, "a") as fp:
            os.fchmod(fp.fileno(), 0o600)
            fp.write(content)
        log.debug("Wrote %r to %r", content, pgpass_path)
    return create_connection_string(info)


def set_syslog_handler(syslog_address, syslog_facility, logger):
    syslog_handler = logging.handlers.SysLogHandler(address=syslog_address, facility=syslog_facility)
    logger.addHandler(syslog_handler)
    formatter = logging.Formatter(syslog_format_str)
    syslog_handler.setFormatter(formatter)
    return syslog_handler


def set_subprocess_stdout_and_stderr_nonblocking(proc):
    for fd in [proc.stdout.fileno(), proc.stderr.fileno()]:
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)


def convert_pg_version_number_to_numeric(version_string):
    parts = version_string.split(".")
    return int(parts[0]) * 10000 + int(parts[1]) * 100 + int(parts[2])
