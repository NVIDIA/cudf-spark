# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Filter untrusted Databricks log records on the controller, not on the PR driver."""

import base64
import json
import os
import re
import stat
from ipaddress import IPv6Address
from urllib.parse import urlsplit


_OMITTED = "[... omitted ...]"
# Only clean, public repository URLs may leave the driver. Never echo URL credentials,
# query strings, fragments or private workspaces simply because a download failed.
_URL = re.compile(r"\b[a-z][a-z0-9+.-]*://[^\s<>\"']+", re.I)
_PUBLIC_REPOS = {
    "repo.maven.apache.org", "repo1.maven.org", "repository.apache.org", "archive.apache.org",
    "downloads.apache.org", "maven-central.storage-download.googleapis.com",
    "oss.sonatype.org", "s01.oss.sonatype.org", "central.sonatype.com", "repo.scala-sbt.org",
    "pypi.org", "files.pythonhosted.org", "github.com", "raw.githubusercontent.com",
}
_SECRET = re.compile(
    r"(?:\b(?:authorization|proxy-authorization|cookie|set-cookie)\s*:|\bBearer\s+[\w+/=-]+|"
    r"\bBasic\s+[A-Za-z0-9+/]{12,}={0,2}\b|"
    r"(?<![a-z0-9])(?:password|passwd|pwd|passphrase|(?:client[_-]?)?secret|api[_-]?key|"
    r"(?:secret[_-]?)?access[_-]?key(?:[_-]?id)?|sig(?:nature)?|(?:sharedaccess)?signature|"
    r"(?:storage)?account[_-]?key|fs\.azure\.account\.key(?:\.[\w.-]+)?|credential[s]?|"
    r"x-amz-security-token|(?:access|refresh|session|id)[_-]?token)[\"']?\s*[:=]|"
    r"token\s*=|[\"']token[\"']\s*:|^\s*token\s*:|--(?:password|token|secret|api-key)\s+\S|"
    r"<(?:password|passphrase|privateKey|secret|token)\b|"
    r"\b(?:gh[pousr]_\w{12,}|github_pat_\w+|glpat-[\w-]{10,}|dapi[\da-f]{16,}|(?:AKIA|ASIA)[A-Z0-9]{16}|"
    r"xox[baprs]-[\w-]+|eyJ[\w-]{6,}\.[\w-]+\.[\w-]+|ssh-(?:rsa|ed25519)\s+\S+)|"
    r"^\s*(?:machine\s+\S+\s+|(?:login|password|passwd)\s+\S)|\b(?:Proc-Type|DEK-Info):)", re.I)
_NETWORK = re.compile(
    r"\b(?:[\w-]+\.)+(?:internal|localdomain|corp|local|databricks\.(?:com|net)|"
    r"nvidia\.com|amazonaws\.com|azure\.com|azuredatabricks\.net)\b|"
    r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b|\b(?:[0-9a-f]{2}:){5}[0-9a-f]{2}\b|"
    r"\b(?:ip-\d+-\d+-\d+-\d+|i-[0-9a-f]{8,17}|\d{4}-\d{6}-[a-z0-9]{6,})\b|"
    r"\b(?:hostname|cluster[_. -]?(?:id|name)|workspace[_. -]?(?:id|url)|host|UnknownHostException)\s*[:=]\s*\S|"
    r"[\w.-]+@[\w.-]+", re.I)
_IPV6 = re.compile(r"(?<![\w:])(?:[0-9a-f]{0,4}:){2,}[0-9a-f:.]*(?!\w)", re.I)
_COMMAND = re.compile(
    r"^(?:\s*\[[A-Z]+\]\s*)?\s*(?:::[\w-]+(?:[ :]|$)|##(?:vso)?\[|"
    r"(?:export\s+|declare -x\s+)?[A-Z_][A-Z_0-9]*=)")
_GH_COMMAND = re.compile(r"::(?:warning|notice|error|debug|add-mask|stop-commands|set-output|"
                         r"set-env|save-state|add-path|group|endgroup|echo)(?: [^\r\n:]+)?::", re.I)
_ARMOR = re.compile(rb"-----(BEGIN|END) [A-Z0-9 ]+-----")
# At an unknown byte boundary don't publish unframed text as if it began a new
# record. Once a new build record starts, ordinary lines need no known vocabulary.
_RECORD = re.compile(rb"^(?:\[(?:INFO|WARN(?:ING)?|ERROR|DEBUG|FATAL|JUnit|driver|GPU)\b|"
                     rb"\d{2,4}[-/]\d\d[-/]\d\d[ T]|=== |Traceback \(|(?:FAIL|ERROR): )")
_HEREDOC = re.compile(r"<<-?\s*['\"]?([A-Za-z_]\w*)['\"]?")
_XML = re.compile(r"<(/?)(password|passphrase|privateKey|secret|token)>", re.I)
_TRACE = re.compile(r"^\s*(?:\[[A-Z]+\]\s*)?(?:\++\s|\$\s|>>>\s|(?:Environment|Environment variables):\s*$)")
_ENCODED_RECORD = re.compile(rb"\[[A-Z]+\]\s+[A-Za-z0-9_+/=-]+\s*")
_TRUNCATED = re.compile(rb"\[\.\.\. \d+ bytes omitted \.\.\.\]")


def _safe_text(line):
    if (_NETWORK.search(line) or _GH_COMMAND.search(line)
            or not all(c == "\t" or c.isprintable() for c in line)):
        return False
    for match in _IPV6.finditer(line):
        if match[0] != "::":
            try:
                IPv6Address(match[0])
            except ValueError:
                continue
            return False

    def public_url(match):
        try:
            url = urlsplit(match[0].rstrip(".,);]"))
            return (url.scheme in ("https", "http") and url.hostname in _PUBLIC_REPOS
                    and url.username is None and url.password is None and not url.query
                    and not url.fragment and url.port in (None, 80, 443))
        except ValueError:
            return False

    return all(public_url(m) for m in _URL.finditer(line))


def public_lines(records, hidden=()):
    clipped = False
    public = []
    armor = False
    until = None
    private_indent = None
    for raw in records:
        # Process delimiters even if their line is otherwise unsafe. If a fragment
        # starts with an orphan END, discard its potentially private leading text.
        markers = _ARMOR.findall(raw)
        if markers:
            for marker in markers:
                if marker == b"END" and not armor:
                    public.clear()
                armor = marker == b"BEGIN"
            clipped = False
            continue
        if armor:
            continue
        if len(raw) > 4096:
            public.append(_OMITTED)
            clipped = True
            continue
        if _TRUNCATED.fullmatch(raw):
            public.append(_OMITTED)
            clipped = True
            continue
        try:
            line = raw.removesuffix(b"\r").decode("utf-8")
        except UnicodeDecodeError:
            continue
        if until is not None:
            if until.fullmatch(line.strip()):
                until = None
            continue
        indent = len(line) - len(line.lstrip())
        if private_indent is not None:
            if not line.strip() or indent > private_indent:
                continue
            private_indent = None
        xml = _XML.search(line)
        if xml and xml[1] == "/":
            public.clear()  # Unmatched closing tag in a fragment supplied by the driver.
            clipped = False
            continue
        trace = _TRACE.search(line)
        if trace or _SECRET.search(line) or _COMMAND.search(line):
            if trace or line.rstrip().endswith((":", "=")):
                clipped = True  # An xtraced echo/printenv can print an arbitrary next line.
            heredoc = _HEREDOC.search(line)
            if heredoc:
                until = re.compile(re.escape(heredoc[1]))
            elif xml and "</" + xml[2].lower() + ">" not in line.lower():
                until = re.compile(r".*</" + re.escape(xml[2]) + r">.*", re.I)
            elif line.count("'") % 2 or line.count('"') % 2:
                quote = "'" if line.count("'") % 2 else '"'
                until = re.compile(r".*" + quote + r"[,;)]?")
            elif re.search(r"[:=]\s*[|>][+-]?\s*$", line) or re.match(
                    r"\s*(?:proxy-authorization|authorization|cookie|set-cookie)\s*:", line, re.I):
                private_indent = indent  # Folded HTTP header or YAML block scalar.
            continue
        if clipped and (not _RECORD.match(raw) or _ENCODED_RECORD.fullmatch(raw)):
            continue
        clipped = False
        if line.strip() and not any(s in line for s in hidden) and _safe_text(line):
            public.append(line if len(line) <= 500 else line[:500] + _OMITTED)
    return public


def snapshot(directory, finished, ok):
    """Return bounded post-run output; never follow links or block on a driver-supplied pipe."""
    if not finished:
        raise ValueError("No live preview")
    cap = 512 * 1024
    fd = os.open(directory / "console.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        meta = os.fstat(fd)
        if not stat.S_ISREG(meta.st_mode) or meta.st_nlink != 1 or meta.st_size > cap:
            raise ValueError("Invalid driver receipt")
        data = stream.read(cap + 1)
    if len(data) > cap:
        raise ValueError("Oversized driver receipt")
    chunks = json.loads(data)
    if not isinstance(chunks, list) or len(chunks) != 4 or not all(isinstance(c, str) for c in chunks):
        raise ValueError("Invalid driver streams")
    streams = []
    for encoded, max_kb in zip(chunks, (129, 129, 33, 33)):
        raw = base64.b64decode(encoded, validate=True)
        if len(raw) > max_kb * 1024:
            raise ValueError("Oversized driver stream")
        streams.append(public_lines(re.sub(rb"\x1b\[[0-9;:]*m", b"", raw).split(b"\n")))
    limit = 1000 if ok else 10
    diag = [] if ok else (streams[2] + streams[3] or (streams[1] or streams[0])[-80:])
    if len(diag) > 1000:
        diag = diag[:499] + [_OMITTED] + diag[-500:]
    return {"stdout": streams[0][-limit:], "stderr": streams[1][-limit:], "diagnostics": diag}
