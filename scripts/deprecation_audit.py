# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Collect compiler deprecations from GitHub Actions logs using Jython 2.7-compatible code."""

from __future__ import print_function

import argparse
import glob
import io
import json
import os
import re
import sys
import time
import zipfile

try:
    import urllib.error as urllib_error
    import urllib.parse as urllib_parse
    import urllib.request as urllib_request
except ImportError:  # Jython 2.7 / Python 2.7
    import urllib2 as urllib_error
    import urllib2 as urllib_request
    import urlparse as urllib_parse


ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
SOURCE_LOCATION = re.compile(
    r"(?P<path>(?:[A-Za-z]:)?[^\s\[\]]+?\.(?:scala|java))"
    r"(?::(?P<line>\d+)|:\[(?P<bracket_line>\d+),\d+\])"
)
DEPRECATION = re.compile(r"\bdeprecated\b", re.IGNORECASE)
ORIGIN = re.compile(r"\borigin(?:=|:)\s*(?P<origin>[\w.$]+)")
DEFAULT_JOB_PATTERN = (
    r"^(?:package-tests(?:-scala213)?|verify-213-modules|"
    r"verify-all-212-modules|install-modules)(?:\s|$)"
)
NVIDIA_ORIGIN_PREFIXES = (
    "ai.rapids.cudf.",
    "com.nvidia.spark.rapids.",
    "org.apache.spark.sql.rapids.",
)
NVIDIA_ORIGIN_SYMBOLS = (
    "org.apache.spark.sql.execution.aggregate.PartialAggUtils",
)


def is_nvidia_origin(origin):
    if origin.startswith(NVIDIA_ORIGIN_PREFIXES):
        return True
    return any(
        origin == symbol or origin.startswith((symbol + ".", symbol + "$"))
        for symbol in NVIDIA_ORIGIN_SYMBOLS
    )


BAD_ZIP_ERROR = getattr(zipfile, "BadZipFile", zipfile.BadZipfile)


class Finding(object):
    def __init__(self, path, line, message, origin="", jobs=None):
        self.path = path
        self.line = line
        self.message = message
        self.origin = origin
        self.jobs = set(jobs or ())

    @property
    def owner(self):
        if is_nvidia_origin(self.origin):
            return "NVIDIA"
        return "third-party/unknown"

    def key(self):
        diagnostic = self.origin or self.message
        return self.path, self.line, diagnostic


def clean_line(line):
    return ANSI_ESCAPE.sub("", line).rstrip()


def read_text(path):
    with io.open(path, "r", encoding="utf-8", errors="replace") as input_file:
        return input_file.read()


def normalize_path(path, repo_root):
    candidate = os.path.normpath(path)
    if not os.path.isabs(candidate):
        return candidate.replace(os.sep, "/")
    root = os.path.realpath(repo_root)
    resolved = os.path.realpath(candidate)
    relative = os.path.relpath(resolved, root)
    if relative != os.pardir and not relative.startswith(os.pardir + os.sep):
        return relative.replace(os.sep, "/")
    parts = candidate.split(os.sep)
    for index in range(len(parts)):
        suffix = os.path.join(*parts[index:])
        if os.path.exists(os.path.join(root, suffix)):
            return suffix.replace(os.sep, "/")
    return candidate.replace(os.sep, "/")


def parse_log(text, job_name, repo_root="."):
    lines = [clean_line(line) for line in text.splitlines()]
    findings = []
    for index, line in enumerate(lines):
        if not DEPRECATION.search(line):
            continue
        location = SOURCE_LOCATION.search(line)
        if location is None:
            for previous in reversed(lines[max(0, index - 3):index]):
                location = SOURCE_LOCATION.search(previous)
                if location is not None:
                    break
        if location is None:
            continue
        origin = ""
        for context in lines[index:min(len(lines), index + 6)]:
            origin_match = ORIGIN.search(context)
            if origin_match is not None:
                origin = origin_match.group("origin")
                break
        line_number = location.group("line") or location.group("bracket_line")
        message = re.sub(r"^.*?\.(?:scala|java)(?::\d+|:\[\d+,\d+\])\s*:?[ ]*", "", line)
        findings.append(Finding(
            path=normalize_path(location.group("path"), repo_root),
            line=int(line_number),
            message=message.strip() or line.strip(),
            origin=origin,
            jobs={job_name},
        ))
    return findings


def merge_findings(findings):
    merged = {}
    for finding in findings:
        existing = merged.get(finding.key())
        if existing is None:
            merged[finding.key()] = finding
        else:
            existing.jobs.update(finding.jobs)
    return sorted(
        merged.values(), key=lambda finding: (finding.path, finding.line, finding.message))


def request_json(url, token):
    request = urllib_request.Request(url, headers={
        "Accept": "application/vnd.github+json",
        "Authorization": "Bearer {0}".format(token),
        "X-GitHub-Api-Version": "2022-11-28",
    })
    response = urllib_request.urlopen(request, timeout=30)
    try:
        return json.load(response)
    finally:
        response.close()


class JobLogRedirectError(RuntimeError):
    """The GitHub job-log endpoint returned an unsafe or malformed redirect."""


class NoRedirectHandler(urllib_request.HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None


def request_bytes(url, token):
    """Download a GitHub API resource without forwarding credentials on its redirect."""
    request = urllib_request.Request(url, headers={
        "Accept": "application/vnd.github+json",
        "Authorization": "Bearer {0}".format(token),
        "X-GitHub-Api-Version": "2022-11-28",
    })
    opener = urllib_request.build_opener(NoRedirectHandler())
    try:
        response = opener.open(request, timeout=30)
        try:
            raise JobLogRedirectError(
                "GitHub job-log endpoint did not return the expected redirect")
        finally:
            response.close()
    except urllib_error.HTTPError as error:
        if error.code != 302:
            error.close()
            raise
        headers = getattr(error, "headers", None)
        if headers is None:
            headers = error.hdrs
        location = headers.get("Location")
        error.close()
    if not location:
        raise JobLogRedirectError(
            "GitHub job-log redirect did not include a Location header")
    parsed_location = urllib_parse.urlsplit(location)
    if parsed_location.scheme != "https" or not parsed_location.netloc:
        raise JobLogRedirectError(
            "GitHub job-log redirect must use an absolute HTTPS URL")

    # The redirect is a short-lived signed URL. It authorizes itself, so use a fresh request
    # without the repository-scoped GitHub token or GitHub-specific API headers.
    signed_request = urllib_request.Request(location)
    response = urllib_request.urlopen(signed_request, timeout=30)
    try:
        return response.read()
    finally:
        response.close()


def decode_job_log(payload):
    if payload.startswith(b"PK"):
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            return u"\n".join(
                archive.read(name).decode("utf-8", errors="replace")
                for name in archive.namelist()
                if not name.endswith("/")
            )
    return payload.decode("utf-8", errors="replace")


def download_logs(api_url, repository, run_id, token, job_pattern):
    matcher = re.compile(job_pattern)
    jobs = []
    page = 1
    while True:
        result = request_json(
            "{0}/repos/{1}/actions/runs/{2}/jobs?per_page=100&page={3}".format(
                api_url, repository, run_id, page),
            token,
        )
        page_jobs = result.get("jobs", [])
        jobs.extend(page_jobs)
        if len(page_jobs) < 100:
            break
        page += 1

    logs = {}
    failures = []
    for job in jobs:
        name = job.get("name", "")
        if job.get("status") != "completed" or matcher.search(name) is None:
            continue
        error = None
        for delay in (0, 1, 2, 4):
            if delay:
                time.sleep(delay)
            try:
                payload = request_bytes(
                    "{0}/repos/{1}/actions/jobs/{2}/logs".format(
                        api_url, repository, job["id"]), token)
                logs[name] = decode_job_log(payload)
                error = None
                break
            except (OSError, urllib_error.HTTPError, BAD_ZIP_ERROR,
                    JobLogRedirectError) as caught:
                error = caught
        if error is not None:
            failures.append(u"{0}: {1}".format(name, error))
    if not logs and not failures:
        failures.append("no completed build-matrix job logs matched the configured job pattern")
    return logs, failures


def markdown_escape(value):
    return value.replace("|", "\\|").replace("\n", " ")


def render_summary(findings, failures):
    lines = [u"## NVIDIA deprecation audit", u""]
    if findings:
        lines.extend([
            u"Found {0} unique compiler deprecation diagnostic(s).".format(len(findings)),
            u"",
            u"| Owner | Location | Deprecated API | Matrix jobs |",
            u"| --- | --- | --- | --- |",
        ])
        for finding in findings[:200]:
            location = u"`{0}:{1}`".format(finding.path, finding.line)
            api = finding.origin or finding.message
            jobs = u", ".join(sorted(finding.jobs))
            lines.append(
                u"| {0} | {1} | `{2}` | {3} |".format(
                    finding.owner, location, markdown_escape(api), markdown_escape(jobs))
            )
        if len(findings) > 200:
            lines.extend([
                u"",
                u"Report truncated; see the raw artifact for all {0} findings.".format(
                    len(findings)),
            ])
    else:
        lines.append(u"No compiler deprecation diagnostics were found in the selected matrix jobs.")
    if failures:
        lines.extend([u"", u"### Incomplete log collection", u""])
        lines.extend(u"- {0}".format(markdown_escape(failure)) for failure in failures)
    lines.extend([
        u"",
        u"This check is optional. Findings or incomplete log collection fail only this audit "
        "check; build-job results remain authoritative.",
        u"",
    ])
    return u"\n".join(lines)


def command_escape(value):
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def command_property_escape(value):
    return command_escape(value).replace(":", "%3A").replace(",", "%2C")


def emit_annotations(findings):
    for finding in findings[:50]:
        message = finding.origin or finding.message
        print(
            u"::warning file={0},line={1},title=NVIDIA deprecation::{2}".format(
                command_property_escape(finding.path), finding.line, command_escape(message))
        )
    if len(findings) > 50:
        print(u"::warning title=NVIDIA deprecation::Only 50 of {0} findings were annotated".format(
            len(findings)))


def write_raw_report(path, findings, failures):
    report = {
        "findings": [
            {
                "owner": finding.owner,
                "path": finding.path,
                "line": finding.line,
                "message": finding.message,
                "origin": finding.origin,
                "jobs": sorted(finding.jobs),
            }
            for finding in findings
        ],
        "log_collection_failures": failures,
    }
    with io.open(path, "w", encoding="utf-8") as report_file:
        report_file.write(json.dumps(report, indent=2, ensure_ascii=False) + u"\n")


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID"))
    parser.add_argument(
        "--api-url", default=os.environ.get("GITHUB_API_URL", "https://api.github.com"))
    parser.add_argument("--job-pattern", default=DEFAULT_JOB_PATTERN)
    parser.add_argument(
        "--logs-dir", default=os.environ.get("DEPRECATION_AUDIT_LOGS_DIR"),
        help="Parse local *.log files instead of downloading job logs")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--summary", default=os.environ.get("GITHUB_STEP_SUMMARY"))
    parser.add_argument("--raw-report", default="nvidia-deprecation-audit.json")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    failures = []
    try:
        if args.logs_dir:
            logs = {
                os.path.splitext(os.path.basename(path))[0]: read_text(path)
                for path in glob.glob(os.path.join(args.logs_dir, "*.log"))
            }
        else:
            token = os.environ.get("GITHUB_TOKEN")
            if not token or not args.repository or not args.run_id:
                raise ValueError("GITHUB_TOKEN, repository, and run ID are required")
            logs, failures = download_logs(
                args.api_url, args.repository, args.run_id, token, args.job_pattern)
        findings = merge_findings(
            finding
            for job_name, log in logs.items()
            for finding in parse_log(log, job_name, args.repo_root)
        )
    except Exception as error:  # Report operational errors through this optional check.
        findings = []
        failures.append(u"audit failed: {0}".format(error))

    summary = render_summary(findings, failures)
    print(summary)
    emit_annotations(findings)
    if args.summary:
        with io.open(args.summary, "a", encoding="utf-8") as summary_file:
            summary_file.write(summary)
    report_failed = False
    try:
        write_raw_report(args.raw_report, findings, failures)
    except (IOError, OSError) as error:
        report_failed = True
        print(u"::warning title=NVIDIA deprecation audit::Could not write raw report: {0}".format(
            error))
    return 1 if findings or failures or report_failed else 0


if __name__ == "__main__":
    sys.exit(main())
