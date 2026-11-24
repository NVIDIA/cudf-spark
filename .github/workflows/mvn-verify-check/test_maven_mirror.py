# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Check hosted Maven routing for a fresh runner and one configured by setup-java."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET


SETUP = Path(__file__).resolve().parents[1] / "setup-maven-mirror.py"


class MavenMirrorTest(unittest.TestCase):
    def test_central_and_wrapper_share_the_mirror(self):
        for existing in (False, True):
            with self.subTest(existing=existing), tempfile.TemporaryDirectory() as directory:
                runner = Path(directory)
                settings = runner / ".m2/settings.xml"
                github_env = runner / "github-env"
                if existing:
                    settings.parent.mkdir()
                    settings.write_text(
                        '<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0">'
                        '<servers><server><id>github</id></server></servers>'
                        '<mirrors><mirror><id>elsewhere</id><mirrorOf>other-repo</mirrorOf>'
                        '</mirror></mirrors></settings>')
                env = dict(os.environ, HOME=directory, GITHUB_ENV=str(github_env))
                for _ in range(2):
                    subprocess.run([sys.executable, str(SETUP)], env=env, check=True)
                root = ET.parse(settings).getroot()
                mirrors = {m.findtext("{*}id"): m for m in root.findall("{*}mirrors/{*}mirror")}
                google = mirrors["google-maven-central"]
                self.assertEqual(google.findtext("{*}mirrorOf"), "central,unity-catalog-maven-central")
                self.assertEqual(root.findtext("{*}servers/{*}server/{*}id"), "github" if existing else None)
                self.assertEqual(len(root.findall("{*}mirrors/{*}mirror")), 2 if existing else 1)
                if existing:
                    self.assertEqual(mirrors["elsewhere"].findtext("{*}mirrorOf"), "other-repo")
                url = google.findtext("{*}url")
                self.assertEqual(url, "https://maven-central.storage-download.googleapis.com/maven2")
                self.assertEqual(github_env.read_text().splitlines(), [f"MVNW_REPOURL={url}"] * 2)
