# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
import unittest
import xml.etree.ElementTree as ET


class MavenMirrorTest(unittest.TestCase):
    def test_central_and_wrapper_share_the_mirror(self):
        settings = Path(__file__).with_name("settings.xml")
        root = ET.parse(settings).getroot()
        mirror, = root.findall("{*}mirrors/{*}mirror")
        self.assertEqual(mirror.findtext("{*}mirrorOf"), "central,unity-catalog-maven-central")
        self.assertFalse(root.findall("{*}servers"))
        workflow = settings.parents[2] / "workflows/mvn-verify-check.yml"
        self.assertIn(f'MVNW_REPOURL: {mirror.findtext("{*}url")}', workflow.read_text())
