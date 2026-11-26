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

    def test_iceberg_packages_prefer_staged_maven_then_the_same_public_mirror(self):
        settings = Path(__file__).with_name("settings.xml")
        google = ET.parse(settings).findtext("{*}mirrors/{*}mirror/{*}url")
        ivy = ET.parse(settings.with_name("ivysettings.xml"))
        chain, = ivy.findall("resolvers/chain")
        self.assertEqual(ivy.find("settings").get("defaultResolver"), chain.get("name"))
        self.assertEqual(chain.get("returnFirst"), "true")
        self.assertEqual([(r.tag, r.get("m2compatible"), r.get("root").rstrip("/")) for r in chain],
                         [("ibiblio", "true", "file:${user.home}/.m2/repository"),
                          ("ibiblio", "true", google)])
        for suffix in ("sources", "javadoc"):
            self.assertEqual(ivy.find(f"property[@name='ivy.maven.lookup.{suffix}']").get("value"), "false")
