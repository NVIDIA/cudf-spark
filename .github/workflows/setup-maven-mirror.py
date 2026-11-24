# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Route Maven Central and Maven Wrapper downloads through Google on hosted CI runners."""

import os
from pathlib import Path
import xml.etree.ElementTree as ET


GOOGLE_CENTRAL = "https://maven-central.storage-download.googleapis.com/maven2"


def configure(settings_file):
    if settings_file.is_file():
        root = ET.parse(settings_file).getroot()
    else:
        root = ET.Element("settings")
    ns = root.tag.removesuffix("settings")
    if ns:
        ET.register_namespace("", ns[1:-1])

    mirrors = root.find(ns + "mirrors")
    if mirrors is None:
        mirrors = ET.SubElement(root, ns + "mirrors")
    mirror_id = "google-maven-central"
    for entry in mirrors.findall(ns + "mirror"):
        if entry.findtext(ns + "id") == mirror_id:
            mirrors.remove(entry)
    mirror = ET.Element(ns + "mirror")
    ET.SubElement(mirror, ns + "id").text = mirror_id
    ET.SubElement(mirror, ns + "url").text = GOOGLE_CENTRAL
    # The Unity Catalog fixture declares Central under its own repository ID.
    ET.SubElement(mirror, ns + "mirrorOf").text = "central,unity-catalog-maven-central"
    mirrors.insert(0, mirror)
    settings_file.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(root).write(settings_file, encoding="utf-8", xml_declaration=True)


if __name__ == "__main__":
    github_env = Path(os.environ["GITHUB_ENV"])
    configure(Path.home() / ".m2" / "settings.xml")
    # The wrapper downloads its Maven distribution before Maven reads settings.xml.
    with github_env.open("a", encoding="utf-8") as env_file:
        print(f"MVNW_REPOURL={GOOGLE_CENTRAL}", file=env_file)
