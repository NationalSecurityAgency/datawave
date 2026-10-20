#!/usr/bin/env python3
"""Check the complete set of records returned by a proxied-entity query."""

import sys
import re
import xml.etree.ElementTree as ET


def local_name(element):
    return element.tag.rsplit("}", 1)[-1]


def check(response_path, expected_csv):
    # createAndExecute streams one XML response per page, so the body can
    # contain several XML declarations and root elements.
    with open(response_path, encoding="utf-8") as response:
        body = re.sub(r"<\?xml[^?]*\?>", "", response.read())
    root = ET.fromstring("<Responses>" + body + "</Responses>")
    expected = set(expected_csv.split(","))
    actual = []
    returned = []

    for element in root.iter():
        if local_name(element) == "ReturnedEvents":
            returned.append(int(element.text))
        if local_name(element) != "Event":
            continue
        ids = [
            "".join(value.itertext())
            for field in element.iter()
            if local_name(field) == "Field" and field.get("name") == "PROXY_ID"
            for value in field
            if local_name(value) == "Value"
        ]
        if len(ids) != 1:
            raise AssertionError(f"Expected one PROXY_ID per event, got {ids}")
        actual.extend(ids)

    if len(actual) != sum(returned) or len(actual) != len(set(actual)) or set(actual) != expected:
        raise AssertionError(
            f"Expected records {sorted(expected)}, got {sorted(actual)}; "
            f"ReturnedEvents={returned}"
        )


if __name__ == "__main__":
    try:
        check(sys.argv[1], sys.argv[2])
    except (AssertionError, ET.ParseError, ValueError) as error:
        print(f"FAIL proxied query response: {error}", file=sys.stderr)
        sys.exit(1)
