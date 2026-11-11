"""Incremental reads of append-only journals; never open a live replaceable report."""

import json
from pathlib import Path


class AppendJournal:
    def __init__(self, path, reader):
        self.path = Path(path)
        self.reader = reader
        self.offset = 0
        self.partial = b""
        self.generation = 0

    def updates(self):
        try:
            size = self.path.stat().st_size
        except OSError:
            return
        if size < self.offset:
            self.offset = 0
            self.partial = b""
            self.generation += 1
        while self.offset < size:
            try:
                raw = self.reader(self.path, 1024 * 1024, offset=self.offset)
            except OSError:
                return
            if not raw:
                break
            self.offset += len(raw)
            parts = (self.partial + raw).split(b"\n")
            self.partial = parts.pop()
            if len(self.partial) > 1024 * 1024:
                self.partial = b""
            for line in parts:
                try:
                    item = json.loads(line)
                    if isinstance(item, dict):
                        yield item
                except (ValueError, UnicodeError):
                    continue
