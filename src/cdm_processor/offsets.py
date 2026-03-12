
from azure.storage.filedatalake import DataLakeDirectoryClient
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
import json

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

@dataclass
class Offset:
    source_file: str
    offset: int
    file_last_processed: datetime
    updated_at: datetime

    @staticmethod
    def from_dict(d: dict) -> "Offset":
        d = d.copy()
        d["file_last_processed"] = datetime.fromisoformat(d["file_last_processed"])
        d["updated_at"] = datetime.fromisoformat(d["updated_at"])
        return Offset(**d)
    
    def to_dict(self) -> dict:
        return {
            "source_file": self.source_file,
            "offset": self.offset,
            "file_last_processed": self.file_last_processed.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }

@dataclass
class OffsetFile:
    offsets: dict[str, Offset]
    updated_at: datetime

    @staticmethod
    def from_dict(d: dict) -> "OffsetFile":
        offsets = {o["source_file"]: Offset.from_dict(o) for o in d["offsets"]}
        updated_at = datetime.fromisoformat(d["updated_at"])
        return OffsetFile(offsets=offsets, updated_at=updated_at)

    def to_dict(self) -> dict:
        return {
            "offsets": [o.to_dict() for k, o in self.offsets.items()],
            "updated_at": self.updated_at.isoformat()
        }

class OffsetStore:

    """
    class to provide an in-memory cache for offsets for a file streamer. 
    This is used to keep track of the last read position in a file,
    so that the file streamer can resume reading from that position in case of a failure or restart.
    """

    def __init__(
        self, 
        directory_client: DataLakeDirectoryClient
    ):

        self.dir = directory_client
        self.file = directory_client.get_file_client("_offsets/offsets.json")
        self._store = self._load()
        self._lock = Lock()

    @property
    def tracked_files(self) -> list[str]:
        return list(self._store.offsets.keys())
    
    @property
    def offsets(self) -> dict[str, Offset]:
        return self._store.offsets

    def _load(self):
        if not self.file.exists():
            self.file.create_file()
            return OffsetFile(
                offsets={},
                updated_at=datetime.now(timezone.utc)
            )

        download = self.file.download_file().readall()
        data = json.loads(download)
        return OffsetFile.from_dict(data)

    def get(self, source_file: str) -> Offset:
        with self._lock:
            if source_file not in self.tracked_files:
                return Offset(
                    source_file=source_file,
                    offset=0,
                    file_last_processed=EPOCH,
                    updated_at=EPOCH
                )
            return self._store.offsets[source_file]

    def update(
        self, 
        source_file: str, 
        offset: int,
        file_last_processed: datetime
    ):
        with self._lock:
            self._store.offsets[source_file] = Offset(
                source_file=source_file,
                offset=offset,
                file_last_processed=file_last_processed,
                updated_at=datetime.now(timezone.utc)
            )

    def commit(self):
        with self._lock:
            self._store.updated_at = datetime.now(timezone.utc)
            json_data = json.dumps(self._store.to_dict())
            self.file.upload_data(
                data=json_data,
                overwrite=True
            )
