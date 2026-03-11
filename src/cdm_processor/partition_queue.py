from queue import Queue, Full, Empty
from dataclasses import dataclass

@dataclass
class Partition:
    source_file: str
    start_offset: int
    end_offset: int
    data: bytes

class PartitionQueue:
    """
    A thread-safe queue for managing data partitions between producer and consumer threads.
    """

    def __init__(
        self,
        max_size: int = 10
    ):
        self.max_size = max_size
        self.queue = Queue(maxsize=self.max_size)  # Limit queue size to prevent memory overflow

    def put(self, partition: Partition):
        pass

    def get(self) -> Partition | None:
        pass
        