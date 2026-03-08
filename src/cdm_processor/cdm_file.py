from azure.storage.filedatalake import DataLakeFileClient
from partition_queue import PartitionQueue, Partition

class CdmFile:

    def __init__(
        self,
        source: DataLakeFileClient
        ):
        self.source = source
        
        # file settings
        self.partition_size = 256 * 1024 * 1024  # 256 MB
        self.chunk_size = 10 * 1024 * 1024  # 10 MB

        # api retry settings
        self.max_retries = 10
        self.backoff_factor = 0.5
        
        # processing
        self.bytes_processed = 0
        self.size = None

    def stream(
        self, 
        offset: int, 
        queue: PartitionQueue
    ) -> Partition | None:
        pass