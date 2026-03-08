from azure.storage.filedatalake import DataLakeDirectoryClient

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
        self.data = self._load()

    def _load(self):
        pass

    def get(self):
        pass

    def update(self):
        pass

    def commit(self):
        pass