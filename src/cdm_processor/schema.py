from azure.storage.filedatalake import DataLakeDirectoryClient, FileSystemClient

class CdmSchema:
    """
    class to represent the schema of a CDM table.
    """
    
    def __init__(
        self, 
        name: str, 
        columns: list[dict]
    ):
        self.name = name
        self.columns = columns

    def to_arrow_schema(self):
        pass

    

class CdmModel:
    """
    class to represent the entire model.json file
    """

    def __init__(
        self, 
        client: FileSystemClient | DataLakeDirectoryClient
    ):
        self.client = client
        self.file = self.client.get_file_client("model.json")

    def load(self):
        pass