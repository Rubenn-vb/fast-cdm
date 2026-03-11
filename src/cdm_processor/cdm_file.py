from typing import Generator
from azure.storage.filedatalake import DataLakeFileClient
from azure.core.exceptions import ResourceModifiedError
from partition_queue import Partition
import logging
import time

class FileStreamer:

    def __init__(
        self,
        source: DataLakeFileClient,
        partition_size: int = 256 * 1024 * 1024, # 256 MB
        chunk_size: int = 10 * 1024 * 1024 # 10 MB
    ):
        self.source = source
        
        # file settings
        self.partition_size = partition_size
        self.chunk_size = chunk_size
        

    def get_stable_size(
        self, 
        timeout: int = 30
    ) -> int:       
        props = self.source.get_file_properties()
        size = props.size
        start_time = time.monotonic()

        while True:
            time.sleep(0.5)
            props = self.source.get_file_properties()

            if props.size == size:
                return size  # file size is stable

            if time.monotonic() - start_time > timeout:
                raise TimeoutError(
                    f"Timeout reached while waiting for stable file size for {self.source.path_name}"
                )


    def stream(
        self, 
        offset: int,
        end_offset: int,
        max_retries: int = 10,
        backoff_factor: float = 0.5
    ) -> Generator[Partition]:
        
        bytes_processed = 0
        buffer = bytearray()

        for attempt in range(1, max_retries + 1):
            current_offset = offset + bytes_processed
            bytes_remaining = end_offset - offset - bytes_processed

            if bytes_remaining <= 0:
                break # all bytes processed

            try:
                stream = self.source.download_file(
                    offset=current_offset, 
                    length=bytes_remaining, 
                    if_match=None,
                    chunk_size=self.chunk_size
                )

                for chunk in stream.chunks():
                    buffer.extend(chunk)

                    if len(buffer) >= self.partition_size:
                        pos = buffer.rfind(b"\n")

                        if pos == -1 and len(buffer) > self.partition_size * 2:
                            if len(buffer) > self.partition_size:
                                raise ValueError(
                                    f"Row exceeds maximum partition size ({self.partition_size} bytes) "
                                    f"in file {self.source.path_name}"
                                )
                            continue
                        
                        logging.info(f"Processing partition of size {len(buffer)} bytes")
                        yield Partition(
                            source_file=self.source.path_name,
                            start_offset=offset + bytes_processed,
                            end_offset=offset + bytes_processed + pos + 1,
                            data=bytes(buffer[:pos + 1]) # Send complete lines
                        )
                        bytes_processed += pos + 1
                        del buffer[:pos + 1] # Keep partial rows in-memory

                if len(buffer) > 0:
                    logging.info(f"Processing final partition of size {len(buffer)} bytes")
                    yield Partition(
                        source_file=self.source.path_name,
                        start_offset=offset + bytes_processed,
                        end_offset=offset + bytes_processed + len(buffer),
                        data=bytes(buffer) # Send remaining bytes
                    )
                    bytes_processed += len(buffer)

            except ResourceModifiedError as e:
                logging.warning(f"Caught exception: {type(e)} {e}")
                logging.debug(f"Bytes processed at this point: {bytes_processed}")
                logging.debug(
                    f"Reattempting to load chunk. Attempt  {attempt}/{max_retries}"
                )

                # exponential backoff before retrying
                sleep = backoff_factor * (2 ** (attempt - 1))
                logging.debug(f"Sleeping for {sleep} seconds before retrying...")
                time.sleep(sleep)

                if attempt == max_retries:
                    raise RuntimeError(
                        f"Max retries reached when attempting to read file {self.source.path_name}: ",
                        e,
                    )