import asyncio
import logging
import threading
from queue import Queue

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from globals import TRACKED_FOLDER_PATH

logger = logging.getLogger("filewatcher")


class FileSharing:
    def __init__(self, node):
        self.node = node

    # async def handle_new_file(self, file_path):
    #     """
    #     Handles the addition of a new file to the tracked folder
    #     """
    #     # Extract file metadata
    #     file_metadata = self.extract_file_metadata(file_path)
    #     # Broadcast file addition message to other connected clients
    #     await self.node.broadcast_file_addition(file_metadata)

    def extract_file_metadata(self, file_path):
        """
        Extracts metadata from the newly added file
        """
        import os
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        file_type = os.path.splitext(file_path)[1]
        file_owner = self.node.node_id  # Assuming the node is the owner of the file
        file_owner_ip = self.node.communication.get_local_ipv4()
        file_metadata = {
            "file_name": file_name,
            "file_size": file_size,
            "file_type": file_type,
            "file_owner": file_owner,
            "file_owner_ip": file_owner_ip,
        }
        logging.debug(f"Generated the following file metadata: {file_metadata}")
        return file_metadata

    async def update_kademlia_dht(self, file_metadata):
        """
        Updates the Kademlia DHT with the metadata of the added file
        """
        self.node.session.add_file_metadata(file_metadata)


class FileEventHandler(FileSystemEventHandler):
    def __init__(self, file_sharing: FileSharing):
        super().__init__()
        self.file_sharing = file_sharing

    def on_created(self, event):
        thread = threading.Thread(target=self.handle_file_creation, args=(event,))
        thread.start()

    def handle_file_creation(self, event):
        asyncio.run(self.async_handle_file_creation(event))

    async def async_handle_file_creation(self, event):
        file_path = event.src_path
        file_metadata = self.file_sharing.extract_file_metadata(file_path)
        await self.file_sharing.node.broadcast_file_addition(file_metadata)


class FileWatcher:
    def __init__(self, file_sharing):
        self.file_sharing = file_sharing
        self.folder_path = TRACKED_FOLDER_PATH
        self.observer = Observer()
        self.event_handler = FileEventHandler(self.file_sharing)

    async def start(self):
        self.observer.schedule(self.event_handler, self.folder_path, recursive=False)
        self.observer.start()
        logger.debug(f"Watching folder: {self.folder_path}")

    async def stop(self):
        self.observer.stop()
        self.observer.join()




