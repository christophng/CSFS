import logging
import pickle
import uuid

from kademlia.network import Server

from globals import FILE_METADATA_KEY_PREFIX

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("session")

class Session:
    def __init__(self, server: Server):
        self.session_id = None
        self.nodes = {}  # Dictionary to store nodes connected to this session
        self.server = server
        self.files = {}

    async def add_session_node(self, node_id, node_ip):
        # Add a node to the session nodes in the DHT
        self.nodes[node_id] = node_ip
        logger.debug(f"Adding node to session: {node_id}, {node_ip}")
        await self.store_session_nodes()
        logger.debug(f"Nodes now stored: {self.nodes}")

    async def remove_session_node(self, node_id):
        # Remove a node from the session nodes in the DHT
        if node_id in self.nodes:
            del self.nodes[node_id]
            await self.store_session_nodes()

    async def store_session_nodes(self):
        # Store the session nodes in the DHT under the key session_nodes:{session_id}
        key = f"session_nodes:{self.session_id}"
        value = self.nodes
        await self.server.set(key.encode(), pickle.dumps(value))
        logger.debug(f"Nodes now stored: {self.nodes}")

    def generate_session_id(self):
        self.session_id = str(uuid.uuid4())

    def set_session_id(self, session_id):
        self.session_id = session_id

    def get_session_id(self):
        return self.session_id

    async def get_session_nodes(self):
        await self.update_session_nodes()
        return self.nodes

    async def update_session_nodes(self):
        # # Retrieve the session nodes from the DHT and update the local nodes dictionary
        # key = f"session_nodes:{self.session_id}"
        # value_bytes = await self.server.get(key.encode())
        # if value_bytes:
        #     self.nodes = pickle.loads(value_bytes)
        logger.debug(f"Updated session nodes. New local session data: {self.nodes}")

    async def add_file_metadata(self, file_metadata):
        try:
            file_name = file_metadata["file_name"]

            # Construct the key for storing file metadata in the Kademlia DHT
            key = f"{FILE_METADATA_KEY_PREFIX}:{file_name}"

            # Add file metadata to the local session class
            self.files[key] = file_metadata

            # Store the file metadata in the Kademlia DHT
            await self.server.set(key.encode(), pickle.dumps(file_metadata))

            logger.debug(f"File metadata added to local session and Kademlia DHT: {file_metadata}")
        except Exception as e:
            logger.debug(f"Error adding file metadata: {e}")

    async def get_file_metadata(self, file_name):
        """
        Retrieves file metadata from the DHT.
        """
        try:
            key = f"{FILE_METADATA_KEY_PREFIX}:{file_name}"
            # Use the Kademlia get method to retrieve the file metadata from the DHT
            pickled_data = await self.server.get(key)
            if pickled_data:
                # Unpickle the retrieved data
                file_metadata = pickle.loads(pickled_data)
                return file_metadata
            else:
                return None
        except Exception as e:
            # Handle any errors that occur during the retrieval process
            print(f"Error retrieving file metadata: {e}")
            return None
