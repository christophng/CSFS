import pickle
import uuid

from kademlia.network import Server

class Session:
    def __init__(self, server: Server):
        self.session_id = None
        self.nodes = {}  # Dictionary to store nodes connected to this session
        self.server = server

    async def add_session_node(self, node_id, node_ip):
        # Add a node to the session nodes in the DHT
        self.nodes[node_id] = node_ip
        print(f"Adding node to session: {node_id}, {node_ip}")
        await self.store_session_nodes()

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
        print(f"Nodes now stored: {self.nodes}")

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
        # Retrieve the session nodes from the DHT and update the local nodes dictionary
        key = f"session_nodes:{self.session_id}"
        value_bytes = await self.server.get(key.encode())
        if value_bytes:
            self.nodes = pickle.loads(value_bytes)
        print(f"Updated session nodes. New local session data: {self.nodes}")
