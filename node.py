import asyncio
import inspect
import logging
import threading

from kademlia.network import Server

from communication import Communication
from globals import LISTENING_PORT, SOCKET_LISTENING_PORT
from session import Session

logging.getLogger("kademlia").setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG)


def bytes_to_hex_string(byte_string):
    return byte_string.hex()


async def init_server():
    try:
        server = Server()
        await server.listen(LISTENING_PORT)  # Listen on port 8468 for incoming connections, this is the Kademlia server
        # When initializing, we don't need to bootstrap yet (in case creating a session)
        # await server.bootstrap([("127.0.0.1", 8468)])  # Bootstrap to itself initially
        print(f"Kademlia server listening on port: {LISTENING_PORT}")
        return server
    except Exception as e:
        print("Error initializing server:", e)
        raise


class Node:
    def __init__(self):
        print("Node initializing...")
        self.node_id = None
        self.server = None
        self.session = None

        self.provided_session_id = ""  # For when the node wants to join a session, store the provided session ID here

        # Communication stuff
        self.communication = Communication()
        self.message_handlers = {
            "join_request": self.handle_verification_message,
            "join_request_response": self.handle_verification_response_message,
            "replicate_dht_bootstrap": self.handle_replicate_dht_request,
            "replicate_dht_bootstrap_response": self.handle_replicate_dht_request_response,
            "new_user_notificataion": self.handle_broadcast_new_user,
        }
        self.running = True

        # Event loop for async execution
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        # Listener thread
        self.listen_thread = threading.Thread(target=self.run_event_loop)
        self.listen_thread.start()

    def run_event_loop(self):
        self.loop.run_until_complete(self.listen_for_messages())

    async def initialize(self):
        self.server = await init_server()
        print("Kademlia server successfully initialized!")
        self.session = Session(self.server)  # Initialize session after server
        # Since session needs server in its constructor
        self.node_id = bytes_to_hex_string(self.server.node.id)
        print(f"Assigned node ID: {self.node_id}")

    async def bootstrap(self, bootstrap_node):
        await self.server.bootstrap([bootstrap_node])

    async def listen_for_messages(self):
        print(f"Socket listening for messages on port {SOCKET_LISTENING_PORT}...")
        while self.running:
            message = self.communication.receive_message(SOCKET_LISTENING_PORT)
            if message:
                print(f"Received socket message: {message}")
                message_type = message.get("type")
                handler = self.message_handlers.get(message_type)
                if handler:
                    if inspect.iscoroutinefunction(handler):
                        print("Found coroutine handler")
                        await self.message_handlers[message_type](message)
                    else:
                        self.message_handlers[message_type](message)
                else:
                    print(f"No handler found for message type '{message_type}'.")

    async def join_session(self, session_id, bootstrap_node):
        try:

            print(f"Trying to join session {session_id} via bootstrap node {bootstrap_node}...")

            self.provided_session_id = session_id

            # Send verification request msg first
            self.communication.send_join_request_message(bootstrap_node[0], SOCKET_LISTENING_PORT, session_id)
            # The response will be intercepted by the listener, and will be handled

        except Exception as e:
            print("Error joining session:", e)
            raise

    async def handle_verification_response_message(self, message):
        """
        Pretty much what to do after we get the verification response back.
        We should
        1. Bootstrap to add ourselves to the network
        2. Replicate our DHT with the bootstrapped node
        3. Add the bootstrapped node to our DHT
        4. Broadcast to all nodes in the updated DHT our join
        :param message:
        :return:
        """
        if message and message.get("verified"):
            sender_ip = message.get("sender_address")[0]
            verified = message.get("verified")
            if verified:
                bootstrap_node = (sender_ip, LISTENING_PORT)
                print(f"Sending bootstrap request to {bootstrap_node}")
                await self.bootstrap(bootstrap_node)
                self.session.set_session_id(self.provided_session_id)
                print(f"Joined session: {self.session.get_session_id()}")

                # Do rest of join session logic on the node who's joining's side
                # REPLICATE DHT

                self.communication.send_replicate_dht_request(sender_ip)  # Send a request to replicate the DHT.
                # Logic will transfer over to handle_replicate_dht_request

            else:
                print("Session ID verification failed. The Session ID could be incorrect or the node you are "
                      "attempting to contact is not connected to a session.")

    async def handle_replicate_dht_request(self, message):
        """
        Handle the replicate DHT request (bootstrapped node runs this)
        :param message:
        :return: Contents of its DHT
        """
        address = message.get("sender_address")[0]
        await self.session.update_session_nodes()  # Load DHT data to local session data
        data = self.session.nodes
        node_id = self.node_id
        print(f"Got data from bootstrapped node: {data}")
        self.communication.send_replicate_dht_request_response(address, data, node_id)
        # Logic will transfer over to handle_replicate_dht_request_response

    async def handle_replicate_dht_request_response(self, message):
        """
        Handle the response from the DHT request
        Take data and put into own DHT
        Add sender information to own DHT
        Broadcast add own data to all connected clients
        :param message:
        :return:
        """
        data = message.get("data")  # Bootstrap node DHT data
        address = message.get("sender_address")[0]  # Address of sender (bootstrap node)
        node_id = message.get("node_id")  # Node id of bootstrap node
        print(f"Got DHT request response: {data}")

        # Insert data into own DHT
        for node_id, node_ip in data:
            await self.session.add_session_node(node_id, node_ip)

        # Now we want to add the bootstrapped node to our DHT
        await self.session.add_session_node(node_id, (address, SOCKET_LISTENING_PORT))

        # Now broadcast the join to all connected clients
        join_node_id = self.node_id
        join_node_ip = self.communication.get_local_ipv4()
        await self.broadcast_new_user(join_node_id, (join_node_ip, SOCKET_LISTENING_PORT))

        # Logic will transfer over to handle_broadcast_new_user

    def handle_broadcast_new_user(self, message):
        node_ip = message.get("node_ip")
        node_id = message.get("node_id")
        print(f"Got broadcast new node: adding new node: {node_id},{node_ip}")
        self.session.add_session_node(node_id, node_ip)

    def handle_verification_message(self, message):
        session_id = message.get("session_id")
        address = message.get("sender_address")
        print(f"Actual session ID: {self.session.get_session_id()}, Provided session ID: {session_id}")
        if session_id == self.session.get_session_id():
            self.communication.send_verification_response(address[0], address[1], True)
        else:
            self.communication.send_verification_response(address[0], address[1], False)

    async def broadcast_new_user(self, node_id, node_ip):
        """

        :param node_id:
        :param node_ip: tuple (ip, socket port)
        :return:
        """
        await self.broadcast("new_user_notificataion", node_id=node_id, node_ip=node_ip)

    async def broadcast(self, topic, **kwargs):
        # Get all nodes in the session/DHT
        session_nodes = await self.session.get_session_nodes()
        for node_id, node_ip in session_nodes.items():
            # Skip broadcasting to self
            if node_id != self.node_id:
                # Construct message
                message = {"type": topic, **kwargs}
                # Send message to each node
                self.communication.send_message(node_ip, SOCKET_LISTENING_PORT, message)

    def store_file(self, file_name, file_data):
        # Implement file storage functionality
        pass

    def find_file(self, file_name):
        # Implement file retrieval functionality
        pass
