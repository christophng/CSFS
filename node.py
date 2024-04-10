import asyncio
import inspect
import logging
import os
import threading

from kademlia.network import Server

from communication import Communication
from filesharing import FileSharing, FileWatcher
from globals import LISTENING_PORT, SOCKET_LISTENING_PORT, ACK_WAIT_TIMEOUT, TRACKED_FOLDER_PATH, FILE_TRANSFER_PORT
from session import Session

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("node")
# logging.getLogger("kademlia").setLevel(logging.DEBUG)


def bytes_to_hex_string(byte_string):
    return byte_string.hex()


async def init_server():
    try:
        server = Server()
        await server.listen(LISTENING_PORT)  # Listen on port 8468 for incoming connections, this is the Kademlia server
        # When initializing, we don't need to bootstrap yet (in case creating a session)
        # await server.bootstrap([("127.0.0.1", 8468)])  # Bootstrap to itself initially
        logger.debug(f"Kademlia server listening on port: {LISTENING_PORT}")
        return server
    except Exception as e:
        logger.debug("Error initializing server:", e)
        raise


class Node:
    def __init__(self):
        logger.debug("Node initializing...")
        self.node_id = None
        self.server = None
        self.session = None

        self.provided_session_id = ""  # For when the node wants to join a session, store the provided session ID here

        self.active_nodes = {}  # Dict to track active nodes. {nodeID:isActive}

        # Communication stuff
        self.communication = Communication()
        self.message_handlers = {
            "join_request": self.handle_verification_message,
            "join_request_response": self.handle_verification_response_message,
            "replicate_dht_bootstrap": self.handle_replicate_dht_request,
            "replicate_dht_bootstrap_response": self.handle_replicate_dht_request_response,
            "new_user_notificataion": self.handle_broadcast_new_user,
            "broadcast_ack": self.handle_broadcast_acks,
            "file_addition": self.handle_file_addition_message,
            "file_download_ack": self.handle_file_addition_ack,
        }
        self.running = True

        # Event loop for async execution
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.fs_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.fs_loop)

        # Listener thread
        self.listen_thread = threading.Thread(target=self.run_event_loop)
        self.listen_thread.start()

        # File sharing thread
        self.file_sharing_thread = threading.Thread(target=self.run_fs_event_loop)
        self.file_sharing_thread.start()

        # File sharing stuff
        self.file_sharing = FileSharing(self)
        self.file_watcher = None

    def run_event_loop(self):
        self.loop.run_until_complete(self.listen_for_messages())

    def run_fs_event_loop(self):
        self.fs_loop.run_until_complete(self.listen_for_file_sharing())

    async def initialize(self):
        self.server = await init_server()
        logger.debug("Kademlia server successfully initialized!")
        self.session = Session(self.server)  # Initialize session after server
        # Since session needs server in its constructor
        self.node_id = bytes_to_hex_string(self.server.node.id)
        logger.debug(f"Assigned node ID: {self.node_id}")
        # File watching
        logger.debug(f"Initializing filewatcher")
        self.file_watcher = FileWatcher(self.file_sharing)
        await self.file_watcher.start()

    async def bootstrap(self, bootstrap_node):
        await self.server.bootstrap([bootstrap_node])

    async def listen_for_file_sharing(self):
        logger.debug(f"Socket listening for filesharing on port {FILE_TRANSFER_PORT}")
        while self.running:
            try:
                self.communication.receive_file()
            except Exception as e:
                print(f"Error receiving file: {e}")

    async def listen_for_messages(self):
        logger.debug(f"Socket listening for messages on port {SOCKET_LISTENING_PORT}...")
        while self.running:
            message = self.communication.receive_message(SOCKET_LISTENING_PORT)
            if message:
                logger.debug(f"Received socket message: {message}")
                message_type = message.get("type")
                handler = self.message_handlers.get(message_type)
                if handler:
                    if inspect.iscoroutinefunction(handler):
                        logger.debug("Found coroutine handler")
                        await self.message_handlers[message_type](message)
                    else:
                        self.message_handlers[message_type](message)
                else:
                    logger.debug(f"No handler found for message type '{message_type}'.")

    async def join_session(self, session_id, bootstrap_node):
        try:

            logger.debug(f"Trying to join session {session_id} via bootstrap node {bootstrap_node}...")

            self.provided_session_id = session_id

            # Send verification request msg first
            self.communication.send_join_request_message(bootstrap_node[0], SOCKET_LISTENING_PORT, session_id)
            # The response will be intercepted by the listener, and will be handled

        except Exception as e:
            logger.debug("Error joining session:", e)
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
                logger.debug(f"Sending bootstrap request to {bootstrap_node}")
                await self.bootstrap(bootstrap_node)
                self.session.set_session_id(self.provided_session_id)
                logger.debug(f"Joined session: {self.session.get_session_id()}")

                # Do rest of join session logic on the node who's joining's side
                # REPLICATE DHT

                self.communication.send_replicate_dht_request(sender_ip)  # Send a request to replicate the DHT.
                # Logic will transfer over to handle_replicate_dht_request

            else:
                logger.debug("Session ID verification failed. The Session ID could be incorrect or the node you are "
                      "attempting to contact is not connected to a session.")

    async def handle_replicate_dht_request(self, message):
        """
        Handle the replicate DHT request (bootstrapped node runs this)
        :param message:
        :return: Contents of its DHT
        """
        address = message.get("sender_address")[0]

        logger.debug(f"Local session data before update session: {self.session.nodes}")
        await self.session.update_session_nodes()  # Load DHT data to local session data
        data = self.session.nodes
        node_id = self.node_id
        logger.debug(f"Local data from bootstrapped node after update session: {data}")
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
        logger.debug(f"Got DHT request response: {data}")

        # Insert data into own DHT
        for data_id, data_ip in data.items():
            logger.debug(f"Attempting to add {data_id}:{data_ip} to session")
            await self.session.add_session_node(data_id, data_ip)

        # Now we want to add the bootstrapped node to our DHT
        logger.debug(f"Attempting to add bootstrapped node {node_id}:{(address, SOCKET_LISTENING_PORT)} to session")
        await self.session.add_session_node(node_id, (address, SOCKET_LISTENING_PORT))

        # Now broadcast the join to all connected clients
        join_node_id = self.node_id
        join_node_ip = self.communication.get_local_ipv4()
        await self.broadcast_new_user(join_node_id, (join_node_ip, SOCKET_LISTENING_PORT))

        # Logic will transfer over to handle_broadcast_new_user

    async def handle_broadcast_new_user(self, message):
        node_ip = message.get("node_ip")
        node_id = message.get("node_id")
        sender_address = message.get("sender_address")
        logger.debug(f"Got broadcast new node: adding new node: {node_id},{node_ip}")
        await self.session.add_session_node(node_id, node_ip)
        self.communication.send_broadcast_ack(sender_address[0], self.node_id)

    def handle_verification_message(self, message):
        session_id = message.get("session_id")
        address = message.get("sender_address")
        logger.debug(f"Actual session ID: {self.session.get_session_id()}, Provided session ID: {session_id}")
        if session_id == self.session.get_session_id():
            self.communication.send_verification_response(address[0], address[1], True)
        else:
            self.communication.send_verification_response(address[0], address[1], False)

    async def handle_file_addition_message(self, message):
        """
        Handles the file addition message received from the broadcasting client.
        Updates the Kademlia DHT with the metadata of the added file.
        Initiates automatic file download.
        """
        file_metadata = message.get("file_metadata")
        address = message.get("sender_address")
        # Update Kademlia DHT with the file metadata
        await self.file_sharing.update_kademlia_dht(file_metadata)
        # Initiate automatic file download
        # First, we send an ACK back to the broadcasting client
        # Once the client receives the ACK, it will start sending the file data over the file sharing socket
        # await self.download_file(file_metadata)
        self.communication.send_file_download_ack(address[0], file_metadata)

    async def handle_file_addition_ack(self, message):
        """
        Handles the acknowledgement as a response to our new file broadcast
        We want to start sending the files here
        :param message:
        :return:
        """
        file_metadata = message.get("file_metadata")
        address = message.get("sender_address")
        logging.debug(f"Sending file {file_metadata['file_name']} to {address[0]} now...")
        await self.download_file(address[0], file_metadata)

    async def broadcast_file_addition(self, file_metadata):
        """
        Broadcasts a message to all connected clients containing the metadata of the added file
        """
        # Broadcast the message to all connected clients
        logging.debug(f"Broadcasting file addition! Metadata={file_metadata}")
        await self.broadcast("file_addition", file_metadata=file_metadata)

    async def broadcast_new_user(self, node_id, node_ip):
        """
        Broadcasts message to all connected clients containing node id and ip of the newly joined node.
        :param node_id:
        :param node_ip: tuple (ip, socket port)
        :return:
        """
        await self.broadcast("new_user_notificataion", node_id=node_id, node_ip=node_ip)

    async def broadcast(self, topic, **kwargs):
        try:
            # Get all nodes in the session/DHT
            session_nodes = await self.session.get_session_nodes()
            # Track active nodes
            # When sending acknowledgements to a broadcast, they should include the sender node's ID
            # This way, we can track which ID has been sent, and which IDs have been sent back via a global array

            for node_id, node_ip in session_nodes.items():
                # Skip broadcasting to self
                if node_id != self.node_id:
                    # Construct message
                    message = {"type": topic, **kwargs}
                    # Send message to each node
                    # Node_ip is a tuple with (IP, port)
                    self.active_nodes[node_id] = False  # Initialize them as false. Only store when the message has
                    self.communication.send_message(node_ip[0], SOCKET_LISTENING_PORT, message)
                    # logger.debug("Starting broadcast wait...")
                    # await asyncio.sleep(ACK_WAIT_TIMEOUT)
                    # been sent.

            # We pause broadcast for ACK_WAIT_TIMEOUT seconds. When the sleep is over, all nodes that haven't had
            # their ACK been processed/handled yet are considered offline

            # After sleep is over, lets check self.active_nodes to see which nodeIDs still have a False value
            # for node_id, is_acked in self.active_nodes.items():
            #     logger.debug(f"Checking if acks are received. ID: {node_id} Acked: {is_acked}")
            #     if not is_acked:
            #         logger.debug(f"Found offline node {node_id}. Removing...")
            #         await self.session.remove_session_node(node_id)

            # Reset active_nodes
            # self.active_nodes = {}

        except Exception as e:
            logger.debug(f"Error during broadcast: {e}")

    async def handle_broadcast_acks(self, message):
        # Here we handle acknowledgements from each client we sent the broadcast to successfully
        # Message should have nodeID (thats pretty much all we need to verify the ack)
        node_id = message.get("node_id")
        logger.debug(f"Received an ack from {node_id}")
        if node_id in self.active_nodes.keys():
            self.active_nodes[node_id] = True
        else:
            logger.debug("Received an ack from node that we didn't broadcast to. Ignoring...")
        logger.debug(f"Current status of active nodes: {self.active_nodes}")

    async def download_file(self, file_metadata, dest_ip):
        file_owner = file_metadata['file_owner']
        file_name = file_metadata['file_name']

        file_path = os.path.join(TRACKED_FOLDER_PATH, file_name)

        try:
            await self.communication.send_file(dest_ip, file_path)
            print(f"File '{file_name}' sent successfully from {file_owner}")
        except Exception as e:
            print(f"Failed to download file '{file_name}' from {file_owner}: {e}")
