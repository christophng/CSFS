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
    def __init__(self, node_id):
        print("Node initializing...")
        self.node_id = node_id
        self.server = None

        self.session = Session()
        self.provided_session_id = ""  # For when the node wants to join a session, store the provided session ID here

        # Communication stuff
        self.communication = Communication()
        self.message_handlers = {
            "join_request": self.handle_verification_message,
            "join_request_response": self.handle_verification_response_message,
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

    async def bootstrap(self, bootstrap_node):
        self.server.bootstrap([bootstrap_node])

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
            # Get verification request response
            # response = self.communication.receive_verification_response(LISTENING_PORT)
            # if response and response.get("verified"):
            #     print(f"Performing bootstrapping for node {bootstrap_node}")
            #     await self.server.bootstrap([bootstrap_node])
            #     print(f"Bootstrapping successful.")
            # else:
            #     print("Session ID verification failed.")

        except Exception as e:
            print("Error joining session:", e)
            raise

    async def handle_verification_response_message(self, message):
        if message and message.get("verified"):
            sender_ip = message.get("sender_address")[0]
            verified = message.get("verified")
            if verified:
                bootstrap_node = (sender_ip, LISTENING_PORT)
                print(f"Sending bootstrap request to {bootstrap_node}")
                await self.bootstrap([bootstrap_node])
                self.session.set_session_id(self.provided_session_id)
                print(f"Joined session: {self.session.get_session_id()}")

                # Do rest of join session logic on the node who's joining's side
            else:
                print("Session ID verification failed. The Session ID could be incorrect or the node you are "
                      "attempting to contact is not connected to a session.")

    def handle_verification_message(self, message):
        session_id = message.get("session_id")
        address = message.get("sender_address")
        print(f"Actual session ID: {self.session.get_session_id()}, Provided session ID: {session_id}")
        if session_id == self.session.get_session_id():
            self.communication.send_verification_response(address[0], address[1], True)
        else:
            self.communication.send_verification_response(address[0], address[1], False)

    def store_file(self, file_name, file_data):
        # Implement file storage functionality
        pass

    def find_file(self, file_name):
        # Implement file retrieval functionality
        pass
