import asyncio

from node import Node
from session import Session

from globals import BOOTSTRAP_NODE_IP, LISTENING_PORT


async def main():
    node = Node("node_id", None)
    await node.initialize()
    session = None

    while True:
        command = input("Enter command (create/join/exit): ").strip()

        if command == "create":
            if session is not None:
                print("You are already in a session.")
                continue
            session = Session()
            print("Session created. Session ID:", session.get_session_id())

        elif command == "join":
            session_id = input("Enter session ID: ").strip()
            bootstrap_node = (BOOTSTRAP_NODE_IP, LISTENING_PORT)  # Assuming bootstrap node is localhost
            await node.join_session(session_id, bootstrap_node)
            # session = Session()  # Creating a new session instance
            # print("Joined session", session_id)

        elif command == "exit":
            break

        else:
            print("Invalid command. Please try again.")

    # Clean up resources if needed

if __name__ == "__main__":
    asyncio.run(main())
