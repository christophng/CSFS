import asyncio
import aioconsole
from node import Node

from globals import BOOTSTRAP_NODE_IP, LISTENING_PORT


async def main():
    node = Node()
    await node.initialize()

    while True:
        command = await aioconsole.ainput("Enter command (create/join/exit): \n\n")

        if command == "create":
            if node.session.session_id is not None:
                print("You are already in a session.")
                continue
            node.session.generate_session_id()
            print("Session created. Session ID:", node.session.get_session_id())

        elif command == "join":
            session_id = input("Enter session ID: ").strip()
            bootstrap_node = (BOOTSTRAP_NODE_IP, LISTENING_PORT)  # Assuming bootstrap node is localhost
            await node.join_session(session_id, bootstrap_node)

        elif command == "exit":
            break

        else:
            print("Invalid command. Please try again.")

    # Clean up resources if needed

if __name__ == "__main__":
    asyncio.run(main())
