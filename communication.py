import socket
import pickle

from globals import LISTENING_PORT


class Communication:
    def __init__(self):
        pass

    def send_message(self, ip, port, message):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((ip, port))
            s.send(pickle.dumps(message))

    def receive_message(self, port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("0.0.0.0", port))
            s.listen()
            conn, addr = s.accept()
            with conn:
                data = conn.recv(1024)
                message = pickle.loads(data)
                print(f"Checked for socket message on port {port} and got: {message}")
                return message

    def send_join_request_message(self, ip, port, session_id):
        """
        Sends a join request to IP:PORT
        :param ip: destination ip
        :param port: destination port
        :param session_id: session ID that caller wants to join
        :return:
        """

        sender_ip = socket.gethostbyname(socket.gethostname())
        message = {"type": "join_request", "session_id": session_id, "address": (sender_ip, LISTENING_PORT)}
        print(f"Sending join request message to ({ip, port}): {message} ...")
        self.send_message(ip, port, message)

    def receive_verification_response(self, port):
        return self.receive_message(port)

    def send_verification_response(self, ip, port, verified):
        """
        Sends a response to the join request, either verifying the session ID or not
        :param ip: destination IP
        :param port: destination Port
        :param verified:
        :return:
        """
        message = {"type": "join_request_response", "verified": verified}
        print(f"Sending response to join request to ({ip, port}): {message}")
        self.send_message(ip, port, message)
