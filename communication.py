import socket
import pickle

from globals import SOCKET_LISTENING_PORT


class Communication:
    def __init__(self):
        pass

    def send_message(self, ip, port, message):

        sender_ip = self.get_local_ipv4()
        if sender_ip:
            message["sender_address"] = (sender_ip, SOCKET_LISTENING_PORT)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((ip, port))
                s.send(pickle.dumps(message))
        else:
            print("Socket send message failed. Local IPv4 not found.")

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

        message = {"type": "join_request", "session_id": session_id}
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

    def send_replicate_dht_request(self, bootstrapped_node_ip):
        # Send a request to the bootstrapped node to retrieve its DHT data

        message = {"type": "replicate_dht_bootstrap"}
        print(f"Sending Replicate-DHT request to ({bootstrapped_node_ip, SOCKET_LISTENING_PORT}: {message})")
        self.send_message(bootstrapped_node_ip, SOCKET_LISTENING_PORT, message)

    def send_replicate_dht_request_response(self, dest_ip, data, node_id):
        # Send a response to the replicate dht request with the bootstrapped node's DHT data

        message = {"type": "replicate_dht_bootstrap_response", "node_id": node_id, "data": data}
        print(f"Sending Replicate-DHT request to ({dest_ip, SOCKET_LISTENING_PORT}: {message})")
        self.send_message(dest_ip, SOCKET_LISTENING_PORT, message)

    def send_broadcast_ack(self, dest_ip, node_id):
        """
        We want to use this in every handle_broadcast_<broadcasttype> method to acknowledge after all logic is done
        :param dest_ip:
        :param node_id:
        :return:
        """
        message = {"type": "broadcast_ack", "node_id": node_id}
        print(f"Sending broadcast ack to ({dest_ip, SOCKET_LISTENING_PORT}: {message})")
        self.send_message(dest_ip, SOCKET_LISTENING_PORT, message)

    def get_local_ipv4(self):
        try:
            # Get the IPv4 address associated with the local machine
            ip = socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET)[0][-1][0]
            return ip
        except Exception as e:
            print("Error getting local IPv4 address:", e)
            return None
