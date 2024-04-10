import uuid

class Session:
    def __init__(self):
        self.session_id = None
        self.nodes = {}  # Dictionary to store nodes connected to this session

    def add_node(self, node_id, node):
        self.nodes[node_id] = node

    def remove_node(self, node_id):
        del self.nodes[node_id]

    def get_session_id(self):
        return self.session_id

    def generate_session_id(self):
        self.session_id = str(uuid.uuid4())

    def set_session_id(self, session_id):
        self.session_id = session_id
