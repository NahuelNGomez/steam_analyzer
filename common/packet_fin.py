class Fin():
    def __init__(self, batch_id, client_id):
        self.batch_id = batch_id
        self.client_id = client_id
        
    @staticmethod    
    def decode(message):
        res = message.split("\n\n")
        return Fin(res[1], res[2])
    
    def encode(self):
        return f"fin\n\n{self.batch_id}\n\n{self.client_id}\n\n"