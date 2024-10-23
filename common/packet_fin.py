class Fin:
    def __init__(self, batch_id, client_id):
        self.batch_id = batch_id
        self.client_id = client_id
        
    def decode(self, message):
        res = message.split("\n\n")
        
        return Fin(res[1], res[2])
    
    def encode(self):
        return "fin\n\n" + str(self.batch_id) + "\n\n" + str(self.client_id) + "\n\n"

    