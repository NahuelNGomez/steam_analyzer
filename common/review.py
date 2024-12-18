import csv

from common.packet import Packet

class Review(Packet):
    def __init__(self, game_id, app_name, review_text, review_score, id, client_id):
        super().__init__(client_id)
        self.id = id
        self.game_id = game_id
        self.app_name = app_name
        self.review_text = review_text
        self.review_score = review_score

    @staticmethod
    def from_csv_row(id_review, row, client_id):
        fields = list(csv.reader([row]))[0]
        return Review(fields[0].strip(), fields[1].strip(), fields[2], fields[3].strip(), id_review, client_id)
    
    def getData(self):
        return [self.game_id, self.app_name, self.review_text, self.review_score, self.id, self.client_id]
    
    @staticmethod
    def decode(fields: list):
        cleaned_fields = [field.strip('"') if isinstance(field, str) else field for field in fields]
        return Review(
            cleaned_fields[0], cleaned_fields[1], cleaned_fields[2],
            cleaned_fields[3], cleaned_fields[4], cleaned_fields[5]
        )
    #Revisar DECODE

    
    def __str__(self):
        return f"Review({self.game_id}, {self.app_name}, {self.review_text}, {self.review_score}, {self.id}, {self.client_id})"
    
    def checkNanElements(self):
        if self.game_id == "": 
            return True
        if self.review_text == "":
            return True
        if self.review_score== "":
            return True
        return False