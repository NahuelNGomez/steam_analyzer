import csv

class Review():
    def __init__(self, game_id, app_name,review_text,review_score, id):
        self.id = id
        self.game_id = game_id
        self.app_name = app_name
        self.review_text = review_text
        self.review_score = review_score

    @staticmethod
    def from_csv_row(id_review, row):
        fields = list(csv.reader([row]))[0]
        return Review(fields[0].strip(), fields[1].strip(), fields[2], fields[3].strip(), id_review)
    
    def getData(self):
        return [self.game_id, self.app_name, self.review_text, self.review_score, self.id]
        
    @staticmethod
    def decode(fields: list):
        
        cleaned_fields = [field.strip('"') if isinstance(field, str) else field for field in fields]
        
        return Review(
            cleaned_fields[0], cleaned_fields[1], cleaned_fields[2],
            cleaned_fields[3], cleaned_fields[4]
        )

    def __str__(self):
        return f"Review({self.game_id}, {self.app_name}, {self.review_text}, {self.review_score}, {self.id})"
    
    def checkNanElements(self):
        if self.game_id == "": 
            return True
        if self.review_text == "":
            return True
        if self.review_score== "":
            return True
        return False