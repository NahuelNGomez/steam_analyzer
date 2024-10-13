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
        return Review(fields[0].strip(), fields[1].strip(), fields[2].strip(), fields[3].strip(), id_review)
    
    def getData(self):
        return [self.game_id, self.app_name, self.review_text, self.review_score, self.id]
    
    @staticmethod
    def decode(fields: list):
        
        cleaned_fields = [field.strip('"') if isinstance(field, str) else field for field in fields]
        
        return Review(
            cleaned_fields[0], cleaned_fields[1], cleaned_fields[2],
            cleaned_fields[3], cleaned_fields[4]
        )
    
    def checkNanElements(self):
        if self.id == None or self.id == "nan" or self.id == "" or self.id == None: 
            return True
        if self.game_id == "nan" or self.game_id == "" or self.game_id == None: 
            return True
        if self.app_name == "nan" or self.app_name== "" or self.app_name== None:
            return True
        if self.review_text == "nan" or self.review_text == "" or self.review_text == None:
            return True
        if self.review_score== "nan" or self.review_score== "" or self.review_score== None:
            return True