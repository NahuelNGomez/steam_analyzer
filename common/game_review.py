class GameReview():
    def __init__(self, game_id, game_name, review_text):
        self.game_id = game_id
        self.game_name = game_name
        self.review_text = review_text
        
    def getData(self):
        return [self.game_id, self.game_name, self.review_text]
    
    @staticmethod
    def decode(fields: list):
        # Limpiar las comillas solo si el campo es una cadena, y manejar None
        cleaned_fields = [field.strip('"') if isinstance(field, str) else field for field in fields]
        
        return GameReview(cleaned_fields[0], cleaned_fields[1], cleaned_fields[2])