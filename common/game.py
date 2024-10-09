import csv


class Game():
    def __init__(self, id, name, release_date,windows,linux,mac, apf, genres):
        self.id = id
        self.name = name
        self.release_date = release_date
        self.windows = windows
        self.linux = linux
        self.mac = mac
        self.apf = apf
        self.genres = genres

    @staticmethod
    def from_csv_row(row):
        fields = list(csv.reader([row]))[0]
        return Game(fields[0].strip(), fields[1].strip(), fields[2].strip(), fields[17].strip(), fields[18].strip(), fields[19].strip(), fields[29].strip(), fields[36].strip())
    
    def getData(self):
        return [self.id, self.name, self.release_date, self.windows, self.linux, self.mac, self.apf, self.genres]
    
    @staticmethod
    def decode(fields: list):
        print("FIELDS PARA DECODE:", fields)
        
        cleaned_fields = [field.strip('"') for field in fields]
        
        return Game(
            cleaned_fields[0], cleaned_fields[1], cleaned_fields[2],
            cleaned_fields[3], cleaned_fields[4], cleaned_fields[5],
            cleaned_fields[6], cleaned_fields[7]
        )