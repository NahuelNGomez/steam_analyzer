import csv
import sys

from common.packet import Packet


class Game(Packet):
    def __init__(self, id, name, release_date, windows, mac, linux, apf, genres, client_id):
        super().__init__(client_id)
        self.id = id
        self.name = name
        self.release_date = release_date
        self.windows = windows
        self.mac = mac
        self.linux = linux
        self.apf = apf
        self.genres = genres

    @staticmethod
    def from_csv_row(row, client_id):
        fields = list(csv.reader([row]))[0]
        return Game(fields[0].strip(), fields[1].strip(), fields[2].strip(), fields[17].strip(), fields[18].strip(), fields[19].strip(), fields[29].strip(), fields[36].strip(), client_id)
    
    def getData(self):
        return [self.id, self.name, self.release_date, self.windows, self.mac, self.linux, self.apf, self.genres, self.client_id]
    
    @staticmethod
    def decode(fields: list):
        cleaned_fields = [str(field).strip('"') if isinstance(field, str) else field for field in fields]
        return Game(
            cleaned_fields[0], cleaned_fields[1], cleaned_fields[2],
            cleaned_fields[3], cleaned_fields[4], cleaned_fields[5],
            cleaned_fields[6], cleaned_fields[7], cleaned_fields[8] 
        )
    #Revisar DECODE

    def checkNanElements(self):
        if self.windows == None or self.windows == "nan" or self.windows == "": 
            return True
        if self.linux == "nan" or self.linux == "" or self.linux == None: 
            return True
        if self.mac == "nan" or self.mac == "" or self.mac == None:
            return True
        if self.apf == "nan" or self.apf == "" or self.apf == None:
            return True
        if self.genres == "nan" or self.genres == "" or self.genres == None:
            return True
        if self.id == "nan" or self.id == "" or self.id == None:
            return True
        if self.release_date == "nan" or self.release_date == "" or self.release_date == None:
            return True
        if self.name == "nan" or self.name == "" or self.name == None:
            return True
        
        