import re

def findtemperature(string):
    Intnumber=re.findall('\d+', string)
    Floatnumber=re.findall("\d+\.\d+", string)
    if (Floatnumber):
        return(Floatnumber)
    else:
        return(Intnumber)