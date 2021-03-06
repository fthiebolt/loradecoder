

# #############################################################################
#
# Global variables
# (scope: this file)
#

_Cursor =  2 #Correspond a l'emplacement dans la payload, on initialise a 2 car les 2 premiers octet de la payload ne sont pas des datas donc la premiere data est a l'emplacement 3 dans la payload 


#Dictionnaire des types de data
TYPE =[
    {'nom':'analog_input',          'unit':'...',      'ID':1,  'size':1, 'mult':1},
    {'nom':'analog_output',         'unit':'...',      'ID':2,  'size':1, 'mult':1},
    {'nom':'digital_input',         'unit':'bool',      'ID':3,  'size':1, 'mult':1},
    {'nom':'digital_output',        'unit':'bool',      'ID':4,  'size':1, 'mult':1},
    {'nom':'luminosity',            'unit':'lux',      'ID':5,  'size':2, 'mult':1},
    {'nom':'presence',              'unit':'bool',      'ID':6,  'size':1, 'mult':1},
    {'nom':'frequency',             'unit':'pers/j',      'ID':7,  'size':2, 'mult':1},
    {'nom':'temperature',           'unit':'celcuis',      'ID':8,  'size':1, 'mult':100, 'ref':20, 'pas':0.25},
    {'nom':'humidity',              'unit':'%r.H',      'ID':9,  'size':1, 'mult':100, 'ref':0, 'pas':0.5},
    {'nom':'CO2',                   'unit':'ppm',      'ID':10, 'size':2, 'mult':1},
    {'nom':'air_quality',           'unit':'ppm',      'ID':11, 'size':1, 'mult':1},
    {'nom':'GPS',                   'unit':'...',      'ID':12, 'size':9, 'mult':1},
    {'nom':'energy',                'unit':'W/m2',      'ID':13, 'size':3, 'mult':1},
    {'nom':'UV',                    'unit':'W/m2',      'ID':14, 'size':3, 'mult':1},
    {'nom':'weight',                'unit':'g',      'ID':15, 'size':3, 'mult':1},
    {'nom':'pressure',              'unit':'mBar',      'ID':16, 'size':1, 'mult':1, 'ref':990, 'pas':1},
    {'nom':'generic_sensor_unsi',   'unit':'...',      'ID':17, 'size':4, 'mult':1},
    {'nom':'generic_sensor_sign',   'unit':'...',      'ID':18, 'size':4, 'mult':1},
]


#*** Retourne une liste avec nom, size, mult, ref, pas d'un type de data ***
def infodata (data_type):
    #data_type : est un eniter qui correspond au type de la data d'apres la convention cayenne 

    print ("datatype :%d" % data_type)
    for ind in TYPE : #on parcour le dictionnaire TYPE et on regarde si data_type correspond a une ID connu 
        global _Cursor
        if ind['ID'] == data_type :

            if 'ref' in ind :
                info = [ind['nom'],ind['unit'],ind['size'],ind['mult'],ind['ref'],ind['pas']]

            else:
                info = [ind['nom'],ind['unit'],ind['size'],ind['mult']]

            # _Cursor += ind['size'] + 1  #car on a dans la payload type, channel, data 
            return info 
    print("Pas bon type de data!!!")
    return False


#*** Transforme les datas de la convention cayenne en float
def transfo_data (info,data):
    #info : est la liste renvoye par infodata qui contien nom, size, mult, ref, pas d'un type de data 
    #data : est la data un tableau qui represente la data sous forme cayenne
    #DATA : est la data sous forme de float 

    # print ("in transfo data :%d ",data[0])
    if len(info) == 4 : #Les datas sans références
        if info[2] == 1: #La data est un binaire sur 1 octet
            DATA = data[0]

        elif info[2] == 2: #La data est un entier mis sur 2 octet
            DATA = float(data[0]+(data[1]<<8)) #LSB + MSB*256   

        elif info[2] == 3: #La data est un float mis sur 3 octet avec la partie entiere sur 2 octet et la partie float sur 1 octet 
            DATA = data[0]+(data[1]<<8) #LSB + MSB*256 ici partie entiere
            DATA += data[2]/256
            DATA = round(DATA,2) #Pour tronquer a 10^-2 

        elif info[2] == 4: #La data est un float mis sur 4 octet 
            DATA = data[0]+(data[1]<<8)+(data[2]<<16)+(data[3]<<24) 

    else :
        if info[0] == "temperature":
            # print("temp :%d"%data[0])
            pf = data[0]>>7

            if pf == 1 : #cas eniter negatif
                data=data[0] - (data[0]>>7)
                print("temp :%d"%data)
                DATA = ((-data) * info[5]*100)/100 + info[4]

            else : #cas entier possitif
                DATA = (data[0] * info[5]*100)/100 + info[4]

        else :
            DATA = (data[0] * info[5]) + info[4]

    return DATA

#TODO faire des cas si il y a des pb !!!!

#*** Retourne la payload avec les informations du capteur pour MQTT
def decoder (PAYLOAD):
    #payload : la payload lora recut en MQTT
    #cursor : emplacement dans la payload du capteur
    #data : [data, unit]
    global _Cursor
    data = []
    row_data = []
    i = 0

    if _Cursor < len(PAYLOAD) :
        INFO = infodata(PAYLOAD[_Cursor])
        _Cursor += 2 #+2 car les datas sont sous la forme : type, channel, data
        while i < INFO[2] :
            row_data.append(PAYLOAD[_Cursor]) 
            print("Cursor :%d"%_Cursor)
            print ("row data[%d] :%d" %(i,row_data[i]))
            _Cursor += 1
            i += 1 

        data.append(transfo_data(INFO,row_data))
        data.append(INFO[1])

    else :
        print("PB en _Cursor dehors de payload dans decoder")
        return False
    
    return data


def main():
    # a = infodata(8)
    # print(a)
    # # b=transfo_data(a,[0x00,0x00,0x99,0x42])
    # b=transfo_data(a,1)
    # print(b)

    payl=[0x01,0x1E,0x05,0x39,0xA5,0x01,0x08,0x44,0x0E,0x09,0x44,0x3F,0x08,0xFF,0x85,0x09,0xFF,0x0A,0x0A,0xFF,0x70,0x17,0x06,0xFF,0xFF,0x0D,0xFF,0x3C,0x00,0xCC]
    #decimal : 1 30 5 57 165 1 8 68 14 9 68 63 8 255 133 9 255 10 10 255 112 23 6 255 255 13 255 60 0 204 

    #data :0x39 Lum = 421.00  0x44 Temp *C = 23.67  0x44 Hum. % = 31.93  0xFF Temp test: -10.80   0xFF Humi test: 5.00  0xFF CO2 test: 6000.00   0xFF Presence test: 1.00    Energy test: 60.80
    print(payl)
    while _Cursor < len(payl): 
        aa=decoder(payl)
        print("Unit :%s"%aa[1])
        print("value final:%f"%aa[0])
        
    print("cursor final:%d" %_Cursor)




if __name__ == "__main__":
    main()
    pass
