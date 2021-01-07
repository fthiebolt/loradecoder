

# #############################################################################
#
# Global variables
# (scope: this file)
#

_Cursor = 0 #Correspond a l'emplacement dans la payload


#Dictionnaire des types de data
TYPE =[
    {'nom':'analog_input',          'ID':1,  'size':1, 'mult':1},
    {'nom':'analog_output',         'ID':2,  'size':1, 'mult':1},
    {'nom':'digital_input',         'ID':3,  'size':1, 'mult':1},
    {'nom':'digital_output',        'ID':4,  'size':1, 'mult':1},
    {'nom':'luminosity',            'ID':5,  'size':2, 'mult':1},
    {'nom':'presence',              'ID':6,  'size':1, 'mult':1},
    {'nom':'frequency',             'ID':7,  'size':2, 'mult':1},
    {'nom':'temperature',           'ID':8,  'size':1, 'mult':100, 'ref':20, 'pas':0.25},
    {'nom':'humidity',              'ID':9,  'size':1, 'mult':100, 'ref':0, 'pas':0.5},
    {'nom':'CO2',                   'ID':10, 'size':2, 'mult':1},
    {'nom':'air_quality',           'ID':11, 'size':1, 'mult':1},
    {'nom':'GPS',                   'ID':12, 'size':9, 'mult':1},
    {'nom':'energy',                'ID':13, 'size':3, 'mult':1},
    {'nom':'UV',                    'ID':14, 'size':3, 'mult':1},
    {'nom':'weight',                'ID':15, 'size':3, 'mult':1},
    {'nom':'pressure',              'ID':16, 'size':1, 'mult':1, 'ref':990, 'pas':1},
    {'nom':'generic_sensor_unsi',   'ID':17, 'size':4, 'mult':1},
    {'nom':'generic_sensor_sign',   'ID':18, 'size':4, 'mult':1},
]

# #*** Retourne la taille en octet d'un type de data ***
# def typesise (data_type):
#     #data_type est un eniter qui correspond au type de la data d'apres la convention cayenne 

#     for ind in TYPE :
#         if ind['ID'] == data_type :
#             return ind['size'] 

#*** Retourne une liste avec nom, size, mult, ref, pas d'un type de data ***
def infodata (data_type):
    #data_type : est un eniter qui correspond au type de la data d'apres la convention cayenne 

    for ind in TYPE : #on parcour le dictionnaire TYPE et on regarde si data_type correspond a une ID connu 
        global _Cursor
        if ind['ID'] == data_type :

            if 'ref' in ind :
                info = [ind['nom'],ind['size'],ind['mult'],ind['ref'],ind['pas']]

            else:
                info = [ind['nom'],ind['size'],ind['mult']]

            _Cursor += ind['size'] + 1  #car on a dans la payload type, channel, data 
            return info 
    print("Pas bon type de data!!!")
    return [0,0,0,0]


#*** Transforme les datas de la convention cayenne en float
def transfo_data (info,data):
    #info : est la liste renvoye par infodata qui contien nom, size, mult, ref, pas d'un type de data 
    #data : est la data un tableau qui represente la data sous forme cayenne
    #DATA : est la data sous forme de float 

    if len(info) == 3 : #Les datas sans références
        if info[1] == 2: #La data est un entier mis sur 2 octet
            DATA = data[0]+(data[1]<<8) #LSB + MSB*256   

        elif info[1] == 3: #La data est un float mis sur 3 octet avec la partie entiere sur 2 octet et la partie float sur 1 octet 
            DATA = data[0]+(data[1]<<8) #LSB + MSB*256 ici partie entiere
            DATA += data[2]/256
            DATA = round(DATA,2) #Pour tronquer a 10^-2 

        elif info[1] == 4: #La data est un float mis sur 4 octet 
            DATA = data[0]+(data[1]<<8)+(data[2]<<16)+(data[3]<<24) 
            #TODO ca marche pas !!!!

    else :
        DATA = (data * info[4])+info[3]
        # DATA = ((data *25)/100)+20

    return DATA

#TODO faire des cas si il y a des pb !!!!

#*** Retourne une ...  avec les informations du capteur pour MQTT
def decoder (payload, cursor):
    #payload : la payload lora recut en MQTT
    #cursor : emplacement dans la payload du capteur

    INFO = infodata(payload[cursor])

    data = payload[cursor+2 : cursor+2+INFO[0]] #on recupere les data du capteur


def main():
    a = infodata(8)
    print(a)
    # b=transfo_data(a,[0x00,0x00,0x99,0x42])
    b=transfo_data(a,1)
    print(b)



if __name__ == "__main__":
    main()
    pass


    
    # return  