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

#*** Retourne une liste avec size, mult, ref, pas d'un type de data ***
def infodata (data_type):
    #data_type : est un eniter qui correspond au type de la data d'apres la convention cayenne 

    for ind in TYPE :
        if ind['ID'] == data_type :

            if 'ref' not in ind :
                info = [ind['size'],ind['mult'],ind['ref'],ind['pas']]

            else:
                info = [ind['size'],ind['mult']]

            return info 


#*** Transforme les datas de la conention cayenne en float
def transfo_data (info):
    #info : est la liste renvoye par infodata qui contien size, mult, ref, pas d'un type de data ***


    
    return data

    #*** Retourne une ...  avec les informations du capteur pour MQTT
def decoder (payload, cursor):
    #payload : la payload lora recut en MQTT
    #cursor : emplacement dans la payload du capteur

    INFO = infodata(payload[cursor])

    data = payload[cursor+2 : cursor+2+INFO[0]] #on recupere les data du capteur






    
    # return  