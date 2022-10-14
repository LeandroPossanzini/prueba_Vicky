from classDynamo import *


evento = [548,"nodo-cbe874","tiempo",578]
evento2 = [549,"nodo-cbn005","tiempo",600]
evento3 = [550,"nodo-cbl005","bsas",700]
evento4 = [551,"nodo-cbs005","chaco",874]

objeto = {"nodo":"nuevo-nodo"}

tabla = DynamoDb()
tabla.deleteTable("la-tabla-final")


