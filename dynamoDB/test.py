from classDynamo import *


evento = [548,"nodo-cbe874","tiempo",578]
evento2 = [549,"nodo-cbn005","tiempo",600]
evento3 = [550,"nodo-cbl005","bsas",700]
tabla = DynamoDb()
print(tabla.scanTable())
# tabla.createTable("tabla-prueba")
# eventoUno = DynamoDb(evento)
# eventoUno.createItem("tabla-prueba")
# eventoDos = DynamoDb(evento2)
# eventoDos.createItem("tabla-prueba")
# eventoTres = DynamoDb(evento3)
# eventoTres.createItem("tabla-prueba")
# tabla.getItem("tabla-prueba",550)
# tabla.existItem("tabla-prueba",550)
# tabla.toCsv()
# tabla.deleteTable("tabla-prueba")