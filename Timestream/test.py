from classTimestream import *


leandro = Timestream("base-prueba")
# leandro.showDatabases()
# leandro.createDatabase()
# leandro.list_tables()
tabla = TimestreamTable("tabla-prueba")
# tabla.create_table()
# evento = ["548","nodo-cbe874","tiempo","578"]
# tabla.createItem(evento)
# tabla.deleteTable()
leandro.deleteDatabase()