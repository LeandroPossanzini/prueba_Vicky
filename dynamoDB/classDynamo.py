import boto3
import pandas as pd

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

class DynamoDb():
    # event es un objeto que trae la informacion que quiero escribir en DynamoDB
    def __init__(self,event = ""):
        self.event = event

    def showTables(self):
        try:
            tables = list(dynamodb.tables.all())
            print(tables)
        except:
            print("No se pueden mostrar las tablas")    

    def destructurar(self):
        id, nodo, region, valor = self.event
        return {'id':id,'nodo':nodo, 'regiones':region, 'valor':valor}

    def createTable(self,tableName):
        # Cuando creo la tabla solo especifico cuales van a ser mis hash o range keys
        try:
            table = dynamodb.create_table(
            TableName=tableName,
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'N'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )
            # Wait until the table exists.
            table.wait_until_exists()
            print("La tabla se creo correctamente")
        except:
            print("La tabla no se creo correctamente")

    def createItem(self,tableName):
        try:
            table = dynamodb.Table(tableName)
            items = self.destructurar()
            table.put_item(
            Item={
                'id': items['id'],
                'nodo': items['nodo'],
                'region': items['regiones'],
                'valor': items['valor'],
            })
            print("Se creo el item")
        except:
            print("No se creo el item")    
    
    def existItem(self,tableName,id):
        try:
            table = dynamodb.Table(tableName)
            response = table.get_item(
            Key={
                'id': id,
                }
            )
            item = response['Item']
            if item:
                print("Existe en la base de datos")
                return True
            else:
                return False    
        except:
            print("Id o Nombre de tabla incorrecto")    

    def getItem(self,tableName,id):
        try:
            table = dynamodb.Table(tableName)
            response = table.get_item(
            Key={
                'id': id,
                }
            )
            item = response['Item']
            print(f"El item con id:{id} es {item}")
        except:
            print("Id o Nombre de tabla incorrecto")    

    def updateItem(self,tableName,id,objeto):
        for clave,valor in objeto.items():
            clave1 = clave
            valor1 = valor
        print(clave1,valor1)    
        try:
            table = dynamodb.Table(tableName)
            table.update_item(
            Key={
                'id': id,
            },
            UpdateExpression=f'SET {clave1} = :val1',
            ExpressionAttributeValues={
                ':val1': valor1
            }
            )
            print(f"Se cambio el atributo: {clave1} correctamente")
        except:
            print("No se cambio el valor del ") 

    def deleteItem(self,tableName,id):
        table = dynamodb.Table(tableName)
        try:
            table.delete_item(
            Key={
                'id':id,
            })
            print(f"Se elimino el item con id:{id}")
        except:
            print(f"No se elimino el item con id: {id}")    

    def deleteTable(self,tableName):
        try:
            table = dynamodb.Table(tableName)
            table.delete()
            print(f"Se elimino la Tabla: {tableName}")  
        except:
            print(f"No se elimino la Tabla: {tableName}")    

    def scanTable(self):
        tabla = dynamodb.Table("tabla-prueba")
        response = tabla.scan()

        result = response["Items"]
        while 'LastEvaluatedKey' in response:
            response = tabla.scan(ExclusiveStarKey= response['LastEvaluatedKey'])
            result.extend(response['Items'])

        return result

    def toCsv(self):
        try:
            hola = self.scanTable()
            data = pd.DataFrame(hola)
            data.to_csv("data.csv",index=False)
            print("La tabla se paso a csv")

        except:
            print("Error en pasar la tabla a csv")

