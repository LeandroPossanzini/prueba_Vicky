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

    def deleteTable(self,tableName):
        try:
            table = dynamodb.Table(tableName)
            table.delete()
            print(f"Se elimino la Tabla: {tableName}")  
        except:
            print(f"No se elimino la Tabla: {tableName}")    


class DynamoDbTable():
    def __init__(self,tableName):
        self.tableName = tableName

    def destructurar(self,item):
        id, nodo, region, valor = item
        return {'id':id,'nodo':nodo, 'regiones':region, 'valor':valor}

    def createItem(self,item):
        items = self.destructurar(item)
        try:
            table = dynamodb.Table(self.tableName)
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

    def getItem(self,id):
        try:
            table = dynamodb.Table(self.tableName)
            response = table.get_item(
            Key={
                'id': id,
                }
            )
            item = response['Item']
            print(f"El item con id:{id} es {item}")
        except:
            print("Id o Nombre de tabla incorrecto")  

    def updateItem(self,id,objeto):
        for clave,valor in objeto.items():
            clave1 = clave
            valor1 = valor   
        try:
            table = dynamodb.Table(self.tableName)
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

    def deleteItem(self,id):
            table = dynamodb.Table(self.tableName)
            try:
                table.delete_item(
                Key={
                    'id':id,
                })
                print(f"Se elimino el item con id:{id}")
            except:
                print(f"No se elimino el item con id: {id}")    

    def scanTable(self):
            tabla = dynamodb.Table(self.tableName)
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

