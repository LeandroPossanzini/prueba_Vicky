import boto3
from botocore.config import Config
import time
client = boto3.client('timestream-write')


class Timestream():
    def __init__(self,databaseName):
        self.databaseName = databaseName

    def createDatabase(self):
        try:
            response = client.create_database(DatabaseName=self.databaseName)
            print("Database [%s] creada." % self.databaseName)
        except Exception as err:
            print("No se creo la tabla" + err)    

    def showDatabases(self):
        print("Las DataBases son: ")
        try:
            result = client.list_databases(MaxResults=5)
            self._print_databases(result['Databases'])
            next_token = result.get('NextToken', None)
            while next_token:
                result =client.list_databases(NextToken=next_token, MaxResults=5)
                self._print_databases(result['Databases'])
                next_token = result.get('NextToken', None)
        except Exception as err:
            print("Error en mostrar la base de datos:", err)

    def deleteDatabase(self):
        print("Borrando Database")
        try:
            result = client.delete_database(DatabaseName=self.databaseName)
            print("Se borro la base de datos [%s]" % self.databaseName)
        except Exception as err:
            print("No se borro la base de datos:", err)


    def list_tables(self):
        print("Listing tables")
        try:
            result = client.list_tables(DatabaseName=self.databaseName, MaxResults=5)
            self.__print_tables(result['Tables'])
            next_token = result.get('NextToken', None)
            while next_token:
                result = client.list_tables(DatabaseName=self.databaseName,
                                                 NextToken=next_token, MaxResults=5)
                self.__print_tables(result['Tables'])
                next_token = result.get('NextToken', None)
        except Exception as err:
            print("List tables failed:", err)

    @staticmethod
    def _print_databases(databases):
        for database in databases:
            print(database['DatabaseName'])

    
    @staticmethod
    def __print_tables(tables):
        for table in tables:
            print(table['TableName'])


class TimestreamTable():
    def __init__(self,tableName):
        self.tableName = tableName

    def createTable(self):
        retention_properties = {
            'MemoryStoreRetentionPeriodInHours': 5,
            'MagneticStoreRetentionPeriodInDays': 1
        }
        try:
            client.create_table(DatabaseName="base-prueba", TableName=self.tableName,
                                     RetentionProperties=retention_properties)
            print("Table [%s] se creo correctamente." % self.tableName)
        except Exception as err:
            print("No se creo la tabla:", err)

    def destructurar(self,item):
        id, nodo, region, valor = item
        return {'id':id,'nodo':nodo, 'regiones':region, 'valor':valor}

    def createItem(self, item):
        print("Escribiendo en la Tabla")
        current_time = self._current_milli_time()
        items = self.destructurar(item)
        dimensions = [
            {'Name': 'id', 'Value': items['id']},
            {'Name': 'nodo', 'Value': items['nodo']},
            {'Name': 'region', 'Value': items['regiones']}
        ]

        random_values = {
            'Dimensions': dimensions,
            'MeasureName': 'random_values',
            'MeasureValue': items['valor'],
            'MeasureValueType': 'DOUBLE',
            'Time': current_time
        }

        records = [random_values]

        try:
            result = client.write_records(DatabaseName="base-prueba", TableName=self.tableName,
                                               Records=records, CommonAttributes={})
            print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except Exception as err:
            print("Error:", err)
    
    def deleteTable(self):
        try:
            result = client.delete_table(DatabaseName="base-prueba", TableName=self.tableName)
            print("Se elimino la tabla con nombre [%s]" % self.tableName)
        except Exception as err:
            print("No se pudo eliminar la tabla:", err)


    @staticmethod
    def _current_milli_time():
        return str(int(round(time.time() * 1000)))