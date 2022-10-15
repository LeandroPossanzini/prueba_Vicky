import boto3
from botocore.config import Config

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

    def delete_database(self):
        print("Borrando Database")
        try:
            result = client.delete_database(DatabaseName=self.databaseName)
            print("Se borro la base de datos [%s]" % self.delete_database)
        except Exception as err:
            print("No se borro la base de datos:", err)


    @staticmethod
    def _print_databases(databases):
        for database in databases:
            print(database['DatabaseName'])