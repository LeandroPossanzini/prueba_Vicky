import boto3

# POR DEFECTO SE LE PASA LA LOCACION US-EAST-1

client = boto3.client("s3")

def createBucket():
    try:
        bucket_name = "bucket-prueba-leandro"

        response = client.create_bucket(Bucket=bucket_name)

        print("SE CREO EL BUCKET")
    except:
        print("PROBLEMAS AL CREAR EL BUCKET")

def allBuckets():
    try:
        response = client.list_buckets()
        print("Los buckets creados son:")
        for bucket in response['Buckets']:
            print(f"-- {bucket['Name']}")
    except:
        print("No se pudo acceder a leer los buckets")

def deleteBucket():
    try:
        bucket_name = "bucket-prueba-leandro"
        client.delete_bucket(Bucket=bucket_name)
        print(f"Se borro el bucket {bucket_name}")
    except:
        print("error en borrar el bucket")    
       