from google.cloud import storage
import urllib.request

project_id = 'credible-glow-362909'
bucket_name = 'datafellowship'
destination_blob_name = 'storage-object-name'
storage_client = storage.Client.from_service_account_json('account-client.json')

source_file_name = 'https://i2-prod.chroniclelive.co.uk/sport/football/football-news/article24095885.ece/ALTERNATES/s810/0_GettyImages-1240989090.jpg'

def upload_blob(bucket_name, source_file_name, destination_blob_name):   
    file = urllib.request.urlopen(source_file_name)

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(file.read(), content_type='image/jpg')

upload_blob(bucket_name, source_file_name, destination_blob_name)