import json
import boto3
import string
import random
import pymysql
import base64
from datetime import datetime

# Database connection details
RDS_HOST = "testrds.c94km0g8oihe.ap-southeast-2.rds.amazonaws.com"
RDS_USER = "admin"
RDS_PASSWORD = "R2ftysibhji"
RDS_DATABASE = "testrds"


def decode_jwt_without_verification(token):
    try:
        token_parts = token.split('.')
        if len(token_parts) != 3:
            raise ValueError("Invalid JWT structure.")
        
        payload = token_parts[1]
        padded_payload = payload + '=' * (-len(payload) % 4)
        decoded_payload = base64.urlsafe_b64decode(padded_payload).decode('utf-8')
        payload_data = json.loads(decoded_payload)
        return payload_data
    except Exception as e:
        raise ValueError(f"Error decoding JWT: {e}")

# Function to extract user ID from the Authorization header
def extract_user_id_from_token(authorization_header):
    if not authorization_header or not authorization_header.startswith("Bearer "):
        raise ValueError("Invalid or missing Authorization header.")
    
    token = authorization_header.split(" ")[1]
    payload = decode_jwt_without_verification(token)
    user_id = payload.get("sub")
    if not user_id:
        raise ValueError("The 'sub' claim is missing in the token.")
    return user_id


def lambda_handler(event, context):
    try:
        print("Headers received:", event.get("headers", {}))

      
        authorization_header = event.get("headers", {}).get("Authorization") or event.get("headers", {}).get("authorization")
        if not authorization_header:
            raise ValueError("Authorization header is missing.")
        
        user_id = extract_user_id_from_token(authorization_header)
        print(f"Extracted user ID: {user_id}")  # Ensure full user_id

        get_file_content = event["body"]
        pic_filename = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)) + ".png"
        
        bucket_name = "poorni-s3"
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Key=pic_filename, Body=get_file_content)
        print(f"File {pic_filename} uploaded to S3 successfully. User ID: {user_id}")
        
        s3_url = f"https://{bucket_name}.s3.amazonaws.com/{pic_filename}"
        
        image_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))
        upload_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        
        # Connect to RDS
        connection = pymysql.connect(
            host=RDS_HOST,
            user=RDS_USER,
            password=RDS_PASSWORD,
            database=RDS_DATABASE,
            connect_timeout=10
        )
        
        with connection.cursor() as cursor:
            cursor.execute("CREATE DATABASE IF NOT EXISTS `testrds`")
            connection.commit()
            
            cursor.execute("USE `testrds`")
            
            alter_query = "ALTER TABLE metadata MODIFY user_id VARCHAR(255);"
            cursor.execute(alter_query)
            connection.commit()
            print("user_id column size increased to VARCHAR(255).")
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS metadata (
                    image_id VARCHAR(12) NOT NULL,
                    user_id VARCHAR(255) NOT NULL,
                    filename VARCHAR(255) NOT NULL,
                    upload_timestamp DATETIME NOT NULL,
                    s3_url VARCHAR(255) NOT NULL,
                    PRIMARY KEY (image_id)
                )
            """)
            connection.commit()
            
            # Insert metadata into RDS
            insert_query = """
                INSERT INTO metadata (image_id, user_id, filename, upload_timestamp, s3_url)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (image_id, user_id, pic_filename, upload_timestamp, s3_url))
            connection.commit()
            print(f"Metadata inserted into RDS successfully: user_id={user_id}, image_id={image_id}, date={upload_timestamp}")
        
        connection.close()
        
        return {
            'statusCode': 200,
            'body': json.dumps("Image and metadata uploaded successfully!")
        }
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Internal server error: {str(e)}")
        }
