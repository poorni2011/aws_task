import pymysql
import json
import base64

# Database connection details
RDS_HOST = ""
RDS_USER = ""
RDS_PASSWORD = ""
RDS_DATABASE = ""

def retrieve_metadata(user_id=None):
    try:
        connection = pymysql.connect(
            host=RDS_HOST,
            user=RDS_USER,
            password=RDS_PASSWORD,
            database=RDS_DATABASE,
            connect_timeout=10
        )

        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            if user_id:
                select_query = """
                    SELECT image_id, user_id, filename, upload_timestamp, s3_url
                    FROM metadata
                    WHERE user_id = %s
                """
                cursor.execute(select_query, (user_id,))
            else:
                select_query = """
                    SELECT image_id, user_id, filename, upload_timestamp, s3_url
                    FROM metadata
                """
                cursor.execute(select_query)

            results = cursor.fetchall()

        connection.close()

        for result in results:
            upload_timestamp = result['upload_timestamp']
            if isinstance(upload_timestamp, str):
                result['upload_timestamp'] = upload_timestamp
            else:
                result['upload_timestamp'] = str(upload_timestamp)
        return results

    except Exception as e:
        print(f"Error occurred while retrieving metadata: {str(e)}")
        raise

def lambda_handler(event, context):
    try:
    
        user_id = event.get("userId")
        
        if not user_id:
            print("No userId provided, retrieving all metadata.")
        
        metadata = retrieve_metadata(user_id)

        if not metadata:
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'message': 'No metadata found.'
                })
            }

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Metadata retrieved successfully.',
                'data': metadata
            })
        }

    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal Server Error', 'error': str(e)})
        }
