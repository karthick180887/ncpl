import boto3
import fitz  # PyMuPDF
from PIL import Image
import io
import os
import urllib.parse
import uuid
from datetime import datetime

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('FileMetadata')

# Environment variables
TARGET_BUCKET_NAME = os.environ.get('TARGET_BUCKET_NAME')  # Bucket to store thumbnails
THUMBNAIL_PREFIX = os.environ.get('THUMBNAIL_PREFIX', 'thumbnails/')  # Prefix for thumbnails
THUMBNAIL_SIZE = os.environ.get('THUMBNAIL_SIZE', '200x200')  # Desired WxH

def should_process_event(event):
    """Determine if we should process this S3 event"""
    # Get object key from either format
    if 'Records' in event:
        key = event['Records'][0]['s3']['object']['key']
    else:
        key = event['detail']['object']['key']
    
    key = urllib.parse.unquote_plus(key)
    return key.lower().endswith('.pdf') and not key.lower().startswith(THUMBNAIL_PREFIX.lower())

def store_metadata(file_id, original_file, thumbnail_path, file_size):
    """Store file metadata in DynamoDB"""
    try:
        response = table.put_item(
            Item={
                'file_id': file_id,
                'original_file': original_file,
                'thumbnail_path': thumbnail_path,
                'file_size': file_size,
                'file_type': 'pdf',
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
        )
        return response
    except Exception as e:
        print(f"Error storing metadata: {str(e)}")
        raise

def lambda_handler(event, context):
    print("Received event:", event)

    # Early exit if this isn't a PDF upload we should process
    if not should_process_event(event):
        print("Skipping event - not a PDF upload or is a thumbnail")
        return {'statusCode': 200, 'body': 'Skipped non-PDF or thumbnail'}

    try:
        # Parse event to get bucket and key
        if 'Records' not in event:
            source_bucket_name = event['detail']['bucket']['name']
            source_object_key = urllib.parse.unquote_plus(event['detail']['object']['key'])
            file_size = event['detail']['object']['size']
        else:
            record = event['Records'][0]
            source_bucket_name = record['s3']['bucket']['name']
            source_object_key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
            file_size = record['s3']['object']['size']

        print(f"Processing: s3://{source_bucket_name}/{source_object_key}")

        # Generate unique ID for this file
        file_id = str(uuid.uuid4())

        # Download PDF
        download_path = f'/tmp/{os.path.basename(source_object_key)}'
        s3_client.download_file(source_bucket_name, source_object_key, download_path)

        # Convert to thumbnail
        doc = fitz.open(download_path)
        if not doc.page_count > 0:
            doc.close()
            os.remove(download_path)
            return {'statusCode': 400, 'body': 'PDF has no pages.'}

        page = doc.load_page(0)
        pix = page.get_pixmap(dpi=150)
        doc.close()

        # Create thumbnail
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        width, height = map(int, THUMBNAIL_SIZE.split('x'))
        img.thumbnail((width, height))

        # Upload thumbnail
        thumbnail_io = io.BytesIO()
        img.save(thumbnail_io, format='PNG')
        thumbnail_io.seek(0)

        thumbnail_key = f"{THUMBNAIL_PREFIX}{os.path.splitext(os.path.basename(source_object_key))[0]}.png"
        target_bucket = TARGET_BUCKET_NAME if TARGET_BUCKET_NAME else source_bucket_name

        s3_client.put_object(
            Bucket=target_bucket,
            Key=thumbnail_key,
            Body=thumbnail_io,
            ContentType='image/png'
        )

        # Store metadata in DynamoDB
        store_metadata(
            file_id=file_id,
            original_file=f"s3://{source_bucket_name}/{source_object_key}",
            thumbnail_path=f"s3://{target_bucket}/{thumbnail_key}",
            file_size=file_size
        )

        # Clean up
        os.remove(download_path)
        return {
            'statusCode': 200,
            'body': {
                'message': 'Successfully processed PDF',
                'file_id': file_id,
                'thumbnail_path': thumbnail_key
            }
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        if 'download_path' in locals() and os.path.exists(download_path):
            try:
                os.remove(download_path)
            except Exception as e:
                print(f"Cleanup error: {str(e)}")
        return {
            'statusCode': 500,
            'body': str(e)
        }
