import json
import logging
import datamodel
from dataaccess.data_access import DataAccess
from utility.product_processor import ProductProcessor
from datamodel.custom_enums import JobStatus
from datamodel.custom_exceptions import ShopifyUnauthorizedError
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

    context: object, required
        Lambda Context runtime methods and attributes

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict
    """

    data_access = DataAccess()
    message_payload = json.loads(event['Records'][0]['Sns']['Message'])
    job_id = message_payload['jobId']
    user_id = message_payload['userId']
    job = data_access.get_job(job_id, user_id)
    user = data_access.get_user_by_id(user_id)
    user_domain = user['domain']
    user_token = user['access_token']
    product_file_key = job['input_products']
    product_file_content = data_access.get_product_file(product_file_key)
    product_items = json.loads(product_file_content)
    
    processor_info = {
        'products': product_items,
        'user_id': user_id,
        'job_id': job_id,
        'batch': int(job.get('current_batch')),
        'domain': user_domain,
        'access_token': user_token,
        'type': job.get('type')
    }
    
    try:
        processor = ProductProcessor(processor_info)
        is_completed = processor.process()
    except Exception as error:
        logging.exception('An exception interrupted the job processing. JobId: %s, Error Details: %s', job_id, str(error))
        data_access.finish_job_transaction({
            'id': job_id,
            'user_id': user_id,
            'status': JobStatus.FAILED.name,
            'duration': get_job_duration(job_id, user_id, data_access)
        })
    else:
        if is_completed: 
            exceeded_limit = job['product_limit_exceeded']
            status = JobStatus.COMPLETED.name
            if exceeded_limit: status = JobStatus.PARTIAL_COMPLETE.name

            print(get_job_duration(job_id, user_id, data_access))
            data_access.finish_job_transaction({
                'id': job_id,
                'user_id': user_id,
                'status': status,
                'duration': get_job_duration(job_id, user_id, data_access)
            })
        else:
            next_batch = int(job.get('current_batch')) + 1,
            data_access.basic_job_update({
                'id': job_id,
                'user_id': user_id,
                'current_batch': next_batch
            })
            data_access.publish_to_product_processor({
                'jobId': job_id,
                'userId': user_id
            })
    return None


def convert_seconds_to_duration(seconds):
    h = 0 
    m = 0 
    s = 0
    duration = ''
    m,s = divmod(seconds, 60)
    h,m = divmod(m, 60)

    if h > 0:
        if h == 1: duration += str(h) + ' hr '
        else: duration += str(h) + ' hrs'
    if m > 0:
        if m == 1: duration += str(m) + ' min '
        else: duration += str(m) + ' mins'
    if s > 0:
        duration += str(s) + ' sec'
    return duration


def get_job_duration(job_id, user_id, data_access):
    job = data_access.get_job(job_id, user_id)
    start_time = datetime.strptime(job['start_time'], '%Y-%m-%dT%H:%M:%S.%fZ')
    end_time = datetime.utcnow()
    duration = round((end_time - start_time).total_seconds())
    return convert_seconds_to_duration(duration)


