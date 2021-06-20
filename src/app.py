import json
from platform import processor
import requests
import datamodel
from dataaccess.data_access import DataAccess
from utility.product_processor import ProductProcessor
from datamodel.custom_enums import JobStatus

def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    # try:
    #     ip = requests.get("http://checkip.amazonaws.com/")
    # except requests.RequestException as e:
    #     # Send some context about this error to Lambda Logs
    #     print(e)

    #     raise e
    # url = 'https://kwame-boateng-571.myshopify.com/admin/api/2021-04/graphql.json'
    # surl = 'https://kwame-boateng-571.myshopify.com/admin/api/2021-04/shop.json'
    # token = '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'
    # headers = {'Content-Type': 'application/json', 'X-Shopify-Access-Token': token}

    # query =  """query ($title: String){
    #                 collections(first:2, query:$title) {
    #                     edges {
    #                         node {
    #                             id
    #                             title
    #                         }
    #                     }
    #                 }
    #             }"""

    # query = """query ($id: ID!) {
    #                 collection(id: $id) {
    #                     title
    #                     id
    #                 }
    #             }"""

    # title = 'title:home p'
    # id = "gid://shopify/Collection/270732165305"
    # variables = {'id': id}
    # results = requests.post(url, json={'query': query, 'variables': variables}, headers=headers, timeout=10.0)
    # results = requests.get(surl, headers=headers)
    # print(results.status_code)
    # response = results.json()
    # print(response)
    # print(response['data']['collection']['id'])
    # print(response['data']['collections']['edges'][0]['node']['id'])

    data_access = DataAccess()
    message_payload = json.loads(event['Records'][0]['Sns']['Message'])
    job_id = message_payload['jobId']
    job = data_access.get_job(job_id)
    user_id = job['user_id']
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
        'access_token': user_token
    }
    processor = ProductProcessor(processor_info)
    is_completed = processor.process()

    if is_completed:
        data_access.basic_job_update({
            'id': job_id,
            'user_id': user_id,
            'status': JobStatus.COMPLETED.name
        })
    else:
        next_batch = int(job.get('current_batch')) + 1,
        data_access.basic_job_update({
            'id': job_id,
            'user_id': user_id,
            'current_batch': next_batch
        })
        data_access.publish_to_product_processor({
            'jobId': job_id
        })
    return None