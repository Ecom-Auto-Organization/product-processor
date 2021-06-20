import logging
import boto3
import json
import requests
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from datamodel.custom_exceptions import DataAccessError
from datamodel.custom_exceptions import ShopifyUnauthorizedError
from datamodel import data_model_utils
from custom_utils import utils
import os
from http import HTTPStatus


class DataAccess:
    """ 
    Class for getting data and adding data to database and other sources

    """

    def __init__(self):
        self._prepared_products_bucket = os.environ.get('prepared_products_bucket')
        self._s3_client = boto3.client('s3')
        bulk_manager_table = os.environ.get('bulk_manager_table')
        self._dynamodb = boto3.resource('dynamodb')
        self._bulk_manager_table =  self._dynamodb.Table(bulk_manager_table) 
        self._sns_client = boto3.client('sns')
        self._api_version = os.environ.get('shopify_api_version')


    def get_job(self, job_id):
        job = utils.join_str('job#', job_id)
        prefix = 'user'

        try:
            response = self._bulk_manager_table.query(
                KeyConditionExpression=Key('PK').eq(job) & Key('SK').begins_with(prefix)
            )

            job = None
            if len(response['Items']) > 0:
                db_job = response['Items'][0]
                job = data_model_utils.extract_job_details(db_job)
            return job
        except ClientError as error:
            raise DataAccessError(error)


    def get_product_file (self, file_key):
        try:
            response = self._s3_client.get_object (
                Bucket=self._prepared_products_bucket,
                Key=file_key
            )
            return response['Body'].read()
        except ClientError as error:
            raise DataAccessError(error)


    def get_user_by_id(self, user_id):
        user_to_get = {'id': user_id}
        db_user = data_model_utils.convert_to_db_user(user_to_get)

        try:
            response = self._bulk_manager_table.get_item(Key=db_user)
            user = None
            if 'Item' in response:
                db_user = response['Item']
                user = data_model_utils.extract_user_details(db_user)
            return user
        except ClientError as error:
            raise DataAccessError(error)

    def basic_job_update (self, job):
        if 'id' not in job or 'user_id' not in job:
            raise KeyError('\'id\' and \'user_id\' value for job cannot be null')
        
        db_job = data_model_utils.convert_to_db_job(job)
        # assign primary key to Keys Attribute and remove primary keys 
        # from db_job since we don't intent to modify them
        primary_key = {'PK': db_job['PK'], 'SK': db_job['SK']}
        del db_job['PK']
        del db_job['SK']

        expression_attr_values = utils.get_expression_attr_values(db_job)
        update_expression = utils.get_update_expression(expression_attr_values)

        try:
            response = self._bulk_manager_table.update_item(
                Key=primary_key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attr_values,
                ReturnValues='UPDATED_NEW'
            )

            logging.info('Updated job successfully: %s', response)
            return True
        except ClientError as error:
            raise DataAccessError(error)
        except Exception as error:
            raise DataAccessError(error)


    def put_result(self, result):
        db_result= data_model_utils.convert_to_db_result(result)

        try:
            response = self._bulk_manager_table.put_item(
                Item=db_result
            )

            logging.info('Added product result successfully. Details: %s', db_result)
            return data_model_utils.extract_result_details(db_result)
        except ClientError as error:
            raise DataAccessError(error)
        except Exception as error:
            raise DataAccessError(error) 


    def publish_to_product_processor(self, message):
        import_topic = os.environ.get('import_topic_arn')
        try:
            response = self._sns_client.publish(
                TopicArn=import_topic,
                Message=json.dumps(message),
                MessageAttributes={
                    'process': {
                        'DataType': 'String',
                        'StringValue': 'process-product'
                    }
                }
            )
            if 'MessageId' in response:
                return True
        except Exception as error:
            raise Exception('Could not publish message successfully. Error:' + str(error))


    def create_shopify_product(self, product_item, domain, access_token):
        url = 'https://' + domain + '/admin/api/' + self._api_version + '/graphql.json'
        headers = {'Content-Type': 'application/json', 'X-Shopify-Access-Token': access_token}

        query =  """mutation productCreate($input: ProductInput!) {
                    productCreate(input: $input) {
                        product {
                            id
                            title
                            createdAt
                            featuredImage {
                                originalSrc
                            }
                        }
                        userErrors {
                            field
                            message
                        }
                    }
                }"""
        variables = {'input': product_item}

        response = None
        results = requests.post(url, json={'query': query, 'variables': variables}, headers=headers, timeout=30.0)
        if results.status_code == HTTPStatus.OK:
            response = results.json()
            return response
        elif results.status_code == HTTPStatus.UNAUTHORIZED:
            raise ShopifyUnauthorizedError("Shopify graphql request did not have the necessary credentials")
        else:
            raise DataAccessError('Product create request failed. Status Code: ' + str(results.status_code))


    def search_collection_by_name(self, collection_name, domain, access_token):
        url = 'https://' + domain + '/admin/api/' + self._api_version + '/graphql.json'
        headers = {'Content-Type': 'application/json', 'X-Shopify-Access-Token': access_token}

        query =  """query ($title: String){
                    collections(first:2, query:$title) {
                        edges {
                            node {
                                id
                                title
                            }
                        }
                    }
                }"""
        title = 'title:' + collection_name
        variables = {'title': title}

        response = None
        results = requests.post(url, json={'query': query, 'variables': variables}, headers=headers, timeout=10.0)
        if results.status_code == HTTPStatus.OK:
            response = results.json()
            return response
        elif results.status_code == HTTPStatus.UNAUTHORIZED:
            raise ShopifyUnauthorizedError("Shopify graphql request did not have the necessary credentials")
        else:
            raise DataAccessError('Product create request failed. Status Code: ' + str(results.status_code))
            

    def get_collection_by_id(self, gid, domain, access_token):
        url = 'https://' + domain + '/admin/api/' + self._api_version + '/graphql.json'
        headers = {'Content-Type': 'application/json', 'X-Shopify-Access-Token': access_token}

        query = """query ($id: ID!) {
                    collection(id: $id) {
                        title
                        id
                    }
                }"""
        variables = {'id': gid}

        response = None
        results = requests.post(url, json={'query': query, 'variables': variables}, headers=headers, timeout=10.0)
        if results.status_code == HTTPStatus.OK:
            response = results.json()
            return response
        elif results.status_code == HTTPStatus.UNAUTHORIZED:
            raise ShopifyUnauthorizedError("Shopify graphql request did not have the necessary credentials")
        else:
            raise DataAccessError('Product create request failed. Status Code: ' + str(results.status_code))
