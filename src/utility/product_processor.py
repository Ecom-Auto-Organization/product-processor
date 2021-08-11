from collections import Counter
import logging
import json
import asyncio
import aiohttp
from boto3 import session
from botocore.vendored.six import reraise
from datamodel.custom_exceptions import MissingArgumentError
from datamodel.custom_exceptions import ShopifyUnauthorizedError
from datamodel.custom_exceptions import DataAccessError
from datamodel.custom_enums import JobStatus
from datamodel.custom_enums import ResultStatus
from dataaccess.data_access import DataAccess
from custom_utils import utils
from datetime import datetime
import os
import time


class ProductProcessor:
    """
    Class to process and create products on shopify
    """

    def __init__(self, product_info):
        if product_info is not None:
            self._products = product_info.get('products')
            self._user_id = product_info.get('user_id')
            self._job_id = product_info.get('job_id')
            self._job_type = product_info.get('type')
            self._batch = product_info.get('batch')
            self._batch_size = int(os.environ.get('batch_size'))
            self._domain = product_info.get('domain')
            self._access_token = product_info.get('access_token')
            self._data_access = DataAccess()
            self._current_rate_limit = 1000
            self._batch_duration = 0
        else:
            raise MissingArgumentError('Missing argument for ProductProcessor class')

    
    def process(self):
        if self._batch == 1:
            self._data_access.basic_job_update({
                'id': self._job_id,
                'user_id': self._user_id,
                'status': JobStatus.RUNNING.name,
                'start_time': datetime.utcnow().isoformat() + 'Z',
                'type': self._job_type
            })

        batch_start_index = (self._batch - 1) * self._batch_size
        batch_end_index = (self._batch * self._batch_size) - 1
        product_end_index = len(self._products) - 1
        is_last_batch = False
        batch_products = []

        if batch_end_index >= product_end_index:
            batch_products = self._products[batch_start_index:]
            is_last_batch = True
        else:
            batch_products = self._products[batch_start_index: (batch_end_index + 1)]

        async_batch_size = 50 #number of products that should be run asynchronously
        async_batches = [batch_products[i: i + async_batch_size] for i in range(0, len(batch_products), async_batch_size)]
        result_counter = batch_start_index + 1

        for batch in async_batches:   
            #We are checking to ensure that we have enough rate limit to 
            #run batch asynchonously. If not we delay a bit so we can recover rate
            #limit to run the batch without hitting our limit
            rate_limit_needed_for_batch = 750
            limit_restore_rate = 50
            actual_rate_limit_left = self._current_rate_limit + (self._batch_duration * limit_restore_rate)
            if actual_rate_limit_left < rate_limit_needed_for_batch:
                extra_limit_needed = rate_limit_needed_for_batch - actual_rate_limit_left
                time_to_wait = (extra_limit_needed // limit_restore_rate) + 1
                time.sleep(time_to_wait)

            start = time.time()
            asyncio.run(self.__create_products(batch, result_counter))
            result_counter += async_batch_size
            end = time.time()
            self._batch_duration = int(end - start)
        
        if is_last_batch:
            return True
        else:
            return False


    async def __create_products(self, products, result_counter):
        counter = result_counter
        async with aiohttp.ClientSession() as session:
            tasks = []
            for product in products:
                if 'option1Name' in product: del product['option1Name']
                if 'option2Name' in product: del product['option2Name']
                if 'option3Name' in product: del product['option3Name']
                if 'variantTitles' in product: del product['variantTitles']
                errors = product['errors']
                warnings = product['warnings']
                del product['errors']
                del product['warnings']
                if len(product['variants']) > 100:
                    errors.append('The number of product variants for this product exceeds the shopify limit of 100 variants')
                if 'collectionsToJoin' in product and len(product['collectionsToJoin'] > 4):
                    errors.append('Maximum Collections to add a product to cannot is limited to 4')
                tasks.append(self.__put_shopify_product(product, counter, errors, warnings, session))
                counter += 1
            await asyncio.gather(*tasks)


    async def __put_shopify_product(self, product_item, counter, errors, warnings, session):
        if 'collectionsToJoin' in product_item:
            await self.__modify_product(product_item, warnings)
        response = None
        try:
            response = await self._data_access.create_shopify_product(product_item, self._domain, self._access_token, session)
        except ShopifyUnauthorizedError as error:
            logging.exception(str(error))
            raise ShopifyUnauthorizedError(error)
        except (DataAccessError, Exception) as error:
            logging.exception('An Error occured whiles creating shopify product. Details are JobId: %s, Products: %s, Error: %s', self._job_id, product_item, str(error))
        else:
            product_result = self.__check_product_result(product_item, response, errors)
            self.__put_result(product_result['product'], product_result['result'], errors, warnings, counter)
            if 'extensions' in response:
                self._current_rate_limit = response['extensions']['cost']['throttleStatus']['currentlyAvailable']


    def __check_product_result(self, product, response, errors):
        result = None
        if response is None:
            errors.append('An issue occured whiles creating the product.')
            result = ResultStatus.FAILED.name
        elif 'errors' in response:
            logging.error('A graphql syntax error occured whiles creating product. Product Details: %s, Error: %s', product, response['errors'])
            errors.append('An issue occured whiles creating the product.')
            result = ResultStatus.FAILED.name
        elif response['data']['productCreate']['product'] is None:
            user_errors = response['data']['productCreate']['userErrors']
            error_messages = self.__get_shopify_user_errors(user_errors)
            errors.extend(error_messages)
            result = ResultStatus.FAILED.name
        elif response['data']['productCreate']['product'] is not None:
            product = response['data']['productCreate']['product']
            result = ResultStatus.SUCCESS.name
        else:
            logging.error('An Unknown error occured whiles creating product on shopify. Response Details: %s', response)
            result = ResultStatus.FAILED.name
        return {
            'result': result,
            'product': product
        }     


    def __get_shopify_user_errors(self, user_errors):
        errors = []
        for error in user_errors:
            field = ''
            for name in error['field']:
                field += str(name) + ' '

            message = error['message']
            error_message = field.strip() + ': ' + message
            errors.append(error_message)
        return errors


    def __put_result(self, product_item, status, errors, warnings, result_id):
        try:
            product_result = {
                'id': str(result_id),
                'job_id': self._job_id,
                'data': json.dumps(product_item),
                'status': status,
            }
            job = {
                'id': self._job_id,
                'user_id': self._user_id
            }
            if len(errors) > 0: product_result['errors'] = json.dumps(errors)
            if len(warnings) > 0: product_result['warnings'] = json.dumps(warnings)
            self._data_access.add_result_transaction(product_result, job)
        except Exception as error:
            logging.error('An error occured whiles adding product result to database. Details: %s', str(error))


    async def __get_collection_id(self, input, get_type, session):
        try:
            response = None
            if get_type == 'ID':
                response = await self._data_access.get_collection_by_id(input, self._domain, self._access_token, session)
                if response['data']['collection'] is not None:
                    return response['data']['collection']['id']
                else:
                    return 'ID_NOT_FOUND'
            elif get_type == 'NAME':
                response = await self._data_access.search_collection_by_name(input, self._domain, self._access_token, session)
                if len(response['data']['collections']['edges']) > 0:
                    return response['data']['collections']['edges'][0]['node']['id']
                else:
                    return 'NAME_NOT_FOUND'
        except Exception as error:
            logging.exception('Failed to get Collection name. Details, Job Id: %s, Input: %s, Error: %s', self._job_id, input, str(error))     
            return None             

    async def __modify_product(self, product, warnings, session):
        collections = product['collectionsToJoin']
        valid_collections = []
        entity_type = 'Collection'
        get_type = ''

        for col in collections:
            value = col
            if utils.is_id(col):
                get_type = 'ID'
                value = utils.get_gid(col, entity_type)
            elif utils.is_gid(col, get_type):
                get_type = 'ID'
            else:
                get_type = 'NAME'

            collection_id = await self.__get_collection_id(value, get_type, session)
            if collection_id is None:
                message = 'Could not find Collection ' + col + '.'
                warnings.append(message)
            elif collection_id == 'ID_NOT_FOUND':
                message = 'Collection with Id ' + col + ', was not found.'
                warnings.append(message)
            elif collection_id == 'ID_NOT_FOUND':
                message = 'Collection with name ' + col + ', was not found.'
                warnings.append(message)
            else:
                valid_collections.append(collection_id)
        
        product['collectionsToJoin'] = valid_collections
