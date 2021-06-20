import logging
import json
from datamodel.custom_exceptions import MissingArgumentError
from datamodel.custom_exceptions import ShopifyUnauthorizedError
from datamodel.custom_exceptions import DataAccessError
from datamodel.custom_enums import JobStatus
from datamodel.custom_enums import ResultStatus
from dataaccess.data_access import DataAccess
from custom_utils import utils
import os


class ProductProcessor:
    """
    Class to process and create products on shopify
    """

    def __init__(self, product_info):
        if product_info is not None:
            self._products = product_info.get('products')
            self._user_id = product_info.get('user_id')
            self._job_id = product_info.get('job_id')
            self._batch = product_info.get('batch')
            self._batch_size = int(os.environ.get('batch_size'))
            self._domain = product_info.get('domain')
            self._access_token = product_info.get('access_token')
            self._data_access = DataAccess()
        else:
            raise MissingArgumentError('Missing argument for ProductProcessor class')

    
    def process(self):
        if self._batch == 1:
            self._data_access.basic_job_update({
                'id': self._job_id,
                'user_id': self._user_id,
                'status': JobStatus.RUNNING.name
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
        
        result_counter = batch_start_index + 1
        for product in batch_products:
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

            if len(errors) > 0:
                self.__add_result(product, ResultStatus.FAILED.name, errors, warnings, result_counter)
            else:
                if 'collectionsToJoin' in product:
                    self.__modify_product(product, warnings)
                response = self.__create_product(product)
                if response is None:
                    self.__add_result(product, ResultStatus.FAILED.name, errors, warnings, result_counter)
                elif 'errors' in response:
                    logging.error('A graphql syntax error occured whiles creating product. Product Details: %s, Error: %s', product, response['errors'])
                    errors.append('An issue occured whiles creating the product.')
                    self.__add_result(product, ResultStatus.FAILED.name, errors, warnings, result_counter)
                elif response['data']['productCreate']['product'] is None:
                    user_errors = response['data']['productCreate']['userErrors']
                    error_messages = self.__get_shopify_user_errors(user_errors)
                    errors.extend(error_messages)
                    self.__add_result(product, ResultStatus.FAILED.name, errors, warnings, result_counter)
                elif response['data']['productCreate']['product'] is not None:
                    product = response['data']['productCreate']['product']
                    self.__add_result(product, ResultStatus.SUCCESS.name, errors, warnings, result_counter)
                else:
                    logging.error('An Unknown error occured whiles creating product on shopify. Response Details: %s', response)
                    self.__add_result(product, ResultStatus.FAILED.name, errors, warnings, result_counter)

            result_counter +=1
        
        # Return True if all submitted products are completed, else return False.
        if is_last_batch:
            return True
        else:
            return False


    def __create_product(self, product_item):
        try:
            response = self._data_access.create_shopify_product(product_item, self._domain, self._access_token)
            return response
        except ShopifyUnauthorizedError as error:
            self._data_access.basic_job_update({
            'id': self._job_id,
            'user_id': self._user_id,
            'status': JobStatus.FAILED.name
        })
            raise ShopifyUnauthorizedError(error)
        except (DataAccessError, Exception) as error:
            logging.error('An Error occured whiles creating shopify product. Details are JobId: %s, Products: %s, Error: %s', self._job_id, product_item, str(error))
            return None


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


    def __add_result(self, product_item, status, errors, warnings, result_id):
        try:
            product_result = {
                'id': str(result_id),
                'job_id': self._job_id,
                'data': json.dumps(product_item),
                'status': status,
            }
            if len(errors) > 0: product_result['errors'] = errors
            if len(warnings) > 0: product_result['warnings'] = warnings
            self._data_access.put_result(product_result)
        except Exception as error:
            logging.error('An error occured whiles adding product result to database. Details: %s', str(error))


    def __get_collection_id(self, input, get_type):
        try:
            response = None
            if get_type == 'ID':
                response = self._data_access.get_collection_by_id(input, self._domain, self._access_token)
                if response['data']['collection'] is not None:
                    return response['data']['collection']['id']
                else:
                    return 'ID_NOT_FOUND'
            elif get_type == 'NAME':
                response = self._data_access.search_collection_by_name(input, self._domain, self._access_token)
                if len(response['data']['collections']['edges']) > 0:
                    return response['data']['collections']['edges'][0]['node']['id']
                else:
                    return 'NAME_NOT_FOUND'
        except Exception as error:
            logging.error('Failed to get Collection name. Details, Job Id: %s, Input: %s, Error: %s', self._job_id, input, str(error))     
            return None             

    def __modify_product(self, product, warnings):
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

            collection_id = self.__get_collection_id(value, get_type)
            if collection_id is None:
                message = 'Could not Collection ' + col + '.'
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
