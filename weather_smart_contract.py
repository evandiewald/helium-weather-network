import datetime
import os
from blockchain import Chain, BlockchainConnection, get_timestamp
import pickle
import blockchain


class SmartContract(object):
    def __init__(self, blockchain_obj: BlockchainConnection, user_role, contract_id=None):
        if user_role is 'admin' or user_role is 'engineer' or user_role is 'technician':
            print('Interacting with smart contract as user role: ', user_role)
        # else:
        #     raise AssertionError('Invalid user role. Must be technician, engineer, or admin')
        if contract_id is None:
            self.contract_id = blockchain.get_next_contract_id(blockchain_obj.cnx)
        else:
            self.contract_id = contract_id
        self.user_role = user_role
        self.blockchain = blockchain_obj

        try:
            self.contract_header = blockchain.download_contract_header(self.blockchain.cnx, self.contract_id)
        except:
            self.contract_header = {
                'deployment_timestamp': [],
                'last_update_timestamp': [],
                'contract_id': contract_id,
                'current_state': '',
                'project_name': [],
                'engineer_email': [],
                'engineer_name': [],
                'technician_email': [],
                'technician_name': []
            }
            self.transaction_data = []


    def refresh(self, transaction_data, last_update_timestamp, current_state):
        # encrypt and upload transaction data file
        recipients = [self.contract_header['engineer_email'],
                      self.contract_header['technician_email']]
        transaction_hash = blockchain.encrypt_and_upload_transaction(self.blockchain, self.contract_id, transaction_data, recipients, current_state)
        # submit new block
        block_added = self.blockchain.new_block(transaction_hash, self.contract_id)
        # update world state
        blockchain.update_world_state(self.blockchain.cnx, self.contract_id, last_update_timestamp, current_state)
        return block_added

    # Smart contract functions
    # constructor: a new contract is submitted
    def deploy_contract(self, project_name, engineer_email, engineer_name, technician_email,
                        technician_name, build_file_name, build_file_hash, comments=None, oracle_string=None):
        # only engineers can deploy a new contract?
        if self.user_role is 'technician':
            raise NameError('Only engineers can submit new contracts')
        # check if contract id already exists
        exists, current_state = blockchain.contract_exists(self.blockchain.cnx, self.contract_id)
        if exists:
            raise NameError(
                'Contract with this contract id has already been deployed. Retry with new contract id or try a different function')

        # self.blockchain = self.blockchain.load_blockchain()
        # create contract header when deploying. This cannot be changed later
        deployment_timestamp = get_timestamp()
        last_update_timestamp = deployment_timestamp
        self.contract_header['deployment_timestamp'] = deployment_timestamp
        self.contract_header['last_update_timestamp'] = last_update_timestamp
        self.contract_header['current_state'] = 'Contract Submitted'
        self.contract_header['project_name'] = project_name
        self.contract_header['engineer_email'] = engineer_email
        self.contract_header['engineer_name'] = engineer_name
        self.contract_header['technician_email'] = technician_email
        self.contract_header['technician_name'] = technician_name
        # transaction data is specific to this transaction
        transaction_data = {
            'current_state': self.contract_header['current_state'],
            'transaction_timestamp': get_timestamp(),
            'filename': build_file_name,
            'build_file_hash': build_file_hash,
            'comments': comments,
            'oracle_string': oracle_string
        }
        blockchain.upload_contract_header(self.blockchain.cnx, self.contract_id, deployment_timestamp, project_name,
                                          engineer_email, technician_email, last_update_timestamp,
                                          self.contract_header['current_state'])
        block_added = self.refresh(transaction_data, last_update_timestamp, self.contract_header['current_state'])
        return block_added

    def submit_powder(self, powder_id):
        if self.user_role == 'engineer':
            raise NameError('Only technicians can submit powder')

        exists, current_state = blockchain.contract_exists(self.blockchain.cnx, self.contract_id)
        if exists is False:
            raise NameError('Invalid state transition (contract does not exist) - contract must be deployed first')
        self.contract_header = blockchain.download_contract_header(self.blockchain.cnx, self.contract_id)

        last_update_timestamp = get_timestamp()
        print(self.contract_header['current_state'])
        self.contract_header['current_state'] = 'Powder Selected'
        self.contract_header['last_update_timestamp'] = last_update_timestamp
        transaction_data = {
            'current_state': self.contract_header['current_state'],
            'transaction_timestamp': last_update_timestamp,
            'powder_id': powder_id
        }
        block_added = self.refresh(transaction_data, last_update_timestamp, self.contract_header['current_state'])
        return block_added

    def submit_build_report(self, build_report_file_name, build_report_hash):
        if self.user_role == 'engineer':
            raise NameError('Only technicians can submit build reports')

        exists, current_state = blockchain.contract_exists(self.blockchain.cnx, self.contract_id)
        if exists is False:
            raise NameError('Invalid state transition - contract must be deployed first')
        self.contract_header = blockchain.download_contract_header(self.blockchain.cnx, self.contract_id)
        if self.contract_header['current_state'] is not 'Powder Submitted':
            print('Warning - powder has not been submitted yet')

        last_update_timestamp = get_timestamp()
        self.contract_header['current_state'] = 'Build Report Submitted'
        self.contract_header['last_update_timestamp'] = last_update_timestamp
        transaction_data = {
            'current_state': self.contract_header['current_state'],
            'transaction_timestamp': last_update_timestamp,
            'filename': build_report_file_name,
            'build_report_hash': build_report_hash
        }
        block_added = self.refresh(transaction_data, last_update_timestamp, self.contract_header['current_state'])
        return block_added

    def submit_post_processing_procedure(self, post_processing_procedure):
        if self.user_role == 'technician':
            raise NameError('Only engineers can submit post processing procedures')

        exists, current_state = blockchain.contract_exists(self.blockchain.cnx, self.contract_id)
        if exists is False:
            raise NameError('Invalid state transition - contract must be deployed first')
        self.contract_header = blockchain.download_contract_header(self.blockchain.cnx, self.contract_id)
        if self.contract_header['current_state'] is not 'Build Report Submitted':
            print('Warning - build report has not been submitted yet')

        last_update_timestamp = get_timestamp()
        self.contract_header['current_state'] = 'Post Processing Submitted'
        self.contract_header['last_update_timestamp'] = last_update_timestamp
        transaction_data = {
            'current_state': self.contract_header['current_state'],
            'transaction_timestamp': last_update_timestamp,
            'post_processing_procedure': post_processing_procedure
        }
        block_added = self.refresh(transaction_data, last_update_timestamp, self.contract_header['current_state'])
        return block_added

    def submit_invoice(self, invoice_file_name, invoice_hash):
        if self.user_role == 'engineer':
            raise NameError('Only technicians can submit invoices')

        exists, current_state = blockchain.contract_exists(self.blockchain.cnx, self.contract_id)
        if exists is False:
            raise NameError('Invalid state transition - contract must be deployed first')
        self.contract_header = blockchain.download_contract_header(self.blockchain.cnx, self.contract_id)
        if self.contract_header['current_state'] is not 'Post Processing Submitted':
            print('Warning - post processing procedure has not been submitted yet')

        last_update_timestamp = get_timestamp()
        self.contract_header['current_state'] = 'Invoice Submitted'
        self.contract_header['last_update_timestamp'] = last_update_timestamp
        transaction_data = {
            'current_state': self.contract_header['current_state'],
            'transaction_timestamp': last_update_timestamp,
            'filename': invoice_file_name,
            'invoice_hash': invoice_hash
        }
        block_added = self.refresh(transaction_data, last_update_timestamp, self.contract_header['current_state'])
        return block_added
