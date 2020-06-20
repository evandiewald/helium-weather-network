import hashlib
import json
from datetime import datetime
from time import time, mktime, sleep
import pickle
import ipfshttpclient
import os


def get_timestamp():
    ts = time()
    str_stamp = datetime.fromtimestamp(ts).strftime('%Y_%m_%d %H_%M_%S')
    return str_stamp


class Chain(object):
    def __init__(self, new_master=False, load_path=None, cnx=None):
        self.chain = []
        if new_master is True:
            self.chain = self.create_new_master()
            print('Creating new local blockchain...')
        else:
            if load_path is not None:
                self.chain = self.load_local_blockchain(load_path)
                print('Loading blockchain from local path...')
            else:
                self.chain = download_ledger(cnx)
                print('Downloading blockchain from SQL connector...')

    def create_new_master(self):
        # genesis block
        chain = []
        chain = self.genesis_block(chain, previous_hash=1)
        print('Creating entirely new master blockchain!')
        return chain

    @staticmethod
    def genesis_block(chain, previous_hash):
        block = {
            'index': 0,
            'timestamp': get_timestamp(),
            'transaction_hash': [],
            'block_hash': previous_hash,
            'contract_id': []
        }
        chain.append(block)
        return chain

    @property
    def last_block(self):
        return self.chain[-1]

    def new_local_block(self, transaction_data, contract_id):
        block = {
            'index': int(len(self.chain)),
            'timestamp': get_timestamp(),
            'transaction_hash': transaction_data,
            'block_hash': hash_block(self.last_block),
            'contract_id': contract_id
        }
        self.chain.append(block)


class BlockchainConnection(object):
    def __init__(self, cnx):
        self.previous_hash = []
        self.cnx = cnx
        try:
            self.client = ipfshttpclient.connect('/ip4/127.0.0.1/tcp/5001/http')
        except ConnectionError:
            raise ConnectionError('Initialize an IPFS session')

    def last_block(self):
        query_str = "SELECT * FROM blockchain_hashes ORDER BY id DESC LIMIT 1;"
        cur = self.cnx.cursor()
        cur.execute(query_str)
        result = cur.fetchall()
        last_block = {
            'index': result[0][1],
            'timestamp': result[0][2],
            'transaction_hash': result[0][4],
            'block_hash': result[0][3],
            'contract_id': result[0][5]
        }
        return last_block

    def new_block(self, transaction_hash, contract_id):
        last_block = self.last_block()
        block = {
            'index': str(int(last_block['index'])+1),
            'timestamp': get_timestamp(),
            'transaction_hash': transaction_hash,
            'block_hash': hash_block(last_block),
            'contract_id': contract_id
        }
        cur = self.cnx.cursor()
        query_str = "INSERT INTO blockchain_hashes (block_index, timestamp, transaction_hash, block_hash, contract_id) VALUES (\'" + block['index'] + "\', \'" + block['timestamp'] + "\', \'" + block['transaction_hash'] + "\', \'" + block['block_hash'] + "\', \'" + block['contract_id'] + "\');"
        print(query_str)
        cur.execute(query_str)
        self.cnx.commit()
        return block


def hash_block(block):
    """
    Creates a SHA-256 hash of a Block
    :param block: <dict> Block
    :return: <str>
    """
    # We must make sure that the Dictionary is Ordered, or we'll have inconsistent hashes
    block_string = json.dumps(block, sort_keys=True).encode()
    return hashlib.sha256(block_string).hexdigest()


def load_local_blockchain(my_ledger_path):
    print('loading local blockchain from path: ', my_ledger_path)
    # consensus protocol: check if my version of the ledger is longer
    chain = pickle.load(open(my_ledger_path, "rb"))
    # if len(myBlockchain.chain) > len(chain):
    #     print('Warning: consensus dispute - your local version of the ledger is longer than the latest distributed version')
    return chain


def save_local_blockchain(chain, ledger_path):
    with open(ledger_path, 'wb') as blockchain_file:
        pickle.dump(chain, blockchain_file)


def read_full_blockchain(chain_obj: Chain):
    return chain_obj.chain


def read_last_few_blocks(blockchain_obj: BlockchainConnection, n):
    query_str = "SELECT * FROM blockchain_hashes ORDER BY id DESC LIMIT " + str(n) + ";"
    cur = blockchain_obj.cnx.cursor()
    cur.execute(query_str)
    result = cur.fetchall()
    try:
        blocks_df = pd.DataFrame.from_dict(result)
    except IndexError:
        print('There are not enough blocks on this chain yet')
    return blocks_df


def verify_chain(chain_obj: Chain):
    full_ledger = pd.DataFrame.from_dict(read_full_blockchain(chain_obj))
    block_hashes = full_ledger.block_hash
    for iBlock in range(len(chain_obj.chain) - 1):
        try:
            assert chain_obj.hash_block(chain_obj.chain[iBlock]) == block_hashes[iBlock + 1]
        except AssertionError:
            print('!!!WARNING!!! Chain hash been tampered with at block index: ', iBlock)
            break

    print('Chain verified.')


def download_ledger(cnx):
    chain = []
    query_str = "SELECT * FROM blockchain_hashes;"
    cur = cnx.cursor()
    cur.execute(query_str)
    result = cur.fetchall()
    for i in range(len(result)):
        block = {
            'index': result[i][1],
            'timestamp': result[i][2],
            'transaction_hash': result[i][4],
            'block_hash': result[i][3]
        }
        chain.append(block)
    return chain

# TODO: similar functions to create & upload keys


def get_next_contract_id(cnx):
    cur = cnx.cursor()
    query_str = "SELECT MAX(contract_id) FROM world_state;"
    cur.execute(query_str)
    result = cur.fetchall()
    n = str(int(result[0][0]) + 1)
    return str(n.zfill(3))


def upload_contract_header(cnx, contract_id, deployed_timestamp, project_name, engineer_email, technician_email, update_timestamp, current_state):
    # get hash from db
    cur = cnx.cursor()
    query_str = "INSERT INTO world_state (contract_id, deployed_timestamp, project_name, engineer_email, technician_email, update_timestamp, current_state) VALUES (\'" + contract_id + "\', \'" + deployed_timestamp + "\', \'" + project_name+ "\', \'" + engineer_email+ "\', \'" + technician_email + "\', \'" + update_timestamp + "\', \'" + current_state + "\');"
    print(query_str)
    cur.execute(query_str)
    cnx.commit()


def download_contract_header(cnx, contract_id):
    # get hash from db
    cur = cnx.cursor()
    query_str = 'SELECT deployed_timestamp, project_name, engineer_email, technician_email, update_timestamp, current_state  FROM world_state WHERE contract_id = ' + "\'" + str(contract_id) + "\';"
    cur.execute(query_str)
    result = cur.fetchall()
    deployed_timestamp = result[0][0]
    project_name = result[0][1]
    engineer_email = result[0][2]
    technician_email = result[0][3]
    update_timestamp = result[0][4]
    current_state = result[0][5]

    contract_header = {
        'deployment_timestamp': deployed_timestamp,
        'last_update_timestamp': update_timestamp,
        'contract_id': contract_id,
        'current_state': current_state,
        'project_name': project_name,
        'engineer_email': engineer_email,
        'technician_email': technician_email
    }
    return contract_header


def update_world_state(cnx, contract_id, update_timestamp, current_state):
    cur = cnx.cursor()
    query_str = 'UPDATE world_state SET update_timestamp = \'' + update_timestamp + '\' WHERE contract_id = \'' + contract_id + '\';'
    print(query_str)
    cur.execute(query_str)

    query_str = 'UPDATE world_state SET current_state = \'' + current_state + '\' WHERE contract_id = \'' + contract_id + '\';'
    print(query_str)
    cur.execute(query_str)
    cnx.commit()


def contract_exists(cnx, contract_id):
    cur = cnx.cursor()
    query_str = 'SELECT current_state  FROM world_state WHERE contract_id = ' + "\'" + contract_id + "\';"
    cur.execute(query_str)
    result = cur.fetchall()
    try:
        current_state = " ".join(result[0])
    except IndexError:
        current_state = None
    if current_state is not None:
        exists = True
    else:
        exists = False
    return exists, current_state


def read_my_transactions(blockchain_obj: BlockchainConnection, contract_id, gpg_pw='pw'):
    gpg = gnupg.GPG()
    cur = blockchain_obj.cnx.cursor()
    query_str = 'SELECT transaction_hash, timestamp FROM blockchain_hashes WHERE contract_id = \'' + contract_id + '\';'
    cur.execute(query_str)
    result = cur.fetchall()
    transaction_paths = []
    for i in range(len(result)):
        ipfs_hash_rec = result[i][0]
        my_timestamp = result[i][1]
        blockchain_obj.client.get(ipfs_hash_rec)
        # decrypt
        output_path = 'contract_files/' + contract_id + '/' + my_timestamp + '_dec'
        with open(ipfs_hash_rec, 'rb') as f:
            status = gpg.decrypt_file(f, passphrase=gpg_pw, output=output_path)
        my_data = pickle.load(open(output_path, "rb"))
        f.close()
        my_path = r'contract_files/' + contract_id + '/' + my_data['current_state']
        try:
            os.rename(output_path, my_path)
        except FileExistsError:
            pass
        os.remove(ipfs_hash_rec)
        transaction_paths.append(my_path)
    return transaction_paths





def find_my_contracts(cnx, email_address):
    my_contracts = {
        'deployed_timestamp': [],
        'contract_id': [],
        'project_name': [],
        'engineer_email': [],
        'technician_email': [],
        'update_timestamp': [],
        'current_state': []
    }
    cur = cnx.cursor()
    query_str = "SELECT * FROM world_state WHERE engineer_email = \'" + email_address + '\' OR technician_email = \'' + email_address + '\';'
    cur.execute(query_str)
    result = cur.fetchall()
    for i in range(len(result)):
        my_contracts['deployed_timestamp'].append(result[i][1])
        my_contracts['contract_id'].append(result[i][2])
        my_contracts['project_name'].append(result[i][3])
        my_contracts['engineer_email'].append(result[i][4])
        my_contracts['technician_email'].append(result[i][5])
        my_contracts['update_timestamp'].append(result[i][6])
        my_contracts['current_state'].append(result[i][7])
    return my_contracts


def login(cnx, email_address):
    cur = cnx.cursor()
    query_str = "SELECT * FROM keyring WHERE email = \'" + email_address + '\';'
    cur.execute(query_str)
    result = cur.fetchall()
    try:
        role = result[0][3]
    except IndexError:
        role = 'undefined'
    return role


def register_user(blockchain_obj: BlockchainConnection, email_address, pw, role):
    gpg = gnupg.GPG()
    input_data = gpg.gen_key_input(name_email=email_address, passphrase=pw)
    key = str(gpg.gen_key(input_data))
    cur = blockchain_obj.cnx.cursor()
    query_str = "INSERT INTO keyring (email, public_key, role) VALUES (\'" + email_address + "\', \'" + key + "\', \'" + role.lower() + "\');"
    cur.execute(query_str)
    blockchain_obj.cnx.commit()


def encrypt_and_upload_transaction(blockchain_obj: BlockchainConnection, contract_id, transaction_data, recipients, current_state):
    gpg = gnupg.GPG()
    contract_dir = 'contract_files/' + contract_id
    update_timestamp = get_timestamp()
    if not os.path.isdir(contract_dir):
        os.mkdir(path=contract_dir)
    fname = contract_dir + '/' + current_state
    # transaction_data = str.encode(str(transaction_data))
    # write transaction data to json
    with open(fname, 'wb') as transaction_file:
        pickle.dump(transaction_data, transaction_file)
    # encrypt
    output_filename = contract_dir + '/' + current_state + '_enc.gpg'
    with open(fname, 'rb') as f:
        status = gpg.encrypt_file(f, recipients=recipients, output=output_filename)
    # send via IPFS
    res = blockchain_obj.client.add(output_filename)
    ipfs_hash_sent = res['Hash']
    return ipfs_hash_sent


def update_gpg_keys(blockchain_obj: BlockchainConnection, recipients):
    gpg = gnupg.GPG()
    for i in range(len(recipients)):
        my_keys = gpg.list_keys(keys=recipients[i])
        if len(my_keys) is 0:
            cur = blockchain_obj.cnx.cursor()
            query_str = "SELECT public_key FROM keyring WHERE email = \'" + recipients[i] + "\';"
            print(query_str)
            cur.execute(query_str)
            result = cur.fetchall()
            key = result[0][0]
            ascii_armored_public_keys = gpg.export_keys(key)
            with open('mykeyfile.asc', 'w') as f:
                f.write(ascii_armored_public_keys)
            key_data = open('mykeyfile.asc').read()
            import_result = gpg.import_keys(key_data)


def encrypt_and_upload_file(blockchain_obj: BlockchainConnection, filepath, recipients):
    gpg = gnupg.GPG()
    # encrypt
    output_filename = filepath + '_enc.gpg'
    with open(filepath, 'rb') as f:
        status = gpg.encrypt_file(f, recipients=recipients, output=output_filename)
    print(status.status)
    # send via IPFS
    res = blockchain_obj.client.add(output_filename)
    ipfs_hash_sent = res['Hash']
    filename = os.path.basename(filepath)
    return ipfs_hash_sent, filename


def decrypt_and_download_file(blockchain_obj: BlockchainConnection, ipfs_hash_rec, contract_id, filename, gpg_pw='pw'):
    gpg = gnupg.GPG()
    blockchain_obj.client.get(ipfs_hash_rec)
    # decrypt
    output_path = 'contract_files/' + contract_id + '/' + filename
    with open(ipfs_hash_rec, 'rb') as f:
        status = gpg.decrypt_file(f, passphrase=gpg_pw, output=output_path)
    os.remove(ipfs_hash_rec)


