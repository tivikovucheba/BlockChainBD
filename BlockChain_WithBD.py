# -*- coding: utf-8 -*-
import datetime
import hashlib
import json
from flask import Flask, jsonify

# Flask ������������ ��� �������� ���-����������, � jsonify - ��� ����������� ���������
import psycopg2
import pandas as pd

# ����� ��� ������������� ���������
class Blockchain:
    def __init__(self):
        # ������������� ������� ������
        self.chain = []
        # �������� ������� ����� (�������-�����)
        self.create_block(proof=1, previous_hash='0')

# ������� ��� ����������� � ���� ������
def connect_db():
    # ��������� ���������� � ����� ������ PostgreSQL
    conn = psycopg2.connect(dbname='labor_market', user='postgres', password='postgres', host='localhost')
    cursor = conn.cursor()
    # ���������� SQL-������� ��� ��������� ������
    cursor.execute('SELECT * FROM "university2"')
    # ��������� ������ � �������������� �� � DataFrame
    df = cursor.fetchall()
    df = pd.DataFrame(df)
    return df

# ��������� ������ �� ���� ������
database = connect_db()

# ����� ��� ������������� ���������
class Blockchain:
    def __init__(self):
        # ������������� ������� ������
        self.chain = []
        # �������� ������� ����� (�������-�����)
        self.create_block(proof=1, previous_hash='0')

    # �������� ������ ����� � ���������
    def create_block(self, proof, previous_hash):
        block = {
            'id': str(database[0].iloc[len(self.chain)]),
            'snils': str(database[1].iloc[len(self.chain)]),
            'final_result': str(database[2].iloc[len(self.chain)]),
            'subject_1': str(database[3].iloc[len(self.chain)]),
            'subject_2': str(database[4].iloc[len(self.chain)]),
            'subject_3': str(database[5].iloc[len(self.chain)]),
            'result': str(database[6].iloc[len(self.chain)]),
            'individ_archiv': str(database[7].iloc[len(self.chain)]),
            'proof': proof,
            'previous_hash': previous_hash
        }
        # ���������� ����� � �������
        self.chain.append(block)
        return block

    # ��������� ����������� ����� � ���������
    def print_previous_block(self):
        return self.chain[-1]

    # ����� ����������� proof ��� ������ �����
    def proof_of_work(self, previous_proof):
        new_proof = 1
        check_proof = False
        while check_proof is False:
            # ���������� ���� � �������������� proof
            hash_operation = hashlib.sha256(
                str(new_proof**2 - previous_proof**2).encode()).hexdigest()
            # �������� ������� ������� '00000' � ������ ����
            if hash_operation[:5] == '00000':
                check_proof = True
            else:
                new_proof += 1
        return new_proof

    # ����������� ����� � ���������
    def hash(self, block):
        encoded_block = json.dumps(block, sort_keys=True).encode()
        return hashlib.sha256(encoded_block).hexdigest()

    # �������� ����������� ���������
    def chain_valid(self, chain):
        previous_block = chain[0]
        block_index = 1
        while block_index < len(chain):
            block = chain[block_index]
            # �������� ������������ previous_hash ���� ����������� �����
            if block['previous_hash'] != self.hash(previous_block):
                return False
            previous_proof = previous_block['proof']
            proof = block['proof']
            # ���������� ���� ��� �������� ������� ������� '00000' � ������ ����
            hash_operation = hashlib.sha256(
                str(proof ** 2 - previous_proof ** 2).encode()).hexdigest()
            if hash_operation[:5] != '00000':
                return False
            previous_block = block
            block_index += 1
        return True

# �������� ������� ���������
blockchain = Blockchain()

# �������� ���-���������� � �������������� Flask
app = Flask(__name__)

# ����������� ��������� (endpoints) ��� ���-����������
@app.route('/')
def index():
    return "Mine a new block: /mine_block  " \
           "Display the blockchain in JSON format: /display_chain  " \
           "Check the validity of the blockchain: /valid  "

@app.route('/mine_block', methods=['GET'])
def mine_block():
    # ��������� ����������� �����
    previous_block = blockchain.print_previous_block()
    # ��������� proof ��� ������ �����
    previous_proof = previous_block['proof']
    proof = blockchain.proof_of_work(previous_block['proof'])
    # ��������� ���� ����������� �����
    previous_hash = blockchain.hash(previous_block)
    # �������� ������ �����
    block = blockchain.create_block(proof, previous_hash)
    # ������������ ������ � ������� ������ �����
    response = {'message': 'A block is MINED',
                'id': block['id'],
                'snils': block['snils'],
                'final_result': block['final_result'],
                'subject_1': block['subject_1'],
                'subject_2': block['subject_2'],
                'subject_3': block['subject_3'],
                'result': block['result'],
                'individ_archiv': block['individ_archiv']}
    return jsonify(response), 200

@app.route('/display_chain', methods=['GET'])
def display_chain():
    # ������������ ������� ������ ��� ������
    chain = []
    for block in blockchain.chain:
        data = {
            'id': block['id'],
            'snils': block['snils'],
            'final_result': block['final_result'],
            'subject_1': block['subject_1'],
            'subject_2': block['subject_2'],
            'subject_3': block['subject_3'],
            'result': block['result'],
            'individ_archiv': block['individ_archiv']
        }
        chain.append(data)
    # ������������ ������ � �������� ������
    response = {'chain': chain, 'length': len(chain)}
    return jsonify(response), 200

@app.route('/valid', methods=['GET'])
def valid():
    # �������� ����������� ���������
    valid = blockchain.chain_valid(blockchain.chain)
    if valid:
        response = {'message': 'The Blockchain is valid.'}
    else:
        response = {'message': 'The Blockchain is not valid.'}
    return jsonify(response), 200

# ������ ���-����������
app.run(debug=True, port=5000)
