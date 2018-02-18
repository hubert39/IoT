#!/usr/bin/python

from Crypto.PublicKey import RSA
from Crypto.Random import get_random_bytes
from Crypto.Cipher import AES, PKCS1_OAEP
from Crypto import Random

import base64
import sys
import datetime
import random

import json
import collections

def generate_json(numMsgs):
  jsonlist = []
  #json_instance = collections.OrderedDict()
  guid_key = 'guid'
  guid_value = ''
  guid_prefix_value = '0-ZZZ12345678'
  dest_key = 'destination'
  dest_value = '0-AAA12345678'
  eventTime_key = 'eventTime'
  eventTime_value = ''
  payload_key = 'payload'
  #payload_value = collections.OrderedDict()
  format_key = 'format'
  format_value = 'urn:finalproject:sensor:bloodpressure'
  data_key = 'data'
  #data_value = collections.OrderedDict()
  systolic_key = 'systolic'
  diastolic_key = 'diastolic'
  systolic_value = 0
  diastolic_value = 0
  letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

  for counter in range(0, numMsgs):
    json_instance = collections.OrderedDict()
    payload_value = collections.OrderedDict()
    data_value = collections.OrderedDict()
    guid_value = guid_prefix_value+str(random.randrange(0, 9))+random.choice(letters)
    eventTime_value = datetime.datetime.today().isoformat()
    systolic_value = random.randint(0, 140) + 60
    diastolic_value = random.randint(0, 80) + 40

    data_value[systolic_key] = systolic_value
    data_value[diastolic_key] = diastolic_value
    payload_value[format_key] = format_value
    payload_value[data_key] = data_value

    json_instance[guid_key] = guid_value
    json_instance[dest_key] = dest_value
    json_instance[eventTime_key] = eventTime_value
    json_instance[payload_key] = payload_value

    jsonlist.append(json_instance)

  return jsonlist

def get_public_key():
  PUBLIC_KEY_FILE_PATH = '/home/ec2-user/Key/public_key.pem'
  with open(PUBLIC_KEY_FILE_PATH, 'r') as file:
    return file.read()

def get_private_key():
  PRIVATE_KEY_FILE_PATH = '/home/ec2-user/Key/private_key.pem'
  with open(PRIVATE_KEY_FILE_PATH, 'r') as file:
    return file.read()

def encrypt(iotmsg):
  #AES session_key
  session_key = get_random_bytes(16)
 
  #Use a RSA public_key to encrypt AES session_key
  public_key_string = get_public_key()
  public_key = RSA.importKey(public_key_string)
  cipher_rsa_public = PKCS1_OAEP.new(public_key) 
  encrypt_session_key = cipher_rsa_public.encrypt(session_key)
  encrypt_session_key_base64_enc = base64.b64encode(encrypt_session_key)

  #Use AES to encrypt iotmsg
  IV = Random.new().read(16)
  cipher_aes = AES.new(session_key, AES.MODE_CFB, IV)
  #concat IV and encrypted iotmsg
  ciphertext = IV + cipher_aes.encrypt(iotmsg)

  #Use base64 to encode ciphertext
  ciphertext_base64_enc = base64.b64encode(ciphertext)

  return encrypt_session_key_base64_enc, ciphertext_base64_enc

def decrypt(encrypt_session_key_base64_enc, ciphertext_base64_enc):
  #Use a RSA private_key to decrypt AES encrypt_session_key
  private_key_string = get_private_key()
  private_key = RSA.importKey(private_key_string)
  cipher_rsa_private = PKCS1_OAEP.new(private_key)

  encrypt_session_key = base64.b64decode(encrypt_session_key_base64_enc)
  session_key = cipher_rsa_private.decrypt(encrypt_session_key)
 
  #Use base64 to decode ciphertext_base64_enc
  ciphertext_base64 = base64.b64decode(ciphertext_base64_enc)
 
  #Use AES to decrypt ciphertext_base64_dec[16:]
  IV = ciphertext_base64[:16]
  cipher_aes = AES.new(session_key, AES.MODE_CFB, IV)
  iotmsg = cipher_aes.decrypt(ciphertext_base64[16:])
  return iotmsg

def getbloodpressure(json_str):
  jsondata = json.loads(json_str)
  bloodpressure = []
  for jsoninstance in jsondata:
    bloodpressure.append( (jsoninstance['payload']['data']['systolic'], jsoninstance['payload']['data']['diastolic']) )
  return bloodpressure
	
if __name__ == "__main__":
  # Set number of simulated messages to generate
  if len(sys.argv) > 1:
    numMsgs = int(sys.argv[1])
  else:
    numMsgs = 1
  
  jsonlist = generate_json(numMsgs)
  iotmsg = json.dumps(jsonlist, indent=2)
  #print iotmsg
  json.loads(iotmsg)

  encrypt_session_key_base64_enc, ciphertext_base64_enc = encrypt(iotmsg)
  print (encrypt_session_key_base64_enc, ciphertext_base64_enc)

  ''' 
  iotmsg = decrypt(encrypt_session_key_base64_enc, ciphertext_base64_enc)
  print iotmsg
  bloodpressurelist = getbloodpressure(iotmsg)
  print bloodpressurelist[0][0], type(bloodpressurelist[0][0])
  print sorted(bloodpressurelist)
  for bloodpressure in bloodpressurelist:
    print bloodpressure[0]
    print bloodpressure[1]
  print [ (str(x), str(y)) for (x,y) in bloodpressurelist ]
  '''
