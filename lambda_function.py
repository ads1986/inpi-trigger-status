import io
import os
import json
import boto3
import logging
import zipfile
import urllib.request
import xml.etree.ElementTree as ET

from urllib.request import Request, urlopen
from botocore.exceptions import ClientError

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
dynamodb_client = boto3.resource('dynamodb')

LOCAL_DIR = '/tmp/'
TABLE_NAME= 'INPI_Table'
URL_BASE = 'http://revistas.inpi.gov.br/txt/'
BUCKET_NAME = 'inpi-bucket'
USER_AGENT = {"User-Agent": "Mozilla/5.0"}
TARGET_ARN = 'arn:aws:sns:sa-east-1:907205049098:InpiNotification'
XML_STATUS_ELEMENT = "./processo[@numero='927270366']/lista-classe-nice/classe-nice/status"

inpiTable=dynamodb_client.Table(TABLE_NAME)

def lambda_handler(event, context):
    fileNumber = getFileNumber()

    fileNameToDonwload = getFileNameToDonwload(fileNumber);

    response = doRequest(fileNameToDonwload)

    if response.status == 200:
        folder = unzipFile(response)

        for fileName in folder.namelist():
            localFilename = createFile(fileName, folder)
            status = findStatus(localFilename)
            notify(status)
            increaseFileNumber(fileNumber)
            return {'ResponseStatus' : response.status}

def getFileNameToDonwload(fileNumber):
    try:
        return 'RM' + fileNumber + '.zip'
    except Exception as e:
        message = ' Nao foi possível definir o nome do arquivo.'
        logging.exception(message)

def getFileNumber():
    try:
        response = inpiTable.get_item(
             Key={'file_id': '1'}
            )
        return response['Item']['file_number']
    except Exception as e:
        message = ' Nao foi possível obter o numero do arquivo.'
        logging.exception(message)

def doRequest(fileNameToDonwload):
    try:
        url = URL_BASE + fileNameToDonwload
        request_site = Request(url, headers=USER_AGENT)
        response = urlopen(request_site)
        return response
    except Exception as e:
        message = ' Nao foi possível obter arquivo : '
        logging.exception(message)

def unzipFile(response):
    try:
        filebytes = io.BytesIO(response.read())
        folder = zipfile.ZipFile(filebytes)
        return folder
    except Exception as e:
        message = ' Nao foi possível descompactar o arquivo.'
        logging.exception(message)

def createFile(fileName, folder):
    try:
        newFile = open(LOCAL_DIR + fileName,'wb')
        newFile.write(folder.read(fileName))
        localFilename = LOCAL_DIR + '{}'.format(os.path.basename(fileName))
        return localFilename
    except Exception as e:
        message = ' Nao foi possível criar o arquivo no diretorio /tmp.'
        logging.exception(message)

def findStatus(localFilename):
    try:
        tree = ET.parse(localFilename)
        root = tree.getroot()
        status = root.findall(XML_STATUS_ELEMENT)[0]
        return status.text
    except Exception as e:
        message = ' Nao foi possível encontrar o status.'
        logging.warning(message)
        return 'Informação não disponível'

def increaseFileNumber(fileNumber):
    try:
        fileNumberUpdated = str(int(fileNumber) + 1)
        response = inpiTable.update_item(
            Key={
            'file_id': '1'
        },
        UpdateExpression="set file_number = :r",
        ExpressionAttributeValues={
                ':r' : fileNumberUpdated
            },
        ReturnValues="UPDATED_NEW"
        )
        return response
    except Exception as e:
        message = ' Nao foi possível definir o numero do proximo arquivo'
        logging.exception(message)

def notify(status):
    try:
        sns_client.publish(
            TargetArn=TARGET_ARN,
            Message='INPI: O status de aprovação da Marca Labelles Shop está como : ' + status,
            MessageStructure='text'
        )
    except Exception as e:
        message = ' Nao foi possível enviar a notificacao'
        logging.exception(message)
