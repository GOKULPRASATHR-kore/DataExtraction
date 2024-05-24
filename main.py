import os
import io
import re
import boto3
import textwrap
import requests
import numpy as np
import pandas as pd
from io import BytesIO
from pypdf import PdfReader
from spire.doc import Document
from urllib.parse import urlparse
from flask import Flask,request,jsonify
from botocore.exceptions import ClientError
from pdfminer.high_level import extract_text
import json
import openpyxl
import xlrd
import asyncio
import aiohttp

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

bucket_name = config['BUCKET_NAME']

app = Flask(__name__)

class CASS:
    
    def __init__(self,pdf_path_or_url : str, email: str = None):
        self.pdf_path_or_url = pdf_path_or_url
        self.flag = bool
        self.email = email
        self.etag = None
        self.data = None  # Initialize data attribute
        self.ext = None   # Initialize ext attribute
    def log(self,message:str,success_flag=True):
        if success_flag: print(f"\n\n###################   {message}   ###################")
        else: print(f"!!!!!!!!!!!!!!!!!!   {message}   !!!!!!!!!!!!!!!!!!!!") 
          
    def get_content_type(self):
        mime_types = {
            '.pdf': 'application/pdf',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            '.doc': 'application/msword',
            '.csv': 'text/csv',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.xls': 'application/vnd.ms-excel'
        }
        _, extension = os.path.splitext(self.pdf_path_or_url)
        return mime_types.get(extension.lower(), 'application/octet-stream')
    
    async def download_url(self):
        try:
            if self.pdf_path_or_url.startswith("http"):
                self.log("Downloading URL")
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.pdf_path_or_url) as response:
                        self.flag = True
                        response.raise_for_status()
                        if response.status == 200:
                            self.log("Downloaded successfully!")
                            return await response.read(), response.headers.get('Content-Type')
            else:
                with open(self.pdf_path_or_url, 'rb') as f:
                    return f.read(), self.get_content_type()
        except aiohttp.ClientError as e:
            self.log(f"Failed to download file from {self.pdf_path_or_url}", success_flag=False)
            return None, None
        except FileNotFoundError as e:
            self.log(f"File not found: {self.pdf_path_or_url}", success_flag=False)
            return None, None

            
    def extract_invoice_number(self,text: str):
            
        invoice_numbers = re.findall(r'\b\d{5}\b', text)
        if invoice_numbers: return invoice_numbers
        else:
            pattern = r'(?:invoice\s*(?:no(?:\.|:)?|number|num)?\s*:?)(\d{5})'
            invoice_numbers = re.search(pattern, text, re.IGNORECASE)
            if invoice_numbers:
                return invoice_numbers.group()
            else:
                return
            
    async def get_text_pdf(self):
        self.data, self.ext = await self.download_url()
        if self.data:
            reader = PdfReader(io.BytesIO(self.data))
            text = ''.join([page.extract_text() for page in reader.pages])
            self.wrapped_text = textwrap.fill(text, width=120)
            if not self.flag:
                self.text = extract_text(self.pdf_path_or_url)
                return [self.wrapped_text, self.text]
            else:
                return [self.wrapped_text]
        else:
            return None
    
    async def get_text_doc(self, file_path="temp.docx"):
        self.data, self.ext = await self.download_url()
        if self.data:
            with open(file_path, "wb") as temp_file:
                temp_file.write(self.data)
            document = Document()
            document.LoadFromFile(file_path)
            document_text = document.GetText()
            document.Close()
            os.remove(file_path)
            self.wrapped_text = textwrap.fill(document_text, width=120)
            return self.wrapped_text
        else:
            return None
    
    async def get_text_csv(self):
        self.data, self.ext = await self.download_url()
        if self.data:
            try:
                file_extension = os.path.splitext(self.pdf_path_or_url)[-1].lower()
                if file_extension in ['.xlsx', '.xls']:
                    df = pd.read_excel(BytesIO(self.data))
                elif file_extension == '.csv':
                    df = pd.read_csv(BytesIO(self.data))
                else:
                    self.log(f"Unsupported file extension: {file_extension}", success_flag=False)
                    return None
                csv_content = df.to_csv(index=False)
                return csv_content
            except (UnicodeDecodeError, TypeError, pd.errors.EmptyDataError) as e:
                self.log(f"Failed to decode CSV file: {e}", success_flag=False)
                return None
        else:
            return None

    
    async def upload_to_s3(self):
        
        if not self.data and not self.ext:
            self.data, self.ext = await self.download_url()
            
        if self.data and self.ext:
            s3 = boto3.client(
                    's3',
                    aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
                    aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY']
                )
            #bucket_name = bucket_name
            s3_file_name = f'{self.email}.{self.ext.split("/")[-1]}'
            try:
    
                response = s3.put_object(Bucket=bucket_name, Key=s3_file_name, Body=self.data, ContentType=self.ext)
                self.etag = response.get('ETag')
                self.log(f'File "{s3_file_name}" uploaded to bucket "{bucket_name}" successfully.')
            
                return s3_file_name
                
            except ClientError as e:
                print(f'An error occurred: {e.response["Error"]["Message"]}')
                return None
        
    
@app.route("/")
def explain():
    return """
    Server Running Successfully
    """

    
@app.route("/get_text",methods=['POST'])
async def text_parser():
    fla = bool
    text = None
    etag = None
    
    if request.is_json:
        data = request.json
        pth_url = data.get('path_url')
        email = data.get('email')
        
        if pth_url and email:
            
            obj = CASS(pth_url,email)
            
            parsed_url = urlparse(pth_url)
            _, file_extension = os.path.splitext(parsed_url.path)
            
            if file_extension.lower() in ('.docx','.doc'):
                text = await obj.get_text_doc()
            elif file_extension.lower() in ('.csv','.xlsx','.xls'):
                text = await obj.get_text_csv()
            elif file_extension.lower() == '.pdf':
                text = await obj.get_text_pdf()
                text = text[0] if text else None
                
            else:
                url_type = "unknown" 
                text = ""
            
            etag = await obj.upload_to_s3()
            
            if text and etag:
                return jsonify({'text': text,'file_name':etag}), 200
            else:
                return jsonify({'error': "Can't extract data from the URL"}), 404

    else:
        return jsonify({'error': 'This server only accepts json please parse json'}), 400

        
if __name__ == '__main__':
    app.run(debug=True)