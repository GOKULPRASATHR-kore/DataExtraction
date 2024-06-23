import os
import io
import textwrap
import numpy as np
import pandas as pd
from io import BytesIO
from pypdf import PdfReader
from spire.doc import Document
from urllib.parse import urlparse
from flask import Flask,request,jsonify
from botocore.exceptions import ClientError
from pdfminer.high_level import extract_text
import aiohttp
import asyncio
from waitress import serve

app = Flask(__name__)

class CASS:
    
    def __init__(self, pdf_path_or_url: str, email: str = None):
        self.pdf_path_or_url = pdf_path_or_url
        self.flag = bool
        self.email = email
        self.data = None 
        self.ext = None

    def log(self, message: str, success_flag=True):
        if success_flag: 
            print(f"\n\n###################   {message}   ###################")
        else: 
            print(f"!!!!!!!!!!!!!!!!!!   {message}   !!!!!!!!!!!!!!!!!!!!") 

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
            self.log("Attempting to download content")
            timeout = aiohttp.ClientTimeout(total=30)  # Set a timeout for the request
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.pdf_path_or_url) as response:
                    response.raise_for_status()
                    if response.status == 200:
                        self.flag = True
                        self.log("Downloaded successfully!")
                        content_type = response.headers.get('Content-Type', 'application/octet-stream')
                        return await response.read(), content_type
                    else:
                        self.log(f"Failed to download. Status code: {response.status}")
                        return None, None
        except (aiohttp.ClientError, aiohttp.InvalidURL, aiohttp.ClientConnectorError) as e:
            self.log(f"HTTP Client Error or Invalid URL: {str(e)}", success_flag=False)
            return None, None
        except asyncio.TimeoutError:
            self.log("Download timed out", success_flag=False)
            return None, None


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

@app.route("/")
def explain():
    return "Server Running Successfully"

@app.route("/get_text", methods=['POST'])
async def text_parser():
    if request.is_json:
        data = request.json
        pth_url = data.get('path_url')
        email = data.get('email')
        
        if pth_url and email:
            obj = CASS(pth_url, email)
            
            parsed_url = urlparse(pth_url)
            _, file_extension = os.path.splitext(parsed_url.path)
            
            if file_extension.lower() in ('.docx', '.doc'):
                text = await obj.get_text_doc()
            elif file_extension.lower() in ('.csv', '.xlsx', '.xls'):
                text = await obj.get_text_csv()
            elif file_extension.lower() == '.pdf':
                text = await obj.get_text_pdf()
                text = text[0] if text else None
            else:
                obj.log(f"Unsupported file extension: {file_extension}", success_flag=False)
                text = None
            
            if text:
                return jsonify({'text': text}), 200
            else:
                return jsonify({'error': "Can't extract data from the URL"}), 404
    else:
        return jsonify({'error': 'This server only accepts JSON please pass JSON'}), 400

if __name__ == '__main__':
    serve(app, host='0.0.0.0', port=5001)
    #app.run(host='0.0.0.0', port=5001)
