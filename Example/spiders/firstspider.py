# -*- coding: utf-8 -*-
import msal
import scrapy
from ..items import quotestut
from ..items import secondclass
import pandas as pd
from scrapy.http.request import Request
import urllib.request
import requests
import os  
import re
from urllib.parse import urlparse
from urllib.parse import urljoin
from difflib import SequenceMatcher
import csv
import time
import logging
import scrapy
import socket
import json

#enter the type of report
reporttypes=['10-K','DEF+14A','PRE+14A']
stopwords=['INS','HLDG','REDH','HLDGS']
header=['conm','conm_new1','conm_new2','conm_new3']
reporttype=' '
class Firstspider(scrapy.Spider):    
    
   
    name='firstshot'
    
    
    #start_urls=[
    #ß    'https://www.sec.gov/cgi-bin/browse-edgar?CIK=COMS&type=10-k&owner=exclude','https://www.sec.gov/cgi-bin/browse-edgar?CIK=orcl&type=10-k&owner=exclude']
    #allowed_domains=["www.sec.gov"]
    
    
    #This function reads the csv and extracts company names, ticker number, gckey
    def start_requests(self):

        global count
        df=pd.read_excel('./mixthem/ERM_COVID_companylist_USA_all_1991-2019_new.xls')
        for i,url in df.iterrows():

            if( url['id']==9303):
                
                for j in reporttypes:
                    if not self.check_connection():
                        print('Connection Lost! Please check your internet connection!', flush=True)
                        time.sleep(13)
                    reporttype=j
                    print(reporttype)
                    name=url['conm']
                    query = re.sub(r"[^\w&]", ' ', name)
                    query=query.replace('&','%26')
                    query=query.upper()
                    resultwords  = [word.strip() for word in re.split(" ",query) if word not in stopwords and len(word)>0]
                    resultwords=''.join([i+'+' for i in resultwords])
                    resultwords.rstrip('+')
                    partial_url='https://www.sec.gov/cgi-bin/browse-edgar?company='
                    full_url=partial_url+resultwords+'&type='+reporttype+'&match=contains'
        
                    yield scrapy.Request(full_url,callback=self.parse,meta={'id':url['id'],'conm':url['conm'],'conm_new1':url['conm_new1'],'conm_new2':url['conm_new2'],'conm_new3':url['conm_new3'],'gvkey':url['gvkey'],'ticker':url['ticker'],'cusip_9digit':url['cusip_9digit'],'cusip':url['cusip'],'loc':url['loc'],'flag':1,'original_url':full_url,'reporttype':reporttype})

    
    def parse(self,response):#for company names only
    
        if not self.check_connection():
            print('Connection Lost! Please check your internet connection!', flush=True)
            time.sleep(13)
        
        print("begin")
        all_docs= response.xpath("//tr/td[2]/a[@id='documentsbutton']")
        time.sleep(0.2)
        
        
        #if the company has reports existing
        if all_docs:
        
            print("done deal",response.url)
            for doc in all_docs:
                
                halfurl=doc.xpath(".//@href").extract_first()
                title=urljoin(response.url, halfurl)
                
                
                yield scrapy.Request(url=title, callback = self.parse_dir_contents,meta={'id':response.meta['id'],'conm':response.meta['conm'],'gvkey':response.meta['gvkey'],'ticker':response.meta['ticker'],'cusip_9digit':response.meta['cusip_9digit'],'cusip':response.meta['cusip'],'loc':response.meta['loc'],'reporttype':response.meta['reporttype']})
                
                
        #check for company name 1, company name 2 company name 3        
        elif(response.meta['flag']>=1 and response.meta['flag']<=4):
            flag=response.meta['flag']
            name_header=header[flag-1]
            name=response.meta[name_header]
            query = re.sub(r"[^\w&]", ' ', name)
            query=query.replace('&','%26')
            query=query.upper()
            resultwords  = [word.strip() for word in re.split(" ",query) if word not in stopwords and len(word)>0]
            resultwords=''.join([i+'+' for i in resultwords])
            resultwords.rstrip('+')
            partial_url='https://www.sec.gov/cgi-bin/browse-edgar?company='
            full_url2=partial_url+resultwords+'&type='+response.meta['reporttype']+'&match=contains'
            print('new piece of code',flag,'full url2',full_url2)
            if(flag==1):
                full_url2=partial_url+resultwords+'&type='+response.meta['reporttype']+'&match=startswith'
            flag=flag+1
            yield scrapy.Request(full_url2,callback=self.parse,meta={'id':response.meta['id'],'conm':response.meta['conm'],'conm_new1':response.meta['conm_new1'],'conm_new2':response.meta['conm_new2'],'conm_new3':response.meta['conm_new3'],'gvkey':response.meta['gvkey'],'ticker':response.meta['ticker'],'cusip_9digit':response.meta['cusip_9digit'],'cusip':response.meta['cusip'],'loc':response.meta['loc'],'flag':flag,'original_url':full_url2,'reporttype':response.meta['reporttype']})
            

            
         # check for best match above 85% if existing       
        elif(response.meta['flag']==5):#search page
            print('entered')
            if(response.xpath("//div[@id='contentDiv']/span[@class='companyMatch']/text()")):
  
                print('going once',response.url)
                
                cik_link=response.xpath("//tr/td[1]/a/@href").extract()
                list_of_names=response.xpath(".//tr/td[2]/text()[1]").extract()
                name_link=dict(zip(list_of_names,cik_link))
                companyname=response.meta['conm']
                score=0
                scores=[]
                for each_element in list_of_names:
                    score=self.similar(each_element.upper(),companyname.upper())
                    scores.append(score)
                print('scores is',scores)
                name_score=dict(zip(list_of_names,scores))
                top_match=[k for k,v in name_score.items() if float(v) ==max(scores)]
                
                if(max(scores)<0.85 or len(top_match)>1):#searcy uncertainity
                    print('goine once inside if',response.url)
                    companyticker=response.meta['ticker']
                    partial_url='https://www.sec.gov/cgi-bin/browse-edgar?CIK='
                    full_url=partial_url+companyticker+'&type='+response.meta['reporttype']
                    print(full_url)
                    yield scrapy.Request(full_url,callback=self.parse,meta={'id':response.meta['id'],'conm':response.meta['conm'],'gvkey':response.meta['gvkey'],'ticker':response.meta['ticker'],'cusip_9digit':response.meta['cusip_9digit'],'cusip':response.meta['cusip'],'loc':response.meta['loc'],'flag':6,'original_url':response.meta['original_url'],'reporttype':response.meta['reporttype']})
            
                else:#search best match
                    print("topmatch[0]",top_match[0])
                    cik_link=name_link[top_match[0]]
                    redirect_url=urljoin(response.url, cik_link)
                    print('going once inside else url',redirect_url,'cik -------------------------------------link is',cik_link)
                    
                    yield scrapy.Request(redirect_url,callback=self.parse,meta={'id':response.meta['id'],'conm':response.meta['conm'],'gvkey':response.meta['gvkey'],'ticker':response.meta['ticker'],'cusip_9digit':response.meta['cusip_9digit'],'cusip':response.meta['cusip'],'loc':response.meta['loc'],'flag':6,'original_url':response.meta['original_url'],'reporttype':response.meta['reporttype']})
             
            #ticker check
            else:
                print('going twice',response.url)
                companyticker=response.meta['ticker']
                partial_url='https://www.sec.gov/cgi-bin/browse-edgar?CIK='
                full_url=partial_url+companyticker+'&type='+response.meta['reporttype']
                print(full_url)
                yield scrapy.Request(full_url,callback=self.parse,meta={'id':response.meta['id'],'conm':response.meta['conm'],'gvkey':response.meta['gvkey'],'ticker':response.meta['ticker'],'cusip_9digit':response.meta['cusip_9digit'],'cusip':response.meta['cusip'],'loc':response.meta['loc'],'flag':6,'original_url':response.meta['original_url'],'reporttype':response.meta['reporttype']})
        
        
        #store unsuccessful companies
  
        elif(response.meta['flag']==6):
            #if both ticker and name are not working
            print('both are wrong',response.meta['flag'])
            print(response.url)
            print(response.meta['id'])
            item2=quotestut()
            
            item2['id1']=response.meta['id']
            item2['conm']=response.meta['conm']
            item2['gvkey']=response.meta['gvkey']
            item2['ticker']=response.meta['ticker']
            item2['cusip9digit']=response.meta['cusip_9digit']
            item2['cusip']=response.meta['cusip']
            item2['loc']=response.meta['loc']
            yield item2
                
         
    #start scraping all the reports
    
    def parse_dir_contents(self, response):
        
    
        path = str(response.meta['gvkey'])
   
        
        all_docs2= response.xpath("//tr")
        
        for doc in all_docs2:
            
            tenk=doc.xpath(".//td[2][@scope='row'][contains(text(),'Complete submission text file')]/text()").extract()
            
            if(len(tenk)>0):
                if not self.check_connection():
                    print('Connection Lost! Please check your internet connection!', flush=True)
                    time.sleep(13)
                halfurl2=doc.xpath(".//td[3][@scope='row']/a/@href").extract_first()
                title2=urljoin(response.url, halfurl2)
                filedownloaded=doc.xpath(".//td[3][@scope='row']/a/text()").extract()[0]
                download_url=path+'/'+response.meta['reporttype'].replace('+',' ')+str(filedownloaded)
                storeonline(download_url,title2)

                with open('./urls.csv', 'a') as file:
                    writer = csv.writer(file)
                    writer.writerow([title2,response.meta['id'],response.meta['conm'],response.meta['ticker'],response.meta['gvkey']])
                print('downloaded')
                #title2="'"+title2+"'"
                
    def check_connection(self):  
        try:
            socket.create_connection(("www.google.com", 443))
            return True
        except Exception:
            pass
        return False
                
    #function to check simolaritiy between two sentences
    def similar(self,a, b):
        if not self.check_connection():
            print('Connection Lost! Please check your internet connection!', flush=True)
        time.sleep(13)
        return SequenceMatcher(None, a, b).ratio()
    
def storeonline(file_name,title2):
    
    CLIENT_ID = 'c3c83bbe-42b0-4094-8b40-9ac5882365e1'
    TENANT_ID = '060b02ae-5775-4360-abba-e2e29cca6627'
    AUTHORITY_URL = 'https://login.microsoftonline.com/{}'.format(TENANT_ID)
    RESOURCE_URL = 'https://graph.microsoft.com/'
    API_VERSION = 'v1.0'
    USERNAME = 'fabian.vaniyamveetil.reginold@smu.ca' #Office365 user's account username
    PASSWORD = 'Byebye@12345'
    SCOPES = ['Sites.ReadWrite.All','Files.ReadWrite.All'] # Add other scopes/permissions as needed.
    
    #Creating a public client app, Aquire a access token for the user and set the header for API calls
    cognos_to_onedrive = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY_URL)
    token = cognos_to_onedrive.acquire_token_by_username_password(USERNAME,PASSWORD,SCOPES)
    #rint(token)
    headers = {'Authorization': 'Bearer {}'.format(token['access_token'])}
    onedrive_destination = '{}/{}/me/drive/root:'.format(RESOURCE_URL,API_VERSION)
    #cognos_reports_source = r"/Users/fabianreginold/Downloads/dummyfolder"
    #file_name='kazhigi.txt'
    #title2='https://www.sec.gov/Archives/edgar/data/1141807/000121465915002534/0001214659-15-002534.txt'
    file_data=requests.get(title2).content
    f = requests.head(title2)
    size= f.headers.get('content-length')
    print(size)
    print(onedrive_destination)
    download_url='./dummy/'+'fab.txt'
    data=requests.get(title2)
    with open(download_url,'wb') as fi:
        fi.write(data.content)
    time.sleep(0.2)
    file_size = os.stat(download_url).st_size
    if file_size < 4100000: 
        #Perform is simple upload to the API
        
        r = requests.put(onedrive_destination+"/"+file_name+":/content", data=file_data, headers=headers)
        print('uploaded',r.content)
    else:
        print('entered else statement')
        #Creating an upload session
        upload_session = requests.post(onedrive_destination+"/"+file_name+":/createUploadSession", headers=headers).json()
    
        
        with open(download_url, 'rb') as f:
            
            total_file_size = os.path.getsize(download_url)
            chunk_size = 327680
            chunk_number = total_file_size//chunk_size
            chunk_leftover = total_file_size - chunk_size * chunk_number
            i = 0
            while True:
                chunk_data = f.read(chunk_size)
                start_index = i*chunk_size
                end_index = start_index + chunk_size
                #If end of file, break
                if not chunk_data:
                    break
                if i == chunk_number:
                    end_index = start_index + chunk_leftover
                #Setting the header with the appropriate chunk data location in the file
                headers = {'Content-Length':'{}'.format(chunk_size),'Content-Range':'bytes {}-{}/{}'.format(start_index, end_index-1, total_file_size)}
                #Upload one chunk at a time
                chunk_data_upload = requests.put(upload_session['uploadUrl'], data=chunk_data, headers=headers)
                print(chunk_data_upload)
                print(chunk_data_upload.json())
                i = i + 1
    os.remove(download_url)
        
    
        
        
        

        
        

        
  
            
            

        

  
        
        
