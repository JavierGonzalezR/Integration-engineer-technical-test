import numpy as np
import pandas as pd
import requests

def process_csv_files():
    #raise NotImplementedError

    file1 = open('backend-integration-test-master\integrations\\richart_wholesale_club\PRODUCTS.csv', encoding="utf8") #open file products
    file2 = open('backend-integration-test-master\integrations\\richart_wholesale_club\PRICES-STOCK.csv', encoding="utf8") #open file prices-stock
    rows1 = []
    rows2 = []
    header1 = next(file1) #taking the name of the columns´s data
    header2 = next(file2) #taking the name of the columns´s data
    
    for r in file1:
        rows1.append(r) #taking the data row by row
    for r in file2:
        rows2.append(r) #taking the data row by row

    #close the files
    file1.close() 
    file2.close()

    #filter the data and adjust the strings for the requirements.

    columns1=header1.strip('\n').split("|")
    columns1=columns1[0:10]+[columns1[10].lower()+"|"+columns1[11].lower()+"|"+columns1[12].lower()]+[columns1[-1]]
    columns2=header2.strip('\n').split("|")
    data1=[]
    data2=[]

    for r in range(len(rows1)):
        if u'\xa0' in rows1[r]: #delete de "hard spaces"
            rows1[r]=rows1[r].replace(u'\xa0', u'')
        data1.append(rows1[r].strip('\n').replace('<p>','').replace('</p>','').replace('Ñ','N').split("|")) #delete the php tags
        data1[r]=data1[r][0:10]+[data1[r][10].lower()+"|"+data1[r][11].lower()+"|"+data1[r][12].lower()]+[data1[r][-1]] #transforms the categories

        #recover the package info, and add it to buy_unit column
        if data1[r][1]=='':
            if (data1[r][8].split(' ')[-1]=='UN' or data1[r][8].split(' ')[-1]=='KG' or data1[r][8].split(' ')[-1]=='1UN' or data1[r][8].split(' ')[-1]=='1KG'):
                data1[r][1]=data1[r][8].split(' ')[-1]
            elif ('ML' in data1[r][8] or 'GRS' in data1[r][8] or 'PZA' in data1[r][8]):
                data1[r][1]=data1[r][8].split(' ')[len(data1[r][8].split(' '))-2] + data1[r][8].split(' ')[len(data1[r][8].split(' '))-1]
            elif('GR' in data1[r][8] or 'LT' in data1[r][8] ):
                data1[r][1]=data1[r][8].split(' ')[-1]

    #filter the 2 branches
    for r in range(len(rows2)):
        if rows2[r].strip('\n').split("|")[3]>'0' and (rows2[r].strip('\n').split("|")[1] == 'RHSM' or rows2[r].strip('\n').split("|")[1] == 'MM'):
            data2.append(rows2[r].strip('\n').split("|"))
        
    #delete the duplicates SKUs
    df1=pd.DataFrame(data1, columns=columns1)
    df1=df1[~df1.duplicated(keep='first')]
    
    df2=pd.DataFrame(data2, columns=columns2)
    df2=df2[~df2.duplicated(keep='first')]
    
    #separate the dataframe into 2 dataframes, one per branch
    dfT=[y for x, y in df2.groupby('BRANCH',as_index=False)]

    #sort the dataframes to get de 100 expensive articles
    dfA=pd.merge(dfT[0].sort_values('PRICE',ascending=False).head(100), df1, how='left', on='SKU')
    dfB=pd.merge(dfT[1].sort_values('PRICE',ascending=False).head(100), df1, how='left', on='SKU')

    return dfA,dfB

#get conection token
def get_new_token():
    url = "http://localhost:5000///oauth/token?client_id=mRkZGFjM&client_secret=ZGVmMjMz&grant_type=client_credentials"

    payload={}
    headers = {}

    response = requests.request("POST", url, headers=headers, data=payload)

    return response.json()["access_token"]

#get the ID
def get_id(token):
    url = "http://localhost:5000//api/merchants"

    payload={}
    headers = {
        'token': 'Bearer '+ token
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    return response.json()["merchants"]

#update Richard's 
def apimerch(id,token):

    url = "http://localhost:5000//api/merchants/"+id

    payload = {
        "can_be_deleted": True,
        "can_be_updated": True,
        "id": ""+id+"",
        "is_active": True,
        "name": "Richard's"
    }
    headers = {
        'token': 'Bearer '+ token
    }
    response = requests.put(url, headers=headers, json=payload)

#delete merchant
def delete(id,token):
    url = "http://localhost:5000//api/merchants/"+id

    payload={}
    headers = {
    'token': 'Bearer '+ token
    }

    response = requests.request("DELETE", url, headers=headers, data=payload)

#send the articles
def send_catalog(a,b,token,id):

    url = "http://localhost:5000//api/products"

    frames=[a,b]
    df=pd.concat(frames, ignore_index=True)
    
    for i in range(len(df)):
        row=df.iloc[[i]]
        payload = {
            "merchant_id": ""+id+"",
            "sku": row.iat[0,0],
            "barcodes": [row.iat[0,9]],
            "brand": row.iat[0,14],
            "name": row.iat[0,10],
            "description": row.iat[0,11],
            "package": row.iat[0,4],
            "image_url": row.iat[0,12],
            "category": row.iat[0,13],
            "url": "",
            "branch_products": [{
                "branch": row.iat[0,1],
                "stock": int(row.iat[0,3]),
                "price": float(row.iat[0,2])
                }]
        }
        headers = {
            'token': 'Bearer '+token
        }

        response = requests.request("POST", url, headers=headers, json=payload)

if __name__ == "__main__":
    a,b=process_csv_files()
    c=get_new_token()
    i=get_id(c)[2]["id"]
    apimerch(i,c)
    i2=get_id(c)[3]["id"]
    delete(i2,c)
    send_catalog(a,b,c,i)