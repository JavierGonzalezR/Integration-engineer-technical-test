# New skill test for integrations

# Cornershop's backend integrations test

## Before you begin

Read this document entirely before you start working on your solution. There are essential tips and notes in every section for you to succeed.

The first step is to create a new private repository to upload your work. Once you have finished the test, invite @jeffersonlizar and @cornershop-hr as collaborators and send the repo link to your Cornershop recruiter, telling them that you have finished.

## Introduction

A common task at Cornershop is collecting product information from external sources like CSV files. This technical test for backend integration engineers, requires you to process CSV files and then to use the data to connect it to an "external" Ingestion API (provided as a separate `integration-skill-test-server` project for you to run locally).

## Case

We have just signed a very important deal with one of the main retailers in Toronto: *Richart's Wholesale Club*. To improve the store's customer experience at Cornershop, our new partner has decided to send us periodical updates of their products, prices, and status. 

To accomplish this, a couple of CSV files were uploaded to an AWS S3 Bucket containing all the information that we need:

- [PRODUCTS.csv](https://cornershop-scrapers-evaluation.s3.amazonaws.com/public/PRODUCTS.csv): Contains the products basic information
- [PRICES-STOCK.csv](https://cornershop-scrapers-evaluation.s3.amazonaws.com/public/PRICES-STOCK.csv): Contains information about the prices and stock.

We need to write a script that stitches the data together, and then send it to our Ingestion API by  the constraints below.

There is a script template at `integrations/richart_wholesale_club/ingestion.py`, feel free to extend it with your own logic.

### Constraints

The following constraints apply:

1. The only external libraries allowed in this case are [Pandas](https://pandas.pydata.org/), [Numpy](https://numpy.org/) and [Requests](https://requests.readthedocs.io/)
2. We're only gonna work with branches identified as  `MM` and `RHSM` so we don't need (and don't want) other branches' information in the API.
3. API credentials cannot be exposed in code

### Considerations

### PRODUCTS.csv

1. The **SKU must be unique** within the store.
2. All **texts must be cleaned** before inserting them into the database. For example, some product descriptions in the files may contain HTML tags, they must be removed.
3. Categories and sub-categories in the file must be joined together into a **single value** containing all categories **in lower case** and **separated by a pipe symbol** (`|`).
4. Some products might have the **package** information in the **description** column. *Extract the package* and store it in its corresponding field in the API.

### PRICES-STOCK.csv

1. We only want products that are currently in stock, this means their **stock** is greater than 0.

### API

- The API is using **OAuth2 as the authorization framework**, we use client credentials as grant type

Use the following data to be able to get a valid token:

```python
GRAND_TYPE = "client_credentials"
CLIENT_ID = "mRkZGFjM"
CLIENT_SECRET = "ZGVmMjMz"
```

Do you need more information about OAuth2? You can get more details in this document

[https://www.digitalocean.com/community/tutorials/an-introduction-to-oauth-2](https://www.digitalocean.com/community/tutorials/an-introduction-to-oauth-2)

- All endpoints within the `/api` path must be called with a valid token
- You must get the id of Richards with a **GET** request to `/api/merchants` endpoint
- Once you have Richard's id then issue a **PUT** request to `/api/merchants/<id>` updating the `is_active` field to True
- You must delete the store with the name **Beauty** by issuing a **DELETE** request to `api/ merchants/<id>`
- Once all the filters required with the shared files have been completed, you must send the catalog information to the API, create a **POST** request to `/api/products/` endpoint with the product data. Send the 100 most expensive products from each branch.

    The `/api/products/` endpoint is only allowed to receive products for the merchant_id related to Richard's

- The API provided has limits, you must respect them:

You will have available 10 thousand requests per day and 2 thousand requests per hour

- Docs for the API:

[https://documenter.getpostman.com/view/1992239/TVmMgxxp](https://documenter.getpostman.com/view/1992239/TVmMgxxp)

## General considerations

The example data provided is just referential. **You are completely free to transform the information collected in the way that you want** (capitalize, replace text, transform symbols, etc.), but data stored must meet the data types and semantics of the API structure. i.e: there must be a brand in the `Brand` field and a URL in the `Image URL` field, etc.

Feel free to **use the Python environment manager of your preference**. We provide a `Pipfile` in case you want to use `Pipenv` and a `requirements.txt` if you prefer to use Python's `pip`. Just remember to commit a file with the list of dependencies.

When working on this test, consider we may want to add more integrations in the future or use the same integration to process more branches of the same store. Please **write your code in a way it's easy to extend and maintain**.

# Aspects to be evaluated

- Software design
- Programing style
- Appropriate use of the packages
- Data processing strategy
- Quality of the collected data
- Testing
- GIT repository history