import os
os.chdir('/Users/bryanshepherd/Projects/ClintonEmails')
# Purpose: Text mine Clinton emails

# The data fetching is based heavily on this:
# https://mail.google.com/mail/u/0/#inbox/156b469a0e5e9100?projector=1

import pandas as pd
import requests
import pickle
import json
import re
import PyPDF2

# Create the database (or connect to it if already created)
# Use sqlite since it is a simple, portable solution
# https://docs.python.org/2/library/sqlite3.html
import sqlite3

conn = sqlite3.connect('db/emails.db')
c = conn.cursor()

create_tbl_qry = ('CREATE TABLE clinton_email (email_id integer primary key, '
                  'email_pdf varchar, email_from varchar, email_to varchar, '
                  'email_date date, pdf_url varchar, email_subject varchar, '
                  'case_no varchar, email_content text)')
                 
c.execute(create_tbl_qry)

# Pull the list of files from the FOIA site
# http://docs.python-requests.org/en/master/user/quickstart/

response = requests.get("https://foia.state.gov/searchapp/Search/SubmitSimpleQuery",
                        params = {"searchText": "*", "beginDate": "false", 
                                  "endDate": "false", "collectionMatch": "Clinton_Email", 
                                  "postedBeginDate": "false", "postedEndDate": "false",
                                  "caseNumber": "false", "page": 1, "start": 0, "limit": 5},
                        verify=False)
                        
# with open('data/httpresponse.pkl', 'wb') as f:
#     pickle.dump(response, f)

# with open('data/small_httpresponse.pkl', 'wb') as f:
#     pickle.dump(response, f)
  
with open('data/httpresponse.pkl', 'rb') as f:
    response = pickle.load(f)

# Remove values for '"docDate" and "postedDate" because dates come through
# weirdly and give the json parser problems.
# http://stackoverflow.com/questions/16720541/python-string-replace-regular-expression
regex = re.compile(r"new Date\(.+?\)")
#regex.sub('null', 'new Date(1353906000000)')

cleaned_text = regex.sub('null', response.text)

parsed_json =json.loads(cleaned_text)

results = parsed_json['Results']

results_df = pd.DataFrame(results)

# Add some useful reference information 
# to the data frame 
results_df['pdf_filename'] = results_df.pdfLink.str[-13:]
results_df['pdf_URL'] = "https://foia.state.gov/searchapp/" + results_df['pdfLink']

results_df.to_pickle('data/results_df.pkl')
results_df = pd.read_pickle('data/results_df.pkl')

# Add data to sqlite table to match article
# Not really necessary since we've saved the 
# data in a pickle
results_df.to_sql('clinton_email', con=conn, if_exists='replace')

# Parse PDFs to get text data
# http://stackoverflow.com/questions/34503412/download-and-save-pdf-file-with-python-requests-module
# https://automatetheboringstuff.com/chapter13/
# http://stackoverflow.com/questions/26494211/extracting-text-from-a-pdf-file-using-pdfminer-in-python

# First get all the documents downloaded then 
# we'll parse the PDFs
# This probably takes a little longer, but
# is more robust
nrows_df = len(results_df)
for i in range(nrows_df):
    pdf_result = requests.get(results_df.pdf_URL[i])

    filename = 'pdfs/' + results_df.pdf_filename[i]
    with open(filename, 'wb') as f:
        f.write(pdf_result.content)
    
# Get the list of downloaded pdfs to iterate through
# http://stackoverflow.com/questions/3207219/how-to-list-all-files-of-a-directory-in-python
# Actually, this is cool, but not really necessary
# since we can just iterate through the dataframe and
# use the 'pdf_filename' column
# import glob
# pdf_files = glob.glob('pdfs/*.pdf')

# Now parse them
comp_text = [None]*nrows_df
for i in range(nrows_df):
    pdfFileObj = open('pdfs/'+results_df.pdf_filename[i], 'rb')
    
    pdfReader = PyPDF2.PdfFileReader(pdfFileObj)

    comp_text[i] = ''
    for pg in range(pdfReader.numPages):
        pageObj = pdfReader.getPage(pg)
        comp_text[i] = comp_text[i] + pageObj.extractText()
    
    if i %% 250 == 0:
        print('Just completed iteration ', i)
        
results_df['parsed_email_text'] = comp_text

# This works and is very simple, but 
# it doesn't seem to have better 
# parsing results than PyPDF though.
# https://github.com/chrismattmann/tika-python
# import tika
# tika.initVM()
# from tika import parser
# parsed = parser.from_file('pdfs/first.pdf')
