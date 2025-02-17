import pandas as pd
import numpy as np
import hashlib
from google.cloud import bigquery
import sys
import os
from datetime import datetime
import pytz

#function to read the data from the excel file
def read_data(file):
    categories = pd.read_excel(file)
    return categories

def create_business_key(data):
    data['bk'] = data['Item_Type'] + '~' + data['Item_NO']
    data['bk'] = data['bk'].str.upper()
    return data


def main():
   # Step 1: Read the data Kategorisera excel file
    categories = read_data('SE-en-bm003-Beds-category-with-items (5).xlsx')  #'SE-en-bm003-Beds-category-with-items (4).xlsx'
    
    #drop nan values from item_no column
    categories = categories.dropna(subset=['Item_NO'])
    
    #convert to int
    categories['Item_NO'] = categories['Item_NO'].astype(int)
    
    #use leading zeros to 8 digits
    categories['Item_NO'] = categories['Item_NO'].apply(lambda x: '{0:0>8}'.format(x))
    
    #make to list
    excel_item_nos = categories['Item_NO'].tolist()
    
    #make to dataframe
    excel_item_nos_df = pd.DataFrame(excel_item_nos, columns=['Item_NO'])
    excel_item_nos_df = excel_item_nos_df.drop_duplicates()
    
    #extract unique item_nos from the excel file
    excel_item_nos = excel_item_nos_df['Item_NO'].tolist()
    
     # Step 2: Read the data from bigquery datasets
    client = bigquery.Client(project='ingka-range360-discovery-dev')
    #client_erix = bigquery.Client(project='ingka-rrm-erix-prod')
    
    datasets = {
    'item_attributes': 'ingka-range360-discovery-dev.item_attributes.item_attributes_vw',
    'item_materials': 'ingka-range360-discovery-dev.item_attributes.item_materials_vw',
    'item_measures': 'ingka-range360-discovery-dev.item_attributes.item_measures_vw',
    'item_valid_design': 'ingka-rrm-erix-prod.erix_production.item_valid_design_local_updated'
    }
    
    # Use excel_item_nos to filter data from each dataset in bigquery
    item_attributes = client.query(f""" SELECT * FROM {datasets['item_attributes']} WHERE item_no IN {tuple(excel_item_nos)} """).result().to_dataframe()
    # for item_materials, sort to country code SE and language code en
    #item_materials = client.query(f""" SELECT * FROM {datasets['item_materials']} WHERE item_no IN {tuple(excel_item_nos)} AND country_code = 'SE' AND language_code = 'en' """).result().to_dataframe()
    item_measures = client.query(f""" SELECT * FROM {datasets['item_measures']} WHERE item_no IN {tuple(excel_item_nos)} """).result().to_dataframe()
    #item_valdes = client_erix.query(f""" SELECT item_no, item_type, valid_design.valid_design_text FROM {datasets['item_valid_design']} WHERE item_no IN {tuple(excel_item_nos)} """).result().to_dataframe()
    
    
    shanawas_excel = read_data('hfb05-sofa-and-accessories.xlsx')
    
    #use leading zeros to 8 digits
    shanawas_excel['Item_NO'] = shanawas_excel['ITEM_NO'].apply(lambda x: '{0:0>8}'.format(x))
    
    #change ITEM_TYPE to Item_Type
    shanawas_excel.rename(columns={'ITEM_TYPE': 'Item_Type'}, inplace=True)
    
    #create business key for item_valdes
    #item_valdes = create_business_key(item_valdes)
    
    # use the create_business_key function to create a business key
    shanawas_excel = create_business_key(shanawas_excel)
    
    #use the create_business_key function to create a business key
    categories = create_business_key(categories)
    
    #use the business key to create a hash key with sha256
    categories['hash_key'] = categories['bk'].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    item_attributes['hash_key'] = item_attributes['bk'].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    #item_materials['hash_key'] = item_materials['bk'].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    item_measures['hash_key'] = item_measures['bk'].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    
    # use the business key to create a hash key with sha256
    shanawas_excel['hash_key'] = shanawas_excel['bk'].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    
    #step 3: merge the data
    #merge the categories and item_attributes data
    merged_data = pd.merge(categories, item_attributes, on='hash_key', how='left', suffixes=('_categories', '_attributes'))
    
    #merge the merged_data and item_measures data
    merged_data = pd.merge(merged_data, item_measures, on='hash_key', how='left', suffixes=('', '_measures'))
    
    merged_data = pd.merge(merged_data, shanawas_excel, on='hash_key', how='left', suffixes=('', '_shanawas'))
    
    #merged_data = pd.merge(merged_data, item_valdes, on='hash_key', how='left', suffixes=('', '_valdes'))
    
    #check for missing item_nos in the merged data by comparing with the excel_item_nos
    missing_item_nos = set(excel_item_nos) - set(merged_data['Item_NO'])
    missing_item_nos_attribute = set(excel_item_nos) - set(merged_data['item_no'])
    missing_item_nos_measures = set(excel_item_nos) - set(merged_data['item_no_measures'])
    #print the missing item_nos
    print('Merged : ', missing_item_nos)
    print('Attribute : ', missing_item_nos_attribute)
    print('Measure : ', missing_item_nos_measures)
    
    #save missing item_nos to a pkl file
    missing_item_nos = pd.DataFrame(missing_item_nos)
    missing_item_nos.to_pickle('missing_item_nos.pkl')
    # save missing item_nos_attribute to a pkl file
    missing_item_nos_attribute = pd.DataFrame(missing_item_nos_attribute)
    missing_item_nos_attribute.to_pickle('missing_item_nos_attribute.pkl')
    # save missing item_nos_measures to a pkl file
    missing_item_nos_measures = pd.DataFrame(missing_item_nos_measures)
    missing_item_nos_measures.to_pickle('missing_item_nos_measures.pkl')
    
    #save the merged data to a pkl file
    merged_data.to_pickle('bed_merged_data.pkl')
    
    
    
    
if __name__ == '__main__':
    main()
    


    