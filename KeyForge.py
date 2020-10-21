# -*- coding: utf-8 -*-
"""
Created on Fri Sep 25 14:28:09 2020

@author: MarcoAmicabile
"""
"""
https://www.keyforgegame.com/api/decks/?page=1&page_size=10&search=&power_level=0,11&chains=0,24&ordering=-date
https://www.keyforgegame.com/api/decks/85ef4c17-2821-45d8-aa75-167057dfb33b/?links=cards,notes
"""

#---------------------#
# Step 0
# Librerie - Parametri
#---------------------#

# Loading libraries
import datetime
import pandas as pd
import requests
import pyodbc
import math
import time


# building the sql connection
SQLconnStr = pyodbc.connect('Driver={SQL Server};'
                      'Server=sqlinstance;'
                      'Database=KeyForge;'
                      'UID=sa;'
                      'PWD=password;')

TableLogAPI = "[stg].[PY_LogAPI]"


# API Parameters
URL = "https://www.keyforgegame.com/api/decks/"

#page = 204124
pagesize = 10

#chiamata API per il numero totale di Decks
response = requests.get(URL)
response_output = response.json()
decktotalnumber = response_output['count']

pagetotalnumber = math.floor(decktotalnumber/10)

SQLquery = "select max([pagenumber]) FROM [KeyForge].[stg].[PY_Decks]"
lpquery = pd.read_sql(SQLquery, SQLconnStr)
lastpage = lpquery.iat[0,0]

#Operazioni da ciclare x page
#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--#--

#-------------#
# Step 1
# Chiamata API
#-------------#

for pageproc in range(lastpage, lastpage + 20):

    print("processing page number: ", pageproc)
    
    #Impostazione URL API da chiamare
    apiurl = URL + "?page=" + str(pageproc) + "&links=cards&page_size=" + str(pagesize) + "&ordering=-date"
    
    #chiamata API
    response = requests.get(apiurl)
    
    #check chiamata API
    response_statuscode = response.status_code
        
    # Insert outcome row in Log_API_Calls
    with SQLconnStr.cursor() as cur:
        cur.execute("INSERT INTO"+ TableLogAPI +"([URL], [Datetime], [ResponseCode]) values (?,?,?)"
            , apiurl
            , datetime.datetime.now()
            , response_statuscode
            )
    
    #Output json della chiamata api
    response_output = response.json()
    
    #diramazione degli output ricevuti
    decktotalnumber = response_output['count']
    response_data_houses = response_output['_linked']['houses']
    response_data_cards = response_output['_linked']['cards']
    response_data_decks = response_output['data']
    
    
    #------------------------------#
    # Step 2
    # Tabellizzazione risultati API
    #------------------------------#
    
    # Houses
    df_houses = pd.DataFrame(response_data_houses)
    
    # Cards
    df_cards = pd.DataFrame(response_data_cards)
    
    # Decks
    df_decks = pd.DataFrame(response_data_decks)
    df_decks['pagenumber'] = pageproc
    
    
    # Deck-Cards
    deckdetails_list = []
    for t1_elem in response_data_decks:
        for t2_elem in t1_elem['_links']['cards']:        
            elem = [t2_elem, t1_elem['id']]
            deckdetails_list.append(elem)
    df_deckcard = pd.DataFrame(deckdetails_list)       
    df_deckcard.columns = ['card_id', 'deck_id']
      
    # Deck-Houses
    deckdetails_list = []
    for t1_elem in response_data_decks:
        for t2_elem in t1_elem['_links']['houses']:        
            elem = [t2_elem, t1_elem['id']]
            deckdetails_list.append(elem)
    df_deckhouse = pd.DataFrame(deckdetails_list)       
    df_deckhouse.columns = ['house_id', 'deck_id']
      
    
    #------------------------------#
    # Step 3
    # Salvataggio dati su SQL
    #------------------------------#
    
    # Houses
    SQLstagingtable = "[stg].[PY_Houses_Stage]"
    
    #Truncate sql staging table
    statement = "TRUNCATE TABLE " + SQLstagingtable
    with SQLconnStr.cursor() as cur:
        cur.execute(statement)
    
    # Insert into staging table
    for index,row in df_houses.iterrows():
              cur.execute("INSERT INTO "+ SQLstagingtable +"([id], [name], [image]) values (?,?,?)"
                     ,row['id']
                     ,row['name']
                     ,row['image']
                     ) 
              SQLconnStr.commit()
              
    # Merge to consolidated staging table
    statement = "exec [stg].[sp_Merge_PY_Houses]"
    with SQLconnStr.cursor() as cur:
        cur.execute(statement)
        
        
        
    #Cards
    SQLstagingtable = "[stg].[PY_Cards_Stage]"
    
    #Truncate sql staging table
    statement = "TRUNCATE TABLE " + SQLstagingtable
    with SQLconnStr.cursor() as cur:
        cur.execute(statement)
    
    # Insert into staging table
    for index,row in df_cards.iterrows():
              cur.execute("INSERT INTO "+ SQLstagingtable +"([id], [card_title], [house],[card_type],[front_image],[card_text],[traits],[power],[armor],[rarity],[flavor_text],[card_number],[expansion],[is_maverick],[is_anomaly],[is_enhanced]) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                      ,row['id']
                      ,row['card_title']
                      ,row['house']
                      ,row['card_type']
                      ,row['front_image']
                      ,row['card_text']
                      ,row['traits']
                      ,row['power']
                      ,row['armor']
                      ,row['rarity']
                      ,row['flavor_text']
                      ,row['card_number']
                      ,row['expansion']
                      ,row['is_maverick']
                      ,row['is_anomaly']
                      ,row['is_enhanced']
                     ) 
              SQLconnStr.commit()
    
    # Merge to consolidated staging table
    statement = "exec [stg].[sp_Merge_PY_Cards]"
    with SQLconnStr.cursor() as cur:
        cur.execute(statement)
    
    
    
    #Decks
    SQLstagingtable = "[stg].[PY_Decks_Stage]"
    
    #Truncate sql staging table
    statement = "TRUNCATE TABLE " + SQLstagingtable
    with SQLconnStr.cursor() as cur:
        cur.execute(statement)
        
    # Insert into staging table
    for index,row in df_decks.iterrows():
              cur.execute("INSERT INTO "+ SQLstagingtable +"([name],[expansion],[power_level],[chains],[wins],[losses],[id],[casual_wins],[casual_losses],[shards_bonus],[pagenumber]) values (?,?,?,?,?,?,?,?,?,?,?)"
                        ,row['name']
                        ,row['expansion']
                        ,row['power_level']
                        ,row['chains']
                        ,row['wins']
                        ,row['losses']
                        ,row['id']
                        ,row['casual_wins']
                        ,row['casual_losses']
                        ,row['shards_bonus']
                        ,row['pagenumber']
                     ) 
              SQLconnStr.commit()
    
    # Merge to consolidated staging table
    statement = "exec [stg].[sp_Merge_PY_Decks]"
    with SQLconnStr.cursor() as cur:
        cur.execute(statement)
              
           
            
    #DecksHouse
    SQLstagingtable = "[stg].[PY_DecksHouses_Stage]"
    
    #Truncate sql staging table
    statement = "TRUNCATE TABLE " + SQLstagingtable
    with SQLconnStr.cursor() as cur:
        cur.execute(statement)
        
    # Insert into staging table
    for index,row in df_deckhouse.iterrows():
              cur.execute("INSERT INTO "+ SQLstagingtable +"([house_id],[deck_id]) values (?,?)"
                        ,row['house_id']
                        ,row['deck_id']
                     ) 
              SQLconnStr.commit()
        
    # Merge to consolidated staging table
    statement = "exec [stg].[sp_Merge_PY_DecksHouses]"
    with SQLconnStr.cursor() as cur:
        cur.execute(statement)  
        
        
       
    #DecksCards
    SQLstagingtable = "[stg].[PY_DecksCards_Stage]"
    
    #Truncate sql staging table
    statement = "TRUNCATE TABLE " + SQLstagingtable
    with SQLconnStr.cursor() as cur:
        cur.execute(statement)
        
    # Insert into staging table
    for index,row in df_deckcard.iterrows():
              cur.execute("INSERT INTO "+ SQLstagingtable +"([card_id],[deck_id]) values (?,?)"
                        ,row['card_id']
                        ,row['deck_id']
                     ) 
              SQLconnStr.commit()   
        
    # Merge to consolidated staging table
    statement = "exec [stg].[sp_Merge_PY_DecksCards]"
    with SQLconnStr.cursor() as cur:
        cur.execute(statement)  
        
    time.sleep(10)
