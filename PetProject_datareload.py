# -*- coding: utf-8 -*-
"""
Created on Sun Mar 14 11:01:04 2021

@author: michael
"""

# -*- coding: utf-8 -*-
"""
Created on Sun Mar 14 10:58:42 2021

@author: michael
"""

import cbpro
import pandas as pd
import time
import logging
import datetime
import warnings, sys,os
warnings.simplefilter(action='ignore', category=FutureWarning)
logging.disable(sys.maxsize)
public_client = cbpro.PublicClient()
logger_plot = logging.getLogger('fbprophet.plot')
fileLoc = r"C:\Users\mryagerr\Desktop\Project Trader\jsons\\"

blocked_list = ['WBTC','YFI','LINK','MKR','ETH','BTC','DAI','XLM']
loop_break = False

while loop_break==False:
    try:
        start_time = time.time()
        print(str(datetime.datetime.now()))
        products = pd.DataFrame(public_client.get_products())
        products = products[products['quote_currency'] == 'USD']
        products = products[~products['quote_currency'].isin(blocked_list)]
        products.to_json(fileLoc+main.json",orient = "records")
        for products_id in products['id'].unique():
            product_lookup = pd.DataFrame(public_client.get_product_historic_rates(products_id, granularity=60))
            time.sleep(0.1)
            if len(product_lookup) > 0:  
                product_lookup.columns = ['time','low','high','open','close','volume']
                try:
                    product_lookup = product_lookup.append(pd.read_json(fileLoc+products_id.lower()+".json"))
                except:
                    pass
                product_lookup = product_lookup.sort_values(['time'],ascending = False).drop_duplicates(['time']).head(4500)
                product_lookup.to_json(fileLoc+products_id.lower()+".json",orient = "records")
        if time.time() - start_time  < (30):
            time.sleep(max(30-(time.time()-start_time),0))   
    except Exception as e:
        print(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        # break
        # loop_break = True
        time.sleep(max(300-(time.time()-start_time),0))
        pass
