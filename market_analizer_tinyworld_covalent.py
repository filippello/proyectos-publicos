import requests
import time
import pandas as pd

APIKEY = 'your_covalent_api_key' #apikey from covalent
SEARCH_HOURS = 12   #hours you want to search
BLOCK_SECONDS = 3   #blockchain specific seconds to mine, bsc is 3
CHAIN_ID = '56' #bsc chain id
TOPICS = '0xc3c92819c2e6e44758469657ba21179cfe49cc05cf59d67bde4396ef9055f4c8'   #topic to search
SENDER_ADDRESS = '0x390e1ba132ed9ff3394d8aa484983cc8c85f8149'   #sender address of the topic to search

""" this function gets the actual block """
def get_block():
    global ending_block
    urlbloque = 'https://api.covalenthq.com/v1/'+CHAIN_ID+'/block_v2/latest/?&key='+APIKEY
    result = requests.get(urlbloque)
    result = result.json()
    ending_block = result['data']['items'][0]['height']
    print(ending_block)

""" this funcion calculates an aproximate time in blocks """ 
def calculate_start_block(hours,blocktime):
    global starting_block
    global ending_block
    blocks_ago = (hours*60*60)/blocktime
    starting_block = int(ending_block)-blocks_ago
    starting_block = str(int(starting_block))
    ending_block = str(ending_block)

def get_tokens(page_number):
    global sells_tokenid_df
    if result :
        for key in result:
            tx_hash = key['tx_hash']
            date = key['block_signed_at']
            block = key['block_height']
            value =key['raw_log_data'][0:66] #parse the value part of the raw_log_data
            value = int(value, 16) #convert the value from hexa to decimal
            value =  value*0.000000000000000001 #convert the value from gwei to BUSD
            value = float(str(round(value, 4)))
            tokenID =key['raw_log_data'][130:194] #parse the value part of the raw_log_data to get the tokenID in hexa
            tokenID = int(tokenID, 16)
            sells_tokenid_df_temp = pd.DataFrame({'tokenID':[tokenID], 'tx_hash':[tx_hash], 'date':[date],'block':[block],'value':[value]})
            sells_tokenid_df = sells_tokenid_df.append(sells_tokenid_df_temp)
        page_number = int(page_number)+1
        return(page_number)
    else:
        return(0)

""" config values """
starting_block = '0'    #starting block you want to search, 0 its actual block
ending_block = '0'
page_number = '0'
sells_tokenid_df = pd.DataFrame(columns=['tokenID','tx_hash','date','block','value']) #panda dataframe to store the sells
get_block()
calculate_start_block(SEARCH_HOURS,BLOCK_SECONDS)
print(starting_block)
print(ending_block)

"configure covalent url"
url = 'https://api.covalenthq.com/v1/'+CHAIN_ID+'/events/topics/'+TOPICS+'/?&key='+APIKEY+'&starting-block='+starting_block+'&ending-block='+ending_block+'&sender-address='+SENDER_ADDRESS+'&page-number='+page_number
result = requests.get(url)
result = result.json()
result = result['data']['items']

while page_number != 0:
    page_number = get_tokens(page_number)
    url = 'https://api.covalenthq.com/v1/'+CHAIN_ID+'/events/topics/'+TOPICS+'/?&key='+APIKEY+'&starting-block='+starting_block+'&ending-block='+ending_block+'&sender-address='+SENDER_ADDRESS+'&page-number='+str(page_number)
    flag = True
    while(flag):
        try:
            result = requests.get(url)
            result = result.json()
            result = result['data']['items']
            flag = False
        except Exception as e:
            print(e)
            time.sleep(10)
    time.sleep(3)

print("sell dataframe")
print(sells_tokenid_df)
number_sells = len(sells_tokenid_df.index)
sell_volume = sells_tokenid_df['value'].sum()
print(sells_tokenid_df)
print("number of sells: " + str(number_sells))
print("sell volume: " +str(sell_volume))