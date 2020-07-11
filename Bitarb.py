from multiprocessing import Process,Pipe
import threading
import queue
import liquidtap
import json
import websocket
import ast
import ccxt
from time import sleep
import pandas as pd

liq = ccxt.liquid({
'apiKey': 'XXXXX', #APIキーを設定
'secret': 'XXXXX', #シークレットキーを設定
})
bitf = ccxt.bitflyer({
'apiKey': 'XXXXX', #APIキーを設定
'secret': 'XXXXX', #シークレットキーを設定
})
ccc = ccxt.coincheck({
'apiKey': 'XXXXX', #APIキーを設定
'secret': 'XXXXX', #シークレットキーを設定
})
bitb = ccxt.bitbank({
'apiKey': 'XXXXX', #APIキーを設定
'secret': 'XXXXX', #シークレットキーを設定
})




def bitflyer_ws(soushin_buy,soushin_sell):
    #print('bitflyer start')
    def on_message(ws, message):
        
        #r=json.loads(message)
        #print(r)
        r = json.loads(message)
        best_bid=r['params']['message']['best_bid']
        best_ask=r['params']['message']['best_ask']
        #print("bid:",best_bid)#bid downside
        #print("ask:",best_ask)
        soushin_buy.send(best_ask)
        soushin_sell.send(best_bid)
        
        
        #print('{product_code} {timestamp} bid {best_bid}({best_bid_size:.1f}) ask {best_ask}({best_ask_size:.1f}) ltp {ltp} volume {volume:.1f}'.format(**r))

    def on_error(ws, error):
        print(error)

    def on_close(ws):
        print("### closed ###")

    def on_open(ws):
        
        ws.send(json.dumps({
                'method': 'subscribe',
                'params': {'channel': 'lightning_ticker_BTC_JPY'},
            }))
        

    ws = websocket.WebSocketApp("wss://ws.lightstream.bitflyer.com/json-rpc",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()
def liquid_ws(soushin_buy,soushin_sell):
    buy=0.0
    sell=0.0
    #print('liquid start')
    
    
    def update_callback(data):
        #Update_Process
        a=json.loads(data)
        #liquid_sell=a['market_bid']
        #liquid_buy=a['market_ask']
                
        soushin_buy.put(a['market_ask'])
        soushin_sell.put(a['market_bid'])
        #print(liquid_buy)
        

    def on_connect(data):
        tap.pusher.subscribe("product_cash_btcjpy_5").bind('updated', update_callback)
        
        
    def start():
        tap = liquidtap.Client()
        tap.pusher.connection.bind('pusher:connection_established', on_connect)
        tap.pusher.connect()
        while True:
            
            sleep(1)
    
    tap = liquidtap.Client()
    tap.pusher.connection.bind('pusher:connection_established', on_connect)
    tap.pusher.connect()
    while True:
        
        sleep(0.5)
        

def integration(liquid_b,liquid_s,bf_buy,bf_sell):
    #print('main start')
    
    #------------------WEBSOCKET-------------------------------------------
    bitflyer_buy=bf_buy.recv()
    bitflyer_sell=bf_sell.recv()
    liquid_buy=liquid_b.get()
    liquid_sell=liquid_s.get()
    #--------------------------------------------------------------
    #----------------REST_API----------------------------------------------
    bitbank=bitb.fetch_ticker("BTC/JPY")
    bitbank_buy=bitbank["ask"]
    bitbank_sell=bitbank["bid"]
    coincheck=ccc.fetch_ticker("BTC/JPY")
    coincheck_buy=coincheck["ask"]
    coincheck_sell=coincheck["bid"]
    #-----------------------------------------------------------------
    matome=dict(ask=[liquid_buy,bitflyer_buy,bitbank_buy,coincheck_buy],bid=[liquid_sell,bitflyer_sell,bitbank_sell,coincheck_sell])
    matome=pd.DataFrame(data=matome,index=['liquid','bitflyer','bitbank','coincheck'])
    #print(matome)
    sleep(0.3)
    while True:
        #------------RESTAPI----------------------------------------------------
        bitbank=bitb.fetch_ticker("BTC/JPY")
        bitbank_buy=int(bitbank["ask"])
        bitbank_sell=int(bitbank["bid"])
        coincheck=ccc.fetch_ticker("BTC/JPY")
        coincheck_buy=int(coincheck["ask"])
        coincheck_sell=int(coincheck["bid"])
        #------------WEBSOCKET---------------------------------------------
        bitflyer_buy=int(bf_buy.recv())
        bitflyer_sell=int(bf_sell.recv())
        liquid_buy=float(liquid_b.get())
        liquid_sell=float(liquid_s.get())
        #------------------------------------------------------------------
        matome=dict(ask=[liquid_buy,bitflyer_buy,bitbank_buy,coincheck_buy],bid=[liquid_sell,bitflyer_sell,bitbank_sell,coincheck_sell])
        matome=pd.DataFrame(data=matome,index=['liquid','bitflyer','bitbank','coincheck'])
        matome1=matome.sort_values('ask')
        matome2=matome.sort_values('bid', ascending=False)
        #best_bid=[liquid_sell,bitflyer_sell,bitbank_sell,coincheck_sell]
        #best_ask=[liquid_buy,bitflyer_buy,bitbank_buy,coincheck_buy]
        #print(matome1)
        
        #print(matome1.index[0] + ":" + str(matome1.iloc[0,0]))
        #print(matome2.index[0] + ":" + str(matome2.iloc[0,1]))#3,1
        best_ask = float(matome1.iloc[0,0])
        best_bid = float(matome2.iloc[0,1])
        bid_ask = best_bid - best_ask
        print("\r 現在 " + matome1.index[0] + " の " + str(matome1.iloc[0,0]) +"円で 1BTC買い " + matome2.index[0] + " の " + str(matome2.iloc[0,1]) + "円で 1BTC売ると " + str(bid_ask) + "円の利益です。", end='')
        
        
    
        
        
        sleep(0.3)
            


if __name__ =='__main__':
    
    #print("main start")
    
    bf_buy,bf_tube1=Pipe()
    bf_sell,bf_tube2=Pipe()
    q_buy=queue.LifoQueue()
    q_sell=queue.LifoQueue()
        
    bf=Process(target=bitflyer_ws,args=(bf_tube1,bf_tube2,))
    lq=threading.Thread(target=liquid_ws,args=(q_buy,q_sell,))
    main_roop=threading.Thread(target=integration,args=(q_buy,q_sell,bf_buy,bf_sell,))
    
    
    lq.start()
    bf.start()
    main_roop.start()
    print("Liquid,Bitflyer--websocket Bitbank,Coincheck--RestApi ")
    