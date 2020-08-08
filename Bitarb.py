from multiprocessing import Process, Pipe
import threading
import queue
import liquidtap
import json
import websocket
import ast
import ccxt
from time import sleep
import pandas as pd

liq = ccxt.liquid({"apiKey": "XXXXX", "secret": "XXXXX",})  # APIキー,シークレットキーを設定
bitf = ccxt.bitflyer({"apiKey": "XXXXX", "secret": "XXXXX",})  # APIキー,シークレットキーを設定
ccc = ccxt.coincheck({"apiKey": "XXXXX", "secret": "XXXXX",})  # APIキー,シークレットキーを設定
bitb = ccxt.bitbank({"apiKey": "XXXXX", "secret": "XXXXX",})  # APIキー,シークレットキーを設定


def bitflyer_ws(soushin_buy, soushin_sell):
    # print('bitflyer start')
    def on_message(ws, message):
        r = json.loads(message)
        best_bid = r["params"]["message"]["best_bid"]
        best_ask = r["params"]["message"]["best_ask"]
        soushin_buy.send(best_ask)
        soushin_sell.send(best_bid)

    def on_error(ws, error):
        print(error)

    def on_close(ws):
        print("### closed ###")

    def on_open(ws):

        ws.send(
            json.dumps(
                {
                    "method": "subscribe",
                    "params": {"channel": "lightning_ticker_BTC_JPY"},
                }
            )
        )

    ws = websocket.WebSocketApp(
        "wss://ws.lightstream.bitflyer.com/json-rpc",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.on_open = on_open
    ws.run_forever()


def liquid_ws(soushin_buy, soushin_sell):
    def update_callback(data):
        a = json.loads(data)
        soushin_buy.put(a["market_ask"])
        soushin_sell.put(a["market_bid"])
        # print(liquid_buy)

    def on_connect(data):
        tap.pusher.subscribe("product_cash_btcjpy_5").bind("updated", update_callback)

    def start():
        tap = liquidtap.Client()
        tap.pusher.connection.bind("pusher:connection_established", on_connect)
        tap.pusher.connect()
        while True:

            sleep(1)

    tap = liquidtap.Client()
    tap.pusher.connection.bind("pusher:connection_established", on_connect)
    tap.pusher.connect()
    while True:

        sleep(0.5)


def gmo_ws(gmo_ask, gmo_bid):
    # print("gmostart")
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp("wss://api.coin.z.com/ws/public/v1")

    def on_open(self):
        message = {"command": "subscribe", "channel": "orderbooks", "symbol": "BTC"}
        ws.send(json.dumps(message))

    def on_message(self, message):

        message2 = ast.literal_eval(message)

        ask = message2["asks"][0]["price"]
        bid = message2["bids"][0]["price"]

        gmo_ask.put(ask)
        gmo_bid.put(bid)
        # print(message2['asks'][0]['price'])
        # print("ask",ask)
        # print("bid",bid)

    ws.on_open = on_open
    ws.on_message = on_message
    ws.run_forever()


def integration(liquid_b, liquid_s, bf_buy, bf_sell, gmo_ask, gmo_bid):

    while True:
        # ------------RESTAPI----------------------------------------------------
        bitbank = bitb.fetch_ticker("BTC/JPY")
        bitbank_buy = int(bitbank["ask"])
        bitbank_sell = int(bitbank["bid"])
        coincheck = ccc.fetch_ticker("BTC/JPY")
        coincheck_buy = int(coincheck["ask"])
        coincheck_sell = int(coincheck["bid"])
        # ------------WEBSOCKET---------------------------------------------
        bitflyer_buy = int(bf_buy.recv())
        bitflyer_sell = int(bf_sell.recv())
        liquid_buy = float(liquid_b.get())
        liquid_sell = float(liquid_s.get())
        gmoask = float(gmo_ask.get())
        gmobid = float(gmo_bid.get())
        # ------------------------------------------------------------------
        matome = dict(
            ask=[liquid_buy, bitflyer_buy, bitbank_buy, coincheck_buy, gmoask],
            bid=[liquid_sell, bitflyer_sell, bitbank_sell, coincheck_sell, gmobid],
        )
        matome = pd.DataFrame(
            data=matome, index=["liquid", "bitflyer", "bitbank", "coincheck", "gmo"]
        )
        matome1 = matome.sort_values("ask")
        matome2 = matome.sort_values("bid", ascending=False)
        best_ask = float(matome1.iloc[0, 0])
        best_bid = float(matome2.iloc[0, 1])
        bid_ask = best_bid - best_ask
        print(
            "\r 現在 "
            + matome1.index[0]
            + " の "
            + str(matome1.iloc[0, 0])
            + "円で 1BTC買い "
            + matome2.index[0]
            + " の "
            + str(matome2.iloc[0, 1])
            + "円で 1BTC売ると "
            + str(bid_ask)
            + "円の利益です。",
            end="",
        )

        sleep(0.3)


if __name__ == "__main__":

    # print("main start")

    bf_buy, bf_tube1 = Pipe()
    bf_sell, bf_tube2 = Pipe()
    q_buy = queue.LifoQueue()
    q_sell = queue.LifoQueue()
    gmo_ask = queue.LifoQueue()
    gmo_bid = queue.LifoQueue()

    bf = Process(target=bitflyer_ws, args=(bf_tube1, bf_tube2,))
    lq = threading.Thread(target=liquid_ws, args=(q_buy, q_sell,))
    gm = threading.Thread(target=gmo_ws, args=(gmo_ask, gmo_bid))
    main_roop = threading.Thread(
        target=integration, args=(q_buy, q_sell, bf_buy, bf_sell, gmo_ask, gmo_bid,)
    )

    lq.start()
    bf.start()
    gm.start()
    main_roop.start()
    # print("Liquid,Bitflyer--websocket Bitbank,Coincheck--RestApi ")
