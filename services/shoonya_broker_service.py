import json
import logging
import urllib
from datetime import datetime
from functools import partial
from threading import Thread

import requests
from NorenRestApiPy.NorenApi import NorenApi, reportmsg

logger = logging.getLogger(__name__)


class ShoonyaBrokerService(NorenApi):
    def __init__(self):
        self.subscription_set = set()
        self.host = 'https://api.shoonya.com/NorenWClientTP/'
        self.routes = {'placegttorder': 'PlaceGTTOrder', 'placeocoorder': 'PlaceOCOOrder',
                       'cancelgttorder': 'CancelGTTOrder'}
        self.REFDATA_MAP: dict = dict()

        NorenApi.__init__(self, host=self.host,
                          websocket='wss://api.shoonya.com/NorenWSTP/'
                          # ,eodhost='https://api.shoonya.com/chartApi/getdata/'
                          )
        # super().__service_config['host']; ['routes']['placegttorder'] = 'PlaceGTTOrder'

    # jData={"uid":"FA100908","ai_t":"LMT_BOS_O","validity":"GTT","tsym":"RELIANCE-EQ","exch":"NSE",
    # "oivariable":[{"d":"4500","var_name":"x"},{"d":"1500", "var_name":"y"}],
    # "place_order_params":{"tsym":"RELIANCE-EQ", "exch":"NSE","trantype":"S","prctyp":"MKT","prd":"C",
    # "ret":"DAY","actid":"FA100908","uid":"FA100908", "ordersource":"WEB","qty":"1", "prc":"0"},
    # "place_order_params_leg2":{"tsym":"RELIANCE-EQ", "exch":"NSE", "trantype":"S",
    # "prctyp":"MKT","prd":"C", "ret":"DAY","actid":"FA100908","uid":"FA100908",
    # "ordersource":"WEB","qty":"1", "prc":"0"}}&jKey=40f4195d931f9b6d5a3bf1dfd282ecce1544f0dc6440e4edc69822a46f071c87
    def place_oco_order(self, buy_or_sell, exchange, trading_symbol,
                        quantity, tgt_price, sl_price, product_type='C', alerttype='LMT_BOS_O',
                        price_type='MKT', retention='DAY'):

        # prepare the uri
        url = f"{self.host}{self.routes['placeocoorder']}"
        reportmsg(url)
        # prepare the data
        values = dict()  # {'ordersource': 'API'}
        values["oivariable"] = [{"d": str(tgt_price), "var_name": "x"}, {"d": str(sl_price), "var_name": "y"}]
        values["uid"] = self._NorenApi__username
        values['ai_t'] = alerttype
        values['validity'] = 'GTT'
        values["exch"] = exchange

        tsym = urllib.parse.quote_plus(trading_symbol)
        values["tsym"] = tsym

        order_params = {"tsym": tsym, "exch": exchange, "trantype": buy_or_sell, "prctyp": price_type,
                        "prd": product_type, "ret": retention,
                        "actid": self._NorenApi__accountid, "uid": self._NorenApi__username, "ordersource": "API",
                        "qty": str(quantity), "prc": "0"}
        values["place_order_params"] = order_params
        values["place_order_params_leg2"] = order_params

        payload = 'jData=' + json.dumps(values) + f'&jKey={self._NorenApi__susertoken}'

        reportmsg(payload)

        res = requests.post(url, data=payload)
        reportmsg(res.text)

        res_dict = json.loads(res.text)
        if res_dict['stat'] == 'Not_Ok':
            return None

        return res_dict

    # jData={"uid":"FA100908","al_id":"23120500004188"}&jKey=aba135c32185bc1cbe7964f7667e91bba449180ba360022392ec52c59c65a1e5
    def cancel_gtt_order(self, al_id: int):
        # prepare the uri
        url = f"{self.host}{self.routes['cancelgttorder']}"
        reportmsg(url)
        # prepare the data
        values = {'ordersource': 'API'}
        values["uid"] = self._NorenApi__username
        values["al_id"] = str(al_id)

        payload = 'jData=' + json.dumps(values) + f'&jKey={self.__susertoken}'

        reportmsg(payload)

        res = requests.post(url, data=payload)
        logger.info(res.text)

        resDict = json.loads(res.text)
        if resDict['stat'] != "OI deleted":
            return None

        return resDict

    def place_gtt_order(self, buy_or_sell, product_type,
                        exchange, tradingsymbol, quantity, discloseqty, alerttype, alertprice,
                        price_type, price=0.0, trigger_price=None,
                        retention='DAY', remarks=None):
        # prepare the uri
        url = f"{self.host}{self.routes['placegttorder']}"
        reportmsg(url)
        # prepare the data
        values = {'ordersource': 'API'}
        values["uid"] = self._NorenApi__username
        values["actid"] = self._NorenApi__accountid
        values["trantype"] = buy_or_sell
        values["prd"] = product_type
        values["exch"] = exchange
        values["tsym"] = urllib.parse.quote_plus(tradingsymbol)
        values["qty"] = str(quantity)
        values["dscqty"] = str(discloseqty)
        values["prctyp"] = price_type
        values["prc"] = str(price)
        values["ret"] = retention
        values["remarks"] = remarks
        values['validity'] = 'GTT'
        values['ai_t'] = alerttype  # LTP_A_O LTP_B_O
        values['d'] = str(alertprice)
        values['trgprc'] = str(trigger_price)

        payload = 'jData=' + json.dumps(values) + f'&jKey={self._NorenApi__susertoken}'

        reportmsg(payload)

        res = requests.post(url, data=payload)
        reportmsg(res.text)

        res_dict = json.loads(res.text)
        if res_dict['stat'] != 'Ok':
            return None

        return res_dict

    def get_available_cash(self):
        limits = self.get_limits()
        cash_available = -1
        cash, used = 0, 0

        if limits and limits['stat'] == 'Ok':
            if 'cash' in limits:
                cash = limits['cash']
            if 'marginused' in limits:
                used = limits['marginused']
            logger.info(limits)
            cash_available = round(float(cash) - float(used), 2)
        return cash_available

    def get_pnl(self) -> (float, float, float):
        ret = self.get_positions()
        mtm = 0
        pnl = 0
        if ret:
            for i in ret:
                mtm += float(i['urmtom'])
                pnl += float(i['rpnl'])
        day_m2m = mtm + pnl
        return pnl, mtm, day_m2m

    def event_handler_order_update(self, message):
        logger.info("order event: " + str(message))

    def alert(self, num_of_beeps=2, freq=3000, duration=150):
        """ Function to alert the user """
        for _ in range(num_of_beeps):
            pass

    def event_handler_quote_update(self, in_message):
        # e   Exchange
        # tk  Token
        # lp  LTP
        # pc  Percentage change
        # v   volume
        # o   Open price
        # h   High price
        # l   Low price
        # c   Close price
        # ap  Average trade price

        fields = ['ts', 'lp', 'h', 'l', 'ltp', 'key', 'Type', 'e', 'ts']
        key = in_message['e'] + '|' + in_message['tk']
        message = {field: in_message[field] for field in set(fields) & set(in_message.keys())}
        message['ft'] = None
        if 'ft' in in_message.keys():
            feed_time = int(in_message['ft'])
            message['ft'] = str(datetime.fromtimestamp(feed_time))

        # message['Type'] = message['ts'][12]
        # TODO
        logger.debug(f"quote event: {in_message}")
        # with self.lock:
        if key in self.REFDATA_MAP:
            symbol_info = self.REFDATA_MAP[key]
            symbol_info.update(message)
            self.REFDATA_MAP[key] = symbol_info
        else:
            self.REFDATA_MAP[key] = message

    def open_callback(self, subscribe_text, feed_type='t'):

        logger.info('app is connected to broker websocket')
        if subscribe_text:
            logger.info(f"Subscribing to {subscribe_text}")
            self.subscribe(list(subscribe_text), feed_type)
        else:
            logger.warning("Nothing to subscribe")

    def subscribe_ws(self, subscribe_text, feed_type='t'):
        logger.info(f"Subscribing to {subscribe_text}")
        self.subscribe(subscribe_text, feed_type)

    def unsubscribe_ws(self, unsubscribe_text, feed_type='t'):
        logger.info(f"Unsubscribing instrument: {unsubscribe_text}")
        self.unsubscribe(unsubscribe_text, feed_type)

    # remove unnecessary subscriptions to improve websocket performance
    # resubscribing is required as of now but will be removed in the future
    def manage_subscription(self, subscribe_list):
        if subscribe_list is not None:
            self.subscription_set.update(subscribe_list)
            self.subscribe_ws(list(self.subscription_set))
        else:
            logger.warning("Subscribe is missing. Skipping subscription management")

    def event_handler_socket_closed(self):
        """ Callback function which gets invoked as and when the socket is closed """
        logger.warning('WEB SOCKET CLOSED: Stopped listening for orders on Parent Account', '\U0001F44E\n')
        Thread(target=self.alert, args=(1, 1000, 1100)).start()

    def connect_websocket(self, order_callback=None, subscribe_text=None):
        if not order_callback:
            order_callback = self.event_handler_order_update
        self.start_websocket(order_update_callback=order_callback,
                             subscribe_callback=self.event_handler_quote_update,
                             socket_open_callback=partial(self.open_callback, subscribe_text),
                             socket_close_callback=self.event_handler_socket_closed)
