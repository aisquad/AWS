import os
from argparse import ArgumentParser
from datetime import datetime

from babel.dates import parse_date
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse as parse_date
from time import sleep
from typing import Any, Dict, List, NoReturn, Optional, Union, Tuple

from sp_api.api import Orders, Reports
from sp_api.base import SellingApiException, Marketplaces, ReportStatus, ApiResponse
from sp_api.base.reportTypes import ReportType


"""
https://sp-api-docs.saleweaver.com/
"""


class Item:
    def __init__(self):
        self.number = 0
        self.tax_amount: Optional[float] = None
        self.qty_shipped = 0
        self.price: Optional[float] = None
        self.asin = ''
        self.sku = ''
        self.title = ''
        self.shipping_tax: Optional[float] = None
        self.shipping_price: Optional[float] = None
        self.order_item_id = ''

    def __repr__(self):
        return f'<Item {self.asin} {self.title[:15]}... {self.price} - {self.shipping_price}>'

    def set(self, item_dict: Dict[str, Union[Dict[str, Any]]]):
        self.number = int(item_dict['ProductInfo']['NumberOfItems'])
        if 'ItemTax' in item_dict:
            self.price = float(item_dict['ItemPrice']['Amount'])
            self.tax_amount = float(item_dict['ItemTax']['Amount'])
        self.qty_shipped = item_dict['QuantityShipped']
        self.asin = item_dict['ASIN']
        self.sku = item_dict['SellerSKU']
        self.title = item_dict['Title']
        if 'ShippingTax' in item_dict:
            self.shipping_tax = float(item_dict['ShippingTax']['Amount'])
            self.shipping_price = float(item_dict['ShippingPrice']['Amount'])
        self.order_item_id = item_dict['OrderItemId']


class Order:
    def __init__(self):
        self.id: Optional[str] = None
        self.status: Optional[str] = None
        self.shipped_items_number = 0
        self.unshipped_items_number = 0
        self.purchase_date: Optional[datetime] = None
        self.total_amount: Optional[float] = None
        self.items: List[Item] = []

    def __repr__(self):
        date = f'{self.purchase_date:%Y-%m-%d}' if self.purchase_date else '<NODATE>'
        amount = f'{self.total_amount:.02f}' if self.total_amount else '<NOAMOUNT>'
        shipping_amount = sum(_.shipping_price for _ in self.items if _.shipping_price)
        return f'<Order {date} {self.id} {self.status} {amount} {len(self.items)} items, ' \
               f'Shipping amt: {shipping_amount:.02f}>'

    def set(self, response_dict: Dict[str, Union[str, Dict[str, Any]]]):
        self.id = response_dict['AmazonOrderId']
        self.status = response_dict['OrderStatus']
        self.shipped_items_number = response_dict['NumberOfItemsShipped']
        self.unshipped_items_number = response_dict['NumberOfItemsUnshipped']
        self.purchase_date = datetime.strptime(response_dict['PurchaseDate'], '%Y-%m-%dT%H:%M:%SZ')
        if 'OrderTotal' in response_dict:
            self.total_amount = float(response_dict['OrderTotal']['Amount'])

    def add_item(self, item: Union[Item, Dict[str, Union[str, Dict[str, Any]]]]):
        if isinstance(item, Item):
            self.items.append(item)
        else:
            it = Item()
            it.set(item)
            self.items.append(it)


class AmazonConnexion:
    def __init__(self):
        r"""
        C:\Users\<USER>\AppData\Roaming\python-sp-api\credentials.yml
        C:\Users\<USER>\PycharmProjects\Accurate\Amazon\sp_api_amazon\credentials.yml
        """
        self.waiting = 0
        self.queries = 0
        self.default_store = 'ES'
        self.access_key = ''
        self.secret_key = ''
        self.role_arn = ''
        self.client_id = ''
        self.client_secret = ''
        self.refresh_token = ''
        self.stores = tuple()
        self._store_map = {}
        self._keys = (
            'sp_api_access_key', 'sp_api_secret_key', 'sp_api_arn_role', 'sp_api_refresh_token', 'lwa_app_id',
            'lwa_client_cecret', 'sp_api_refresh_token'
        )
        self.env_vars = {self.simplify_attr(_): _.upper() for _ in self._keys}
        self.order_statuses: Optional[Tuple[str, str, ...]] = None

        # params
        self.start_date: Optional[datetime] = None
        self.end_date: Optional[datetime] = None
        self.days_ago: int = 0
        self.year: int = datetime.utcnow().year
        self.month: int = (datetime.utcnow() - relativedelta(months=5)).month

    def __repr__(self):
        return f'<AZN CXN:\n\t{self.access_key}\n\t{self.secret_key}\n\t{self.role_arn}\n\t{self.client_id}\n\t' \
               f'{self.client_secret}\n\t{self.refresh_token}>'

    def dispatch(self):
        """
        Configuramos filtros de fechas y estados de pedidos.
        :return:
        """
        self.start_date: Optional[datetime] = None
        self.end_date: Optional[datetime] = None
        self.days_ago: int = 0
        date_fmt = '%Y-%m-%d'

        if args.days_ago:
            self.days_ago = args.days_ago
            created_after = datetime.utcnow() - relativedelta(days=self.days_ago)
            self.start_date = created_after.isoformat()

        elif args.year:
            self.year = args.year
            self.month = args.month or 1
            created_after = datetime.today() + relativedelta(year=self.year, month=self.month, day=1)
            self.start_date = created_after.isoformat()

        elif args.start_date and args.end_date:
            created_after = datetime.strptime(args.start_date, date_fmt)
            self.start_date = created_after.isoformat()
            created_before = datetime.strptime(args.end_date, date_fmt)
            self.end_date = created_before.isoformat()

        if not self.start_date:
            date = datetime.utcnow() - relativedelta(days=7)
            self.start_date = date.isoformat()

        if args.order_statuses:
            self.order_statuses = tuple(args.order_statuses)
        else:
            self.order_statuses = ('Shipped', 'Unshipped')

    def simplify_attr(self, param):
        return param.replace('sp_api_', '').replace('lwa_app', 'client')

    def get_param(self, param):
        return os.getenv(self.env_vars[param])

    def set_param(self, param):
        attr = self.simplify_attr(param)
        value = self.get_param(attr)
        if hasattr(self, attr):
            setattr(self, attr, value)

    def load_stores(self):
        with open('sp_api_amazon/endpoints.txt', 'r') as fp:
            endpoints = fp.read()
        for line in endpoints.splitlines():
            if line.startswith('#'):
                continue
            iso, url, code = line.split('\t')
            self._store_map[iso] = code

    def load_keys(self):
        for key in self._keys:
            self.set_param(key)
        self.load_stores()
        stores = os.getenv('SP_API_STORES')
        self.stores = tuple(self._store_map[_] if _ != 'UK' else self._store_map['GB'] for _ in stores.split(','))

    def prevent_throttling(self):
        self.queries += 1
        wait = 0
        if self.queries % 15 == 0:
            wait = self.queries % 25
        if wait:
            print(f" ==== WAITING FOR: {wait} seconds. TOTAL: {self.waiting} [QUERIES: {self.queries}] ====")
            sleep(wait)
            self.waiting += wait

    def run(self) -> NoReturn:
        """
        NOTA: Solo se obtiene resultados de los primeros 100 pedidos desde la fecha de inicio que se le pase.
        """
        # aws_access_key, aws_secret_key, lwa_app_id, lwa_client_secret
        self.dispatch()
        start = datetime.now()
        orders = Orders(Marketplaces.ES)
        # https://developer-docs.amazon.com/sp-api/docs/orders-api-v0-reference#getorders
        if self.end_date:
            order_resp: ApiResponse = orders.get_orders(
                CreatedAfter=self.start_date,
                CreatedBefore=self.end_date,
                OrderStatuses=self.order_statuses
            )
        else:
            order_resp: ApiResponse = orders.get_orders(
                CreatedAfter=self.start_date, OrderStatuses=self.order_statuses)
        order_list = []
        for order_dict in order_resp.Orders:
            order = Order()
            order.set(order_dict)
            self.prevent_throttling()
            order_list.append(order)
            if 'AmazonOrderId' in order_dict:
                item_resp = orders.get_order_items(order.id)
                for item_dict in item_resp.OrderItems:
                    self.prevent_throttling()
                    order.add_item(item_dict)
            print(order)
        print(f" ==== TOTAL WAITING: {self.waiting} ## QUERIES: {self.queries} ====")
        print(f"{start:%H:%M:%S} <--> {datetime.now():%H:%M:%S}")
        order_list.sort(key=lambda o: o.purchase_date, reverse=True)
        if len(order_list):
            print(f'Se recolectaron {len(order_list)} pedidos')
            print(order_list)
        else:
            print('No hay pedidos para el periodo elegido.')
        diff = relativedelta(datetime.now(), start)
        items = {
            'Y': diff.years, 'M': diff.months, 'D': diff.days, 'h': diff.hours, 'm': diff.minutes, 's': diff.seconds
        }
        diffs = [f'{v}{k.lower()}' for k, v in items.items() if v > 0]
        dates = ''
        if len(diffs) > 1:
            diff_str = ', '.join(_ for _ in diffs[:-1])
            diff_str += f' & {diffs[-1]}'
            dates = f'FIRST DATE: {order_list[-1].purchase_date:%Y-%m-%d %H:%M}\n' \
                    f'LAST DATE: {order_list[0].purchase_date:%Y-%m-%d %H:%M}\n'
        else:
            diff_str = diffs[0]
        start_date = parse_date(self.start_date)
        print(f'START DATE: {start_date:%Y-%m-%d %H:%M}\n{dates}ELAPSED TIME: {diff_str}')


if __name__ == '__main__':
    arg_parser = ArgumentParser()
    arg_parser.add_argument('-a', '-d', '--daysago', type=int, dest='days_ago')
    arg_parser.add_argument('-e', '--end', dest='end_date')
    arg_parser.add_argument('-m', '--month', type=int, dest='month')
    arg_parser.add_argument('-s', '--start', dest='start_date')
    arg_parser.add_argument('-S', '--orderstatuses', dest='order_statuses', nargs='*')
    arg_parser.add_argument('-y', '--year', type=int, dest='year')
    args = arg_parser.parse_args()

    # args.days_ago = 365*3
    azn_cxn = AmazonConnexion()
    azn_cxn.load_keys()
    azn_cxn.run()
