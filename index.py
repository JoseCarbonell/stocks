import requests
import json
from tradingview_ta import TA_Handler, Interval
from datetime import datetime, timedelta

my_cedears = [
                {'symbol':"AMZN", 'exchange': 'NASDAQ', 'max_qty': 2, 'sector': 'Consumer'}, 
                {'symbol':"NKE", 'exchange': 'NYSE', 'max_qty': 2, 'sector': 'Consumer'}, 
                {'symbol':"WMT", 'exchange': 'NYSE', 'max_qty': 2, 'sector': 'Consumer'}, 
                {'symbol':"EBAY", 'exchange': 'NASDAQ', 'max_qty': 2, 'sector': 'Consumer'}, 
                {'symbol':"KO", 'exchange': 'NYSE', 'max_qty': 2, 'sector': 'Consumer'}, 
                {'symbol':"GOOGL", 'exchange': 'NASDAQ', 'max_qty': 2, 'sector': 'Technology'}, 
                {'symbol':"BABA", 'exchange': 'NYSE', 'max_qty': 2, 'sector': 'Consumer'}, 
                {'symbol':"CAT", 'exchange': 'NYSE', 'max_qty': 2, 'sector': 'Technology'},
                {'symbol':"TSLA", 'exchange': 'NASDAQ', 'max_qty': 2, 'sector': 'Consumer'}, 
                {'symbol':"MSFT", 'exchange': 'NASDAQ', 'max_qty': 2, 'sector': 'Technology'}, 
                {'symbol':"AAPL", 'exchange': 'NASDAQ', 'max_qty': 2, 'sector': 'Technology'}, 
                {'symbol':"JPM", 'exchange': 'NYSE', 'max_qty': 2, 'sector': 'Finance'},
                {'symbol':"META", 'exchange': 'NASDAQ', 'max_qty': 2, 'sector': 'Technology'}, 
                {'symbol':"NVDA", 'exchange': 'NASDAQ', 'max_qty': 2, 'sector': 'Technology'}, 
                {'symbol':"GLOB", 'exchange': 'NYSE', 'max_qty': 2, 'sector': 'Technology'}, 
                {'symbol':"CSCO", 'exchange': 'NASDAQ', 'max_qty': 2, 'sector': 'Telecommunications'}, 
                {'symbol':"XOM", 'exchange': 'NYSE', 'max_qty': 2, 'sector': 'Energy'},
                {'symbol':"PBR", 'exchange': 'NYSE', 'max_qty': 2, 'sector': 'Consumer'},
                {'symbol':"PG", 'exchange': 'NYSE', 'max_qty': 2, 'sector': 'Consumer'},
                {'symbol':"T", 'exchange': 'NYSE', 'max_qty': 2, 'sector': 'Technology'},
                {'symbol':"VIST", 'exchange': 'NYSE', 'max_qty': 2, 'sector': 'Energy'},
                {'symbol':"MELI", 'exchange': 'NYSE', 'max_qty': 2, 'sector': 'Technology'}, 
            ]
my_acciones = ["CEPU","CRES","PAMP","TGSU2"]



def get_access_token():
    api_call = requests.post('https://api.invertironline.com/token', data= {
        'username': 'micaela.garibotti@gmail.com',
        'password': 'Invertir.456',
        'grant_type': 'password'
    }).text

    json_data = json.loads(api_call)
    return json_data['access_token']

def get_portfolio(access_token: str):
    api_call = requests.get('https://api.invertironline.com/api/v2/portafolio/AR', headers={
        'Authorization': 'Bearer ' + access_token
    }).text

    json_data = json.loads(api_call)

    return json_data['activos']
def get_market_cedears_quotations(access_token: str):
    api_call = requests.get('https://api.invertironline.com/api/v2/Cotizaciones/cedears/argentina/Todos', headers={
        'Authorization': 'Bearer ' + access_token
    }).text

    json_data = json.loads(api_call)

    return json_data['titulos']

def get_market_acciones_quotations(access_token: str):
    api_call = requests.get('https://api.invertironline.com/api/v2/Cotizaciones/acciones/argentina/Todos', headers={
        'Authorization': 'Bearer ' + access_token
    }).text

    json_data = json.loads(api_call)

    return json_data['titulos']

def get_current_pending_operations(access_token:str):
    api_call = requests.get('https://api.invertironline.com/api/v2/operaciones?estado=Pendientes', headers={
        'Authorization': 'Bearer ' + access_token
    }).text

    json_data = json.loads(api_call)

    return json_data

def delete_operation(access_token: str, operation: int):
    api_call = requests.delete(f'https://api.invertironline.com/api/v2/operaciones/{operation}', headers={
        'Authorization': 'Bearer ' + access_token
    }).text

    json_data = json.loads(api_call)

    return json_data['ok']

def buy_stock(access_token: str, symbol: str, qty: int, price: int, portfolio: list):
    print(f'Buying stock {symbol}')

    pending_operations = get_current_pending_operations(access_token)


    for pend_op in pending_operations:
        if pend_op['simbolo'] == symbol and pend_op['tipo'] == "Compra":
            print("Deleting operation " + str(pend_op['numero']) + "for symbol " + symbol)
            delete_operation(access_token, pend_op['numero'])

    date = (datetime.today()).isoformat()

    symbol_qty_from_portfolio = 0

    for prt in portfolio:
        if prt['titulo']['simbolo'] == symbol:
            symbol_qty_from_portfolio = prt['cantidad']

    print(f"Qty from portfolio (BUY) {symbol_qty_from_portfolio}")


    # ALLOW TO BUY 3 STOCKS
    try:
        if symbol_qty_from_portfolio < qty:

            data= {
                'mercado': 'bCBA',
                'simbolo': symbol,
                'cantidad': qty - symbol_qty_from_portfolio,
                'precio': price,
                'plazo': 't2',
                'validez': date ,
                'tipoOrden': 'precioLimite'
            }

            api_call = requests.post('https://api.invertironline.com/api/v2/operar/Comprar', 
            headers={
                'Authorization': 'Bearer ' + access_token
            },
            data= data).text
            json_data = json.loads(api_call)
            print(json_data)
        else:
            print("BOUGHT")
    except Exception as e:
        print(f"Error buying stock: {e}")

def sell_stock(access_token: str, symbol: str, price: int, portfolio: list):
    pending_operations = get_current_pending_operations(access_token)

    for pend_op in pending_operations:
        if pend_op['simbolo'] == symbol:
            print("Deleting operation " + str(pend_op['numero']) + "for symbol " + symbol)
            delete_operation(access_token, pend_op['numero'])

    date = (datetime.today()).isoformat()

    symbol_qty_from_portfolio = 0
    bought_price = 0

    for prt in portfolio:
        if prt['titulo']['simbolo'] == symbol:
            symbol_qty_from_portfolio = prt['cantidad']
            bought_price = prt['ppc']

    print(f"Selling {symbol_qty_from_portfolio}x of {symbol}, was bought by {bought_price}, total earn is {(price - bought_price)} ARS")

    data= {
        'mercado': 'bCBA',
        'simbolo': symbol,
        'cantidad': symbol_qty_from_portfolio,
        'precio': price,
        'plazo': 't2',
        'validez': date ,
        'tipoOrden': 'precioLimite'
    }

    try:
        api_call = requests.post('https://api.invertironline.com/api/v2/operar/Vender', 
        headers={
            'Authorization': 'Bearer ' + access_token
        },
        data= data).text

        json_data = json.loads(api_call)
        print(json_data)
    except Exception as e:
        print(f"Error selling stock: {e}")

def lambda_handler(event, context):
    access_token = get_access_token()
    cedears_from_iol = get_market_cedears_quotations(access_token)
    portfolio = get_portfolio(access_token)

    for s in my_cedears:
        for ced in cedears_from_iol:
            if ced['simbolo'] == s['symbol']:
                output = TA_Handler(
                        symbol=s['symbol'],
                        screener="america",
                        exchange=s['exchange'],
                        interval=Interval.INTERVAL_15_MINUTES)
                
                print(f"Company {ced['simbolo']}, Recomendation {output.get_analysis().summary['RECOMMENDATION']}")

                buy_price = ced['puntas']['precioCompra']
                sell_price = ced['puntas']['precioVenta']

                recommendation = output.get_analysis().summary['RECOMMENDATION']

                # if recommendation == "BUY" or recommendation == "STRONG_BUY":
                if recommendation == "STRONG_BUY":
                    buy_stock(access_token, ced['simbolo'], (s['max_qty']), buy_price, portfolio)


                # SELL STOCK IF MARKET VALUE IS UPPER THAN 1% OF PURCHASED PRICE
                for p in portfolio:
                    stock_selled = False
                    if p['titulo']['simbolo'] == s['symbol']:
                        buy_price_with_taxes = round((p['ppc'] * 1.007018) * 1.01, 0)
                        sell_price_with_taxes = sell_price / 1.000968
                        print(f"Ganancia {p['gananciaPorcentaje']}")
                        print(f"Buy price with taxes plus 1% = {buy_price_with_taxes})")
                        print(f"Sell price with taxes = {round(sell_price_with_taxes, 0)}")

                        if sell_price_with_taxes > buy_price_with_taxes:
                            print(f"{ced['descripcion']} selling in {sell_price_with_taxes}")
                            sell_stock(access_token, ced['simbolo'], round(sell_price_with_taxes, 0), portfolio)
                            stock_selled = True

                        # STOP LOSS
                        if p['gananciaPorcentaje'] < -4:
                            print(f"STOP LOSS {s['symbol']}")
                            sell_stock(access_token, ced['simbolo'], round(sell_price_with_taxes, 0), portfolio)
                        # print(f"Stock selled by percentage logic {stock_selled}")

                        # if stock_selled == False:
                        #     if recommendation == "SELL" or recommendation == "STRONG_SELL":
                        #         sell_stock(access_token, ced['simbolo'], sell_price, portfolio)
                        if stock_selled == False:
                            if recommendation == "STRONG_SELL":
                                sell_stock(access_token, ced['simbolo'], sell_price, portfolio)
    
    return { 
        'message' : 'OK'
    }
