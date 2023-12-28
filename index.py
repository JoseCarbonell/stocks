import requests
import json
from tradingview_ta import TA_Handler, Interval
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import boto3

aws_region = 'us-east-1'
table_name = 'trading'
dynamodb = boto3.resource('dynamodb', region_name=aws_region)
table = dynamodb.Table(table_name)

my_cedears = [
                {'symbol':"AMZN", 'exchange': 'NASDAQ', 'max_qty': 2},
                {'symbol':"NKE", 'exchange': 'NYSE', 'max_qty': 2},
                {'symbol':"WMT", 'exchange': 'NYSE', 'max_qty': 2},
                {'symbol':"EBAY", 'exchange': 'NASDAQ', 'max_qty': 2},
                {'symbol':"KO", 'exchange': 'NYSE', 'max_qty': 2},
                {'symbol':"BABA", 'exchange': 'NYSE', 'max_qty': 2},
                {'symbol':"CAT", 'exchange': 'NYSE', 'max_qty': 2},
                {'symbol':"TSLA", 'exchange': 'NASDAQ', 'max_qty': 2},
                {'symbol':"MSFT", 'exchange': 'NASDAQ', 'max_qty': 2},
                {'symbol':"AAPL", 'exchange': 'NASDAQ', 'max_qty': 2},
                {'symbol':"JPM", 'exchange': 'NYSE', 'max_qty': 2},
                {'symbol':"META", 'exchange': 'NASDAQ', 'max_qty': 2},
            ]
my_acciones = ["CEPU","CRES","PAMP","TGSU2"]



def get_access_token():
    api_call = requests.post('https://api.invertironline.com/token', data= {
        'username': '',
        'password': '',
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

def buy_stock(access_token: str, symbol: str, qty: int, price: int, portfolio: list, max_qty: int):
    print('Buying stock' + symbol)

    pending_operations = get_current_pending_operations(access_token)


    for pend_op in pending_operations:
        if pend_op['simbolo'] == symbol:
            print("Deleting operation " + str(pend_op['numero']) + "for symbol " + symbol)
            delete_operation(access_token, pend_op['numero'])

    date = (datetime.today() + timedelta(days=1)).isoformat()

    data= {
        'mercado': 'bCBA',
        'simbolo': symbol,
        'cantidad': qty,
        'precio': price,
        'plazo': 't2',
        'validez': date ,
        'tipoOrden': 'precioLimite'
    }

    symbol_qty_from_portfolio = 0

    for prt in portfolio:
        if prt['titulo']['simbolo'] == symbol:
            symbol_qty_from_portfolio = prt['cantidad']

    print(f"Qty from portfolio (BUY) {symbol_qty_from_portfolio}")

    try:
        if symbol_qty_from_portfolio < max_qty:
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

    date = (datetime.today() + timedelta(days=1)).isoformat()

    symbol_qty_from_portfolio = 0

    for prt in portfolio:
        if prt['titulo']['simbolo'] == symbol:
            symbol_qty_from_portfolio = prt['cantidad']

    print(f"Qty from portfolio (SELL) {symbol_qty_from_portfolio}")

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

def save_symbol_in_db(symbol: str, qty: int):
    symbol_qty_in_db = get_symbol_qty_in_db(symbol)
    try:
        response = table.put_item(Item={
            'stock': symbol,
            'qty': symbol_qty_in_db + qty
        })
        print("Put Item Successful:", response)
    except Exception as e:
        print(f"Error putting item: {e}")
def update_stock_qty_from_db(symbol: str, qty: int):
    try:
        response = table.put_item(Item={
            'stock': symbol,
            'qty': qty
        })
        print("Removing Stock Qty Successful:", response)
    except Exception as e:
        print(f"Error putting item: {e}")

def get_symbol_qty_in_db(symbol: str):
    try:
        response = table.get_item(
            Key={
                'stock': symbol
            }
        )
        item = response.get('Item')
        if item:
            return item['qty']
        else:
            return 0
    except Exception as e:
        print(f"Error getting item: {e}")

def delete_symbol_in_db(symbol: str):
    try:
        response = table.delete_item(
            Key={
                'stock': symbol
            }
        )
        print("Delete Item Successful:", response)
    except Exception as e:
        print(f"Error deleting item: {e}")

def send_email(sender_email, recipient_email, subject, body):
    # Set up the SES client
    ses_client = boto3.client('ses', region_name='us-east-1')  # Replace 'your-region' with your AWS region

    # Create the email message
    email_message = {
        'Subject': {'Data': subject},
        'Body': {
            'Html': 
                {
                    'Charset': "UTF-8",
                    'Data': body
                }
        },
    }

    try:
        # Send the email
        response = ses_client.send_email(
            Source=sender_email,
            Destination={'ToAddresses': recipient_email},
            Message=email_message
        )

        print(f"Email sent! Message ID: {response['MessageId']}")
    except ClientError as e:
        print(f"Error sending email: {e.response['Error']['Message']}")

def lambda_handler(event, context):
    access_token = get_access_token()
    cedears_from_iol = get_market_cedears_quotations(access_token)
    stocks_html = ""
    portfolio = get_portfolio(access_token)

    for s in my_cedears:
        for ced in cedears_from_iol:
            if ced['simbolo'] == s['symbol']:
                output = TA_Handler(
                        symbol=s['symbol'],
                        screener="america",
                        exchange=s['exchange'],
                        interval=Interval.INTERVAL_15_MINUTES)
                buy_price = ced['puntas']['precioCompra']
                sell_price = ced['puntas']['precioVenta']
                
                print(f"Company {ced['simbolo']}, Recomendation {output.get_analysis().summary['RECOMMENDATION']}")

                if output.get_analysis().summary['RECOMMENDATION'] == "BUY" or output.get_analysis().summary['RECOMMENDATION'] == "STRONG_BUY":
                    buy_stock(access_token, ced['simbolo'], 1, buy_price, portfolio, s['max_qty'])
                elif output.get_analysis().summary['RECOMMENDATION'] == "SELL" or output.get_analysis().summary['RECOMMENDATION'] == "STRONG_SELL":
                    sell_stock(access_token, ced['simbolo'], sell_price, portfolio)

    email_body = f"""
    <html>
        <body>
            <h2>Recommendations</h2>
            <table border="1">
                <tr>
                    <th>Stock</th>
                    <th>Symbol</th>
                    <th>Recommendation</th>
                </tr>

                {stocks_html}
            </table>
        </body>
    </html>
    """
    # send_email('carbonellperez7@gmail.com', ['micaela.garibotti@gmail.com','carbonellperez7@gmail.com'], 'Stocks', email_body)
    
    return { 
        'message' : 'OK'
    }