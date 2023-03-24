import urllib.request, urllib.parse, urllib.error
import xml.etree.ElementTree as ET
from datetime import date
import ssl
import pandas as pd
import config
from sqlalchemy import create_engine

precios = pd.DataFrame()
todays_date = date.today()

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

url=config.urlCRE
uh = urllib.request.urlopen(url, context=ctx)

data = uh.read()
tree = ET.fromstring(data)

places = tree.findall('place')

for place in places:
    place_id = place.get('place_id')
    prices = place.findall('gas_price')
    for price in prices:
        product = price.get('type')
        elPrecio = float(price.text)
        elDict ={ 'place_id' : place_id, 'prices': elPrecio, 'product': product, 'date' : todays_date}
        temp = pd.DataFrame([elDict])
        precios = pd.concat([precios,temp])

# establish connections
conn_string = config.urlDB
  
db = create_engine(conn_string)
conn = db.connect()

precios.to_sql('precios_site', conn, if_exists='replace', index=False)
conn.close()
