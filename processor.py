from geotiff_data import RasterData
import json
import requests
from ingestion_handler import V2Handler
from sys import argv, stderr, exit

if len(argv) < 3:
    print("""
        Invalid command line args. Must contain an id.

        usage:
        driver.py <fid> <token>
    """, file = stderr)
    exit(1)

fid = argv[1]
config_name = f"configs/{fid}.json"
temp_tif_name = f"tiffs/{fid}.tif"
token = argv[2]

hcdp_api_url = "https://cistore.its.hawaii.edu"

raster_url = f"{hcdp_api_url}/raster"
headers = {
    "authorization": f"Bearer {token}"
}

tapis_config = {
    "tenant_url": "https://agaveauth.its.hawaii.edu/meta/v2/data",
    "token": token,
    "retry": 3,
    "db_write_api_url": hcdp_api_url
}

config = None
with open(config_name) as f:
    config = json.load(f)


data = {}

year = config["year"]
dates = config["dates"]
datatype = config["datatype"]
period = config["period"]
extent = config["extent"]
part_label = "production" if datatype == "rainfall" else "aggregation"
part = config[part_label]

for date in dates:
    params = {
        "datatype": datatype,
        "date": date,
        "period": period,
        "date": date,
        "extent": extent
    }
    params[part_label] = part
    res = requests.get(raster_url, headers = headers, params = params, verify = False, stream = True)
    content = res.iter_content()
    with open(temp_tif_name, "wb") as f:
        for chunk in content:
            f.write(chunk)
    raster = RasterData(temp_tif_name)
    for index in raster.data:
        index_data = data.get(index)
        if index_data is None:
            index_data = {}
            data[index] = index_data
        index_data[date] = raster.data[index]
    break
handler = V2Handler(tapis_config)

key_fields = ["datatype", part_label, "period", "year"]
for index in data:
    name = "hcdp_timeseries_data"

    doc = {
        "name": name,
        "value": {
            "index": index,
            "datatype": datatype,
            "period": period,
            "year": year,
            "data": data[index]
        }
    }

    doc["value"][part_label] = part

    handler.create_check_duplicates(doc, key_fields, replace = True)
    break