from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import pandas as pd
import time

timestamp1gen2017 = 1483228800000
timestamp_curr  = int(time.time())

print(timestamp_curr)

url = "https://api.coincap.io/v2/assets/bitcoin/history?interval=m1"

start = "&start="

end = "&end"

try:
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    response = urlopen(req, timeout=20).read()
    save_folder = parent_folder / "data/history/"
    json_save = save_folder / img_name

    output = open(json_save, "wb")
    output.write(response)
    output.close()

except (HTTPError, URLError) as e:
    file.write("Player: " + player_name + " not found. \tReason: " + e.reason + "\n")
    continue

file.close()
