import pandas as pd

# file = open("bitcoin-1483228800000.json",)
# json = json.load(file)

df = pd.read_json("bitcoin-1483228800000.json")["data"].to_list()

timestamp = []     # in milliseconds
date = []          # as yyyy-mm-dd
hour = []          # as hh-mm-ss
price = []         # cast to float

for minute in df:
    timestamp.append(minute["time"])
    price.append(minute["priceUsd"])
    replace_date = minute["date"].replace(".000Z", "")
    split_date = replace_date.split("T")
    date.append(split_date[0])
    hour.append(split_date[1])


print(timestamp)
print(date)
print(hour)
print(price)

df = pd.DataFrame(data={"timestamp": timestamp, "price": price, "date": date, "hour": hour})
df.to_csv("prova.csv", sep=',', index=False)
