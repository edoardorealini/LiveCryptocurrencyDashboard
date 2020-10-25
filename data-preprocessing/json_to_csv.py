import pandas as pd
from os import listdir
from os.path import isfile, join


assets = ["ethereum", "tether", "xrp", "litecoin", "cardano", "iota", "eos", "stellar", "bitcoin-cash"]

for currency in assets:

    folder_path = "../data/history/" + currency + "/json/"

    json_files = [f for f in listdir(folder_path) if isfile(join(folder_path, f))]

    # print(json_files)

    timestamp = []     # in milliseconds
    date = []          # as yyyy-mm-dd
    hour = []          # as hh-mm-ss
    price = []         # cast to float

    cont = 0

    for json in json_files:
        print("For currency " + currency + ", loading is " + str(cont) + "/" + str(len(json_files)))

        df = pd.read_json(folder_path + json)["data"].to_list()

        for minute in df:
            timestamp.append(minute["time"])
            price.append(minute["priceUsd"])
            replace_date = minute["date"].replace(".000Z", "")
            split_date = replace_date.split("T")
            date.append(split_date[0])
            hour.append(split_date[1])

        cont += 1

    df = pd.DataFrame(data={"timestamp": timestamp, "price": price, "date": date, "hour": hour})
    df.to_csv("../data/history/" + currency + "/" + currency + ".csv", sep=',', index=False)
    only_day_df = df[df["hour"] == "00:00:00"]
    only_day_df.to_csv("../data/history/" + currency + "/only_days_" + currency + ".csv", sep=',', index=False)

    print("\n\n------CURRENCY: " + currency + " csv computed!--------\n\n")
