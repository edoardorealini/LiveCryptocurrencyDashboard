from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import time
from tqdm import tqdm


def retrieve_history(currency, log_file):
    global all_days

    for day in tqdm(range(len(all_days.keys()))):

        time.sleep(1)   # provare a diminuire a 0.5 domani

        start_day = str(all_days[day])
        if (day != 1391):       # skip today
            end_day = str(all_days[day + 1])

        url = "https://api.coincap.io/v2/assets/" + currency + "/history?interval=m1"

        start = "&start=" + start_day

        end = "&end=" + end_day

        final_url = url + start + end

        try:
            req = Request(final_url, headers={'User-Agent': 'Mozilla/5.0'})
            response = urlopen(req, timeout=500).read()
            save_folder = "../data/history/" + currency + "/"
            json_file = save_folder + currency + "-" + start_day + ".json"      # example: bitcoin-1483228800000.json

            output = open(json_file, "wb")
            output.write(response)
            output.close()

        except (HTTPError, URLError) as e:
            log_file.write("Request for currency " + currency + " for day " + start_day + " failed!\tReason: " + e.reason + "\n")
            print("ERROR at " + start_day + " for the following reason: " + e.reason)

    log_file.close()


# ---------------------------MAIN---------------------------

timestamp1gen2017 = 1483228800000
timestamp_curr = int(time.time()) * 1000    # to get milliseconds

milliseconds_per_day = 86400000

tmstp = timestamp1gen2017

day = 0

all_days = {}

while tmstp < timestamp_curr:
    all_days[day] = tmstp
    day += 1
    tmstp += milliseconds_per_day

# "stellar", "litecoin", "cardano", "tether", "iota"

assets = ["tether"]
# MERDA fixa XRP e RIPPLE!!
for currency in assets:
    name_log_file = currency + "_log_file.txt"
    log_file = open(name_log_file, "w")
    retrieve_history(currency, log_file)
