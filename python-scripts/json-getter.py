from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import time


def retrieve_history(currency, log_file):
    timestamp1gen2017 = 1483228800000
    timestamp_curr = int(time.time()) * 1000    # to get milliseconds

    milliseconds_per_day = 86400000

    tmstp = timestamp1gen2017

    while tmstp < timestamp_curr:
        start_day = str(tmstp)
        end_day = str(tmstp + milliseconds_per_day)

        url = "https://api.coincap.io/v2/assets/" + currency + "/history?interval=m1"

        start = "&start=" + start_day

        end = "&end=" + end_day

        final_url = url + start + end

        try:
            req = Request(final_url, headers={'User-Agent': 'Mozilla/5.0'})
            response = urlopen(req, timeout=700).read()
            save_folder = "../data/history/" + currency + "/"
            json_file = save_folder + currency + "-" + start_day + ".json"      # example: bitcoin-1483228800000.json

            output = open(json_file, "wb")
            output.write(response)
            output.close()

        except (HTTPError, URLError) as e:
            log_file.write("Request for currency " + currency + " for day " + start_day + " failed!\tReason: " + e.reason + "\n")

        tmstp += milliseconds_per_day

    log_file.close()


# ---------------------------MAIN---------------------------

assets = ["bitcoin", "ethereum", "ripple", "bitcoin-cash", "eos", "stellar", "litecoin", "cardano", "tether", "iota"]

for currency in assets:
    name_log_file = currency + "_log_file.txt"
    log_file = open(name_log_file, "w")
    retrieve_history(currency, log_file)
