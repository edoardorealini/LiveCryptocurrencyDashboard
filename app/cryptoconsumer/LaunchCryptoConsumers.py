from CryptoConsumerThread import CryptoConsumerThread

if __name__ == "__main__":

    cryptos = ["bitcoin", "ethereum", "tether", "xrp", "litecoin", "cardano", "iota", "eos", "stellar"]
    #cryptos = ["bitcoin"]

    thread_pool = []

    for crypto in cryptos:
        consumer_thread = CryptoConsumerThread(crypto)

        print("Consumer Thread Launcher: launching consumer thread for crypto: " + crypto)
        consumer_thread.setName(crypto + "_thread")
        consumer_thread.start()
        thread_pool.append(consumer_thread)

    print("Consumer Thread Launcher: Waiting for all threads termination")
    for thread in thread_pool:
        thread.join()
