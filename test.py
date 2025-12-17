import yfinance as yf

dat = yf.Ticker("MSFT")

# The list of words you want to look for
target_words = ["regular", "previous", "post", "pre"]

for key, value in dat.info.items():
    # We convert key to lowercase to ensure we catch 'Pre' or 'PRE' as well
    if any(word in key.lower() for word in target_words):
        print(f"{key}: {value}")