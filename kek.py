import pandas as pd

data = {'user_id': [1, 2], 'event_id': ['a', 'f'], 'timestamp': ["13:12", "15:56"], 'product_code': ['intelij', 'pycharm']}
df = pd.DataFrame(data)
# df.to_csv("/home/omar/Desktop/spark/test_data.csv", index=False)

# user_id – a user’s anonymized identifier;
# event_id – identifier of an event that happened inside an IDE.
# Each event corresponds to action either of a user or an IDE itself.
# For the sake of simplicity we assume that an event is a user action if event_id in (‘a’, ‘b’, ‘c’);
# timestamp;
# product_code – shortened name of an IDE
