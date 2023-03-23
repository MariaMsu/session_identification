# Spark function to identify user sessions

The function takes as input a dataframe with the following columns:  
`user_id` – a user’s anonymized identifier;  
`event_id` – identifier of an event that happened inside an IDE.  
Each event corresponds to action either of a user or an IDE itself. 
For the sake of simplicity we assume that an event is a user action if event_id in (‘a’, ‘b’, ‘c’);
`timestamp` in "yyyy-MM-dd HH:mm:ss" format;  
`product_code` – shortened name of an IDE;

The function identify user session based on events timestamps, user_ids and product_ids
and assign the same user_session_ids to the table's rows describing the same user session.
The user_session_ids have format user_id#product_code#timestamp.

## How to create environment and run
```shell
apt-get install python3.9-dev python3.9-venv
python3.9 -m venv venv
source venv/bin/activate
pip3 install wheel
pip3 install -r requirements.txt

python3 main.py  # run the spark function itself
```

## User session definition

**Policy 1: time bounded actions (tb).**  
A user session is a set of actions performed by a user or IDE
with a short time interval between these actions.

**Policy 2: actions between start and close (sc).**
A user session is a set of all events for a distinct user happened 
between the events 'ide.start' and 'ide.close'. 
If an ide was opened but was not yet closed, this set of action is also considered as a session.
