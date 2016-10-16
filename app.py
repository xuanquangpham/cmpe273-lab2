#author Quang Pham

import logging
logging.basicConfig(level=logging.DEBUG)

from spyne import Application, rpc, ServiceBase, Unicode
from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument
from spyne.server.wsgi import WsgiApplication

import datetime as dt
import requests
import re
import json

class Lab2(ServiceBase):
    @rpc(str, str, str, _returns=Unicode)
    def checkcrime(self, lat, lon, radius):
        url = "https://api.spotcrime.com/crimes.json?lat=" + lat + "&lon=" + lon + "&radius=" + radius + "&key=."
        req = requests.get(url)
        crimes_dict = json.loads(req.text)

        crime_type_dict = {}
        time_count_dict = {}
        street_dict = {}
        total_crimes = len(crimes_dict["crimes"])

        #set up time variables
        am_12_3 = am_3_6 = am_6_9 = am_9_12 = 0
        pm_12_3 = pm_3_6 = pm_6_9 = pm_9_12 = 0
        time_12am = dt.datetime.strptime('12:00 AM', '%I:%M %p').time()
        time_3am = dt.datetime.strptime('3:00 AM', '%I:%M %p').time()
        time_6am = dt.datetime.strptime('6:00 AM', '%I:%M %p').time()
        time_9am = dt.datetime.strptime('9:00 AM', '%I:%M %p').time()
        time_12pm = dt.datetime.strptime('12:00 PM', '%I:%M %p').time()
        time_3pm = dt.datetime.strptime('3:00 PM', '%I:%M %p').time()
        time_6pm = dt.datetime.strptime('6:00 PM', '%I:%M %p').time()
        time_9pm = dt.datetime.strptime('9:00 PM', '%I:%M %p').time()

        #parsing the json return from spotcrime
        for key in crimes_dict["crimes"]:

            #parse time from response json
            time = dt.datetime.strptime(key["date"], '%m/%j/%y %I:%M %p').time()
            if (time > time_12am) & (time <= time_3am):
                am_12_3 += 1
            elif(time > time_3am) & (time <= time_6am):
                am_3_6 += 1
            elif(time > time_6am) & (time <= time_9am):
                am_6_9 += 1
            elif(time > time_9am) & (time <= time_12pm):
                am_9_12 += 1
            elif(time > time_12pm) & (time <= time_3pm):
                pm_12_3 += 1
            elif(time > time_3pm) & (time <= time_6pm):
                pm_3_6 += 1
            elif(time > time_6pm) & (time <= time_9pm):
                pm_6_9 += 1
            else:
                pm_9_12 += 1
            
            #parse crime types & add up count
            if key["type"] not in crime_type_dict:
                crime_type_dict[key["type"]] = 1
            else:
                crime_type_dict[key["type"]] += 1

            #parse street & add up count
            streets = key['address'].split("&")
            for st in streets:
                #use regular expression to cover all the cases
                st = re.sub('\d* +block ?o?f?', "", st, flags=re.IGNORECASE)
                st = st.strip()
                if st not in street_dict:
                    street_dict[st] = 1
                else:
                    street_dict[st] += 1

        #sort street_dict from descending order
        top_street = sorted(street_dict, key=street_dict.get, reverse=True)

        #create time dictionary
        time_count_dict = {"12:01am-3am":am_12_3,"3:01am-6am":am_3_6,"6:01am-9am":am_6_9,
                    "9:01am-12noon":am_9_12,"12:01pm-3pm":pm_12_3,"3:01pm-6pm":pm_3_6,
                    "6:01pm-9pm":pm_6_9,"9:01pm-12midnight":pm_9_12}

        output = {'total_crime' : total_crimes,
                    'the_most_dangerous_streets' : top_street[:3],
                     'crime_type_count' : crime_type_dict,
                     'event_time_count' : time_count_dict
                    }
        return output

app = Application([Lab2],
    tns='spyne.cmpe273_lab2',
    in_protocol=HttpRpc(validator='soft'),
    out_protocol=JsonDocument()
)

if __name__ == '__main__':
    from wsgiref.simple_server import make_server
    wsgi_app = WsgiApplication(app)
    server = make_server('0.0.0.0', 8000, wsgi_app)
    server.serve_forever()