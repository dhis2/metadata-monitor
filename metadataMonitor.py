import json
import sys
import time
from urllib.parse import urlencode

import urllib3
import configparser
import base64


class MetadataMonitor:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read("config.ini")
        self.metadata_url = self.config.get("server", "server_url")
        self.metadata_username = self.config.get("server", "server_username")
        self.metadata_password = self.config.get("server", "server_password")
        self.checks_to_monitor = self.config.get("checks_to_monitor", "checks_to_monitor").split(",")
        self.metadata_headers = {
            "Content-Type": "application/json",
            "Authorization": "Basic " + base64.b64encode(
                (self.metadata_username + ":" + self.metadata_password).encode()).decode()
        }
        self.datavalue_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Basic " + base64.b64encode(
                (self.metadata_username + ":" + self.metadata_password).encode()).decode()
        }
        self.http = urllib3.PoolManager()
        self.metadata = None

    def get_metadata_integrity_checks(self):
        # GET /api/dataIntegrity
        try:
            response = self.http.request("GET", self.metadata_url + "/api/dataIntegrity", headers=self.metadata_headers)
            return json.loads(response.data.decode("utf-8"))
        except Exception as e:
            print("Error: " + str(e))
            return None

    def trigger_metadata_integrity_summaries(self):
        # POST /api/dataIntegrity/summary
        try:
            response = self.http.request("POST", self.metadata_url + "/api/dataIntegrity/summary",
                                         headers=self.metadata_headers)
            return json.loads(response.data.decode("utf-8"))
        except Exception as e:
            print("Error: " + str(e))
            return None

    def trigger_selected_metadata_integrity_summaries(self, checks):
        # POST /api/dataIntegrity/summary?checks=<name1>,<name2>
        try:
            response = self.http.request("POST",
                                         self.metadata_url + "/api/dataIntegrity/summary?checks=" + ",".join(checks),
                                         headers=self.metadata_headers)
            return json.loads(response.data.decode("utf-8"))
        except Exception as e:
            print("Error: " + str(e))
            return None

    def get_running_integrity_summary_checks(self):
        # GET /api/dataIntegrity/summary/running
        try:
            response = self.http.request("GET", self.metadata_url + "/api/dataIntegrity/summary/running",
                                         headers=self.metadata_headers)
            return json.loads(response.data.decode("utf-8"))
        except Exception as e:
            print("Error: " + str(e))
            return None

    def get_completed_integrity_summary_checks(self):
        # GET /api/dataIntegrity/summary
        try:
            response = self.http.request("GET", self.metadata_url + "/api/dataIntegrity/summary",
                                         headers=self.metadata_headers)
            return json.loads(response.data.decode("utf-8"))
        except Exception as e:
            print("Error: " + str(e))
            return None

    def get_all_metadata_integrity_summaries(self):
        # Get a list of all checks
        self.get_metadata_integrity_checks()
        # Trigger all of the checks
        self.trigger_metadata_integrity_summaries()
        # Wait about 5 seconds and then poll to see if the checks are done
        time.sleep(5)
        running = self.get_running_integrity_summary_checks()
        while len(running) > 0:
            print("Waiting for checks to complete...")
            time.sleep(5)
            running = self.get_running_integrity_summary_checks()
        # Return the completed checks
        return self.get_completed_integrity_summary_checks()

    def get_integrity_summary_from_name(self,name, summaries):
        filtered_summary = summaries.get(name)
        if filtered_summary is not None:
            return filtered_summary
        else:
            return None

    def get_check_from_name(self,name,list_of_checks):

        for check in list_of_checks:
            if check["name"] == name:
                return check


    def get_datelement_with_code(self, check_name, list_of_checks):
        # GET /api/dataElement?code=<code>
        try:
            check = self.get_check_from_name(check_name, list_of_checks)
            response = self.http.request("GET", self.metadata_url + "/api/dataElements?fields=id&filter=code:eq:" + check["code"],
                                         headers=self.datavalue_headers)
            print(response.data.decode("utf-8"))
            return json.loads(response.data.decode("utf-8"))
        except Exception as e:
            print("Error: " + str(e))
            return None

    def transform_integrity_check_to_data_value(self, summary, dataelement_uid, period, orgunit):
        data = {
            "dataElement": dataelement_uid,
            "period": period,
            "orgUnit": orgunit,
            "value": summary["count"]
        }
        return data

    def create_data_value(self, data):
        # POST /api/dataValue
        try:
            #Need to POST the data as form data like this
            #curl "https://play.dhis2.org/demo/api/dataValues?de=s46m5MS0hxu&pe=201301&ou=DiszpKrYNg8&co=Prlt0C1RF0s&value=12"
            query_params = {
                "de": data["dataElement"],
                "co" : "HllvX50cXC0",
                "ds" : "ySAQjSSyLQg",
                "ou": data["orgUnit"],
                "pe": data["period"],
                "value": data["value"]
            }
            encoded_params = urlencode(query_params)
            print(encoded_params)
            response = self.http.request("POST", self.metadata_url + "/api/dataValues?" + encoded_params,
                                         headers=self.metadata_headers )
            #Get the text response
            print(response.data.decode("utf-8"))
            return response.status
        except Exception as e:
            print("Error: " + str(e))
            return None

    def get_level1_orgunits(self):
        # GET /api/organisationUnits?level=1
        try:
            response = self.http.request("GET", self.metadata_url + "/api/organisationUnits?level=1",
                                         headers=self.metadata_headers)
            return json.loads(response.data.decode("utf-8"))
        except Exception as e:
            print("Error: " + str(e))
            return None

    def process_completed_checks_to_data_values(self, summaries, period, orgunit, list_of_checks):
        # For each of the checks to monitor, get the data elements to store the results, and then create the data values
        for check in self.checks_to_monitor:
            summary = self.get_integrity_summary_from_name(check, summaries)
            if summary is not None:
                dataelement = self.get_datelement_with_code(check, list_of_checks)
                if dataelement is not None and "dataElements" in dataelement and len(dataelement["dataElements"]) > 0:
                    dataelement_uid = dataelement["dataElements"][0]["id"]
                    data = self.transform_integrity_check_to_data_value(summary, dataelement_uid, period, orgunit)
                    self.create_data_value(data)
                else:
                    print("Data element not found for check: " + check)
            else:
                print("Summary not found for check: " + check)


if __name__ == '__main__':
    monitor = MetadataMonitor()
    all_checks = monitor.get_metadata_integrity_checks()
    summaries = monitor.get_all_metadata_integrity_summaries()
    orgunit = monitor.get_level1_orgunits()
    period = time.strftime("%Y%m%d")
    monitor.process_completed_checks_to_data_values(summaries, period, orgunit["organisationUnits"][0]["id"], all_checks)
