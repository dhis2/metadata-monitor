import json
import time
from urllib.parse import urlencode

import urllib3
import random
import string
import configparser
import argparse
import base64
import logging


def transform_integrity_check_to_data_value(summary, dataelement_uid, period, orgunit):
    if summary is None:
        return None
    data = {
        "dataElement": dataelement_uid,
        "period": period,
        "orgUnit": orgunit,
        "value": summary["count"]
    }
    return data


def get_integrity_summary_from_code(code, summaries):
    for key in summaries.keys():
        if summaries[key]["code"] == code:
            return summaries[key]
    return None

def generate_uid():
    first_letter = random.choice(string.ascii_lowercase)
    last_part = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    return first_letter + last_part



class MetadataMonitor:
    def __init__(self,config_path):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.metadata_url = self.config.get("server", "server_url")
        self.metadata_username = self.config.get("server", "server_username", fallback=None)
        self.metadata_password = self.config.get("server", "server_password", fallback=None)
        self.metadata_token = self.config.get("server", "server_token", fallback=None)
        self.default_coc = self.config.get("server", "default_coc",fallback="HllvX50cXC0")
        self.default_cc = self.config.get("server", "default_cc", fallback="bjDvmb4bfuf")
        self.logging_level = self.config.get("server", "logging_level", fallback="INFO")
        self.log_file = self.config.get("server", "log_file", fallback="metadata_monitor.log")
        self.aggregate_dataset = self.config.get("server", "aggregate_dataset")
        self.monitoring_group = self.config.get("server", "monitor_data_element_group")

        if not self.metadata_token:
            if not (self.metadata_username and self.metadata_password):
                raise ValueError("You must specify either a username and password or a token")

        if self.metadata_token:
            self.metadata_headers = {
                "Content-Type": "application/json",
                "Authorization": "ApiToken " + self.metadata_token
            }
            self.datavalue_headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": "ApiToken " + self.metadata_token
            }
        else:
            auth_header = "Basic " + base64.b64encode(
            (self.metadata_username + ":" + self.metadata_password).encode()).decode()
            self.metadata_headers = {
                "Content-Type": "application/json",
                "Authorization": auth_header
            }
            self.datavalue_headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": auth_header
            }

        self.http = urllib3.PoolManager()
        self.metadata = None

        # init the log if needed
        logging.basicConfig(filename=self.log_file, level=self.logging_level,
                            format='%(asctime)s:%(levelname)s:%(message)s')

    def get_metadata_integrity_checks(self):
        # GET /api/dataIntegrity
        try:
            response = self.http.request("GET", self.metadata_url + "/api/dataIntegrity", headers=self.metadata_headers)
            return json.loads(response.data.decode("utf-8"))
        except Exception as e:
            logging.error("Error: " + str(e))
            return None

    def get_data_elements_to_monitor(self):
        # GET /api/dataElementGroups
        try:
            response = self.http.request("GET",
                                         self.metadata_url + "/api/dataElementGroups/" + self.monitoring_group + "?fields=dataElements[id,code]",
                                         headers=self.metadata_headers)
            des = json.loads(response.data.decode("utf-8")).get("dataElements")
            # Remove the MI_prefix from each code
            for de in des:
                de["code"] = de["code"][3:]
            return des
        except Exception as e:
            logging.error("Error: " + str(e))
            return None

    def get_integrity_checks_no_data_elements(self):
        checks = self.get_metadata_integrity_checks()
        #Exclude any slow or programmatic checks
        checks = [check for check in checks if not check["isSlow"] and not check["isProgrammatic"]]
        des = self.get_dataelements_in_dataset()
        #Remove the MI_ prefix from each code
        for de in des["dataSetElements"]:
            this_code = de["dataElement"]["code"][3:]
            for check in checks:
                if check["code"] == this_code:
                    checks.remove(check)
        return checks

    def create_data_element_from_integrity_check(self,check):
        uid = generate_uid()
        data = {
            "id": uid,
            "name": "[MI] " + check.get("displayName", "Placeholder metadata integrity check " + uid),
            "shortName": check["name"].replace("_", " ").title()[:50],
            "aggregationType": "AVERAGE",
            "valueType": "INTEGER_ZERO_OR_POSITIVE",
            "domainType": "AGGREGATE",
            "code": "MI_" + check["code"],
            "categoryCombo": {"id": self.default_cc}
        }
        return data

    def create_missing_data_elements(self):
        checks = self.get_integrity_checks_no_data_elements()
        if not checks:
            logging.info("No missing data elements to create")
            return

        for check in checks:
            #Check to see if the data elements exists
            response = self.http.request("GET", self.metadata_url + "/api/dataElements?filter=code:eq:" + "MI_" + check["code"],
                                            headers=self.metadata_headers)
            if response.status == 200:
                logging.info("Data element already exists: " + check["code"] + " but may not be in dataset. Skipping creation.")
                continue

            logging.info("Creating new  data element for check" + check["code"])
            data_element = self.create_data_element_from_integrity_check(check)
            response = self.http.request("POST", self.metadata_url + "/api/dataElements",
                                         headers=self.metadata_headers,
                                         body=json.dumps(data_element))
            if response.status == 201:
                logging.info("Successfully created new data element: " + data_element["name"])
            else:
                logging.error("Failed to create data element: " + data_element["name"] + " with response: " + response.data.decode("utf-8"))
                continue

            response_ds =self.http.request("POST", self.metadata_url + "/api/dataSets/" + self.aggregate_dataset + "/dataElements",
                                            headers=self.metadata_headers,
                                            body=json.dumps({"additions" : [{"id": data_element["id"]}]}))
            if response_ds.status == 200:
                logging.info("Adding data element to dataset: " + data_element["name"])
            else:
                logging.error("Failed to add data element to dataset: " + data_element["name"] + " with response: " + response_ds.data.decode("utf-8"))



    def trigger_metadata_integrity_summaries(self):
        # POST /api/dataIntegrity/summary
        try:
            response = self.http.request("POST", self.metadata_url + "/api/dataIntegrity/summary",
                                         headers=self.metadata_headers)
            return json.loads(response.data.decode("utf-8"))
        except Exception as e:
            logging.error("Error: " + str(e))
            return None

    def trigger_selected_metadata_integrity_summaries(self, checks):
        # POST /api/dataIntegrity/summary?checks=<name1>,<name2>
        try:
            response = self.http.request("POST",
                                         self.metadata_url + "/api/dataIntegrity/summary?checks=" + ",".join(checks),
                                         headers=self.metadata_headers)
            return json.loads(response.data.decode("utf-8"))
        except Exception as e:
            logging.error("Error: " + str(e))
            return None

    def get_running_integrity_summary_checks(self):
        # GET /api/dataIntegrity/summary/running
        try:
            response = self.http.request("GET", self.metadata_url + "/api/dataIntegrity/summary/running",
                                         headers=self.metadata_headers)
            return json.loads(response.data.decode("utf-8"))
        except Exception as e:
            logging.error("Error: " + str(e))
            return None

    def get_completed_integrity_summary_checks(self):
        # GET /api/dataIntegrity/summary
        try:
            response = self.http.request("GET", self.metadata_url + "/api/dataIntegrity/summary",
                                         headers=self.metadata_headers)
            return json.loads(response.data.decode("utf-8"))
        except Exception as e:
            logging.error("Error: " + str(e))
            return None

    def get_all_metadata_integrity_summaries(self):
        self.get_metadata_integrity_checks()
        logging.info("Triggering metadata integrity summaries")
        self.trigger_metadata_integrity_summaries()
        time.sleep(5)
        running = self.get_running_integrity_summary_checks()
        timeout = 600
        while running is not None and len(running) > 0 and timeout > 0:
            time.sleep(5)
            running = self.get_running_integrity_summary_checks()
            timeout -= 5
        logging.info("Completed metadata integrity summaries")
        logging.info("The process took: " + str(600 - timeout) + " seconds")
        return self.get_completed_integrity_summary_checks()

    def get_integrity_summary_from_name(self, name, summaries):
        filtered_summary = summaries.get(name)
        if filtered_summary is not None:
            return filtered_summary
        else:
            return None

    def create_data_value(self, data):
        # POST /api/dataValue
        try:
            query_params = {
                "de": data["dataElement"],
                "co": self.default_coc,
                "ds": self.aggregate_dataset,
                "ou": data["orgUnit"],
                "pe": data["period"],
                "value": data["value"]
            }
            encoded_params = urlencode(query_params)
            response = self.http.request("POST", self.metadata_url + "/api/dataValues?" + encoded_params,
                                         headers=self.metadata_headers)
            return response
        except Exception as e:
            logging.error("Error: " + str(e))
            return None

    def get_dataelements_in_dataset(self):
        try:
            response = self.http.request("GET",
                                         self.metadata_url + "/api/dataSets/" + self.aggregate_dataset + "?fields=dataSetElements[dataElement[id,code]]",
                                         headers=self.metadata_headers)
            return json.loads(response.data.decode("utf8"))
        except Exception as e:
            logging.error("Error:" + str(e))
            return None

    def get_level1_orgunits(self):
        # GET /api/organisationUnits?level=1
        try:
            response = self.http.request("GET", self.metadata_url + "/api/organisationUnits?level=1",
                                         headers=self.metadata_headers)
            logging.debug("Level 1 orgunits: " + str(json.loads(response.data.decode("utf-8"))))
            return json.loads(response.data.decode("utf-8"))
        except Exception as e:
            logging.error("Error: " + str(e))
            return None

    def process_completed_checks_to_data_values(self, summaries, period, orgunit, des):
        successful_checks = []
        failed_checks = []
        for de in des:
            summary = get_integrity_summary_from_code(de["code"], summaries)
            data = transform_integrity_check_to_data_value(summary, de["id"], period, orgunit)
            if data is not None:
                response = self.create_data_value(data)
                if response.status == 201:
                    successful_checks.append(de["code"])
                else:
                    logging.error(f"Failed to process data value for: {de['code']} with response: {response.data.decode('utf-8')}")
                    failed_checks.append(de["code"])
        logging.info(f"Successfully processed data values for: {len(successful_checks)}")
        if failed_checks:
            logging.info(f"Failed to process data values for: {failed_checks}")

    def update_metadata_integrity_data_elements(self):
        logging.info("Starting metadata monitor")
        checks = self.get_metadata_integrity_checks()
        summaries = self.get_all_metadata_integrity_summaries()
        des = self.get_data_elements_to_monitor()
        orgunit = self.get_level1_orgunits()
        period = time.strftime("%Y%m%d")
        self.process_completed_checks_to_data_values(summaries, period, orgunit["organisationUnits"][0]["id"], des)
        logging.info("Metadata monitor completed")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Metadata Monitor')
    parser.add_argument('--config', type=str, required=True, help='Path to the config.ini file')
    args = parser.parse_args()
    monitor = MetadataMonitor(args.config)
    monitor.create_missing_data_elements()
    monitor.update_metadata_integrity_data_elements()
