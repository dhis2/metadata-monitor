import json
import time
from urllib.parse import urlencode

import urllib3
import configparser
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


class MetadataMonitor:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read("config.ini")
        self.metadata_url = self.config.get("server", "server_url")
        self.metadata_username = self.config.get("server", "server_username", fallback=None)
        self.metadata_password = self.config.get("server", "server_password", fallback=None)
        self.metadata_token = self.config.get("server", "server_token", fallback=None)
        self.default_coc = self.config.get("server", "default_coc",fallback="HllvX50cXC0")
        self.logging_level = self.config.get("server", "logging_level", fallback="INFO")
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
        logging.basicConfig(filename='metadata_monitor.log', level=self.logging_level,
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
        # GET dataSets/ySAQjSSyLQg?fields=dataSetElements[dataElement[id,code]]
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
            logging.info(response.data.decode("utf-8"))
            logging.info(response.status)
            logging.info("Level 1 orgunits: " + str(json.loads(response.data.decode("utf-8"))))
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


if __name__ == '__main__':
    monitor = MetadataMonitor()
    all_checks = monitor.get_metadata_integrity_checks()
    summaries = monitor.get_all_metadata_integrity_summaries()
    des = monitor.get_data_elements_to_monitor()
    orgunit = monitor.get_level1_orgunits()
    period = time.strftime("%Y%m%d")
    monitor.process_completed_checks_to_data_values(summaries, period, orgunit["organisationUnits"][0]["id"], des)
