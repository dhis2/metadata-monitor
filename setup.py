import json
import random
import string
import urllib3
import configparser
import base64
import os

def generate_uid():
    first_letter = random.choice(string.ascii_lowercase)
    last_part = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    return first_letter + last_part

class MetadataMonitorSetup:
    def __init__(self):
        self.config = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(__file__), "config.ini")
        self.config.read(config_path)
        self.metadata_url = self.config.get("server", "server_url", fallback="")
        self.metadata_username = self.config.get("server", "server_username", fallback="")
        self.metadata_password = self.config.get("server", "server_password", fallback="")
        self.aggregate_dataset = self.config.get("server", "aggregate_dataset", fallback="")
        self.monitoring_group = self.config.get("server", "monitor_data_element_group", fallback="")
        if not self.metadata_url or not self.metadata_username or not self.metadata_password:
            raise ValueError("Missing required server configuration values")
        self.metadata_headers = {
            "Content-Type": "application/json",
            "Authorization": "Basic " + base64.b64encode(
                (self.metadata_username + ":" + self.metadata_password).encode()).decode()
        }
        self.http = urllib3.PoolManager()
        self.metadata = None

    def get_integrity_check_metadata(self):
        # GET /api/dataIntegrity
        try:
            response = self.http.request("GET", self.metadata_url + "/api/dataIntegrity", headers=self.metadata_headers)
            # Filter any integrity checks that where isSlow is True
            return [check for check in json.loads(response.data.decode("utf-8")) if not check["isSlow"]]
        except Exception as e:
            print("Error: " + str(e))
            return None

    def create_data_element_from_integrity_check(self, check):
        data = {
            "id": generate_uid(),
            "name": "[MI] " + check["description"],
            "shortName": check["name"].replace("_", " ").title()[:50],
            "aggregationType": "AVERAGE",
            "valueType": "INTEGER_ZERO_OR_POSITIVE",
            "domainType": "AGGREGATE",
            "code": "MI_" + check["code"],
            "categoryCombo": {"id": "bjDvmb4bfuf"},
            "dataElementGroups": [{"id": "yNpUqX1BiFC"}]
        }
        return data

    def create_data_elements(self, checks):
        data_elements = []
        unique_names = set()

        for check in checks:
            data_element = self.create_data_element_from_integrity_check(check)
            if data_element["name"] not in unique_names:
                unique_names.add(data_element["name"])
                data_elements.append(data_element)

        return data_elements

    def create_data_element_group(self):
        data = {
            "id": "yNpUqX1BiFC",
            "name": "Metadata Integrity Checks",
            "shortName": "MI Checks",
            "dataElements": []
        }
        return data

    def create_metadata(self):
        checks = self.get_integrity_check_metadata()
        if checks:
            data_elements = self.create_data_elements(checks)
            payload = {"dataElements": data_elements,
                       "dataElementGroups": [self.create_data_element_group()]}
            with open('metadata_monitoring.json', 'w') as f:
                json.dump(payload, f, indent=4)
            print(f"Generated {len(data_elements)} data elements")
        else:
            print("No data elements generated")

if __name__ == '__main__':
    monitor = MetadataMonitorSetup()
    monitor.create_metadata()