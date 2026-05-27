import yaml
import os
import sys
from typing import List, Dict
from GlobalDefs import ExitCode, PurposeManagementMethod
import GlobalDefs
from LoggingModule import console_log, ConsoleLogLevel

class TestConfiguration:
    
    # === Overall information
    name: str
    test_duration_ms: int
    qos: int = 0
    
    # Device definitions
    device_definitions: Dict = dict()
    device_instances_config: List = list()
    
    # Purposes
    purpose_definitions: Dict = dict()
    
    # Broker monitoring
    monitor_broker: bool = False
    node_exporter_url: str = "http://localhost:9100/metrics"
    monitor_interval_ms: int = 1000
    
    # Event Scheduling
    scheduled_events: List = list()
    
    # === Operation information ===
    op_send_rate: int = 0
    c1_reg_operations: List[str] = list()
    all_operations: Dict[str, str] = dict()
    
    def __init__(self, name:str, test_duration_ms: int):
        self.name = name
        self.test_duration_ms = test_duration_ms

class BenchmarkConfiguration:
    
    this_node_name: str
    method: PurposeManagementMethod
    client_module_name: str
    log_output_dir: str
    
    # === Required topics for some PM methods ===
    reg_by_msg_reg_topic: str = ""
    reg_by_topic_pub_reg_topic: str = ""
    reg_by_topic_sub_reg_topic: str = ""
    
    # === Operational Information ===
    or_topic_name: str
    ors_topic_name: str
    on_topic_name: str
    onp_topic_name: str
    osys_topic__name: str
    op_response_topic: str
    op_purpose: str
    
    test_list: List[TestConfiguration] = list()

class ConfigParser():
    
    the_config: BenchmarkConfiguration = BenchmarkConfiguration()

    def parse_config(self, file_path) -> BenchmarkConfiguration:

        # Verify file exists
        if not os.path.exists(file_path):
            print(f"Configuration file not found at {file_path}")
            sys.exit(ExitCode.BAD_ARGUMENT)

        # Load YAML
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
            if data is None:
                print(f"Configuration file could not be parsed")
                sys.exit(ExitCode.MALFORMED_CONFIG)

        # Parse base configuration
        self._parse_config_yaml(data)

        return self.the_config
    
    def _parse_config_yaml(self, data):
        
        # Parse the overall config
        if not "node_name" in data:
            raise Exception("node_name not found in config")
        self.the_config.this_node_name = data["node_name"]
        
        if not "client_module_name" in data:
            raise Exception("client_module_name not found in config")
        self.the_config.client_module_name = data["client_module_name"]
        
        if not "output_dir" in data:
            raise Exception("output_dir not found in config")
        self.the_config.log_output_dir = data["output_dir"]
        
        if not "purpose_management_method" in data:
            raise Exception("purpose_management_method not found in config")
        
        # Only the unified DAP method is supported; legacy value 3 is still accepted
        method_int = data["purpose_management_method"]
        if method_int != 3:
            raise Exception("Only the unified DAP method is supported (purpose_management_method: 3)")
        self.the_config.method = PurposeManagementMethod.PM_UNIFIED

        if not "reg_by_msg_reg_topic" in data:
            raise Exception("reg_by_msg_reg_topic not found in config (required for the unified method's MP registration)")
        self.the_config.reg_by_msg_reg_topic = data["reg_by_msg_reg_topic"]

        if not "reg_by_topic_pub_reg_topic" in data:
            raise Exception("reg_by_topic_pub_reg_topic not found in config (required for the unified method's MP registration)")
        self.the_config.reg_by_topic_pub_reg_topic = data["reg_by_topic_pub_reg_topic"]

         # Parse the opertional config
        if not "or_topic_name" in data:
            raise Exception("or_topic_name not found in config")
        self.the_config.or_topic_name = data["or_topic_name"]
        
        if not "ors_topic_name" in data:
            raise Exception("ors_topic_name not found in config")
        self.the_config.ors_topic_name = data["ors_topic_name"]
        
        if not "on_topic_name" in data:
            raise Exception("on_topic_name not found in config")
        self.the_config.on_topic_name = data["on_topic_name"]
        
        if not "onp_topic_name" in data:
            raise Exception("onp_topic_name not found in config")
        self.the_config.onp_topic_name = data["onp_topic_name"]
        
        if not "osys_topic_name" in data:
            raise Exception("osys_topic_name not found in config")
        self.the_config.osys_topic__name = data["osys_topic_name"]
        
        if not "operational_response_topic_prefix" in data:
            raise Exception("operational_response_topic_prefix not found in config")
        self.the_config.op_response_topic = data["operational_response_topic_prefix"]
        
        if not "operational_purpose" in data:
            raise Exception("operational_purpose not found in config")
        self.the_config.op_purpose = data["operational_purpose"]
            
        # Parse the test config
        if not "test" in data:
            raise Exception("test not found in config")
        
        # Parse test (currently only supports one)
        test_yaml = data["test"]
        
        if not "name" in test_yaml:
            raise Exception("name not found for test config")
        name = test_yaml["name"]

        if not "duration_ms" in test_yaml:
            raise Exception(f"duration_ms not found for test {name} config")
        test_duration_ms = test_yaml["duration_ms"]
        
        test_config = TestConfiguration(name, test_duration_ms)
        
        if not "data_qos" in test_yaml:
            raise Exception(f"data_qos not found for test {test_config.name} config")
        test_config.qos = test_yaml["data_qos"]

        # Parse purpose definitions
        test_config.purpose_definitions = self._parse_purpose_definitions(data)

        # Parse device definitions
        test_config.device_definitions = self._parse_device_definitions(data)

        # Parse device instances
        test_config.device_instances_config = self._parse_device_instances(test_yaml)

        # Parse scheduled events
        test_config.scheduled_events = self._parse_scheduled_events(test_yaml)

        # Parse broker monitoring settings
        test_config.monitor_broker = data.get('monitor_broker', False)
        test_config.node_exporter_url = data.get('node_exporter_url', 'http://localhost:9100/metrics')
        test_config.monitor_interval_ms = data.get('monitor_interval_ms', 1000)
        
        # Operational information
        # Ops not required, may be empty
        if "op_send_rate" in test_yaml:
            test_config.op_send_rate = test_yaml["op_send_rate"]
            
        if "c1_reg_ops" in test_yaml:
            for op in test_yaml["c1_reg_ops"]:
                test_config.c1_reg_operations.append(op)
            
        if "c1_ops" in test_yaml:
            for op in test_yaml["c1_ops"]:
                test_config.all_operations[op] = "C1"
                
        if "c2_ops" in test_yaml:
            for op in test_yaml["c2_ops"]:
                test_config.all_operations[op] = "C2"
                
        if "c3_ops" in test_yaml:
            for op in test_yaml["c3_ops"]:
                test_config.all_operations[op] = "C3"
    
        self.the_config.test_list.append(test_config)
        
        if GlobalDefs.VERBOSE_LOGGING:
            console_log(ConsoleLogLevel.DEBUG, f"Parsed configuration:", __name__)
            console_log(ConsoleLogLevel.DEBUG, f"  - Purposes: {len(test_config.purpose_definitions)}", __name__)
            console_log(ConsoleLogLevel.DEBUG, "  - Device definitions: {len(test_config.device_definitions)}", __name__)
            console_log(ConsoleLogLevel.DEBUG, "  - Device instances: {len(test_config.device_instances_config)}", __name__)
            console_log(ConsoleLogLevel.DEBUG, f"  - Scheduled events: {len(test_config.scheduled_events)}", __name__)

    def _parse_purpose_definitions(self, data: Dict) -> Dict:
        """Parse purpose definitions from config"""
        purposes = {}

        purpose_defs = data.get('purpose_definitions', [])
        for purpose_def in purpose_defs:
            purpose_id = purpose_def.get('id')
            if not purpose_id:
                raise Exception("purpose_definitions entry missing 'id'")

            purposes[purpose_id] = {
                'description': purpose_def.get('description', '')
            }

        return purposes

    def _parse_device_definitions(self, data: Dict) -> Dict:
        """Parse device definitions from config"""
        devices = {}

        device_defs = data.get('device_definitions', [])
        for device_def in device_defs:
            device_id = device_def.get('id')
            if not device_id:
                raise Exception("device_definitions entry missing 'id'")

            device_type = device_def.get('type')
            if not device_type:
                raise Exception(f"device_definitions entry {device_id} missing 'type'")

            if device_type == 'publisher':
                devices[device_id] = self._parse_publisher_definition(device_def)
            elif device_type == 'subscriber':
                devices[device_id] = self._parse_subscriber_definition(device_def)
            else:
                raise Exception(f"Unknown device type: {device_type}")

        return devices

    def _parse_publisher_definition(self, device_def: Dict) -> Dict:
        """Parse publisher device definition"""
        device_id = device_def['id']

        # Required fields
        required_fields = ['topic', 'pub_period_ms',
                          'min_payload_bytes', 'max_payload_bytes']
        for field in required_fields:
            if field not in device_def:
                raise Exception(f"Publisher {device_id} missing required field: {field}")

        return {
            'type': 'publisher',
            'topic': device_def['topic'],
            'pub_period_ms': device_def['pub_period_ms'],
            'min_payload_bytes': device_def['min_payload_bytes'],
            'max_payload_bytes': device_def['max_payload_bytes']
        }

    def _parse_subscriber_definition(self, device_def: Dict) -> Dict:
        """Parse subscriber device definition"""
        device_id = device_def['id']

        # Required fields
        required_fields = ['topic_filter']
        for field in required_fields:
            if field not in device_def:
                raise Exception(f"Subscriber {device_id} missing required field: {field}")

        return {
            'type': 'subscriber',
            'topic_filter': device_def['topic_filter']
        }

    def _parse_device_instances(self, test_data: Dict) -> List:
        """Parse device instance configurations"""
        instances = []

        device_instances = test_data.get('device_instances', [])
        for instance_config in device_instances:
            
            # Required fields
            required_fields = ['device_def_id', 'instance_id', 'purpose_filter']
            for field in required_fields:
                if field not in instance_config:
                    raise Exception(f"Device instance {instance_config} missing required field: {field}")
            
            device_def_id = instance_config.get('device_def_id')
            instance_id = instance_config.get('instance_id')
            purpose_filter = instance_config.get('purpose_filter')

            instances.append({
                'device_def_id': device_def_id,
                'instance_id': instance_id,
                'purpose_filter': purpose_filter,
                'count': instance_config.get('count', 1)
            })

        return instances

    def _parse_scheduled_events(self, test_data: Dict) -> List:
        """Parse scheduled events"""
        events = []

        scheduled_events = test_data.get('scheduled_events', [])
        for event_config in scheduled_events:
            time_ms = event_config.get('time_ms')
            if time_ms is None:
                raise Exception("scheduled_events entry missing 'time_ms'")

            event_type = event_config.get('type')
            if not event_type:
                raise Exception("scheduled_events entry missing 'type'")

            # Copy entire config as params
            event = {
                'time_ms': time_ms,
                'type': event_type,
                'description': event_config.get('description', '')
            }

            # Add type-specific parameters
            if event_type in ['connect', 'disconnect', 'reconnect',
                            'start_publishing', 'stop_publishing']:
                event['devices'] = event_config.get('devices', [])

            elif event_type == 'change_purpose':
                event['devices'] = event_config.get('devices')
                event['new_purpose'] = event_config.get('new_purpose')
                if not event['devices'] or not event['new_purpose']:
                    raise Exception("change_purpose event requires 'devices' and 'new_purpose'")

            events.append(event)

        return events
