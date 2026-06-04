import paho.mqtt.client as mqtt
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.properties import Properties
import time
import random
import ischedule
import threading
import sched
from math import ceil  # Need this for encoding correlation data in operational responses
from typing import Dict, List, Optional, Tuple, Any
import GlobalDefs
from ConfigParser import TestConfiguration
from EventScheduler import EventScheduler
from DeviceDefinitions import (
    DeviceManager, DeviceInstance, PublisherDefinition,
    SubscriberDefinition, PurposeDefinition, DeviceDefinition
)
from BrokerMonitor import BrokerMonitor
from LoggingModule import console_log, ConsoleLogLevel

class TestExecutor():
    """Test executor with deterministic event scheduling and per-device publication rates"""

    my_id: str
    broker_address: str
    broker_port: int
    method: GlobalDefs.PurposeManagementMethod
    current_config: TestConfiguration
    stop_event: threading.Event
    duration_scheduler: sched.scheduler
    
    pending_publishes: Dict[str, Dict[int, Tuple[str, str, str, float, str | None, bool]]] # client name => [message id => (topic, purpose, message_type, timestamp, op_id, is_response)]
    pending_subscribes: Dict[str, Dict[int, Tuple[str, str, int, float]]] # client name => [message id => (topic_filter, purpose_filter, sub_id, timestamp)]
    sub_ids: Dict[str, Dict[str, int]]
    publish_lock: threading.Lock
    subscribe_lock: threading.Lock

    # Operational request tracking
    next_op_time_ms: float
    c1_reg_ops: List[str]
    all_operations: Dict[str, str]

    def __init__(self, executor_id: str, broker_address: str, broker_port: int,
                 method: GlobalDefs.PurposeManagementMethod):

        self.my_id = executor_id
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.method = method
        self.pending_publishes = dict()
        self.pending_subscribes = dict()
        self.sub_ids = dict()
        self.stop_event = threading.Event()
        self.publish_lock = threading.Lock()
        self.subscribe_lock = threading.Lock()

        # Operational request tracking
        self.next_op_time_ms = 0
        self.c1_reg_ops = []
        self.all_operations = {}

        # Operational requests are issued by a single pinned publisher (seeded),
        # matching the paper's "a single publisher". Selected once on first use.
        self._op_issuer_id = None
        self._op_issuer_rng = random.Random(1074)

        #  Seed the random number generator and write seed to log file
        seed = int(time.time())
        random.seed(time.time())
        GlobalDefs.LOGGING_MODULE.log_seed(seed)

        # Configure scheduler
        self.duration_scheduler = sched.scheduler()

        # Scheduling and device management
        self.event_scheduler = EventScheduler()
        self.device_manager = DeviceManager()
        self.broker_monitor: Optional[BrokerMonitor] = None

    def setup_test(self, test_config: TestConfiguration):
        # """Setup test with deterministic scheduling"""
        console_log(ConsoleLogLevel.DEBUG, f"Configuring test {test_config.name}", __name__)

        # Clear previous test data
        self._clear_previous_test_data()
        self.current_config = test_config

        # Setup purpose definitions
        self._setup_purpose_definitions(test_config)

        # Setup device definitions
        self._setup_device_definitions(test_config)

        # Create device instances
        self._create_device_instances(test_config)

        # Setup event scheduler
        self._setup_event_scheduler(test_config)
        self.event_scheduler.print_schedule()

        # Setup operational requests
        self._setup_operational_requests(test_config)

        # Setup broker monitoring if enabled
        if test_config.monitor_broker:
            self._setup_broker_monitoring(test_config)

        console_log(ConsoleLogLevel.DEBUG, f"Test configured!",  __name__)
        
    def _clear_previous_test_data(self):
        self.device_manager.clear()
        self.event_scheduler.clear()
        self.pending_publishes = dict()
        if self.broker_monitor:
            self.broker_monitor.clear_samples()
        
        self.current_config = None
        self.pending_publishes = dict()
        ischedule.reset()

    def perform_test(self, test_config: TestConfiguration):
        """Run a test with deterministic scheduling"""

        console_log(ConsoleLogLevel.INFO, f"Starting test {test_config.name} at {time.strftime('%Y-%m-%d %H:%M:%S')}", __name__)
        console_log(ConsoleLogLevel.INFO, f"Test will run for {test_config.test_duration_ms / 1000.0} second(s)")

        # Log PM method for metrics calculation
        GlobalDefs.LOGGING_MODULE.log_pm_method(self.method.value)

        # Start scheduler and timer
        test_start_time_ms = time.monotonic() * 1000.0
        self.event_scheduler.start()

        if self.broker_monitor:
            self.broker_monitor.start_monitoring()

        # Main test loop
        test_end_time_ms = test_start_time_ms + test_config.test_duration_ms

        try:
            while time.monotonic() * 1000.0 < test_end_time_ms:
                current_time_ms = time.monotonic() * 1000.0
                elapsed_ms = current_time_ms - test_start_time_ms

                # Process scheduled events
                self.event_scheduler.process_due_events()

                # Publish from devices that are ready
                self._publish_from_ready_devices(elapsed_ms)

                # Send operational requests if it's time
                self._send_operational_requests_if_ready(elapsed_ms)

                # Collect broker metrics if needed
                if self.broker_monitor and self.broker_monitor.should_collect_sample(test_config.monitor_interval_ms):
                    self.broker_monitor.collect_sample()

                # Sleep briefly to avoid busy waiting
                # Calculate sleep time based on next event
                sleep_time = self._calculate_optimal_sleep_time(test_config)
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            console_log(ConsoleLogLevel.WARNING, f"Test interrupted by user")
        except Exception as e:
            console_log(ConsoleLogLevel.WARNING, f"Test failed with error: {e}")
            raise

        console_log(ConsoleLogLevel.INFO, f"Test complete! Cleaning up...", __name__)

        # Stop monitoring
        if self.broker_monitor:
            self.broker_monitor.stop_monitoring()

        # Give a moment to finish pending operations
        time.sleep(2)

        # Cleanup
        self._disconnect_all_devices()
        
        # Log out broker metrics
        if self.broker_monitor:
            self.broker_monitor.log_summary()
            
        # Give a moment to finish pending operations
        time.sleep(2)
        
        self._clear_previous_test_data()

        console_log(ConsoleLogLevel.INFO, f"Cleanup complete!", __name__)

    def _setup_purpose_definitions(self, test_config: TestConfiguration):
        """Load purpose definitions into the device manager"""
        for purpose_id, purpose_info in test_config.purpose_definitions.items():
            purpose_def = PurposeDefinition(
                id=purpose_id,
                description=purpose_info.get('description', '')
            )
            self.device_manager.register_purpose_definition(purpose_def)

    def _setup_device_definitions(self, test_config: TestConfiguration):
        """Load device definitions into the device manager"""
        for dev_id, dev_config in test_config.device_definitions.items():
            dev_type = dev_config['type']

            device_def: DeviceDefinition
            if dev_type == 'publisher':
                device_def = PublisherDefinition(
                    id=dev_id,
                    topic=dev_config['topic'],
                    pub_period_ms=dev_config['pub_period_ms'],
                    min_payload_bytes=dev_config['min_payload_bytes'],
                    max_payload_bytes=dev_config['max_payload_bytes']
                )
            elif dev_type == 'subscriber':
                device_def = SubscriberDefinition(
                    id=dev_id,
                    topic_filter=dev_config['topic_filter']
                )
            else:
                raise ValueError(f"Unknown device type: {dev_type}")

            self.device_manager.register_device_definition(device_def)

    def _create_device_instances(self, test_config: TestConfiguration):
        """Create device instances from definitions"""
        for instance_config in test_config.device_instances_config:
            device_def_id = instance_config['device_def_id']
            instance_id = instance_config['instance_id']
            purpose_filter = instance_config['purpose_filter']
            count = instance_config.get('count', 1)

            # Create multiple instances if count > 1
            for i in range(count):
                if count > 1:
                    full_instance_id = f"{instance_id}_{i}"
                else:
                    full_instance_id = instance_id

                # Create MQTT client
                client_name = f"{full_instance_id}"
                mqtt_client = GlobalDefs.CLIENT_MODULE.create_v5_client(client_name)

                # Set callbacks
                mqtt_client.on_connect = self._on_connect
                mqtt_client.on_disconnect = self._on_disconnect
                mqtt_client.on_subscribe = self._on_subscribe
                mqtt_client.on_publish = self._on_publish
                mqtt_client.on_message = self._on_message_recv

                # Create device instance
                device_instance = self.device_manager.create_device_instance(
                    device_def_id, full_instance_id, purpose_filter, mqtt_client, client_name
                )

                # Set user data for callbacks
                mqtt_client.user_data_set(device_instance)

    def _setup_event_scheduler(self, test_config: TestConfiguration):
        """Setup scheduled events"""
        # Register event handlers
        self.event_scheduler.register_handler("connect_all", self._handle_connect_all)
        self.event_scheduler.register_handler("disconnect_all", self._handle_disconnect_all)
        self.event_scheduler.register_handler("connect", self._handle_connect_devices)
        self.event_scheduler.register_handler("disconnect", self._handle_disconnect_devices)
        self.event_scheduler.register_handler("reconnect", self._handle_reconnect_devices)
        self.event_scheduler.register_handler("start_publishing", self._handle_start_publishing)
        self.event_scheduler.register_handler("start_publishing_all", self._handle_start_publishing_all)
        self.event_scheduler.register_handler("stop_publishing", self._handle_stop_publishing)
        self.event_scheduler.register_handler("stop_publishing_all", self._handle_stop_publishing_all)
        self.event_scheduler.register_handler("change_purpose", self._handle_change_purpose)

        # Schedule all events
        for event_config in test_config.scheduled_events:
            self.event_scheduler.schedule_event(
                time_ms=event_config['time_ms'],
                event_type=event_config['type'],
                params=event_config,
                description=event_config.get('description', '')
            )

    def _setup_broker_monitoring(self, test_config: TestConfiguration):
        """Setup broker monitoring"""
        self.broker_monitor = BrokerMonitor(test_config.node_exporter_url)

    def _setup_operational_requests(self, test_config: TestConfiguration):
        """Setup operational request tracking"""
        # Build operations dictionary from config
        self.all_operations = {}

        # Add C1 registration operations
        if hasattr(test_config, 'c1_reg_operations'):
            for op in test_config.c1_reg_operations:
                self.c1_reg_ops.append(op)

        # Add all other operations
        if hasattr(test_config, 'all_operations'):
            self.all_operations.update(test_config.all_operations)

        # Initialize next op time based on op_send_rate
        if hasattr(test_config, 'op_send_rate') and test_config.op_send_rate > 0:
            self.next_op_time_ms = test_config.op_send_rate
        else:
            self.next_op_time_ms = float('inf')  # Disable if not configured
            
    def _register_subscribers_c1_operation_data(self, subscriber: DeviceInstance) -> None:
        
        # Register the subscriber for all C1 ops (the unified broker supports this directly)
        for c1_op in self.c1_reg_ops:
            
            self.publish_lock.acquire()

            message_counter = subscriber.message_count
            subscriber.message_count += 1

            results = GlobalDefs.CLIENT_MODULE.publish_operation_request(subscriber.mqtt_client, self.method, c1_op, message_counter, self.current_config.qos)

            now = time.time()

            for message_info, topic in results:
                if subscriber.mqtt_client_name not in self.pending_publishes:
                    self.pending_publishes[subscriber.mqtt_client_name] = {}

                # C1 registration request -> PUBLISH_OP, no broker op_id
                self.pending_publishes[subscriber.mqtt_client_name][message_info.mid] = (
                    topic, GlobalDefs.OP_PURPOSE, c1_op, now, None, False
                )

                subscriber.message_id_to_send_counter[message_info.mid] = message_counter

            self.publish_lock.release()
            

    def _select_op_issuer(self, publishers):
        """Return the single pinned op-issuing publisher (seeded, reused).

        Re-selects only if the previously pinned publisher is not in the given
        (connected) list. `publishers` is assumed to be connected publishers.
        """
        by_id = {p.instance_id: p for p in publishers}
        if self._op_issuer_id is not None and self._op_issuer_id in by_id:
            return by_id[self._op_issuer_id]
        chosen = self._op_issuer_rng.choice(sorted(publishers, key=lambda p: p.instance_id))
        self._op_issuer_id = chosen.instance_id
        return chosen

    def _send_operational_requests_if_ready(self, elapsed_ms: float):
        """Send operational requests from publishers if it's time"""
        if elapsed_ms < self.next_op_time_ms or not self.all_operations:
            return

        # Get all connected publishers
        publishers = [d for d in self.device_manager.get_all_publishers() if d.is_connected]

        if not publishers:
            return
        
        # All operations are issued by a single pinned publisher (paper: "a single publisher")
        publisher = self._select_op_issuer(publishers)
        for op, op_category in self.all_operations.items():
            self._send_operational_request(publisher, op, op_category)

        # Schedule next operational request
        if hasattr(self.current_config, 'op_send_rate') and self.current_config.op_send_rate > 0:
            self.next_op_time_ms = elapsed_ms + self.current_config.op_send_rate
            

    def _send_operational_response(self, subscriber: DeviceInstance, response_topic: str, operation_type: str, correlation_data: int, op_id: str | None = None):
        """Have a subscriber send a response to an operational request

        This gets called when a subscriber receives an operational request (e.g., Access, Portability)
        from a subscriber and needs to respond with the requested data.

        op_id is the broker's tracking id echoed back verbatim (may be "0" or None for ops the
        broker doesn't track); it is forwarded as-is so the broker can match the notification.
        """
        if not isinstance(subscriber.device_definition, SubscriberDefinition):
            return

        results = GlobalDefs.CLIENT_MODULE.publish_operation_response(subscriber.mqtt_client, self.method, response_topic, operation_type, "Success", correlation_data, self.current_config.qos, op_id=op_id)

        # Track this response in pending publishes for logging
        self.publish_lock.acquire()
        now = time.time()
        
        for message_info, topic in results:
            if subscriber.mqtt_client_name not in self.pending_publishes:
                self.pending_publishes[subscriber.mqtt_client_name] = {}

            # is_response=True routes this through PUBLISH_OP_RESP logging
            self.pending_publishes[subscriber.mqtt_client_name][message_info.mid] = (
                topic, GlobalDefs.OP_PURPOSE, operation_type, now, op_id, True
            )

            # Save message counter for correlations
            subscriber.message_id_to_send_counter[message_info.mid] = correlation_data

        self.publish_lock.release()

    def _send_operational_request(self, publisher: DeviceInstance, operation: str, operation_category: str):
        """Send an operational request from a subscriber"""
        _ = operation_category  # Not currently used but available for future logic

        self.publish_lock.acquire()

        message_counter = publisher.message_count
        publisher.message_count += 1

        results = GlobalDefs.CLIENT_MODULE.publish_operation_request(
            publisher.mqtt_client,
            self.method,
            operation,
            message_counter,
            qos=self.current_config.qos
        )

        now = time.time()

        for message_info, topic in results:
            if publisher.mqtt_client_name not in self.pending_publishes:
                self.pending_publishes[publisher.mqtt_client_name] = {}

            # Op request: no broker op_id yet, logged as NONE
            self.pending_publishes[publisher.mqtt_client_name][message_info.mid] = (
                topic, GlobalDefs.OP_PURPOSE, operation, now, None, False
            )

            # Save message counter for correlations
            publisher.message_id_to_send_counter[message_info.mid] = message_counter

        self.publish_lock.release()

    def _publish_from_ready_devices(self, elapsed_ms: float):
        """Publish from all devices that are ready based on their individual publication rates"""
        ready_publishers = self.device_manager.get_publishers_ready_to_publish(elapsed_ms)

        for device_instance in ready_publishers:
            self._publish_from_device(device_instance, elapsed_ms)

    def _publish_from_device(self, device_instance: DeviceInstance, elapsed_ms: float):
        """Publish a message from a specific device"""
        device_def = device_instance.device_definition
        if not isinstance(device_def, PublisherDefinition):
            return

        # Generate payload
        payload_size = random.randint(device_def.min_payload_bytes, device_def.max_payload_bytes)
        payload = random.randbytes(payload_size) if payload_size > 0 else None

        # Publish message
        self.publish_lock.acquire()
        message_counter = device_instance.message_count

        results = GlobalDefs.CLIENT_MODULE.publish_with_purpose(
            device_instance.mqtt_client,
            self.method,
            device_def.topic,
            device_instance.current_purpose_filter,
            qos=self.current_config.qos,
            payload=payload,
            correlation_data=message_counter
        )

        now = time.time()

        for message_info, topic in results:
            if device_instance.mqtt_client_name not in self.pending_publishes:
                self.pending_publishes[device_instance.mqtt_client_name] = {}

            self.pending_publishes[device_instance.mqtt_client_name][message_info.mid] = (
                topic, device_instance.current_purpose_filter, "DATA", now, None, False
            )
            
            # Save message counter for correlations
            device_instance.message_id_to_send_counter[message_info.mid] = message_counter

        self.publish_lock.release()

        # Mark as published
        device_instance.mark_published(elapsed_ms)

    def _calculate_optimal_sleep_time(self, test_config: TestConfiguration) -> float:
        """Calculate optimal sleep time based on next event and publication schedules"""
        min_sleep = 0.001  # 1ms minimum
        max_sleep = 0.01  # 10ms maximum

        # Check next scheduled event
        time_until_next_event = self.event_scheduler.get_time_until_next_event_ms()

        # Check next publication from any device
        min_pub_period = float('inf')
        for publisher in self.device_manager.get_all_publishers():
            if isinstance(publisher.device_definition, PublisherDefinition):
                min_pub_period = min(min_pub_period, publisher.device_definition.pub_period_ms)

        # Use the minimum of event time and publication period
        if time_until_next_event is not None:
            sleep_ms = min(time_until_next_event, min_pub_period / 10.0)
        else:
            sleep_ms = min_pub_period / 10.0

        sleep_seconds = sleep_ms / 1000.0
        return max(min_sleep, min(max_sleep, sleep_seconds))

    # Event Handlers
    def _handle_connect_all(self, params):
        """Connect all devices"""
        for device in self.device_manager.get_all_instances():
            self._connect_device(device)

    def _handle_disconnect_all(self, params):
        """Disconnect all devices"""
        for device in self.device_manager.get_all_instances():
            self._disconnect_device(device)

    def _handle_connect_devices(self, params, clean_start = True):
        """Connect specific devices"""
        device_ids = params.get('devices', [])
        for device_id in device_ids:
            device_list = self.device_manager.get_all_device_instance_for_id(device_id)
            for device in device_list:
                self._connect_device(device, clean_start)

    def _handle_disconnect_devices(self, params):
        """Disconnect specific devices"""
        device_ids = params.get('devices', [])
        for device_id in device_ids:
            device_list = self.device_manager.get_all_device_instance_for_id(device_id)
            for device in device_list:
                self._disconnect_device(device)

    def _handle_reconnect_devices(self, params):
        """Reconnect specific devices"""
        self._handle_connect_devices(params, False)

    def _handle_start_publishing(self, params):
        """Start publishing for specific devices"""
        device_ids = params.get('devices', [])
        for device_id in device_ids:
            device_list = self.device_manager.get_all_device_instance_for_id(device_id)
            for device in device_list:
                if device and isinstance(device.device_definition, PublisherDefinition):
                    if not device.is_publishing:
                        device.is_publishing = True
                        elapsed_ms = self.event_scheduler.get_elapsed_ms()
                        device.last_publish_time_ms = elapsed_ms
                        console_log(ConsoleLogLevel.DEBUG, f"Started publishing for {device_id}", __name__)
                
    def _handle_start_publishing_all(self, params):
        """Start publishing for all devices"""
        for device in self.device_manager.get_all_publishers():
            if not device.is_publishing:
                device.is_publishing = True
                elapsed_ms = self.event_scheduler.get_elapsed_ms()
                device.last_publish_time_ms = elapsed_ms
                console_log(ConsoleLogLevel.DEBUG, f"Started publishing for {device.instance_id}", __name__)
                
    def _handle_stop_publishing(self, params):
        """Stop publishing for specific devices"""
        device_ids = params.get('devices', [])
        for device_id in device_ids:
            device_list = self.device_manager.get_all_device_instance_for_id(device_id)
            for device in device_list:
                if device:
                    device.is_publishing = False
                    console_log(ConsoleLogLevel.DEBUG, f"Stopped publishing for {device_id}", __name__)
                
    def _handle_stop_publishing_all(self, params):
        """Stop publishing for all devices"""
        for device in self.device_manager.get_all_publishers():
            if device.is_publishing:
                device.is_publishing = False
                console_log(ConsoleLogLevel.DEBUG, f"Stopped publishing for {device.instance_id}", __name__)

    def _handle_change_purpose(self, params):
        """Change purpose for specific devices"""
        device_ids = params.get('devices', [])
        new_purpose = params.get('new_purpose')
        
        for device_id in device_ids:
            device_list = self.device_manager.get_all_device_instance_for_id(device_id)
                 
            for device in device_list:
                
                # Publisher purposes
                if device:
                    old_purpose = device.current_purpose_filter
                    device.current_purpose_filter = new_purpose
                    console_log(ConsoleLogLevel.DEBUG, f"Changed purpose for {device.mqtt_client_name}: {old_purpose} -> {new_purpose}", __name__)

                    # Publisher change
                    if isinstance(device.device_definition, PublisherDefinition):
                        # Re-register with broker if needed (for PM methods 3 and 4)
                        device_def = device.device_definition
                        GlobalDefs.CLIENT_MODULE.register_publish_purpose_for_topic(
                            device.mqtt_client, self.method, device_def.topic, new_purpose
                        )
                    
                    # Subscriber change 
                    if isinstance(device.device_definition, SubscriberDefinition):
                        self._subscribe_device(device, True, old_purpose)
                

    def _connect_device(self, device: DeviceInstance, clean_start: bool = True):
        """Connect a single device"""
        if device.is_connected:
            return

        result_code = GlobalDefs.CLIENT_MODULE.connect_client(
            device.mqtt_client, self.broker_address, self.broker_port, clean_start
        )

        if result_code == 0:  # Success
            device.mqtt_client.loop_start()
            console_log(ConsoleLogLevel.DEBUG, f"Connecting device: {device.instance_id}", __name__)
        else:
            raise RuntimeError(f"Failed to connect device {device.instance_id}")

    def _disconnect_device(self, device: DeviceInstance):
        """Disconnect a single device"""
        if not device.is_connected:
            return

        GlobalDefs.CLIENT_MODULE.disconnect_client(device.mqtt_client)
        device.is_connected = False
        console_log(ConsoleLogLevel.INFO, f"Disconnected device: {device.instance_id}", __name__)

    def _disconnect_all_devices(self):
        """Disconnect all devices"""
        for device in self.device_manager.get_all_instances():
            self._disconnect_device(device)

    ###################################     
    #   CALLBACK FUNCTIONS
    ###################################
    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: mqtt.ConnectFlags, reason_code: ReasonCode, properties: Properties):
        """Callback for device connection"""
        
        device_instance: DeviceInstance = userdata
                
        if reason_code == 0:  # Success
            device_instance.is_connected = True

            GlobalDefs.LOGGING_MODULE.log_connect(time.time(), self.my_id, device_instance.mqtt_client_name)

            # Subscribe if this is a subscriber
            if isinstance(device_instance.device_definition, SubscriberDefinition):
                self._subscribe_device(device_instance)
                
                # Subscribe to operational topics and initialize c1 data
                self._subscribe_device_for_operations(device_instance)
                self._register_subscribers_c1_operation_data(device_instance)

            # Register publisher if needed
            if isinstance(device_instance.device_definition, PublisherDefinition):
                device_def = device_instance.device_definition
                GlobalDefs.CLIENT_MODULE.register_publish_purpose_for_topic(
                    device_instance.mqtt_client, self.method,
                    device_def.topic, device_instance.current_purpose_filter
                )
                
                self._subscribe_device_for_operations(device_instance)
                    

    def _on_disconnect(self, client: mqtt.Client, userdata: Any, flags: mqtt.DisconnectFlags, reason_code: ReasonCode, properties: Properties):
        """Callback for device disconnection"""
        if reason_code == 0:  # Success
            device_instance: DeviceInstance = userdata
            device_instance.is_connected = False

            GlobalDefs.LOGGING_MODULE.log_disconnect(time.time(), self.my_id, device_instance.mqtt_client_name)

    def _subscribe_device(self, device: DeviceInstance, existing_subscription: bool = False, previous_purpose_filter: str = ""):
        """Subscribe a device to its configured topics"""
        if not isinstance(device.device_definition, SubscriberDefinition):
            return

        device_def = device.device_definition

        self.subscribe_lock.acquire()

        results = GlobalDefs.CLIENT_MODULE.subscribe_with_purpose_filter(
            device.mqtt_client, self.method,
            device_def.topic_filter, device.current_purpose_filter,
            self.current_config.qos, existing_subscription, previous_purpose_filter
        )

        now = time.time()

        for result_code, mid, sub_id in results:
            if result_code == 0:
                if device.mqtt_client_name not in self.pending_subscribes:
                    self.pending_subscribes[device.mqtt_client_name] = {}
                self.pending_subscribes[device.mqtt_client_name][mid] = (
                    device_def.topic_filter, device.current_purpose_filter, sub_id, now
                )
                device.subscribed_topics[device_def.topic_filter] = device.current_purpose_filter

        self.subscribe_lock.release()

    def _subscribe_device_for_operations(self, device: DeviceInstance):
        """Subscribe clients to relevant operational topics"""
        
        client_id = device.mqtt_client_name
        operation_response_topic = f"{GlobalDefs.OP_RESPONSE_TOPIC}/{client_id}"
        
        if isinstance(device.device_definition, PublisherDefinition):
            topics_to_sub = [GlobalDefs.ON_TOPIC, GlobalDefs.ONP_TOPIC + "/" + client_id, operation_response_topic]
            
        elif isinstance(device.device_definition, SubscriberDefinition):
            topics_to_sub = [GlobalDefs.OR_TOPIC, GlobalDefs.ORS_TOPIC + "/" + client_id, operation_response_topic]

        # Subscribe to all operational topics
        for topic in topics_to_sub:
            self.subscribe_lock.acquire()

            results = GlobalDefs.CLIENT_MODULE.subscribe_for_operations(
                device.mqtt_client, self.method, topic
            )

            now = time.time()

            # Track subscriptions so we can log them when they complete
            for result_code, mid, sub_id in results:
                if result_code == 0:
                    if device.mqtt_client_name not in self.pending_subscribes:
                        self.pending_subscribes[device.mqtt_client_name] = {}
                    self.pending_subscribes[device.mqtt_client_name][mid] = (
                        topic, GlobalDefs.OP_PURPOSE, sub_id, now
                    )

            self.subscribe_lock.release()

    def _on_subscribe(self, client: mqtt.Client, userdata: Any, mid: Any, reason_code_list: List[ReasonCode], properties : Properties):
        
        # Make sure we're not in a subscribe
        self.subscribe_lock.acquire()
        
        # We can release immediately since this is a callback
        # We only had to block until the publications which will 
        # trigger this function had their information set
        self.subscribe_lock.release()
        
        device_instance: DeviceInstance = userdata
        
        # Check if message exists and was successful
        if device_instance.mqtt_client_name in self.pending_subscribes:
            if mid in self.pending_subscribes[device_instance.mqtt_client_name]: 
                    
                # We only do one subscribe per packet so this is always len one and is success for QoS 0/1/2
                if reason_code_list[0] == 0 or reason_code_list[0] == 1 or reason_code_list[0] == 2:
                    topic_filter, purpose_filter, sub_id, time = self.pending_subscribes[device_instance.mqtt_client_name][mid]
                    
                    if device_instance.mqtt_client_name not in self.sub_ids:
                        self.sub_ids[device_instance.mqtt_client_name] = dict()
                    self.sub_ids[device_instance.mqtt_client_name][topic_filter] = sub_id
                    
                    # Check if this is an operational subscriber
                    if topic_filter.startswith(GlobalDefs.OP_RESPONSE_TOPIC) or topic_filter.startswith(GlobalDefs.ON_TOPIC) or topic_filter.startswith(GlobalDefs.ONP_TOPIC) or topic_filter.startswith(GlobalDefs.OR_TOPIC) or topic_filter.startswith(GlobalDefs.ORS_TOPIC):
                        GlobalDefs.LOGGING_MODULE.log_op_subscribe(time, self.my_id, device_instance.mqtt_client_name, topic_filter, purpose_filter, sub_id)
                    else:
                        GlobalDefs.LOGGING_MODULE.log_subscribe(time, self.my_id, device_instance.mqtt_client_name, topic_filter, purpose_filter, sub_id)
        
    
    def _on_publish(self, client: mqtt.Client, userdata: Any, mid:int, reason_code: ReasonCode, properties: Properties):    
        
        # Make sure we're not in a publish
        self.publish_lock.acquire()
        
        # We can release immediately since this is a callback
        # We only had to block until the publications which will 
        # trigger this function had their information set
        self.publish_lock.release()
        
        device_instance: DeviceInstance = userdata
        
        # Check if message exists and was successful
        if device_instance.mqtt_client_name in self.pending_publishes:
            if mid in self.pending_publishes[device_instance.mqtt_client_name]: 
                
                corr_data = device_instance.message_id_to_send_counter[mid]
                    
                # If successful
                if reason_code == 0:
                    topic, purpose, op_type, time, op_id, is_response = self.pending_publishes[device_instance.mqtt_client_name][mid]

                    # Do not log communications to the broker
                    # if not topic[0] == '$':

                    # Check if operational or data
                    if op_type == "DATA":
                        # Log message
                        GlobalDefs.LOGGING_MODULE.log_publish(time, self.my_id, device_instance.mqtt_client_name, corr_data, topic, purpose, op_type)
                    else:
                        # Log operational message - use self.all_operations which includes C1_REG ops
                        op_category = self.all_operations.get(op_type, "UNKNOWN")
                        if is_response:
                            # Subscriber's response to an op request -> PUBLISH_OP_RESP, carrying the echoed op_id
                            GlobalDefs.LOGGING_MODULE.log_operation_response_publish(time, self.my_id, device_instance.mqtt_client_name, corr_data, topic, purpose, op_type, op_category, op_id)
                        else:
                            # Original op request -> PUBLISH_OP
                            GlobalDefs.LOGGING_MODULE.log_operation_publish(time, self.my_id, device_instance.mqtt_client_name, corr_data, topic, purpose, op_type, op_category, op_id)
                
                
    def _on_message_recv(self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage):
        
        timestamp = time.time()
        
        operational_message = False
        operational_response = False
        operation_type = ""
        sending_client = "UNKNOWN"
        op_message_type = "OP"
        correlation_data = -1
        op_id: str | None = None
        sub_id: List[int] = list()
        device_instance: DeviceInstance = userdata
        
        # Check properties
        if message.properties is not None:
            if hasattr(message.properties, "UserProperty"):
                for name, value in message.properties.UserProperty:
                    if name == GlobalDefs.PROPERTY_OPTYPE:
                        operational_message = True
                        operation_type = value
                    elif name == GlobalDefs.PROPERTY_ID:
                        sending_client = value
                    elif name == GlobalDefs.PROPERTY_OP_STATUS:
                        operational_response = True
                        op_message_type = value
                    elif name == GlobalDefs.PROPERTY_OP_ID:
                        op_id = value  # broker op_id (decimal string)
                        
            if hasattr(message.properties, "CorrelationData"):
                correlation_data = int.from_bytes(message.properties.CorrelationData, byteorder='big', signed=False)
                
            if hasattr(message.properties, "SubscriptionIdentifier"):
                sub_id = message.properties.SubscriptionIdentifier
            else:
                sub_id.append(-1)

        # Log messages based on type
        if operational_message:
            op_category = self.current_config.all_operations.get(operation_type, "UNKNOWN")
            
            if operational_response:
                GlobalDefs.LOGGING_MODULE.log_operation_response_recv(timestamp, self.my_id, device_instance.mqtt_client_name, sending_client, correlation_data, message.topic, operation_type, op_category, op_message_type, sub_id[0], op_id)
            else:    
                GlobalDefs.LOGGING_MODULE.log_operation_recv(timestamp, self.my_id, device_instance.mqtt_client_name, sending_client, correlation_data, message.topic, operation_type, op_category, op_message_type, sub_id[0])

                # Send respones to all received requests assuming we're a subscriber
                if isinstance(device_instance.device_definition, SubscriberDefinition) and hasattr(message.properties, "ResponseTopic") and message.properties.ResponseTopic:
                    self._send_operational_response(device_instance, message.properties.ResponseTopic, operation_type, correlation_data, op_id)
        else:
            GlobalDefs.LOGGING_MODULE.log_recv(timestamp, self.my_id, device_instance.mqtt_client_name, sending_client, correlation_data, message.topic, "DATA", sub_id[0])