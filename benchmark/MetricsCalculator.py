import statistics
import sys
from paho.mqtt.client import topic_matches_sub
from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional, Tuple, Any
from pathlib import Path
import GlobalDefs
from LoggingModule import (
    SEPARATOR, PM_METHOD_LABEL, CPU_METRICS_LABEL, MEM_METRICS_LABEL,
    CONNECT_LABEL, DISCONNECT_LABEL, SUBSCRIBE_LABEL, OP_SUBSCRIBE_LABEL, PUBLISH_LABEL,
    OP_PUBLISH_LABEL, RECV_LABEL, OP_RECV_LABEL, OP_RESP_PUBLISH_LABEL, OP_RESP_RECV_LABEL
)

@dataclass
class ConnectEvent:
    """Client connection event"""
    timestamp: float
    benchmark_id: str
    client_id: str
    
@dataclass
class DisconnectEvent:
    """Client disconnection event"""
    timestamp: float
    benchmark_id: str
    client_id: str

@dataclass
class PublishEvent:
    """Message publication event"""
    timestamp: float
    benchmark_id: str
    client_id: str
    topic: str
    purpose: str
    msg_type: str
    corr_data: int


@dataclass
class RecvEvent:
    """Message reception event"""
    timestamp: float
    benchmark_id: str
    recv_client_id: str
    sending_client_id: str
    topic: str
    sub_id: str
    msg_type: str
    corr_data: int


@dataclass
class SubscribeEvent:
    """Subscription event"""
    timestamp: float
    end_timestamp: float
    benchmark_id: str
    client_id: str
    topic_filter: str
    purpose_filter: str
    sub_id: str
    
@dataclass
class OperationSubscribeEvent:
    """Operations subscription event"""
    timestamp: float
    benchmark_id: str
    client_id: str
    topic_filter: str
    purpose_filter: str
    sub_id: str


@dataclass
class OperationPublishEvent:
    """Operational message publication event (PUBLISH_OP)"""
    timestamp: float
    benchmark_id: str
    client_id: str
    topic: str
    purpose: str
    op_type: str
    op_category: str
    corr_data: int
    op_id: str = "NONE"  # broker-assigned operation id


@dataclass
class OperationRespPublishEvent:
    """Operational response publication event (PUBLISH_OP_RESP)"""
    timestamp: float
    benchmark_id: str
    client_id: str
    topic: str
    purpose: str
    op_type: str
    op_category: str
    corr_data: int
    op_id: str = "NONE"  # broker-assigned operation id


@dataclass
class OperationRecvEvent:
    """Operational message reception event"""
    timestamp: float
    benchmark_id: str
    recv_client_id: str
    sending_client_id: str
    topic: str
    sub_id: str
    op_type: str
    op_category: str
    op_status: str
    corr_data: int

@dataclass
class OperationRespRecvEvent:
    """Operational message response reception event"""
    timestamp: float
    benchmark_id: str
    recv_client_id: str
    sending_client_id: str
    topic: str
    sub_id: str
    op_type: str
    op_category: str
    op_status: str
    corr_data: int
    op_id: str = "NONE"


@dataclass
class BrokerStats:
    """Broker resource usage stats"""
    cpu_min: float = 0.0
    cpu_max: float = 0.0
    cpu_avg: float = 0.0
    cpu_variance: float = 0.0
    mem_min: float = 0.0
    mem_max: float = 0.0
    mem_avg: float = 0.0
    mem_variance: float = 0.0


@dataclass
class MessagingStats:
    """Messaging performance stats"""
    latency_min_ms: float = 0.0
    latency_max_ms: float = 0.0
    latency_avg_ms: float = 0.0
    latency_variance_ms: float = 0.0
    throughput_msgs_per_sec: float = 0.0
    avg_header_size_bytes: float = 0.0
    non_data_msg_count: int = 0
    total_data_msg_count: int = 0


@dataclass
class SubscriberPurposeCorrectness:
    """Purpose correctness per subscriber"""
    subscriber_id: str
    valid_recv_count: int = 0
    invalid_recv_count: int = 0
    expected_msg_count: int = 0
    bad_reject: int = 0
    total_recv_count: int = 0
    false_accept: float = 0.0
    false_reject: float = 0.0


@dataclass
class OPCorrectnessMetrics:
    """OP correctness per request"""
    expected_responses: int = 0
    actual_responses: int = 0
    relevant_recv: int = 0
    irrelevant_recv: int = 0
    relevant_subs: int = 0
    irrelevant_subs: int = 0
    all_subscribers: int = 0
    coverage: float = 0.0
    leakage: float = 0.0
    completion: float = 0.0


@dataclass
class OperationCorrelation:
    """Per-operation request/response correlation, one row per PUBLISH_OP request"""
    op_id: str
    op_type: str
    completed: bool = False                              # did any responder reply?
    num_responders: int = 0                              # distinct responders
    first_response_latency_ms: Optional[float] = None    # request -> first response
    last_response_latency_ms: Optional[float] = None     # request -> last response


@dataclass
class O1InformedMetrics:
    """Right-to-be-Informed (O1): broker auto-fulfils on first data receipt, so it is
    measured separately from the correlated ops rather than joined to its requests"""
    register_info_requests: int = 0
    informed_notifications: int = 0
    expected_pairs: int = 0
    completion_rate: float = 0.0


@dataclass
class TestMetrics:
    """Test metrics"""
    test_name: str
    pm_method: str
    broker_stats: BrokerStats = field(default_factory=BrokerStats)
    messaging_stats: MessagingStats = field(default_factory=MessagingStats)
    purpose_correctness_per_sub: Dict[str, SubscriberPurposeCorrectness] = field(default_factory=dict)
    op_correctness: List[OPCorrectnessMetrics] = field(default_factory=list)
    op_correlations: List[OperationCorrelation] = field(default_factory=list)
    o1_informed: O1InformedMetrics = field(default_factory=O1InformedMetrics)


class MetricsCalculator:
    """Calculate metrics from test logs"""
    
    connect_events: List[ConnectEvent]
    disconnect_events: List[DisconnectEvent]
    publish_events: List[PublishEvent]
    recv_events: List[RecvEvent]
    subscribe_events: List[SubscribeEvent]
    op_subscribe_events: List[OperationSubscribeEvent]
    op_publish_events: List[OperationPublishEvent]
    op_resp_publish_events: List[OperationRespPublishEvent]
    op_recv_events: List[OperationRecvEvent]
    op_resp_recv_events: List[OperationRespRecvEvent]
    
    client_subscription_periods: Dict[str, Dict[str, List[Tuple[float, float | None]]]] # Map of clients -> topic:purpose -> valid ranges of time

    pm_method: Optional[str] = None
    cpu_metrics: Optional[Tuple[float, float, float, float]] = None
    mem_metrics: Optional[Tuple[float, float, float, float]] = None

    subscriber_subscriptions: Dict[str, List[SubscribeEvent]]

    def __init__(self):
        self.connect_events = []
        self.disconnect_events = []
        self.publish_events = []
        self.recv_events = []
        self.subscribe_events = []
        self.op_subscribe_events = []
        self.op_publish_events = []
        self.op_resp_publish_events = []
        self.op_recv_events = []
        self.op_resp_recv_events = []
        
        self.client_subscription_periods = {}

        self.subscriber_subscriptions = {}

    def parse_log_file(self, log_file_path: str) -> bool:
        """Parse log file and extract events"""
        log_path = Path(log_file_path)
        if not log_path.exists():
            print(f"Log file not found: {log_file_path}")
            return False

        with open(log_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                parts = line.split(SEPARATOR)
                if len(parts) < 2:
                    continue

                label = parts[0]
                event: Any

                try:
                    if label == PM_METHOD_LABEL:
                        self.pm_method = parts[1]

                    elif label == CPU_METRICS_LABEL:
                        self.cpu_metrics = (
                            float(parts[1]),  # min
                            float(parts[2]),  # max
                            float(parts[3]),  # avg
                            float(parts[4])   # variance
                        )

                    elif label == MEM_METRICS_LABEL:
                        self.mem_metrics = (
                            float(parts[1]),  # min
                            float(parts[2]),  # max
                            float(parts[3]),  # avg
                            float(parts[4])   # variance
                        )
                        
                    elif label == CONNECT_LABEL:
                        # CONNECT@@timestamp@@benchmark_id@@client_id
                        event = ConnectEvent(
                            timestamp=float(parts[1]),
                            benchmark_id=parts[2],
                            client_id=parts[3]
                        )
                        self.connect_events.append(event)
                        
                    elif label == DISCONNECT_LABEL:
                        # DISCONNECT@@timestamp@@benchmark_id@@client_id
                        event = DisconnectEvent(
                            timestamp=float(parts[1]),
                            benchmark_id=parts[2],
                            client_id=parts[3]
                        )
                        self.disconnect_events.append(event)

                    elif label == PUBLISH_LABEL:
                        # PUBLISH@@timestamp@@benchmark_id@@client_id@@topic@@purpose@@msg_type@@corr_data
                        event = PublishEvent(
                            timestamp=float(parts[1]),
                            benchmark_id=parts[2],
                            client_id=parts[3],
                            topic=parts[4],
                            purpose=parts[5],
                            msg_type=parts[6],
                            corr_data=int(parts[7])
                        )
                        self.publish_events.append(event)

                    elif label == RECV_LABEL:
                        # RECV@@timestamp@@benchmark_id@@recv_client@@sending_client@@topic@@sub_id@@msg_type@@corr_data
                        event = RecvEvent(
                            timestamp=float(parts[1]),
                            benchmark_id=parts[2],
                            recv_client_id=parts[3],
                            sending_client_id=parts[4],
                            topic=parts[5],
                            sub_id=parts[6],
                            msg_type=parts[7],
                            corr_data=int(parts[8])
                        )
                        self.recv_events.append(event)

                    elif label == SUBSCRIBE_LABEL:
                        # SUBSCRIBE@@timestamp@@benchmark_id@@client_id@@topic_filter@@purpose_filter@@sub_id
                        event = SubscribeEvent(
                            timestamp=float(parts[1]),
                            end_timestamp=sys.float_info.max,
                            benchmark_id=parts[2],
                            client_id=parts[3],
                            topic_filter=parts[4],
                            purpose_filter=parts[5],
                            sub_id=parts[6]
                        )
                        self.subscribe_events.append(event)

                        # Track subscriber subscriptions
                        if event.client_id not in self.subscriber_subscriptions:
                            self.subscriber_subscriptions[event.client_id] = []
                        self.subscriber_subscriptions[event.client_id].append(event)
                        
                    elif label == OP_SUBSCRIBE_LABEL:
                        # SUBSCRIBE_OP@@timestamp@@benchmark_id@@client_id@@topic_filter@@purpose_filter@@sub_id
                        event = OperationSubscribeEvent(
                            timestamp=float(parts[1]),
                            benchmark_id=parts[2],
                            client_id=parts[3],
                            topic_filter=parts[4],
                            purpose_filter=parts[5],
                            sub_id=parts[6]
                        )
                        self.op_subscribe_events.append(event)

                    elif label == OP_PUBLISH_LABEL:
                        # PUBLISH_OP@@timestamp@@benchmark_id@@client_id@@topic@@purpose@@op_type@@op_category@@corr_data@@op_id
                        event = OperationPublishEvent(
                            timestamp=float(parts[1]),
                            benchmark_id=parts[2],
                            client_id=parts[3],
                            topic=parts[4],
                            purpose=parts[5],
                            op_type=parts[6],
                            op_category=parts[7],
                            corr_data=int(parts[8]),
                            op_id=parts[9] if len(parts) > 9 else "NONE"
                        )
                        self.op_publish_events.append(event)

                    elif label == OP_RESP_PUBLISH_LABEL:
                        # PUBLISH_OP_RESP@@timestamp@@benchmark_id@@client_id@@topic@@purpose@@op_type@@op_category@@corr_data@@op_id
                        event = OperationRespPublishEvent(
                            timestamp=float(parts[1]),
                            benchmark_id=parts[2],
                            client_id=parts[3],
                            topic=parts[4],
                            purpose=parts[5],
                            op_type=parts[6],
                            op_category=parts[7],
                            corr_data=int(parts[8]),
                            op_id=parts[9] if len(parts) > 9 else "NONE"
                        )
                        self.op_resp_publish_events.append(event)

                    elif label == OP_RECV_LABEL:
                        # RECV_OP@@timestamp@@benchmark_id@@recv_client@@sending_client@@topic@@sub_id@@op_type@@op_category@@op_status@@corr_data
                        event = OperationRecvEvent(
                            timestamp=float(parts[1]),
                            benchmark_id=parts[2],
                            recv_client_id=parts[3],
                            sending_client_id=parts[4],
                            topic=parts[5],
                            sub_id=parts[6],
                            op_type=parts[7],
                            op_category=parts[8],
                            op_status=parts[9],
                            corr_data=int(parts[10])
                        )
                        self.op_recv_events.append(event)
                        
                    elif label == OP_RESP_RECV_LABEL:
                        # RECV_OP_RESP@@timestamp@@benchmark_id@@recv_client@@sending_client@@topic@@sub_id@@op_type@@op_category@@op_status@@corr_data@@op_id
                        event = OperationRespRecvEvent(
                            timestamp=float(parts[1]),
                            benchmark_id=parts[2],
                            recv_client_id=parts[3],
                            sending_client_id=parts[4],
                            topic=parts[5],
                            sub_id=parts[6],
                            op_type=parts[7],
                            op_category=parts[8],
                            op_status=parts[9],
                            corr_data=int(parts[10]),
                            op_id=parts[11] if len(parts) > 11 else "NONE"
                        )
                        self.op_resp_recv_events.append(event)

                except (IndexError, ValueError) as e:
                    print(f"Error parsing line: {line}")
                    print(f"Error: {e}")
                    continue

        return True
    
    def _parse_subscription_periods(self) -> None:        
        for client_id, sub_list in self.subscriber_subscriptions.items():
            
            # Get all disconnection events for client
            disconnect_events = [e for e in self.disconnect_events if e.client_id == client_id]
            
            for sub_event in sub_list:       
                
                # Find the next disconnection event if there is one
                post_sub_disconnect_events = [e for e in disconnect_events if e.timestamp >= sub_event.timestamp]

                next_disconnect_event = None
                if len(post_sub_disconnect_events) != 0:
                    next_disconnect_event = min(post_sub_disconnect_events, key=lambda e: e.timestamp)

                # Also find sub events for this client and topic after this point
                same_topic_sub_events = [e for e in self.subscribe_events if e.client_id == client_id and e.topic_filter == sub_event.topic_filter and e.timestamp > sub_event.timestamp]       

                next_sub_event = None
                if len(same_topic_sub_events) != 0:
                    next_sub_event = min(same_topic_sub_events, key=lambda e: e.timestamp)

                # Find the next event that overrides this subscription
                if next_disconnect_event is not None and next_sub_event is not None:
                    end_time = min(next_disconnect_event.timestamp, next_sub_event.timestamp)
                elif next_disconnect_event is not None:
                    end_time = next_disconnect_event.timestamp
                elif next_sub_event is not None:
                    end_time = next_sub_event.timestamp
                else:
                    end_time = sys.float_info.max
                    
                # Now save
                sub_event.end_timestamp = end_time

    def calculate_broker_stats(self) -> BrokerStats:
        """Calculate broker resource usage"""
        stats = BrokerStats()

        if self.cpu_metrics:
            stats.cpu_min = self.cpu_metrics[0]
            stats.cpu_max = self.cpu_metrics[1]
            stats.cpu_avg = self.cpu_metrics[2]
            stats.cpu_variance = self.cpu_metrics[3]

        if self.mem_metrics:
            stats.mem_min = self.mem_metrics[0]
            stats.mem_max = self.mem_metrics[1]
            stats.mem_avg = self.mem_metrics[2]
            stats.mem_variance = self.mem_metrics[3]

        return stats

    def calculate_messaging_stats(self) -> MessagingStats:
        """Calculate messaging performance"""
        stats = MessagingStats()

        # Calculate latencies by matching publish and recv events
        latencies: List[float] = []

        # Create mapping of (client_id, corr_data) to publish timestamp
        publish_map: Dict[Tuple[str, int], float] = {}
        for pub_event in self.publish_events:
            key = (pub_event.client_id, pub_event.corr_data)
            publish_map[key] = pub_event.timestamp

        # Match recv events with publish events
        for recv_event in self.recv_events:
            key = (recv_event.sending_client_id, recv_event.corr_data)
            if key in publish_map:
                latency_ms = (recv_event.timestamp - publish_map[key]) * 1000.0
                latencies.append(latency_ms)

        # Calculate latency statistics
        if latencies:
            stats.latency_min_ms = min(latencies)
            stats.latency_max_ms = max(latencies)
            stats.latency_avg_ms = statistics.mean(latencies)
            if len(latencies) > 1:
                stats.latency_variance_ms = statistics.variance(latencies)

        # Calculate throughput
        stats.total_data_msg_count = len(self.recv_events)
        if self.recv_events:
            test_duration_s = (
                self.recv_events[-1].timestamp - self.recv_events[0].timestamp
            )
            if test_duration_s > 0:
                stats.throughput_msgs_per_sec = len(self.recv_events) / test_duration_s

        # Count non-data messages
        stats.non_data_msg_count = len(self.op_publish_events)

        # Calculate average header size (estimate based on MQTT v5)
        # This is a simplified estimation
        header_sizes: List[int] = []
        for pub_event in self.publish_events:
            # MQTT v5 fixed header: ~2-5 bytes
            # Variable header for PUBLISH: topic length + topic + properties
            # Properties: purpose (user property) + correlation data
            base_header = 5
            topic_bytes = len(pub_event.topic.encode('utf-8')) + 2
            purpose_bytes = len(pub_event.purpose.encode('utf-8')) + 4  # user property overhead
            corr_data_bytes = 8  # correlation data size

            total_header = base_header + topic_bytes + purpose_bytes + corr_data_bytes
            header_sizes.append(total_header)

        if header_sizes:
            stats.avg_header_size_bytes = statistics.mean(header_sizes)

        return stats

    def calculate_purpose_correctness(self) -> Dict[str, SubscriberPurposeCorrectness]:
        """Calculate purpose correctness per subscriber"""
        results: Dict[str, SubscriberPurposeCorrectness] = {}

        # Index publishes by (client_id, corr_data) and group recvs by subscriber so the
        # per-message work below is O(1) lookups instead of rescanning every publish
        pub_by_key: Dict[Tuple[str, int], PublishEvent] = {}
        for pub in self.publish_events:
            pub_by_key.setdefault((pub.client_id, pub.corr_data), pub)

        recvs_by_subscriber: Dict[str, List] = {}
        for recv in self.recv_events:
            recvs_by_subscriber.setdefault(recv.recv_client_id, []).append(recv)

        # Cache the matcher results; publishes share topics and purposes
        topic_match_cache: Dict[Tuple[str, str], bool] = {}
        def topic_matched_cached(topic_filter: str, topic: str) -> bool:
            key = (topic_filter, topic)
            if key not in topic_match_cache:
                topic_match_cache[key] = topic_matches_sub(topic_filter, topic)
            return topic_match_cache[key]

        purpose_match_cache: Dict[Tuple[str, str], bool] = {}
        def purpose_matched_cached(purpose: str, purpose_filter: str) -> bool:
            key = (purpose, purpose_filter)
            if key not in purpose_match_cache:
                purpose_match_cache[key] = GlobalDefs.purpose_described_by_filter(purpose, purpose_filter)
            return purpose_match_cache[key]

        # For each subscriber, check if received messages match their purpose filter
        for subscriber_id, subscriptions in self.subscriber_subscriptions.items():
            metrics = SubscriberPurposeCorrectness(subscriber_id=subscriber_id)

            # Get all messages received by this subscriber
            subscriber_recvs = recvs_by_subscriber.get(subscriber_id, [])

            metrics.total_recv_count = len(subscriber_recvs)

            # Group this subscriber's recvs by their source publish for the expected-count pass
            recvs_by_pub_key: Dict[Tuple[str, int], List] = {}
            for recv in subscriber_recvs:
                recvs_by_pub_key.setdefault((recv.sending_client_id, recv.corr_data), []).append(recv)

            # For each received message, check if purpose matches any subscription
            for recv_event in subscriber_recvs:
                # Find the corresponding publish event to get the purpose
                pub_event = pub_by_key.get((recv_event.sending_client_id, recv_event.corr_data))

                if pub_event is None:
                    continue

                # Check if this message's purpose matches any of the subscriber's filters during the subscription time
                for sub in subscriptions:
                    topic_matched = topic_matched_cached(sub.topic_filter, pub_event.topic)
                    purpose_matched = purpose_matched_cached(pub_event.purpose, sub.purpose_filter)
                    time_valid = (recv_event.timestamp >= sub.timestamp and (sub.end_timestamp is None or recv_event.timestamp <= sub.end_timestamp))
                    
                    if topic_matched and purpose_matched and time_valid:
                        metrics.valid_recv_count += 1
                    elif time_valid and topic_matched and not purpose_matched:
                        metrics.invalid_recv_count += 1

            # Calculate expected message count
            # For this, we need to check all messages sent, and see if there was a valid subscription
            # during this time. This is similar to above, but we care about pub events not recv events
            for pub_event in self.publish_events:

                # Check if we have a subscription for this message with compatible purposes and the relevant time
                matched_subs = 0
                for sub in subscriptions:
                    
                    topic_matched = topic_matched_cached(sub.topic_filter, pub_event.topic)
                    purpose_matched = purpose_matched_cached(pub_event.purpose, sub.purpose_filter)
                    time_valid = (pub_event.timestamp >= sub.timestamp and (sub.end_timestamp is None or pub_event.timestamp <= sub.end_timestamp))
                    
                    if topic_matched and purpose_matched and time_valid:
                        matched_subs += 1
                        
                        
                if matched_subs > 0:
                    metrics.expected_msg_count += matched_subs
                    candidate_recvs = recvs_by_pub_key.get((pub_event.client_id, pub_event.corr_data), [])
                    actual_received = sum(1 for e in candidate_recvs if e.topic.startswith(pub_event.topic))
                    
                    if actual_received < matched_subs:
                        metrics.bad_reject += (matched_subs - actual_received)               
            
            # Calculate false accept and false reject
            if metrics.total_recv_count > 0:
                metrics.false_accept = metrics.invalid_recv_count / metrics.total_recv_count
            else:
                metrics.false_accept = 0.0

            if metrics.expected_msg_count > 0:
                metrics.false_reject = metrics.bad_reject / metrics.expected_msg_count
            else:
                metrics.false_reject = 0.0

            results[subscriber_id] = metrics

        return results

    def calculate_op_correctness(self) -> List[OPCorrectnessMetrics]:
        """Calculate OP correctness per request"""
        results: List[OPCorrectnessMetrics] = []

        # Group operation requests by correlation data (request ID)
        request_pubs: Dict[int, OperationPublishEvent] = {}
        for op_pub in self.op_publish_events:
            if op_pub.op_type == "REGISTER-INFO":
                continue  # O1 is measured separately in calculate_o1_informed
            request_pubs[op_pub.corr_data] = op_pub
            
            metrics = OPCorrectnessMetrics()
            
            # Count all subscribers
            metrics.all_subscribers = len(self.subscriber_subscriptions)
            
            # Check which subscribers recieved the op
            op_reqs_recv = [e for e in self.op_recv_events 
                            if op_pub.client_id == e.sending_client_id
                            and op_pub.op_type == e.op_type
                            and op_pub.corr_data == e.corr_data]
            
            # Check which responses were recieved for this op
            op_resp_recv = [e for e in self.op_resp_recv_events 
                            if op_pub.client_id == e.recv_client_id
                            and op_pub.op_type == e.op_type
                            and op_pub.corr_data == e.corr_data
                            and e.sending_client_id != "Broker"]
            
            # Check if the broker responded
            op_resp_from_broker = [e for e in self.op_resp_recv_events 
                                    if op_pub.client_id == e.recv_client_id
                                    and op_pub.op_type == e.op_type
                                    and op_pub.corr_data == e.corr_data
                                    and e.sending_client_id == "Broker"]

            # Determine relevant vs irrelevant requests
            
            # We first need to see every message this client sent on which topics and purposes
            pubs_by_client = [e for e in self.publish_events if e.client_id == op_pub.client_id]
            
            # Find subscribers that should respond
            relevant_sub_clients = list()
            for pub in pubs_by_client:
                
                for subscriber_id, subs in self.subscriber_subscriptions.items():
                    
                    # Don't run this if we already have this subscriber
                    if subscriber_id in relevant_sub_clients:
                        continue
                    
                    found = False      
                    for sub in subs:
                        
                        topic_matched = topic_matches_sub(sub.topic_filter, pub.topic)
                        purpose_matched = GlobalDefs.purpose_described_by_filter(pub.purpose, sub.purpose_filter)
                        time_valid = (pub.timestamp >= sub.timestamp and (sub.end_timestamp is None or pub.timestamp <= sub.end_timestamp))
                        
                        if topic_matched and purpose_matched and time_valid:
                            found = True
                            break
                        
                    if found:
                        relevant_sub_clients.append(subscriber_id)
                        
            metrics.relevant_subs = len(relevant_sub_clients)
            metrics.irrelevant_subs = (metrics.all_subscribers - len(relevant_sub_clients))    
            
            # Now for every recv, see if subscriber was relevant
            for req_recv in op_reqs_recv:
                if req_recv.recv_client_id in relevant_sub_clients:
                    metrics.relevant_recv += 1
                else:
                    metrics.irrelevant_recv += 1
                    
            if metrics.relevant_recv < metrics.relevant_subs and op_pub.op_category == 'C1':
                
                # For C1 ops alone, it's possible only the broker will respond
                if len(op_resp_from_broker) > 0:
                    
                    # In this case alone, we treat this as 100% response
                    metrics.relevant_subs = len(op_resp_from_broker)
                    metrics.relevant_recv = len(op_resp_from_broker)

            # Completion: Did every subscriber who recieved send a response
            for req_recv in op_reqs_recv:
                metrics.expected_responses += 1
                
                # Check for matching response
                found = False
                for resp_recv in op_resp_recv:             
                    if resp_recv.sending_client_id == req_recv.recv_client_id:
                        found = True
                        break
                
                if found:
                    metrics.actual_responses += 1

            # Calculate derived metrics
            if metrics.expected_responses > 0 or metrics.relevant_subs > 0:
                
                if metrics.relevant_subs != 0:
                    metrics.coverage = metrics.relevant_recv / metrics.relevant_subs
                else:
                    metrics.coverage = 1.0
                    
                if metrics.irrelevant_subs != 0:    
                    metrics.leakage = metrics.irrelevant_recv / metrics.irrelevant_subs
                else:
                    metrics.leakage = 0.0
                    
                if metrics.expected_responses != 0:    
                    metrics.completion = metrics.actual_responses / metrics.expected_responses
                else:
                    metrics.completion = 1.0

                results.append(metrics)

        return results

    def calculate_op_correlation(self) -> List[OperationCorrelation]:
        """Correlate PUBLISH_OP requests with their responses by (publisher, op_type, corr_data)"""
        results: List[OperationCorrelation] = []

        # Index responses by (publisher, op_type, corr_data); track source to de-duplicate
        resp_by_key: Dict[tuple, List[dict]] = {}

        for resp in self.op_resp_publish_events:  # subscriber responses
            publisher = resp.topic.rsplit('/', 1)[-1]
            resp_by_key.setdefault((publisher, resp.op_type, resp.corr_data), []).append(
                {'ts': resp.timestamp, 'responder': resp.client_id,
                 'op_id': resp.op_id, 'source': 'subscriber'})

        for resp in self.op_resp_recv_events:  # broker-origin responses (e.g. AUDIT, no-subscriber failures)
            if resp.sending_client_id != "Broker":
                continue
            if resp.op_status not in ("Success", "Failure"):
                continue  # ignore non-terminal Pending/Informed notifications
            resp_by_key.setdefault((resp.recv_client_id, resp.op_type, resp.corr_data), []).append(
                {'ts': resp.timestamp, 'responder': "Broker",
                 'op_id': resp.op_id, 'source': 'broker'})

        for op_pub in self.op_publish_events:
            if op_pub.op_type == "REGISTER-INFO":
                continue  # O1 is measured separately in calculate_o1_informed
            responses = resp_by_key.get((op_pub.client_id, op_pub.op_type, op_pub.corr_data), [])

            # Prefer subscriber responses over broker ones for the same request
            subscriber_resps = [r for r in responses if r['source'] == 'subscriber']
            chosen = subscriber_resps if subscriber_resps else responses

            op_id = "NONE"
            for r in chosen:
                if r['op_id'] != "NONE":
                    op_id = r['op_id']
                    break

            corr = OperationCorrelation(op_id=op_id, op_type=op_pub.op_type)
            if chosen:
                corr.completed = True
                corr.num_responders = len({r['responder'] for r in chosen})
                resp_times = sorted(r['ts'] for r in chosen)
                corr.first_response_latency_ms = (resp_times[0] - op_pub.timestamp) * 1000.0
                corr.last_response_latency_ms = (resp_times[-1] - op_pub.timestamp) * 1000.0

            results.append(corr)

        return results

    @staticmethod
    def _percentile(sorted_values: List[float], pct: float) -> float:
        """Linear-interpolation percentile of an already-sorted list. Returns 0.0 if empty."""
        if not sorted_values:
            return 0.0
        if len(sorted_values) == 1:
            return sorted_values[0]
        rank = (pct / 100.0) * (len(sorted_values) - 1)
        low = int(rank)
        high = min(low + 1, len(sorted_values) - 1)
        frac = rank - low
        return sorted_values[low] + (sorted_values[high] - sorted_values[low]) * frac

    def _aggregate_op_correlation(self, correlations: List[OperationCorrelation]) -> Dict[str, Dict[str, float]]:
        """Aggregate per-operation correlation rows by op_type"""
        by_type: Dict[str, List[OperationCorrelation]] = {}
        for c in correlations:
            by_type.setdefault(c.op_type, []).append(c)

        agg: Dict[str, Dict[str, float]] = {}
        for op_type, items in by_type.items():
            total = len(items)
            completed = sum(1 for c in items if c.completed)
            latencies = sorted(
                c.first_response_latency_ms for c in items
                if c.first_response_latency_ms is not None
            )
            agg[op_type] = {
                'total': float(total),
                'completed': float(completed),
                'completion_rate': (completed / total) if total else 0.0,
                'median_latency_ms': self._percentile(latencies, 50),
                'p90_latency_ms': self._percentile(latencies, 90),
                'p99_latency_ms': self._percentile(latencies, 99),
            }
        return agg

    def calculate_o1_informed(self) -> O1InformedMetrics:
        """O1 (REGISTER-INFO / Informed) is auto-fulfilled by the broker on first data
        receipt (paper 7.1), so it is measured as Informed notifications received against
        one expected per publisher-subscriber data-flow pair, not by a request join"""
        m = O1InformedMetrics()
        m.register_info_requests = sum(1 for e in self.op_publish_events if e.op_type == "REGISTER-INFO")
        m.informed_notifications = sum(1 for e in self.op_resp_recv_events if e.op_type == "Informed")
        pairs = {(e.sending_client_id, e.recv_client_id) for e in self.recv_events}
        m.expected_pairs = len(pairs)
        if m.expected_pairs > 0:
            m.completion_rate = m.informed_notifications / m.expected_pairs
        return m

    def calculate_all_metrics(self, log_file_path: str, test_name: str = "test") -> Optional[TestMetrics]:
        """Calculate all metrics"""
        if not self.parse_log_file(log_file_path):
            return None

        # Need to determine the ranges for which clients were subscribed to topics for purposes to
        # make sure we don't count a miss for a subscriber that wasn't active
        self._parse_subscription_periods()

        metrics = TestMetrics(
            test_name=test_name,
            pm_method=self.pm_method or "Unknown"
        )

        metrics.broker_stats = self.calculate_broker_stats()
        metrics.messaging_stats = self.calculate_messaging_stats()
        metrics.purpose_correctness_per_sub = self.calculate_purpose_correctness()
        metrics.op_correctness = self.calculate_op_correctness()
        metrics.op_correlations = self.calculate_op_correlation()
        metrics.o1_informed = self.calculate_o1_informed()

        return metrics

    def print_metrics(self, metrics: TestMetrics):
        """Print metrics"""
        print(f"\n{'='*80}")
        print(f"METRICS REPORT: {metrics.test_name}")
        print(f"Purpose Management Method: {metrics.pm_method}")
        print(f"{'='*80}")

        # Broker Stats
        print(f"\n--- Broker Statistics ---")
        print(f"CPU Usage:")
        print(f"  Min:      {metrics.broker_stats.cpu_min:.5f}%")
        print(f"  Max:      {metrics.broker_stats.cpu_max:.5f}%")
        print(f"  Average:  {metrics.broker_stats.cpu_avg:.5f}%")
        print(f"  Variance: {metrics.broker_stats.cpu_variance:.5f}")
        print(f"\nMemory Usage:")
        print(f"  Min:      {metrics.broker_stats.mem_min:.5f} MB")
        print(f"  Max:      {metrics.broker_stats.mem_max:.5f} MB")
        print(f"  Average:  {metrics.broker_stats.mem_avg:.5f} MB")
        print(f"  Variance: {metrics.broker_stats.mem_variance:.5f}")

        # Messaging Stats
        print(f"\n--- Messaging Statistics ---")
        print(f"Latency:")
        print(f"  Min:      {metrics.messaging_stats.latency_min_ms:.5f} ms")
        print(f"  Max:      {metrics.messaging_stats.latency_max_ms:.5f} ms")
        print(f"  Average:  {metrics.messaging_stats.latency_avg_ms:.5f} ms")
        print(f"  Variance: {metrics.messaging_stats.latency_variance_ms:.5f}")
        print(f"\nThroughput: {metrics.messaging_stats.throughput_msgs_per_sec:.5f} msgs/sec")
        print(f"Average Header Size: {metrics.messaging_stats.avg_header_size_bytes:.5f} bytes")
        print(f"Data Messages: {metrics.messaging_stats.total_data_msg_count}")
        print(f"Non-Data Messages: {metrics.messaging_stats.non_data_msg_count}")

        # Purpose Correctness Summary
        print(f"\n--- Purpose Correctness Summary ---")
        if metrics.purpose_correctness_per_sub:
            total_subs = len(metrics.purpose_correctness_per_sub)
            total_valid = sum(pc.valid_recv_count for pc in metrics.purpose_correctness_per_sub.values())
            total_invalid = sum(pc.invalid_recv_count for pc in metrics.purpose_correctness_per_sub.values())
            total_expected = sum(pc.expected_msg_count for pc in metrics.purpose_correctness_per_sub.values())
            total_received = sum(pc.total_recv_count for pc in metrics.purpose_correctness_per_sub.values())
            total_rejected = sum(pc.bad_reject for pc in metrics.purpose_correctness_per_sub.values())
            avg_false_accept = sum(pc.false_accept for pc in metrics.purpose_correctness_per_sub.values()) / total_subs
            avg_false_reject = sum(pc.false_reject for pc in metrics.purpose_correctness_per_sub.values()) / total_subs

            print(f"Total Subscribers:       {total_subs}")
            print(f"Total Valid Messages:    {total_valid}")
            print(f"Total Invalid Messages:  {total_invalid}")
            print(f"Total Expected:          {total_expected}")
            print(f"Total Received:          {total_received}")
            print(f"Total Rejected:          {total_rejected}")
            print(f"Avg False Accept Rate:   {avg_false_accept:.4f} ({avg_false_accept*100:.5f}%)")
            print(f"Avg False Reject Rate:   {avg_false_reject:.4f} ({avg_false_reject*100:.5f}%)")
        else:
            print("No purpose correctness data available")

        # OP Correctness Summary
        if metrics.op_correctness:
            print(f"\n--- OP Correctness Summary ---")
            total_requests = len(metrics.op_correctness)
            total_expected_resp = sum(op.expected_responses for op in metrics.op_correctness)
            total_actual_resp = sum(op.actual_responses for op in metrics.op_correctness)
            total_relevant_req_recv = sum(op.relevant_recv for op in metrics.op_correctness)
            total_irrelevant_req_recv = sum(op.irrelevant_recv for op in metrics.op_correctness)
            avg_coverage = sum(op.coverage for op in metrics.op_correctness) / total_requests
            avg_leakage = sum(op.leakage for op in metrics.op_correctness) / total_requests
            avg_completion = sum(op.completion for op in metrics.op_correctness) / total_requests

            print(f"Total Operational Requests:         {total_requests}")
            print(f"Total Expected Responses:           {total_expected_resp}")
            print(f"Total Actual Responses:             {total_actual_resp}")
            print(f"Total Relevant Requests Recieved:   {total_relevant_req_recv}")
            print(f"Total Irrelevant Requests Recieved: {total_irrelevant_req_recv}")
            print(f"Avg Coverage:                       {avg_coverage:.4f} ({avg_coverage*100:.5f}%)")
            print(f"Avg Leakage:                        {avg_leakage:.4f} ({avg_leakage*100:.5f}%)")
            print(f"Avg Completion:                     {avg_completion:.4f} ({avg_completion*100:.5f}%)")

            # Show the breakdown by category (C1_REG, C2, C3) to see what's going on
            #self._print_op_by_category(metrics)

        # OP Correlation Summary
        print(f"\n--- OP Correlation Summary ---")
        if metrics.op_correlations:
            total_ops = len(metrics.op_correlations)
            total_completed = sum(1 for c in metrics.op_correlations if c.completed)
            overall_rate = (total_completed / total_ops) if total_ops else 0.0
            print(f"Total Tracked Operations:   {total_ops}")
            print(f"Total Completed Operations: {total_completed}")
            print(f"Overall Completion Rate:    {overall_rate:.4f} ({overall_rate*100:.5f}%)")

            op_agg = self._aggregate_op_correlation(metrics.op_correlations)
            for op_type in sorted(op_agg.keys()):
                a = op_agg[op_type]
                print(f"  {op_type}: issued={int(a['total'])} "
                      f"completion={a['completion_rate']:.4f} "
                      f"median={a['median_latency_ms']:.3f}ms "
                      f"p90={a['p90_latency_ms']:.3f}ms "
                      f"p99={a['p99_latency_ms']:.3f}ms")
        else:
            print("No operational requests")

        # O1 (Right to be Informed) is broker-auto-fulfilled; reported separately
        o1 = metrics.o1_informed
        print(f"\n--- O1 (REGISTER-INFO / Informed) Summary ---")
        print(f"REGISTER-INFO Requests Issued:   {o1.register_info_requests}")
        print(f"Informed Notifications Received: {o1.informed_notifications}")
        print(f"Expected (pub-sub data pairs):   {o1.expected_pairs}")
        print(f"Informed Completion Rate:        {o1.completion_rate:.4f} ({o1.completion_rate*100:.5f}%)")

        print(f"\n{'='*80}\n")

    def _get_op_category_stats(self, metrics: TestMetrics) -> dict:
        """Break down OP correctness by operation category (C1_REG, C2, C3)

        Basically just groups all the operational requests by category and adds up
        the metrics for each one. Makes it easier to see which types have issues.
        """
        category_stats = {}

        # Go through all the operational requests and sort them by category
        for op_pub in self.op_publish_events:
            if 'op_resp' in op_pub.topic:
                continue  # Skip responses, we only want the original requests

            category = op_pub.op_category
            if category not in category_stats:
                category_stats[category] = {
                    'requests': 0,
                    'expected': 0,
                    'actual': 0,
                    'relevant': 0,
                    'irrelevant': 0
                }

            category_stats[category]['requests'] += 1

            # Grab the metrics for this specific request
            req_id = f"req_{op_pub.corr_data}"
            if req_id in metrics.op_correctness:
                op_metrics = metrics.op_correctness[req_id]
                category_stats[category]['expected'] += op_metrics.expected_responses
                category_stats[category]['actual'] += op_metrics.actual_responses
                category_stats[category]['relevant'] += op_metrics.relevant_responses
                category_stats[category]['irrelevant'] += op_metrics.irrelevant_responses

        return category_stats

    def _print_op_by_category(self, metrics: TestMetrics):
        """Print OP correctness breakdown by category

        Shows each operation category (C1_REG, C2, C3) separately so I can
        see which types are having coverage problems.
        """
        if not metrics.op_correctness:
            return

        category_stats = self._get_op_category_stats(metrics)

        print(f"\n--- OP Correctness by Operation Category ---")
        for category in sorted(category_stats.keys()):
            stats = category_stats[category]
            if stats['requests'] == 0:
                continue

            # Calculate the percentages
            coverage = (stats['relevant'] / stats['expected'] * 100) if stats['expected'] > 0 else 0
            leakage = (stats['irrelevant'] / max(1, stats['expected']) * 100)

            print(f"\n{category} Operations:")
            print(f"  Requests:            {stats['requests']}")
            print(f"  Expected Responses:  {stats['expected']}")
            print(f"  Actual Responses:    {stats['actual']}")
            print(f"  Relevant Responses:  {stats['relevant']}")
            print(f"  Coverage:            {coverage:.5f}%")
            print(f"  Leakage:             {leakage:.5f}%")

    def _export_op_by_category_to_csv(self, writer, metrics: TestMetrics):
        """Export OP correctness breakdown by category to CSV

        Writes a row for each category with all the stats so I can look at them
        in Excel or whatever.
        """
        if not metrics.op_correctness:
            return

        category_stats = self._get_op_category_stats(metrics)

        # Write out each category's metrics
        for category in sorted(category_stats.keys()):
            stats = category_stats[category]
            if stats['requests'] == 0:
                continue

            coverage = (stats['relevant'] / stats['expected']) if stats['expected'] > 0 else 0
            leakage = (stats['irrelevant'] / max(1, stats['expected']))

            # Write all the stats for this category
            writer.writerow([f"OP {category}", "Requests", f"{stats['requests']}"])
            writer.writerow([f"OP {category}", "Expected Responses", f"{stats['expected']}"])
            writer.writerow([f"OP {category}", "Actual Responses", f"{stats['actual']}"])
            writer.writerow([f"OP {category}", "Relevant Responses", f"{stats['relevant']}"])
            writer.writerow([f"OP {category}", "Coverage", f"{coverage:.4f}"])
            writer.writerow([f"OP {category}", "Leakage", f"{leakage:.4f}"])

    def export_metrics_to_csv(self, metrics: TestMetrics, output_path: str):
        """Export metrics to CSV"""
        import csv

        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)

            # Header
            writer.writerow(["Metric Category", "Metric Name", "Value"])

            # Broker Stats
            writer.writerow(["Broker", "CPU Min (%)", f"{metrics.broker_stats.cpu_min:.5f}"])
            writer.writerow(["Broker", "CPU Max (%)", f"{metrics.broker_stats.cpu_max:.5f}"])
            writer.writerow(["Broker", "CPU Avg (%)", f"{metrics.broker_stats.cpu_avg:.5f}"])
            writer.writerow(["Broker", "CPU Variance", f"{metrics.broker_stats.cpu_variance:.5f}"])
            writer.writerow(["Broker", "Memory Min (MB)", f"{metrics.broker_stats.mem_min:.5f}"])
            writer.writerow(["Broker", "Memory Max (MB)", f"{metrics.broker_stats.mem_max:.5f}"])
            writer.writerow(["Broker", "Memory Avg (MB)", f"{metrics.broker_stats.mem_avg:.5f}"])
            writer.writerow(["Broker", "Memory Variance", f"{metrics.broker_stats.mem_variance:.5f}"])

            # Messaging Stats
            writer.writerow(["Messaging", "Latency Min (ms)", f"{metrics.messaging_stats.latency_min_ms:.5f}"])
            writer.writerow(["Messaging", "Latency Max (ms)", f"{metrics.messaging_stats.latency_max_ms:.5f}"])
            writer.writerow(["Messaging", "Latency Avg (ms)", f"{metrics.messaging_stats.latency_avg_ms:.5f}"])
            writer.writerow(["Messaging", "Latency Variance", f"{metrics.messaging_stats.latency_variance_ms:.5f}"])
            writer.writerow(["Messaging", "Throughput (msgs/sec)", f"{metrics.messaging_stats.throughput_msgs_per_sec:.5f}"])
            writer.writerow(["Messaging", "Avg Header Size (bytes)", f"{metrics.messaging_stats.avg_header_size_bytes:.5f}"])
            writer.writerow(["Messaging", "Data Messages", f"{metrics.messaging_stats.total_data_msg_count}"])
            writer.writerow(["Messaging", "Non-Data Messages", f"{metrics.messaging_stats.non_data_msg_count}"])

            # Purpose Correctness Summary
            if metrics.purpose_correctness_per_sub:
                total_subs = len(metrics.purpose_correctness_per_sub)
                total_valid = sum(pc.valid_recv_count for pc in metrics.purpose_correctness_per_sub.values())
                total_invalid = sum(pc.invalid_recv_count for pc in metrics.purpose_correctness_per_sub.values())
                total_expected = sum(pc.expected_msg_count for pc in metrics.purpose_correctness_per_sub.values())
                total_received = sum(pc.total_recv_count for pc in metrics.purpose_correctness_per_sub.values())
                total_rejected = sum(pc.bad_reject for pc in metrics.purpose_correctness_per_sub.values())
                avg_false_accept = sum(pc.false_accept for pc in metrics.purpose_correctness_per_sub.values()) / total_subs
                avg_false_reject = sum(pc.false_reject for pc in metrics.purpose_correctness_per_sub.values()) / total_subs

                writer.writerow(["Purpose Correctness", "Total Subscribers", f"{total_subs}"])
                writer.writerow(["Purpose Correctness", "Total Valid Messages", f"{total_valid}"])
                writer.writerow(["Purpose Correctness", "Total Invalid Messages", f"{total_invalid}"])
                writer.writerow(["Purpose Correctness", "Total Expected", f"{total_expected}"])
                writer.writerow(["Purpose Correctness", "Total Received", f"{total_received}"])
                writer.writerow(["Purpose Correctness", "Total Rejected", f"{total_rejected}"])
                writer.writerow(["Purpose Correctness", "Avg False Accept Rate", f"{avg_false_accept:.4f}"])
                writer.writerow(["Purpose Correctness", "Avg False Reject Rate", f"{avg_false_reject:.4f}"])

            # OP Correctness Summary
            if metrics.op_correctness:
                total_requests = len(metrics.op_correctness)
                total_expected_resp = sum(op.expected_responses for op in metrics.op_correctness)
                total_actual_resp = sum(op.actual_responses for op in metrics.op_correctness)
                total_relevant_req_recv = sum(op.relevant_recv for op in metrics.op_correctness)
                total_irrelevant_req_recv = sum(op.irrelevant_recv for op in metrics.op_correctness)
                avg_coverage = sum(op.coverage for op in metrics.op_correctness) / total_requests
                avg_leakage = sum(op.leakage for op in metrics.op_correctness) / total_requests
                avg_completion = sum(op.completion for op in metrics.op_correctness) / total_requests

                writer.writerow(["OP Correctness", "Total Operational Requests", f"{total_requests}"])
                writer.writerow(["OP Correctness", "Total Expected Responses", f"{total_expected_resp}"])
                writer.writerow(["OP Correctness", "Total Actual Responses", f"{total_actual_resp}"])
                writer.writerow(["OP Correctness", "Total Relevant Requests Recieved", f"{total_relevant_req_recv}"])
                writer.writerow(["OP Correctness", "Total Irrelevant Requests Recieved", f"{total_irrelevant_req_recv}"])
                writer.writerow(["OP Correctness", "Avg Coverage", f"{avg_coverage:.4f}"])
                writer.writerow(["OP Correctness", "Avg Leakage", f"{avg_leakage:.4f}"])
                writer.writerow(["OP Correctness", "Avg Completion", f"{avg_completion:.4f}"])

                # Add the category breakdown (C1_REG, C2, C3) to the CSV too
                # self._export_op_by_category_to_csv(writer, metrics)

            # OP Correlation Summary
            op_agg = self._aggregate_op_correlation(metrics.op_correlations)
            total_ops = len(metrics.op_correlations)
            total_completed = sum(1 for c in metrics.op_correlations if c.completed)
            writer.writerow(["OP Correlation", "Total Tracked Operations", f"{total_ops}"])
            writer.writerow(["OP Correlation", "Total Completed Operations", f"{total_completed}"])
            writer.writerow(["OP Correlation", "Overall Completion Rate",
                             f"{(total_completed / total_ops) if total_ops else 0.0:.4f}"])
            for op_type in sorted(op_agg.keys()):
                a = op_agg[op_type]
                writer.writerow(["OP Correlation", f"{op_type} Operations Issued", f"{int(a['total'])}"])
                writer.writerow(["OP Correlation", f"{op_type} Completion Rate", f"{a['completion_rate']:.4f}"])
                writer.writerow(["OP Correlation", f"{op_type} Median Latency (ms)", f"{a['median_latency_ms']:.5f}"])
                writer.writerow(["OP Correlation", f"{op_type} p90 Latency (ms)", f"{a['p90_latency_ms']:.5f}"])
                writer.writerow(["OP Correlation", f"{op_type} p99 Latency (ms)", f"{a['p99_latency_ms']:.5f}"])

            # O1 (Right to be Informed): broker-auto-fulfilled, reported separately
            o1 = metrics.o1_informed
            writer.writerow(["O1 Informed", "REGISTER-INFO Requests Issued", f"{o1.register_info_requests}"])
            writer.writerow(["O1 Informed", "Informed Notifications Received", f"{o1.informed_notifications}"])
            writer.writerow(["O1 Informed", "Expected Pub-Sub Pairs", f"{o1.expected_pairs}"])
            writer.writerow(["O1 Informed", "Informed Completion Rate", f"{o1.completion_rate:.4f}"])

        # Per-operation rows go to a sibling CSV with its own column schema
        self.export_op_correlation_to_csv(metrics, output_path)

    def export_op_correlation_to_csv(self, metrics: TestMetrics, main_output_path: str):
        """Export per-operation request/response correlation to a sibling CSV file"""
        import csv

        p = Path(main_output_path)
        corr_path = p.with_name(f"{p.stem}_op_correlation{p.suffix or '.csv'}")

        with open(corr_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                "op_id", "op_type", "completed", "num_responders",
                "first_response_latency_ms", "last_response_latency_ms"
            ])
            for c in metrics.op_correlations:
                first = f"{c.first_response_latency_ms:.5f}" if c.first_response_latency_ms is not None else ""
                last = f"{c.last_response_latency_ms:.5f}" if c.last_response_latency_ms is not None else ""
                writer.writerow([
                    c.op_id,
                    c.op_type,
                    "yes" if c.completed else "no",
                    c.num_responders,
                    first,
                    last,
                ])

        print(f"Per-operation correlation exported to: {corr_path}")
