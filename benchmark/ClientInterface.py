import paho.mqtt.client as mqtt
import MQTTClient
from paho.mqtt.subscribeoptions import SubscribeOptions
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.enums import MQTTProtocolVersion, CallbackAPIVersion
from typing import Optional, Tuple, List
from math import ceil
import GlobalDefs

SUBSCRIPTION_ID_COUNTER: int = 1

"""Initializes a Paho MQTTv5 client and returns it to the requester

Parameters
----------
client_id : str
    The client ID to assign to the client

Returns
----------
paho.mqtt.client.Client
    The created client
"""
def create_v5_client(client_id: str) -> mqtt.Client:

    # Instantiate client
    mqtt_client = MQTTClient.MQTTClient(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=MQTTProtocolVersion.MQTTv5,
        reconnect_on_failure=False
    )

    return mqtt_client

"""Attempts to connect the client to a broker. Note that this function should ONLY
send a connect message and should not attempt to start the network loop, this is
handled by the benchmark.

Parameters
----------
client : paho.mqtt.client.Client
    The client to connect
success_callback : function
    A function to call on connect success
failure_callback : function
    A function to call on connect failure
broker_address : str
    The IP or FQDN of the broker
port : int, optional
    The port on which to connect to the broker (default is 1883)
clean_start : bool, optional
    Whether to clear existing session information for the client

Returns
----------
paho.mqtt.client.MQTTErrorCode
    The return code of the connect attempt
"""
def connect_client(client: mqtt.Client, broker_address: str, port: int = 1883,
                  clean_start: bool = True) -> mqtt.MQTTErrorCode:
        
    # Attempt to send connect packet
    try:
        return client.connect(host=broker_address, port=port, clean_start=clean_start)
    except Exception:
        return mqtt.MQTTErrorCode.MQTT_ERR_UNKNOWN
    
    
"""Attempts to disconnect the client from a broker

Parameters
----------
client : paho.mqtt.client.Client
    The client to connect
callback : function
    A function to call on disconnect
reason_code: paho.mqtt.reasoncodes.ReasonCode
    The reason for the disconnect

Returns
----------
paho.mqtt.client.MQTTErrorCode
    The return code of the disconnect attempt
"""
def disconnect_client(client: mqtt.Client,reason_code: ReasonCode | None = None) -> mqtt.MQTTErrorCode:
        
    # Attempt to send connect packet
    try:
        return client.disconnect(reasoncode=reason_code)
    except Exception:
        return mqtt.MQTTErrorCode.MQTT_ERR_UNKNOWN
    

"""Attempts to SUBSCRIBE client to a topic filter in MQTT with a specified purpose filter

Parameters
----------
client : paho.mqtt.client.Client
    The client to subscribe with
method : Benchmark.PurposeManagementMethod
    The method of purpose management for the broker
callback : function
    A function to call on SUBACK
topic_filter : str
    The topic filter on which to subscribe
purpose_filter : str
    The purpose filter for which the messages will be used
subscriber_id : int, optional
    The subscriber id to assign to the subscription
qos : int, optional
    The quality of service for the subscription

Returns
----------
tuple[paho.mqtt.client.MQTTErrorCode, int | None]
    A tuple containing the error code and (if successful) the granted quality of service for the subscription
"""
def subscribe_with_purpose_filter(client: mqtt.Client, method: GlobalDefs.PurposeManagementMethod, 
                                  topic_filter: str, purpose_filter: str, 
                                  qos: int = 0, no_local=True, existing_subscription=False, previous_purpose_filter="") -> List[Tuple[mqtt.MQTTErrorCode, Optional[int], int]]:
    
    global SUBSCRIPTION_ID_COUNTER
    
    if purpose_filter == None:
        purpose_filter = GlobalDefs.ALL_PURPOSE_FILTER
        
    subscribe_options = SubscribeOptions(qos=qos, noLocal=no_local)
    
    return_list: List[Tuple[mqtt.MQTTErrorCode, Optional[int], int]] = list()

    # Unified method: the subscription purpose (SP) is supplied as a user property on the
    # SUBSCRIBE. (existing_subscription / previous_purpose_filter are unused under the unified
    # method but kept in the signature for call-site compatibility.)
    properties = mqtt.Properties(packetType=mqtt.PacketTypes.SUBSCRIBE)
    properties.UserProperty = (GlobalDefs.PROPERTY_SP, purpose_filter)
    properties.SubscriptionIdentifier = SUBSCRIPTION_ID_COUNTER

    try:
        # Drop any previous subscription on this filter so the new SP replaces the old one
        unsub_properties = mqtt.Properties(packetType=mqtt.PacketTypes.UNSUBSCRIBE)
        client.unsubscribe(topic_filter, properties=unsub_properties)
        result, mid = client.subscribe(topic_filter, properties=properties, options=subscribe_options)
        return_list.append((result, mid, SUBSCRIPTION_ID_COUNTER))
    except Exception:
        return_list.append((mqtt.MQTTErrorCode.MQTT_ERR_UNKNOWN, None, SUBSCRIPTION_ID_COUNTER))

    SUBSCRIPTION_ID_COUNTER += 1

    return return_list


def subscribe_for_operations(client: mqtt.Client, method: GlobalDefs.PurposeManagementMethod, topic_filter: str) -> List[Tuple[mqtt.MQTTErrorCode, Optional[int], int]]:
    
    # Call normal subcribe with QoS 2 and defined purpose
    return subscribe_with_purpose_filter(client, method, topic_filter, GlobalDefs.OP_PURPOSE, 2)


"""Registers the message purpose (MP) for publications to a topic via $DAP/MP_reg/<topic>

Parameters
----------
client : paho.mqtt.client.Client
    The client to register
method : Benchmark.PurposeManagementMethod
    The method of purpose management for the broker
topic : str
    The topic on which to set the filter
purpose : str
    The purpose filter to register
qos : int, optional
    The quality of service for the message

Returns
----------
paho.mqtt.client.MQTTErrorCode
    The message publication information
"""
def register_publish_purpose_for_topic(client: mqtt.Client, method: GlobalDefs.PurposeManagementMethod, 
                                       topic: str, purpose: str, qos: int = 0) -> mqtt.MQTTMessageInfo | None:
    
    # Unified method (paper 4.3, broker Registration-by-Message): register the message
    # purpose (MP) for a topic by publishing to the single $DAP/purpose_management topic
    # with the MP carried as a DAP-MP user property whose value is "<MP>:<topic>".
    # (Empty payload; the broker parses the registration from the property and does not
    # forward the message.)
    mp_reg_topic = GlobalDefs.REG_BY_MSG_REG_TOPIC

    properties = mqtt.Properties(packetType=mqtt.PacketTypes.PUBLISH)
    properties.UserProperty = (GlobalDefs.PROPERTY_ID, client._client_id)
    properties.UserProperty = (GlobalDefs.PROPERTY_MP, f"{purpose}:{topic}")
    properties.UserProperty = (GlobalDefs.PROPERTY_CONSENT, "1")

    return client.publish(mp_reg_topic, qos=qos, properties=properties)


"""Attempts to PUBLISH a message a topic in MQTT with a specified purpose filter

Parameters
----------
client : paho.mqtt.client.Client
    The client to publish with
method : Benchmark.PurposeManagementMethod
    The method of purpose management for the broker
topic : str
    The topic on which to publish
purpose : str, optional (for some methods)
    The purpose on which to send
qos : int, optional
    The quality of service for the message
payload : str, optional
    The payload to send within the message

Returns
----------
list[tuple[paho.mqtt.client.MQTTErrorCode, str]]
    A list of tuples which contain the error code of the message publication and the topic for the error code
"""
def publish_with_purpose(client: mqtt.Client, method: GlobalDefs.PurposeManagementMethod, 
                         topic: str, purpose: Optional[str] = None, qos: int = 0, 
                         retain: bool = False, payload: str | None = None, correlation_data: int | None = None) -> List[Tuple[mqtt.MQTTMessageInfo, str]]:
    
    
    if purpose == None:
        purpose = GlobalDefs.ALL_PURPOSE_FILTER
        
    ret_list = list()
    
    properties = mqtt.Properties(packetType=mqtt.PacketTypes.PUBLISH)
    properties.UserProperty = (GlobalDefs.PROPERTY_ID, client._client_id)
    properties.UserProperty = (GlobalDefs.PROPERTY_CONSENT, "1")
    
    if correlation_data is not None:
        required_bytes = ceil(correlation_data.bit_length() / 8.0)
        properties.CorrelationData = correlation_data.to_bytes(length=required_bytes, byteorder='big', signed=False)

    # Unified method: data is published normally; the broker resolves delivery using the
    # message purpose (MP) registered for this topic via register_publish_purpose_for_topic.
    msg_info = client.publish(topic, payload, qos=qos, retain=retain, properties=properties)
    return [(msg_info, topic)]  # Return list of (message info, topic) tuples


def publish_operation_request(client: mqtt.Client, method: GlobalDefs.PurposeManagementMethod, operation: str, correlation_data: int | None = None, qos: int = 0) -> List[Tuple[mqtt.MQTTMessageInfo, str]]:
    
    # Unified method: operational requests go to the operational system topic.
    topic = GlobalDefs.OSYS_TOPIC

    # Set properties and payload based on operations
    properties = mqtt.Properties(packetType=mqtt.PacketTypes.PUBLISH)
    properties.UserProperty = (GlobalDefs.PROPERTY_OPTYPE, operation)
    properties.ResponseTopic = f'{GlobalDefs.OP_RESPONSE_TOPIC}/{client._client_id.decode("utf-8")}'

    if correlation_data is not None:
        required_bytes = ceil(correlation_data.bit_length() / 8.0)
        properties.CorrelationData = correlation_data.to_bytes(length=required_bytes, byteorder='big', signed=False)

    # Operation vocabulary (new paper):
    #   REGISTER-INFO - register "right to be informed" data (carries a payload)
    #   AUDIT         - who received the data
    #   HISTORY       - what data did they receive
    #   DELETE        - erase data
    #   RESTRICT      - restrict processing
    #   UPDATE        - rectify/replace data (carries replacement payload)
    if operation == "REGISTER-INFO":
        return _handle_operation_publish(client, method, topic, GlobalDefs.OP_PURPOSE, properties, qos=qos, payload=f'{client._client_id.decode("utf-8")} Right to Know Data')
    elif operation in ("AUDIT", "HISTORY", "DELETE", "RESTRICT"):
        properties.UserProperty = (GlobalDefs.PROPERTY_OP_INFO, "*")
        return _handle_operation_publish(client, method, topic, GlobalDefs.OP_PURPOSE, properties, qos=qos)
    elif operation == "UPDATE":
        properties.UserProperty = (GlobalDefs.PROPERTY_OP_INFO, "*")
        return _handle_operation_publish(client, method, topic, GlobalDefs.OP_PURPOSE, properties, qos=qos, payload="ReplacementData")
    else:
        return list()
    
def publish_operation_response(client: mqtt.Client, method: GlobalDefs.PurposeManagementMethod, topic: str, operation: str, op_result: str, correlation_data: int | None = None, qos: int = 0, op_id: str | None = None) -> List[Tuple[mqtt.MQTTMessageInfo, str]]:

    # Set properties and payload based on operations
    properties = mqtt.Properties(packetType=mqtt.PacketTypes.PUBLISH)
    properties.UserProperty = (GlobalDefs.PROPERTY_OPTYPE, operation)
    properties.UserProperty = (GlobalDefs.PROPERTY_OP_STATUS, op_result)

    # Echo the broker's op id back verbatim so it can match this notification to the
    # operation it is tracking. The broker writes op_id as a decimal string and parses
    # with strtoull, so we must send back the exact string we received (no int round-trip).
    if op_id is not None:
        properties.UserProperty = (GlobalDefs.PROPERTY_OP_ID, op_id)

    properties.ResponseTopic = f'{GlobalDefs.OP_RESPONSE_TOPIC}/{client._client_id.decode("utf-8")}'

    if correlation_data is not None:
        required_bytes = ceil(correlation_data.bit_length() / 8.0)
        properties.CorrelationData = correlation_data.to_bytes(length=required_bytes, byteorder='big', signed=False)

    # Unified method: register the operational purpose, then publish the response.
    properties.UserProperty = (GlobalDefs.PROPERTY_ID, client._client_id)
    properties.UserProperty = (GlobalDefs.PROPERTY_CONSENT, "1")

    register_publish_purpose_for_topic(client, method, topic, GlobalDefs.OP_PURPOSE, qos)  # Need to register
    msg_info = client.publish(topic, qos=qos, properties=properties)
    return [(msg_info, topic)]  # Return list of (message info, topic) tuples


def _handle_operation_publish(client: mqtt.Client, method: GlobalDefs.PurposeManagementMethod, 
                         topic: str, purpose: str, properties: mqtt.Properties, qos: int = 0, 
                         retain: bool = False, payload: str | None = None) -> List[Tuple[mqtt.MQTTMessageInfo, str]]:
        
    # Unified method: publish the operational message with client id + consent properties.
    properties.UserProperty = (GlobalDefs.PROPERTY_ID, client._client_id)
    properties.UserProperty = (GlobalDefs.PROPERTY_CONSENT, "1")

    msg_info = client.publish(topic, payload, qos=qos, retain=retain, properties=properties)
    return [(msg_info, topic)]  # Return list of (message info, topic) tuples