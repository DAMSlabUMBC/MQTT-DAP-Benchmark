from enum import Enum, IntEnum
import itertools
from types import ModuleType
from typing import TYPE_CHECKING, List  # Import List for compatibility

# These provide type checking without cyclic imports
if TYPE_CHECKING:
    from LoggingModule import ResultLogger

# Framework Method Enums
class PurposeManagementMethod(Enum):
    PM_UNIFIED = "Unified"

class C1RightsMethod(Enum):
    C1_0 = "None"
    C1_1 = "Direct Publication"
    C1_2 = "Pre-Registration"

class C2RightsMethod(Enum):
    C2_0 = "None"
    C2_1 = "Direct Publication"
    C2_2 = "Broker-Facilitated"

class C3RightsMethod(Enum):
    C3_0 = "None"
    C3_1 = "Direct Publication"
    C3_2 = "Broker-Facilitated"
    
ALL_PURPOSE_FILTER: str = "*"

# Exit Code definitions
class ExitCode(IntEnum):
    SUCCESS = 0,
    BAD_ARGUMENT = 1,
    MALFORMED_CONFIG = 2,
    BAD_CLIENT_API = 3,
    FAILED_TO_INIT_LOGGING = 4,
    MALFORMED_LOG_FILE = 5,
    CONFLICTING_LOG_FILES = 6,
    SIGINT_RECEIVED = 7,
    UNKNOWN_ERROR = 99

# These should be assigned as created
CLIENT_MODULE: ModuleType
LOGGING_MODULE: 'ResultLogger'

VERBOSE_LOGGING: bool = False

# These should be assigned to based on the config file
REG_BY_TOPIC_PUB_REG_TOPIC: str = ""
REG_BY_TOPIC_SUB_REG_TOPIC: str= ""
REG_BY_MSG_REG_TOPIC: str = ""

PROPERTY_MP: str = "DAP-MP"
PROPERTY_SP: str = "DAP-SP"
PROPERTY_ID: str = "DAP-ClientID"
PROPERTY_CONSENT: str = "DAP-Allow"
PROPERTY_OPTYPE: str = "DAP-OpType"
PROPERTY_OP_INFO: str = "DAP-OpInfo"
PROPERTY_OP_STATUS: str = "DAP-Status"

# Unified-workflow properties
PROPERTY_TIMESTAMP: str = "DAP-Timestamp"
PROPERTY_OP_BEFORE: str = "DAP-OpBefore"
PROPERTY_OP_AFTER: str = "DAP-OpAfter"
PROPERTY_OP_ID: str = "DAP-OpId"
PROPERTY_OP_TFS: str = "DAP-OpTFs"
PROPERTY_OP_PFS: str = "DAP-OpPFs"
PROPERTY_OP_CLIENTS: str = "DAP-OpClients"

# Operational topics
OR_TOPIC: str = "OR"
ORS_TOPIC: str = "ORS"
ON_TOPIC: str = "ON"
ONP_TOPIC: str = "ONP"
OSYS_TOPIC: str = "$OP_SYS"
OP_RESPONSE_TOPIC: str = "OP_NOTIF"
OP_PURPOSE: str = "DAP_OP"

# Required functions for the client
CLIENT_FUNCTIONS: List[str] = [  
    "create_v5_client", 
    "connect_client", 
    "disconnect_client",
    "subscribe_with_purpose_filter",
    "register_publish_purpose_for_topic", 
    "publish_with_purpose"
]

## UTILITY METHODS ##
def find_described_purposes(purpose_filter: str) -> list[str]:

    # Break purpose filter into individual purposes
    filter_levels = purpose_filter.split('/')
    decomposed_levels = list()

    for level in filter_levels:
        if '{' in level:
            new_level = level.replace('{','').replace('}','').split(',')
        else:
            new_level = [level]

        decomposed_levels.append(new_level)

    described_purposes = list()
    decomposed_purpose_list = itertools.product(*decomposed_levels)
    for purpose_list in decomposed_purpose_list:
        purpose = '/'.join(purpose_list)
        if not './' in purpose:
            purpose = purpose.replace('/.', '')
            described_purposes.append(purpose)

    return described_purposes

def purpose_described_by_filter(purpose: str, purpose_filter: str) -> bool:
    purposes_described_by_filter = find_described_purposes(purpose_filter)
    return (purpose in purposes_described_by_filter)