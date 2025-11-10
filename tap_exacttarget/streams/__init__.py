# from .campaigns import Campaigns
# from .content_area import ContentArea
# from .dataextensionobjects import DataExtensionObjectFt, DataExtensionObjectInc  # noqa: F401
# from .email import Email
# from .event_bounce import BounceEvent
# from .event_click import ClickEvent
# from .event_notsent import NotSentEvent
# from .event_open import OpenEvent
# from .event_sent import SentEvent
# from .event_unsub import UnsubEvent
# from .datafolder import DataFolder
# from .list_send import ListSend
# from .list_subscribers import ListSubscribers
# from .list import ETList
# from .send import Sends
# from .subscriber import Subscribers

# STREAMS = {
#     Campaigns.tap_stream_id: Campaigns,
#     DataFolder.tap_stream_id: DataFolder,
#     ContentArea.tap_stream_id: ContentArea,
#     Email.tap_stream_id: Email,
#     Sends.tap_stream_id: Sends,
#     ETList.tap_stream_id: ETList,
#     SentEvent.tap_stream_id: SentEvent,
#     ClickEvent.tap_stream_id: ClickEvent,
#     BounceEvent.tap_stream_id: BounceEvent,
#     OpenEvent.tap_stream_id: OpenEvent,
#     NotSentEvent.tap_stream_id: NotSentEvent,
#     UnsubEvent.tap_stream_id: UnsubEvent,
#     ListSend.tap_stream_id: ListSend,
#     ListSubscribers.tap_stream_id: ListSubscribers,
#     Subscribers.tap_stream_id: Subscribers,
# }

from account_user import AccountUser
from automation import Automation
from automation_instance import AutomationInstance
from data_extension import DataExtension
from data_extension_field import DataExtensionField
from link_send import LinkSend
from link import Link
from send_summary import SendSummary

STREAMS = {
    AccountUser.tap_stream_id: AccountUser,
    Automation.tap_stream_id: Automation,
    AutomationInstance.tap_stream_id: AutomationInstance,
    DataExtension.tap_stream_id: DataExtension,
    DataExtensionField.tap_stream_id: DataExtensionField,
    LinkSend.tap_stream_id: LinkSend,
    Link.tap_stream_id: Link,
    SendSummary.tap_stream_id: SendSummary,
}
