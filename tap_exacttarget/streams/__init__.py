from .dataextentionobjects import DataExtentionObjectInc,DataExtentionObjectFt
from .folders import DataFolder
from .campaigns import Campaigns
from .content_area import ContentArea
from .emails import Email

from .event_sent import SentEvent
from .event_click import ClickEvent
from .event_open import OpenEvent
from .event_bounce import BounceEvent
from .event_notsent import NotSentEvent
from .event_unsub import UnsubEvent

from .list_send import ListSend
from .list_subscribers import ListSubscribe
from .subscribers import Subscribers


from .lists import ETList
from .sends import Sends




STREAMS = {
    # Campaigns.tap_stream_id:Campaigns,
    DataFolder.tap_stream_id:DataFolder,
    # ContentArea.tap_stream_id:ContentArea,
    Email.tap_stream_id:Email,
    # Sends.tap_stream_id:Sends,
    # ETList.tap_stream_id:ETList,
    # SentEvent.tap_stream_id:SentEvent,
    # ClickEvent.tap_stream_id:ClickEvent,
    # BounceEvent.tap_stream_id:BounceEvent,
    # OpenEvent.tap_stream_id:OpenEvent,
    # NotSentEvent.tap_stream_id: NotSentEvent,
    # UnsubEvent.tap_stream_id: UnsubEvent,
    # ListSend.tap_stream_id: ListSend,
    # ListSubscribe.tap_stream_id: ListSubscribe,
    # Subscribers.tap_stream_id: Subscribers
}

