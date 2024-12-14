# ticket_cleaned_entity.py
from src.entity.ticket_raw_entity import TicketRawEntity


class TicketCleanedEntity(TicketRawEntity):
    def __init__(self, **kwargs):
        # Call the parent class constructor
        super().__init__(**kwargs)

        # Add new fields
        self.Resolution_Format = str(kwargs.get('Resolution_Format', ''))
        self.Resolution_Issue = str(kwargs.get('Resolution_Issue', ''))
        self.Resolution_Reason = str(kwargs.get('Resolution_Reason', ''))
        self.Resolution_Resolution = str(kwargs.get('Resolution_Resolution', ''))