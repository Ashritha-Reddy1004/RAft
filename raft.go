package main


AddEntry(
	status=self.status,
	sender_id=self.id,
	recipient_id=recipient,
	term=self.term,
	commit=False,
	success=None,
	last_log_index=self.last_log_index,
	last_log_entry=self.last_log_entry,
	new_log_entry=self.new_log_entry
)