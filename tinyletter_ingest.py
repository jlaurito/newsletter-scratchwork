import tinyapi
import pandas as pd

session = tinyapi.Session("nycdatajobs", "xxx")

all_messages = pd.DataFrame(session.get_messages())

# make message stats first-class
fields_to_extract = ['unsubs',
	 'soft_bounces',
	 'total_clicks',
	 'unique_clicks',
	 'hard_bounces',
	 'total_opens',
	 'spam_complaints',
	 'unique_opens']

extracted_fields = all_messages['stats'].apply(lambda x: pd.Series([x[f] for f in fields_to_extract]))
extracted_fields.rename(columns = lambda x: fields_to_extract[x], inplace = True)

all_messages = pd.concat([all_messages, extracted_fields], axis=1)

# get link clicks 

def get_links_from_email(id):
	return pd.DataFrame(session.get_message_urls(id))

# impure function
def generate_link_df(link_df, id):
	_ = get_links_from_email(id)
	if link_df == []:
		all_links = _
	else:
		all_links.append(_)


all_links = []
all_messages['id'].apply(lambda x: all_links.append(get_links_from_email(x)))
all_messages['send_time'] = pd.to_datetime(all_messages['sent_at'], unit='s')
all_messages.to_csv('data/messages.csv', index=False)

all_links_df = pd.concat(all_links, ignore_index=True)


all_link_clicks = all_links_df.merge(
	all_messages[['id', 'unique_opens', 'send_count','send_time']], 
	left_on='message_id',
	right_on='id',
	how='left',
	suffixes=('_link', '_message'))

all_link_clicks.to_csv('data/clicks.csv', index=False)

# get subscribers

subscribers = pd.DataFrame(session.get_subscribers())

# make subscriber stats first-class
fields_to_extract = ['last_clicked_at', 
	'soft_bounces',
	'first_sent_at',
	'total_clicks',
	'unique_clicks',
	'hard_bounces',
	'total_sent',
	'last_opened_at',
	'last_sent_at',
	'total_opens',
	'first_opened_at',
	'unique_opens',
	'first_clicked_at'
	]

extracted_fields = subscribers['stats'].apply(lambda x: pd.Series([x[f] for f in fields_to_extract]))
extracted_fields.rename(columns = lambda x: fields_to_extract[x], inplace = True)

fields_to_cast_to_dates = [
	'last_clicked_at', 'first_sent_at', 
	'last_opened_at', 'last_sent_at',
	'first_opened_at', 'first_clicked_at']

date_fields = []
for f in fields_to_cast_to_dates:
	date_fields.append(
		extracted_fields[f].apply(lambda x: pd.to_datetime(x, unit='s') if pd.notnull(x) else None)
	)
date_fields_df = pd.concat(date_fields, ignore_index=False, axis = 1)

extracted_fields.drop(fields_to_cast_to_dates, inplace=True, axis=1)


all_subscribers = pd.concat([subscribers, extracted_fields, date_fields_df], axis=1)
all_subscribers.to_csv('data/subscribers.csv', index=False)

# message content (moved to end bc heavier and likely to fail)
message_content = pd.DataFrame(session.get_messages(content=True))
message_content[['id', 'content', 'subject', 'snippet']].to_csv('data/content.csv', index=False)
