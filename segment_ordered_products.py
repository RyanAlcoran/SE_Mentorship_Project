from klaviyo_api import KlaviyoAPI

SEGMENT_ID = "XvXWFN"
# Placed Order metric
PLACED_ORDER_METRIC= "THmCJ7"
PRIVATE_API_KEY = ""

klaviyo = KlaviyoAPI(PRIVATE_API_KEY, max_delay=60, max_retries=3, test_host=None)


def get_segment_profiles(segment_id):
	""" Get a dictionary of profiles by segment id

	Args:
		segment_id (string): Segment ID
	Returns:
		segment_profiles (dict): Dictionary of profiles
	"""
	return klaviyo.Segments.get_segment_profiles(segment_id)["data"]


def get_profile_events(profile_id):
	""" Get a dictionary of Placed Order events by profile id

	Args:
		profile_id (string): Unique identifier for profile
	Returns:
		profile_events (dict): All placed order events for a profile
	"""
	return klaviyo.Events.get_events(fields_event=['event_properties'],filter=f"equals(metric_id,\"{PLACED_ORDER_METRIC}\"),equals(profile_id,\"{profile_id}\")")["data"]

def main():
	profiles = get_segment_profiles(SEGMENT_ID)

	for profile in profiles:
		profile_id = profile["id"]

		# Chceck if "ordered_product" is set for a profile
		if "ordered_products" in profile["attributes"]["properties"]:
			ordered_products = profile["attributes"]["properties"]["ordered_products"]
		else:
			ordered_products = []
		print(f'Profile id: {profile_id}')
		print(f'Email: {profile["attributes"]["email"]}')
		
		events = get_profile_events(profile_id)

		for event in events:
			item_list = event["attributes"]["event_properties"]["Items"]
			ordered_products = list(set(ordered_products + item_list))

		print(f'Ordered products: {ordered_products}')

if __name__ == '__main__':
	main()