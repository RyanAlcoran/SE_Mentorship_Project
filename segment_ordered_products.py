from klaviyo_api import KlaviyoAPI
import csv
from datetime import datetime

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
    return 

def get_profile_events(profile_id, last_update=None):
    """ Get a dictionary of Placed Order events by profile id

    Args:
        profile_id (string): Unique identifier for profile
        last_update (datetime): Optional argument to include last update date
    Returns:
        profile_events (dict): All placed order events for a profile
    """
    if last_update is None:
        return klaviyo.Events.get_events(fields_event=['event_properties'],filter=f"equals(metric_id,\"{PLACED_ORDER_METRIC}\"),equals(profile_id,\"{profile_id}\")")["data"]
    else:
        return klaviyo.Events.get_events(fields_event=['event_properties'],filter=f"equals(metric_id,\"{PLACED_ORDER_METRIC}\"),equals(profile_id,\"{profile_id}\"),greater-or-equal(datetime,{last_update})")["data"]

def process_profile(profile):
    """ Get all ordered products for a profile

    Args:
        profile (dictionary): Profile to process
    Returns:
        profile_dict (dict): Profile information with all ordered products
    """
        # Get profile information
        profile_id = profile["id"]
        profile_email = profile["attributes"]["email"]
        profile_first_name = profile["attributes"]["first_name"]
        profile_last_name = profile["attributes"]["last_name"]

        # Check if "ordered_product" is set for a profile
        if "ordered_products" in profile["attributes"]["properties"]:
            ordered_products = profile["attributes"]["properties"]["ordered_products"]
        else:
            ordered_products = []
        print(f'Profile id: {profile_id}')
        print(f'Email: {profile["attributes"]["email"]}')

        # Check if "ordered_products_list_last_update" is set for a profile then get events
        if "ordered_products_list_last_update" in profile["attributes"]["properties"]:
            last_update = profile["attributes"]["properties"]["ordered_products_list_last_update"]
            events = get_profile_events(profile_id, last_update)
        else:
            events = get_profile_events(profile_id)

        # Set last_udpate to now
        last_update = datetime.now().isoformat()

        # Get all events and items to the ordered_products list
        for event in events:
            item_list = event["attributes"]["event_properties"]["Items"]
            ordered_products = list(set(ordered_products + item_list))

        # Create payload to update custom properties
        payload = { "data": {
            "type": "profile",
            "id": profile_id,
            "attributes": { "properties": { "ordered_products": ordered_products, 
            "ordered_products_list_last_update": last_update} }
            } }

        # Write custom properties to profile
        klaviyo.Profiles.update_profile(profile_id, payload)
        print(f'Ordered products: {ordered_products}\n')

        # Create dictionary of profile information
        profile_dict = dict()
        profile_dict["profile_email"] = profile_email
        profile_dict["profile_first_name"] = profile_first_name
        profile_dict["profile_last_name"] = profile_last_name
        profile_dict["ordered_products"] = ordered_products

        return profile_dict


def main():
    response = get_segment_profiles(SEGMENT_ID)
    profiles = response["data"]
    next_url = response["links"]["next"]

    # Create CSV writer
    csv_header = ["Email", "First Name", "Last Name", "Ordered Products" ]
    filename =  f"{SEGMENT_ID}-{datetime.now().isoformat()}.csv"

    with open(f"exports/{filename}", "w", newline="") as segment_file:
        segment_writer = csv.writer(segment_file)
        segment_writer.writerow(csv_header)

        for profile in profiles:
            output = process_profile(profile)
            segment_writer.writerow([output["profile_email"], output["profile_first_name"], output["profile_last_name"], ', '.join(output["ordered_products"])])

        # Paginate through remaining profiles
        while next_url is not None:
            response = get_segment_profiles(SEGMENT_ID)
            profiles = response["data"]
            for profile in profiles:
                output = process_profile(profile)
                # Write ordeded products to CSV
                segment_writer.writerow([output["profile_email"], output["profile_first_name"], output["profile_last_name"], ', '.join(output["ordered_products"])])
            next_url = response["links"]["next"]


if __name__ == '__main__':
    main()