import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
import ssl
import re

def html_to_json():
    # Setup BS object
    ssl._create_default_https_context = ssl._create_unverified_context
    url = 'http://rbi.ddns.net/getStopEvents'
    html = urlopen(url)
    soup = BeautifulSoup(html, 'lxml')

    # Get/clean table rows
    list_rows = []
    rows = soup.find_all('tr')
    for row in rows:
        cells = row.find_all('td')
        str_cells = str(cells)
        match = re.compile('<.*?>')
        clean = (re.sub(match, '', str_cells))
        list_rows.append(clean)
    table_data_df = pd.DataFrame(list_rows)

    # Reformat values
    table_data_df = table_data_df[0].str.split(',', expand=True)

    # Get and clean table headers
    list_headers = []
    for row in rows:
        cells = row.find_all('th')
        str_cells = str(cells)
        match = re.compile('<.*?>')
        clean = (re.sub(match, '', str_cells))
        list_headers.append(clean)
    table_header_df = pd.DataFrame(list_headers)

    # Reformat
    table_header_df = table_header_df[0].str.split(',', expand=True)
    stop_event_df = pd.concat([table_header_df.head(1), table_data_df])
    stop_event_df = stop_event_df.rename(columns=stop_event_df.iloc[0])

    # Drop null/redundant rows
    stop_event_df = stop_event_df.dropna(axis=0, how='any')
    stop_event_df = stop_event_df.drop(stop_event_df.index[0])

    # More reformatting
    stop_event_df.rename(columns={'[vehicle_number': 'vehicle_number'}, inplace=True)
    stop_event_df.rename(columns={' schedule_status]': 'schedule_status'}, inplace=True)
    stop_event_df.rename(columns={' leave_time': 'leave_time'}, inplace=True)
    stop_event_df.rename(columns={' train': 'train'}, inplace=True)
    stop_event_df.rename(columns={' route_number': 'route_number'}, inplace=True)
    stop_event_df.rename(columns={' direction': 'direction'}, inplace=True)
    stop_event_df.rename(columns={' service_key': 'service_key'}, inplace=True)
    stop_event_df.rename(columns={' stop_time': 'stop_time'}, inplace=True)
    stop_event_df.rename(columns={' arrive_time': 'arrive_time'}, inplace=True)
    stop_event_df.rename(columns={' dwell': 'dwell'}, inplace=True)
    stop_event_df.rename(columns={' location_id': 'location_id'}, inplace=True)
    stop_event_df.rename(columns={' door': 'door'}, inplace=True)
    stop_event_df.rename(columns={' lift': 'lift'}, inplace=True)
    stop_event_df.rename(columns={' ons': 'ons'}, inplace=True)
    stop_event_df.rename(columns={' offs': 'offs'}, inplace=True)
    stop_event_df.rename(columns={' estimated_load': 'estimated_load'}, inplace=True)
    stop_event_df.rename(columns={' maximum_speed': 'maximum_speed'}, inplace=True)
    stop_event_df.rename(columns={' train_mileage': 'train_mileage'}, inplace=True)
    stop_event_df.rename(columns={' pattern_distance': 'pattern_distance'}, inplace=True)
    stop_event_df.rename(columns={' location_distance': 'location_distance'}, inplace=True)
    stop_event_df.rename(columns={' x_coordinate': 'x_coordinate'}, inplace=True)
    stop_event_df.rename(columns={' y_coordinate': 'y_coordinate'}, inplace=True)
    stop_event_df.rename(columns={' data_source': 'data_source'}, inplace=True)
    stop_event_df.rename(columns={' schedule_status': 'schedule_status'}, inplace=True)

    stop_event_df['vehicle_number'] = stop_event_df['vehicle_number'].str.strip('[')
    stop_event_df['schedule_status'] = stop_event_df['schedule_status'].str.strip(']')

    stop_event_df.to_json(r'stop_event.json', orient='records',indent=2)

