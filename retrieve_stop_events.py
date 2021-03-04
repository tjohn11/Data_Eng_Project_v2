import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
import ssl
import re

def get_headers(table):
    list_headers = []

    # strip tags from headers
    row = table.find_all('tr')[0]
    cells = row.find_all('th')
    str_cells = str(cells)
    match = re.compile('<.*?>')
    clean = (re.sub(match, '', str_cells))
    list_headers.append(clean)
    df = pd.DataFrame(list_headers)
    df = df[0].str.split(',', expand=True)
    df.insert(0, 'trip_id', 'trip_id')
    return df


def create_df_from_table(trip_id, table):
    list_rows = []

    # strip header to get trip id
    match = re.compile('<.*?>')
    match2 = re.compile(r'[^\d\W]')
    clean = (re.sub(match, '', str(trip_id)))
    trip_id = (re.sub(match2, '', str(clean))).strip()

    # Get first row of table data
    row = table.find_all('tr')[1]
    cells = row.find_all('td')
    str_cells = str(cells)
    match = re.compile('<.*?>')
    clean = (re.sub(match, '', str_cells))
    list_rows.append(clean)
    df = pd.DataFrame(list_rows)
    df = df[0].str.split(',', expand=True)
    df.insert(0, 'trip_id', trip_id)
    return df


def html_to_json():
    # Setup BS object
    ssl._create_default_https_context = ssl._create_unverified_context
    url = 'http://rbi.ddns.net/getStopEvents'
    html = urlopen(url)
    soup = BeautifulSoup(html, 'lxml')
    main_df = pd.DataFrame()
    trip_ids = soup.find_all('h3')
    tables = soup.find_all('table')
    headers_df = get_headers(tables[0])

    idx = 0
    for id in trip_ids:
        main_df = main_df.append(create_df_from_table(id, tables[idx]), ignore_index=True)
        idx += 1

    # Reformat values
    stop_event_df = pd.concat([headers_df.head(1), main_df], ignore_index=True)
    stop_event_df = stop_event_df.rename(columns=stop_event_df.iloc[0])
    stop_event_df = stop_event_df.drop(stop_event_df.index[0])

    # for column in stop_event_df.columns:
    #     print(type(column))
    #     if '[' in column:
    #         # stop_event_df.rename(columns=column.str.strip('['), inplace=True)
    #         stop_event_df.columns[column]
    #     if ']' in column:
    #         stop_event_df.rename(columns=column.str.strip(']'), inplace=True)
    #     if ' ' in column:
    #         stop_event_df.rename(columns=column.strip(' '), inplace=True)
        
    #     print('**%s**', column)

    stop_event_df.columns = [
        'trip_id', 'vehicle_number', 'leave_time', 'train', 'route_number', 'direction',
        'service_key', 'stop_time', 'arrive_time', 'dwell', 'location_id', 'door', 'lift', 'ons',
        'offs', 'estimated_load', 'maximum_speed', 'train_mileage', 'pattern_distance',
        'location_distance', 'x_coordinate', 'y_coordinate', 'data_source', 'schedule_status'
    ]
    stop_event_df['vehicle_number'] = stop_event_df['vehicle_number'].str.strip('[')
    stop_event_df['schedule_status'] = stop_event_df['schedule_status'].str.strip(']')

    stop_event_df['leave_time'] = stop_event_df['leave_time'].str.strip(' ')
    stop_event_df['train'] = stop_event_df['train'].str.strip(' ')
    stop_event_df['route_number'] = stop_event_df['route_number'].str.strip(' ')
    stop_event_df['direction'] = stop_event_df['direction'].str.strip(' ')
    stop_event_df['service_key'] = stop_event_df['service_key'].str.strip(' ')
    stop_event_df['stop_time'] = stop_event_df['stop_time'].str.strip(' ')
    stop_event_df['arrive_time'] = stop_event_df['arrive_time'].str.strip(' ')
    stop_event_df['dwell'] = stop_event_df['dwell'].str.strip(' ')
    stop_event_df['location_id'] = stop_event_df['location_id'].str.strip(' ')
    stop_event_df['door'] = stop_event_df['door'].str.strip(' ')
    stop_event_df['lift'] = stop_event_df['lift'].str.strip(' ')
    stop_event_df['ons'] = stop_event_df['ons'].str.strip(' ')
    stop_event_df['offs'] = stop_event_df['offs'].str.strip(' ')
    stop_event_df['estimated_load'] = stop_event_df['estimated_load'].str.strip(' ')
    stop_event_df['maximum_speed'] = stop_event_df['maximum_speed'].str.strip(' ')
    stop_event_df['train_mileage'] = stop_event_df['train_mileage'].str.strip(' ')
    stop_event_df['pattern_distance'] = stop_event_df['pattern_distance'].str.strip(' ')
    stop_event_df['location_distance'] = stop_event_df['location_distance'].str.strip(' ')
    stop_event_df['x_coordinate'] = stop_event_df['x_coordinate'].str.strip(' ')
    stop_event_df['y_coordinate'] = stop_event_df['y_coordinate'].str.strip(' ')
    stop_event_df['data_source'] = stop_event_df['data_source'].str.strip(' ')
    stop_event_df['schedule_status'] = stop_event_df['schedule_status'].str.strip(' ')

    stop_event_df.to_json(r'stop_event.json', orient='records',indent=2)
