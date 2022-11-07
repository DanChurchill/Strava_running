
import numpy as np
import pandas as pd
import requests
import json
import time
import env

def get_strava_token():
    '''
    Get the tokens to access strava api
    returns token json
    '''
    ## get json from file
    with open('strava_tokens.json') as json_file:
        strava_tokens = json.load(json_file)
    ## If access_token has expired then use the refresh_token to get the new access_token
    if strava_tokens['expires_at'] < time.time():
    #Make Strava auth API call with current refresh token
        response = requests.post(
                            url = 'https://www.strava.com/oauth/token',
                            data = {
                                    'client_id': 96678,
                                    'client_secret': '10705c973422a20357c7b7f89d21fe8189c90ad3',
                                    'grant_type': 'refresh_token',
                                    'refresh_token': strava_tokens['refresh_token']
                                    }
                        )
    #Save response as json in new variable
        new_strava_tokens = response.json()
    # Save new tokens to file
        with open('strava_tokens.json', 'w') as outfile:
            json.dump(new_strava_tokens, outfile)
    #Use new Strava tokens from now
        strava_tokens = new_strava_tokens
    return strava_tokens

def init_activities_df():
    '''
    Initialize the activities dataframe to hold the data retrieved from Strava
    returns an empty dataframe with 46 named columns
    '''
    columns = [
       'id', 'name', 'distance', 'moving_time', 'elapsed_time',
       'total_elevation_gain', 'type', 'sport_type', 'workout_type',
       'start_date', 'start_date_local', 'timezone', 'utc_offset',
       'location_city', 'location_state', 'location_country',
       'achievement_count', 'kudos_count', 'comment_count', 'athlete_count',
       'photo_count', 'trainer', 'commute', 'manual', 'private', 'visibility',
       'flagged', 'gear_id', 'start_latlng', 'end_latlng', 'average_speed',
       'max_speed', 'average_cadence', 'has_heartrate', 'average_heartrate',
       'max_heartrate', 'heartrate_opt_out', 'display_hide_heartrate_option',
       'elev_high', 'elev_low', 'upload_id', 'upload_id_str', 'external_id',
       'pr_count', 'total_photo_count', 'has_kudoed',
       'suffer_score'
    ]


    activities = pd.DataFrame(columns = columns)
    return activities

def fix_lat_long(activities):
    '''
    Splits the start and end latlong columns into separate lat and long columns
    Values as retrieved are lists, which cannot be loaded directly to the MySQL database
    accepts the populated activities dataframe, returns the dataframe with expanded columns
    
    '''
    # split into new columns
    start = pd.DataFrame(activities.start_latlng.to_list(), columns=['start_lat', 'start_long'])
    end = pd.DataFrame(activities.end_latlng.to_list(), columns=['end_lat', 'end_long'])
    # concat back to dataframe
    activities = pd.concat([activities, start, end], axis=1)
    # drop original columns
    activities.drop(columns=['start_latlng', 'end_latlng'], inplace=True)

    return activities

def get_all_activities(if_exists='replace'):
    '''
    Function to retrieve all activities for the user from Strava and load them to 
    the MySQL database.  If table already existed it will be overwritten unless other 
    argument is passed to the function (append, fail)
    returns a dataframe containing all activities
    '''
    # init
    strava_tokens = get_strava_token()
    activities = init_activities_df()
    columns = activities.columns
    
    #Loop through all activities
    page = 1
    url = "https://www.strava.com/api/v3/activities"
    access_token = strava_tokens['access_token']

    while True:
        # get page of activities from Strava
        r = requests.get(url + '?access_token=' + access_token + '&per_page=200' + '&page=' + str(page))
        r = r.json()
    # if no results then exit loop
        if (not r):
            break
        # otherwise add new data to dataframe
        for x in range(len(r)):
            for col in columns:
                activities.loc[x + (page-1)*200,col] = r[x].get(col, None)  

    # increment page
        page += 1
    # fix lat/long
    activities = fix_lat_long(activities)
    # set index
    activities.id = activities.id.astype(int)
    activities = activities.set_index('id')
    
    activities.to_sql('activities', env.get_url('leavitt_1861'), if_exists='replace')
    
    return activities

def get_new_activities(depth=10):
    '''
    Function to retrieve any new activities from Strava.  New activities are 
    those that are not already in the SQL database.  By default the function checks the 
    last 10 activities from Strava, but this can be changed by passing a depth argument
    The results are appended to the SQL database, and returned as a dataframe
    '''
    # init
    strava_tokens = get_strava_token()
    activities = init_activities_df()
    columns = activities.columns

    # Get ID of latest activity in database
    latest = pd.read_sql('''SELECT MAX(id) as latest FROM activities''', 
            env.get_url('leavitt_1861')).latest[0]
    
    url = "https://www.strava.com/api/v3/activities"
    access_token = strava_tokens['access_token']
    
    # get one page of activities
    r = requests.get(url + '?access_token=' + access_token)
    r = r.json()

    # check last 10 
    for x in range(depth):
        if r[x]['id'] > latest:
            for col in columns:
                activities.loc[x,col] = r[x].get(col, None)  
                
    # fix lat/long
    activities = fix_lat_long(activities)
    # set index
    activities.id = activities.id.astype(int)
    activities = activities.set_index('id')
    
    activities.to_sql('activities', env.get_url('leavitt_1861'), if_exists='append')
    
    return activities