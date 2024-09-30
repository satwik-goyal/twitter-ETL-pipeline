import json
import sqlite3
import argparse
from pathlib import Path
# Needs to be at least Python version 3.6

# DO NOT IMPORT ANY OTHER MODULES!

# Please make sure you don't add any "print" statements in your final version.
# Only existing prints should write to the terminal!
# If you "print" to help with development or debugging, make sure to remove them
# before you submit!


# YOU DO NOT NEED TO MODIFY THIS FUNCTION
# If you break this function, you likely break the whole script, so
# it's best not to touch it.
def load_json_from_js(p):
    """Takes a path to Twitter ad impression data and returns parsed JSON.
    
    Note that the Twitter files are *not* valid JSON but a Javascript file
    with a blog of JSON assigned to a Javascript variable, so some 
    preprocessing is needed.""" 
  
    # Note that this is a horrid hack. It's *fragile* i.e., if Twitter changes it's
    # variable name (currently "window.YTD.ad_engagements.part0 =") this will break.
    # It also requires loading the entire string into memory before parsing. If we're
    # running this on user machines on their own data this is probably fine, but if 
    # we're running it on a server the fact that we have the entire string AND the entire
    # parsed JSON structure in memory will add up.
    
    # If we use the standard json module, then there's no advantage to *not* doing this
    # if we want to json.load the file...it brings the string into memory anyway.
    #     https://pythonspeed.com/articles/json-memory-streaming/
    # We'd need to handle buffering ourselves or explore existing streaming solutions 
    # like:
    #     https://pypi.org/project/json-stream/
    # But then we'll have to play some tricks to avoid the junk at the beginning.
    #
    # Also, the weird, pointless, top level objects might break streaming. So we might
    # need to do a LOT of preprocessing.
    
    # ... further investigation of json-stream suggests it can handle the junk ok!
    #     https://github.com/daggaz/json-stream 
    return json.loads(p.read_text(encoding='utf-8')[33:])


# Don't touch this function!
def populate_db(adsjson, db):
    """Takes a blob of Twitter ad impression data and pushes it into our database.
    
    Note that this is responsible for avoiding redundant entries. Furthermore,
    it should be robust to errors and always get as much data in as it can.
    """ 
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    try:
        json2db(adsjson, cur)
    except:
        # We'd prefer no exceptions reached this level.
        print("There was a problem with the loader. This shouldn't happen")
    conn.commit()
    conn.close()

def json2db(adsjson, cur):
    """Processes the JSON and INSERTs it into the db via the cursor, cur"""
    
    # THIS IS WHAT YOU SHOULD MODIFY!
    # Feel free to add helper functions...you don't *need* to make a giant
    # hard to test function...indeed, that will come up in code review!
    for ad in adsjson:
        impressions = ad["ad"]["adsUserData"]["adImpressions"][ "impressions"]

        query = '''INSERT INTO impressions (device, displayLocation, promotedTweet, impressionTime, advertiser)
                VALUES (?, ?, ?, ?, ?);'''

        for impression in impressions:

            # Device info
            deviceId = json2db_device_info(impression , cur)

            # Disply Location
            displayLocation = None
            try:
                if "displayLocation" in impression.keys():
                  displayLocation = impression["displayLocation"]
            except Exception as e:
                pass

            # promotedTweetInfo
            promotedTweetId = json2db_promotedTweet_data(impression,cur)

            # ImpressionTime
            impressionTime = None
            try:
                if "impressionTime" in impression.keys():
                    impressionTime = impression["impressionTime"]
            except:
                pass

            #Advertisers Info
            advertiserName = json2db_advertisers_info(impression,cur)

            # Targeting Matching Criteria
            json2db_targeting_criteria(impression,cur)


            # populate the impression table
            try:
                cur.execute(query , (deviceId , displayLocation , promotedTweetId , impressionTime , advertiserName))
            except:
                pass

            # get the id of the latest row and populate matchingTargetingCriteria using it
            last_inserted_id = cur.lastrowid
            json2db_matching_targeting_criteria(last_inserted_id , impression , cur)


#Function to load device Info Data
def json2db_device_info(impression, cur):
    deviceId  = None
    osType = None
    deviceType = "digital"


    device_query = '''INSERT OR IGNORE INTO deviceInfo(osType, deviceId , deviceType)
    VALUES (?, ? , ?)'''

    
    if "osType" in impression["deviceInfo"].keys():
        osType = impression["deviceInfo"]["osType"]
    
    if "deviceType" in impression["deviceInfo"].keys():
        deviceType = impression["deviceInfo"]["deviceType"]

    try:
        
        if "deviceId" in impression["deviceInfo"].keys():
            deviceId = impression["deviceInfo"]["deviceId"]
            deviceId = f"{deviceId}-{deviceType}"
        else:
            deviceId = syn_device_id(cur)
    except Exception as e:
        pass

    

    try:
        cur.execute(device_query, (osType, deviceId, deviceType))
    except Exception as e:
        pass

    return deviceId

#Function to synthesize device Ids for missing deviceIds          
def syn_device_id(cur):
    def generate_key(increment):
        return f"SYT-device-{increment:04d}"
    
    max_increment =0
    try:

        cur.execute("SELECT deviceId FROM deviceInfo WHERE deviceId LIKE 'SYT-device-%'")
        keys = cur.fetchall()

        if not keys:
            return generate_key(1)

        # Extract the numeric part of the key and find the maximum increment
        max_increment = max(int(key[0].split('-')[-1]) for key in keys)

    except Exception as e:
        pass

    return generate_key(max_increment + 1)  


# Function to load promotedTweet_data
def json2db_promotedTweet_data(impression, cur):

    promoted_tweet_query = '''
    INSERT OR IGNORE INTO promotedTweetInfo 
    (tweetId, tweetText, urls, mediaUrls) 
    VALUES (?, ?, ?, ?)
    '''
    promotedTweetInfo = None

    if "promotedTweetInfo" not in impression.keys():
        unknowTweetId = syn_promotedTweet_id(cur)
        promotedTweetInfo = {"tweetId" : unknowTweetId, "tweetText": "This is text for unkown tweet" , "urls" : [] , "mediaUrls" : []}
    else:
        promotedTweetInfo = impression["promotedTweetInfo"]        



    tweetId = None
    tweetText = None
    urls = None  
    mediaUrls = None  

    if "tweetText" in promotedTweetInfo.keys():
        tweetText = promotedTweetInfo["tweetText"]
    
    if "urls" in promotedTweetInfo.keys():
        if promotedTweetInfo['urls']:
            urls = str(promotedTweetInfo['urls'])
    
    if "mediaUrls" in promotedTweetInfo.keys():
        if promotedTweetInfo['mediaUrls']:
            mediaUrls = str(promotedTweetInfo['mediaUrls'])

    try:
        if "tweetId" in promotedTweetInfo.keys():
            tweetId = promotedTweetInfo["tweetId"]
        else:
            tweetId = syn_promotedTweet_id(cur)
    except Exception as e:
        pass


    try:
        cur.execute(promoted_tweet_query, (tweetId, tweetText, urls, mediaUrls))
    except Exception as e:
        pass
    
    return tweetId

# Function to synthesise keys for Unknow and Same promoted Tweets
def syn_promotedTweet_id(cur):
    def generate_key(increment):
        return f"SYT-promotedTweet-{increment:04d}"
    
    max_increment =0
    try:
        
        cur.execute("SELECT tweetId FROM promotedTweetInfo WHERE tweetId LIKE 'SYT-promotedTweet-%'")
        keys = cur.fetchall()

        if not keys:
            return generate_key(1)

        # Extract the numeric part of the key and find the maximum increment
        max_increment = max(int(key[0].split('-')[-1]) for key in keys)

    except Exception as e:
        pass

    return generate_key(max_increment + 1)  

# Function to load advertiser info into the db
def json2db_advertisers_info(impression , cur):

    # query ignores the entry if it already exists in the db
    insert_query = '''
    INSERT OR IGNORE INTO advertiserInfo (advertiserName, screenName)
    VALUES (?, ?)''' 

    if "advertiserInfo" not in impression.keys():
        return
    advertiser = impression["advertiserInfo"]
    name = None
    screenName = None
    try:
        name = advertiser["advertiserName"]
        screenName = advertiser["screenName"]
        cur.execute(insert_query, (name, screenName))
    except Exception as error:
        pass
    
    return name


# Function to load targeting criteria into the db
def json2db_targeting_criteria(impression,cur):

    if "matchedTargetingCriteria" not in impression.keys():
        return []
    elif len(impression["matchedTargetingCriteria"]) == 0:
        return []
    
    query = '''
    INSERT INTO TargetingCriteria (targetingType, targetingValue)
    VALUES (?, ?)''' 

    targetingCriteria = impression["matchedTargetingCriteria"]

    for criteria in targetingCriteria:
        targetingType = None
        targetingValue = None


        if "targetingType" in criteria.keys():
            targetingType = criteria["targetingType"]
        
        if "targetingValue" in criteria.keys():
            targetingValue = criteria["targetingValue"]

        if not check_targetingCriteria_exits(targetingType, targetingValue , cur):
            try:
                cur.execute(query , (targetingType, targetingValue))
            except:
                pass

# This function checks for duplicate entries in the targeting Criteris table
def check_targetingCriteria_exits(targetingtype , targetingvalue, cur):


    # If the targetValue is Null 
    query2 ='''
    SELECT COUNT(*) FROM TargetingCriteria 
    WHERE targetingType = ? AND targetingValue IS NULL;
    '''
    
    if targetingvalue is None:
        cur.execute(query2, (targetingtype,))
        result = cur.fetchone()[0]
        return result > 0
    
    # If none of the values are null
    query1 = '''
    SELECT EXISTS (
        SELECT 1 
        FROM TargetingCriteria 
        WHERE targetingType = ? AND targetingValue = ?
    ) AS pair_exists;
    '''

    cur.execute(query1, (targetingtype, targetingvalue))
    result = cur.fetchone()[0]


    return bool(result)


# Function to populate the matching targeting criteria table
def json2db_matching_targeting_criteria(impression_id, impression , cur):
    if "matchedTargetingCriteria" not in impression.keys():
        return []
    elif len(impression["matchedTargetingCriteria"]) == 0:
        return []
    

    targetingCriteria = impression["matchedTargetingCriteria"]
    tIds = []

    for criteria in targetingCriteria:
        targetingType = None
        targetingValue = None
        if "targetingType" in criteria.keys():
            targetingType = criteria["targetingType"]
        if "targetingValue" in criteria.keys():
            targetingValue = criteria["targetingValue"]

        
        if targetingValue is None:
            query1 ='''
                    SELECT COUNT(*) FROM TargetingCriteria 
                    WHERE targetingType = ? AND targetingValue IS NULL;
                    '''
            cur.execute(query1, (targetingType,))
            result = cur.fetchone()[0]
            tIds.append(result)
        else:
        
            # If none of the values are null
            query2 = '''
            SELECT id FROM TargetingCriteria
            WHERE targetingType = ? AND targetingValue = ?;
            '''

            cur.execute(query2, (targetingType, targetingValue))
            result = cur.fetchone()[0]
            tIds.append(result)


    query = "INSERT INTO matchedTargetingCriteria (impression, criteria) VALUES (?, ?)"
    
    # Create a list of tuples for each pair of (impression, criteria)
    values = [(impression_id, criteria) for criteria in tIds]
    
    # Execute the query for all rows
    cur.executemany(query, values)


# DO NOT MODIFY ANYTHING BELOW!
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Load JSON from Twitter's ad-impressions.js into our database.")
    parser.add_argument('--source',  
                        type=Path,
                        default=Path('./ad-impressions.js'),
                        help='path to source  file')    
    parser.add_argument('--output', 
                        type=Path,
                        default=Path('./twitterads.db'),
                        help='path to output DB')    
    args = parser.parse_args()
    
    print('Loading JSON.')
    ads_json = load_json_from_js(args.source)
    print('Populating database.')    
    populate_db(ads_json, args.output)
    print('Done')
