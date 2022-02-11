import os
import json
import logging
import requests
import azure.functions as func
from pymongo import MongoClient
from collections import defaultdict
from azure.storage.blob import BlobServiceClient, ContentSettings

from typing import List, Dict


connect_str = os.environ['AzureWebJobsStorage']
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_name = "members"

Verified = '740429875046776953'
Alumni = '779048768145457173'


def getAllMembers(guild: str) -> List[Dict]:
    endpoint = "https://discordapp.com/api/guilds/{}/members".format(guild)
    headers = {"Authorization" : f"Bot {os.environ['DISCORD_BOT_TOKEN']}"}

    params = {
        "limit": 200
    }
    
    while True:
        logging.info('Getting members with params: %s', params)
        res = requests.get(endpoint, headers=headers, params=params)
        members = res.json()
        if not members:
            break
        params['after'] = members[-1]['user']['id']
        yield members


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    uri = os.environ["MONGO_URI"]
    mongodb = MongoClient(uri)
    db = mongodb["CodingClub"] 

    users = db.member.find({},{'_id':0,'name':1,'entry':1, 'discordid' : 1, 'rating':1})
    dbMap = {}
    for user in users:
        dbMap[user['discordid']] = {
            'name' : user['name'],
            'entry' : user['entry'],
            'rating' : user['rating']
        }
        
    complete_members = defaultdict(list)
    
    for memberList in getAllMembers('664156473944834079'):
        for member in memberList:

            if member['user'].get('bot'):
                continue
            if Alumni in member['roles'] or Verified not in member['roles']:
                continue

            discord_id = member['user']['id']

            if member['user']['avatar'] == None:
                vk = int(member['user']['discriminator'])%5
                img_link = f"https://cdn.discordapp.com/embed/avatars/{vk}.png"
            else:
                img_link = f"https://cdn.discordapp.com/avatars/{discord_id}/{member['user']['avatar']}.png"

            member_obj = {
                'name' : dbMap[discord_id]['name'],
                'entry': dbMap[discord_id]['entry'],
                'discordid' : discord_id,
                'image': img_link,
                'username' : f"{member['user']['username']}#{member['user']['discriminator']}",
                'rating' : dbMap[discord_id].get('rating',0)
            }

            batch = f"{member_obj['entry'][0:5]}G"
            complete_members[batch].append(member_obj)

    final_response = {}

    for batch in complete_members:
        final_response[batch] = {
            'length' : len(complete_members[batch]),
            'members' : sorted(complete_members[batch], key=lambda x: x['entry'][-4:])
        }

    out_tex = json.dumps(final_response, indent=4, sort_keys=True)

    logging.info('Uploading to blob storage')
    my_content_settings = ContentSettings(content_type='application/json')
    blob_client = blob_service_client.get_blob_client(container=container_name, blob="memberlist.json")
    blob_client.upload_blob(out_tex, overwrite=True, content_settings=my_content_settings)
    return func.HttpResponse('{"message" : "Refreshed successfully "}', mimetype="application/json")
