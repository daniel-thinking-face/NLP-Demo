import requests
from bs4 import BeautifulSoup
import re
import urllib

import pandas as pd
import numpy as np

# for datetime
from datetime import datetime

import json
from time import sleep

# Import this package
from operator import itemgetter

# Reddit API
import praw
import pprint

import csv

from youtube_scraper import *

from urllib.parse import urlparse

from googleapiclient.discovery import build
key_api = 'AIzaSyBjxpSWaZhil81FusrCQtbNlY4ufBIVRvI'

#### Functions

# Define a function to grabe page content
def content_grabber(soup_obj):

    '''This function gets the comments from each page'''

    # Now navigate to post content
    post_content = soup_obj.find('div',id="posts")

    #print('No of posts on page is {}'.format(len(post_content)))

    # Now get all messages
    all_messages = post_content.find_all('div', class_ = 'post')

    output_list = []

    # Now loop through
    for message in all_messages:

        content_dict = {}

        # Name
        content_dict['author'] = message.find('span', class_ = 'nick').get_text(strip=True)

        # Time
        content_dict['datetime'] = message.find('span', class_ = 'post_time').get_text(strip=True)

        # Content
        content_dict['content'] = message.find('div', class_ = 'talk-post').get_text(strip=True)

        output_list.append(content_dict)

    return output_list

# Getting product info
def product_grabber_df(link):

    '''this function allows you to get all the content for a subject by looping through the pages'''

    # Now go to page
    a = requests.get(link)

    sleep(0.5)

    # Define the content
    page_content = a.text

    # Look at soup
    soup = BeautifulSoup(page_content, 'html.parser')

    # Get first part of stem
    stem_url = link.split('?')[0]

    total_output = []

    #try:

    # First get number of posts
    number_of_posts_text = soup.find('div',class_ = 'message_pages').find('p').get_text(strip = True)
    number_of_posts = int(re.findall(r'\d+', number_of_posts_text)[1])

    #print('No posts is {}'.format(number_of_posts))



    # Now loop through
    for i in range(number_of_posts):

        page_no = i + 1

        #print('On page number {}'.format(page_no))

        if i == 0:

            url = stem_url

        else:

            url = stem_url + '?pg=' + str(page_no)

        page_request = requests.get(url)

        new_data = page_request.text

        # Look at soup
        soup = BeautifulSoup(new_data, 'html.parser')

        total_output += content_grabber(soup)
    #except:
     #   print('No dice')
      #  pass

    # Make df
    df = pd.DataFrame(total_output)

    try:
        # Make column
        df['url'] = link
    except:
        pass

    df['content'] = df['content'].map(lambda x: x.encode('utf-16','surrogatepass').decode('utf-16'))

    # return list
    return df

def grab_mumsnet_dataframe(list_of_links):

    '''this is a collection function for the two functions above'''

    # Define output
    master_list = []

    for link in list_of_links:

        try:

            temp_df = product_grabber_df(link)

            master_list.append(temp_df)

        except:
            pass

    master_df = pd.concat(master_list, ignore_index=True)

    master_df['source'] = 'Mumsnet'

    return master_df



# Reddit
def reddit_scraper(list_of_items):

    # Set up API credentials
    reddit = praw.Reddit(client_id='lLWVkFGL7xumbQ',
                         client_secret='JItPzlhx5ZdZ4TVB2u3V_mb0i8c',
                         user_agent='Captain_PotatoTomato')

    post_comments = []
    # Loop through links
    for url in list_of_items:

        post = reddit.submission(url=url)  # if you have the URL
        #post = reddit.submission(id='8ck9mb')  # if you have the ID

        # Iterate over all of the top-level comments on the post:
        post.comments.replace_more(limit=0)
        for comment in post.comments.list():


            comment_dict = {'content' : comment.body,
                           'author' : str(comment.author),
                           'time' : comment.created_utc,
                            'score' : comment.score,
                           'post_title' : post.title,
                           'url' : post.permalink,
                           'subreddit' : post.subreddit}

            post_comments.append(comment_dict)

    reddit_df = pd.DataFrame(post_comments)

    reddit_df['datetime'] = reddit_df['time'].map(lambda x: datetime.fromtimestamp(x).isoformat())

    reddit_df['source'] = 'Reddit'

    return reddit_df[['author', 'content', 'datetime', 'url', 'source']]

def s3_bucket_reader(csv_link):

    with requests.Session() as s:
        download = s.get(csv_link)

        decoded_content = download.content.decode('utf-16')

        cr = csv.reader(decoded_content.splitlines(), delimiter='\t')
        my_list = list(cr)

    twit_df = pd.DataFrame(my_list[1:], columns = my_list[0])

    # create nan columns
    twit_df['author'] = twit_df['Twitter Screen Name']

    # change to nan
    twit_df['author'] = twit_df['author'].map(lambda x: np.nan if x == '' else x)

    # fill
    twit_df['author'] = twit_df['author'].fillna(twit_df['Influencer'])

    # Change columns
    twit_df = twit_df.rename(columns = {'Date' : 'datetime',
                                       'URL' : 'url',
                                       'Hit Sentence' : 'content',
                                       'Source' : 'source'})

    return twit_df[['author', 'content', 'datetime', 'url', 'source']]

# The voice
def voice_scraper(v_url):

    r = requests.get(v_url)

    soup = BeautifulSoup(r.text, 'html.parser')

    discussion_total = soup.find_all('div', class_ = 'c-discussion__content')

    output_list = []
    for in_block in discussion_total:

        dict_c = {}
        dict_c['author'] = in_block.find('p',class_ = 'lead').get_text(strip=True)
        dict_c['datetime'] = in_block.find('p',class_ = 'date').get_text(strip=True)
        dict_c['content'] = in_block.find('div',class_ = 'text').get_text(strip=True)

        output_list.append(dict_c)

    voice_df = pd.DataFrame(output_list)
    voice_df['url'] = v_url
    voice_df['source'] = 'Voice Global'

    return voice_df

# make function
def remove_ptag(text):

    new_text = text.replace('<p>', '').replace('</p>', '')

    return new_text

def text_cleaner(input_text):

    soup = BeautifulSoup(input_text, 'html.parser')

    bad_tags = soup.find_all()

    for tag in bad_tags:

        input_text = input_text.replace(str(tag), '')

    soup = BeautifulSoup(input_text, 'html.parser')

    clean_text = soup.get_text().strip()

    return clean_text

def guardian_api_scraper(guardian_url):

    '''
    A scraper to tap the guardina comments api for scraping
    '''

    # request
    r = requests.get(guardian_url)

    # soup
    soup = BeautifulSoup(r.text, 'html.parser')

    api_space = soup.find_all('script')

    # get the script contining apikey
    script_elem = [x for x in api_space if str(x.string).strip().startswith('window.guardian')][0]

    script_elem = script_elem.text.strip()

    end_clean = script_elem.split('}')[-1]

    new_text = script_elem.replace('window.guardian = ', '').replace(end_clean, '')

    # json
    key_json = json.loads(new_text)

    # navigate to space holding api data
    data_space = key_json['app'].get('data')

    # Now extract the key
    api_key = data_space.get('CAPI').get('config').get('shortUrlId')

    # format string
    guardian_api_str = 'https://discussion.theguardian.com/discussion-api/discussion{}?api-key=dotcom-rendering&orderBy=oldest&pageSize=100&displayThreaded=false&maxResponses=100&page={}'

    # Make request to find out paging
    a = requests.get(guardian_api_str.format(api_key, 1))
    guardian_json = a.json()

    # page number
    max_page = guardian_json.get('pages') + 1

    output_guardian = []

    for i in range(1, max_page):
        r = requests.get(guardian_api_str.format(api_key, i))

        json_switch = r.json()

        output_guardian += json_switch['discussion']['comments']

    guardian_df = pd.DataFrame(output_guardian)

    # Tidy up
    guardian_df['content'] = guardian_df.body.map(remove_ptag)
    guardian_df.content = guardian_df.content.map(text_cleaner)
    guardian_df = guardian_df.loc[~guardian_df.content.str.startswith('This comment was removed')].copy()

    # Further cols
    guardian_df['author'] = guardian_df.userProfile.map(lambda x: x.get('displayName', 'NA'))
    guardian_df['datetime'] = guardian_df.isoDateTime
    guardian_df['url'] = guardian_url
    guardian_df['source'] = 'Guardian'

    return guardian_df[['author', 'content', 'datetime', 'url', 'source']]


def youtube_comment_grabber(youtube_link):

    video_id = youtube_link.split('v=')[1].split('&')[0]

    # container
    results_list = []

    # creating youtube resource object
    youtube = build('youtube', 'v3',
                    developerKey=key_api)

    # retrieve youtube video results
    video_response=youtube.commentThreads().list(
    part='snippet,replies',
    videoId=video_id
    ).execute()

    # iterate video response
    while video_response:

        # loop through
        for yt_item in video_response['items']:

            # make dict
            tube_dict = {}

            # Now get info
            #author
            tube_dict['author'] = yt_item['snippet']['topLevelComment']['snippet']['authorDisplayName']

            # datetime
            tube_dict['datetime'] = yt_item['snippet']['topLevelComment']['snippet']['publishedAt']

            # Likes
            tube_dict['like_count'] = yt_item['snippet']['topLevelComment']['snippet']['likeCount']

            # video id
            tube_dict['video_id'] = yt_item['snippet']['topLevelComment']['snippet']['videoId']

            # comment_id
            comment_id = yt_item['snippet']['topLevelComment']['id']

            tube_dict['comment_id'] = comment_id

            # comment
            tube_dict['content'] = yt_item['snippet']['topLevelComment']['snippet']['textOriginal']

            # replies
            tube_dict['is_reply'] = False
            tube_dict['reply_to_comment_id'] = ''

            # append to results list
            results_list.append(tube_dict)

            # counting number of reply of comment
            replycount = yt_item['snippet']['totalReplyCount']

            # if reply is there
            if replycount>0:

                # iterate through all reply
                for reply in yt_item['replies']['comments']:

                    # make dict
                    tube_dict = {}

                    # Now get info
                    #author
                    tube_dict['author'] = reply['snippet']['authorDisplayName']

                    # datetime
                    tube_dict['datetime'] = reply['snippet']['publishedAt']

                    # Likes
                    tube_dict['like_count'] = reply['snippet']['likeCount']

                    # video id
                    tube_dict['video_id'] = reply['snippet']['videoId']

                    # comment_id
                    reply_id = reply['id']

                    tube_dict['comment_id'] = reply_id

                    # comment
                    tube_dict['content'] = reply['snippet']['textOriginal']

                    # replies
                    tube_dict['is_reply'] = True
                    tube_dict['reply_to_comment_id'] = comment_id

                    # append to results list
                    results_list.append(tube_dict)

        page_token = video_response.get('nextPageToken')

        # Again repeat
        if 'nextPageToken' in video_response:
            video_response = youtube.commentThreads().list(
                    part = 'snippet,replies',
                    pageToken = page_token,
                    videoId = video_id
                ).execute()
        else:
            break

    d_test = pd.DataFrame(results_list)
    d_test['source'] = 'Youtube'
    d_test['url'] = youtube_link

    d_test = d_test[['author', 'content', 'datetime', 'url', 'source']].copy()
    return d_test

def daily_mail_comment_reader(url):

    '''
    Function to get dailymail comments. pass in the url and the function will do the rest!
    '''

    # Template for comment link
    dm_comment_template = 'https://www.dailymail.co.uk/reader-comments/p/asset/readcomments/{}?max=100&offset={}&order=desc&rcCache=shout'

    # req headers
    req_headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}

    # get article number
    art_number = re.findall(r'(?<=article-)\d+', url)[0]

    # Make a request
    a = requests.get(dm_comment_template.format(art_number, 0), headers = req_headers)

    ## json
    page_json = a.json()

    parent_count = page_json.get('payload').get('parentCommentsCount')

    max_pages = int(np.ceil(parent_count/100))

    output_container = []

    for i in range(max_pages):

        r = requests.get(dm_comment_template.format(art_number, i*100), headers = req_headers)

        json_data = r.json()

        comments = json_data.get('payload').get('page')

        output_container += comments

    df = pd.DataFrame(output_container)

    return df

def web_app_scraper(scrape_list):

    '''
    Function to scrape data with a list of urls as an input.

    Note this function has two outputs:

    1. Dataframe of scraped data
    2. List of rejected urls

    '''


    non_processed = []


    # sort data into lists
    mumsnet_urls = [link for link in scrape_list if 'mumsnet' in urlparse(link).netloc]
    reddit_urls = [link for link in scrape_list if 'reddit' in urlparse(link).netloc]
    guardian_urls = [link for link in scrape_list if 'guardian' in urlparse(link).netloc]
    voice_urls = [link for link in scrape_list if 'voice-global' in urlparse(link).netloc]
    amazon_urls = [link for link in scrape_list if 'amazonaws' in urlparse(link).netloc]
    youtube_urls = [link for link in scrape_list if 'youtube' in urlparse(link).netloc]
    daliymail_urls = [link for link in scrape_list if ('dailymail' in urlparse(link).netloc) or ('thisismoney' in urlparse(link).netloc)]

    total_links = [*mumsnet_urls, *reddit_urls, *guardian_urls, *voice_urls, *amazon_urls, *youtube_urls, *daliymail_urls]
    # rejected links
    reject_list = [link for link in scrape_list if link not in total_links]

    # Get the data
    try:
        mum_df = grab_mumsnet_dataframe(mumsnet_urls)
    except:
        mum_df = pd.DataFrame()
        reject_list += mumsnet_urls

    try:
        reddit_df = reddit_scraper(reddit_urls)

    except:
        reddit_df = pd.DataFrame()
        reject_list += reddit_urls
    try:
        guardian_df = pd.concat([guardian_api_scraper(x) for x in guardian_urls], ignore_index=True)
    except:
        guardian_df = pd.DataFrame()
        reject_list += guardian_urls
    try:
        voice_df = pd.concat([voice_scraper(x) for x in voice_urls], ignore_index=True)
    except:
        voice_df = pd.DataFrame()
        reject_list += voice_urls
    try:
        amazon_df = pd.concat([s3_bucket_reader(x) for x in amazon_urls], ignore_index=True)
    except:
        amazon_df = pd.DataFrame()
        reject_list += amazon_urls

    try:
        youtube_df = pd.concat([youtube_comment_grabber(x) for x in youtube_urls], ignore_index=True)
    except:
        youtube_df = pd.DataFrame()
        reject_list += youtube_urls

    try:
        dailymail_df = pd.concat([daily_mail_comment_reader(x) for x in daliymail_urls], ignore_index=True)
    except:
        dailymail_df = pd.DataFrame()
        reject_list += daliymail_urls

    processed_df = pd.concat([mum_df, reddit_df, guardian_df, voice_df, amazon_df, youtube_df], ignore_index=True)
    non_processed += reject_list

    return processed_df,non_processed
