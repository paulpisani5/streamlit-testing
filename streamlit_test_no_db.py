# Streamlit - Topic Stats / Details

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from streamlit.report_thread import get_report_ctx
# from sqlalchemy import create_engine
from datetime import datetime 
# import json
import os

import base64
from PIL import Image
import requests
from io import BytesIO
import math


# def extract_photo_url(json_field):
#     dict_field = json.loads(json_field)
#     if 'photo' in dict_field:
#         return dict_field['photo']
#     else:
#         return 'https://docs.microsoft.com/en-us/windows/win32/uxguide/images/mess-error-image4.png'

# def refresh_tables():

#     # engine.execute("""DROP TABLE sessions;""")
#     # engine.execute("""DROP TABLE sessions_2;""")
#     # engine.execute("""DROP TABLE submitted_responses;""")
#     # engine.execute("""DROP TABLE submitted_responses_topic;""")

#     engine.execute("""
#                 CREATE TABLE IF NOT EXISTS sessions (
#                 session_id VARCHAR (255) NOT NULL,
#                 created_on VARCHAR (255) NOT NULL,
#                 username VARCHAR (255) NOT NULL
#                 );
#     """)

#     engine.execute("""
#                 CREATE TABLE IF NOT EXISTS submitted_responses (
#                 session_id VARCHAR (255) NOT NULL,
#                 created_on VARCHAR (255) NOT NULL,
#                 topic_label VARCHAR (255) NOT NULL,
#                 topic_1 varchar (255) NOT NULL,
#                 topic_2 varchar (255) NOT NULL,
#                 topic_3 varchar (255) NOT NULL,
#                 other_topics varchar (255) NOT NULL
#                 );
#     """)

#     engine.execute("""
#                 CREATE TABLE IF NOT EXISTS submitted_responses_topic (
#                 session_id VARCHAR (255) NOT NULL,
#                 created_on VARCHAR (255) NOT NULL,
#                 topic_label VARCHAR (255) NOT NULL,
#                 topic_label_appropriateness VARCHAR (255) NOT NULL,
#                 improved_topic_label VARCHAR (255) NOT NULL,
#                 concerns_opportunities VARCHAR (255) NOT NULL
#                 );
#     """)

# def authenticate_user():
    
#     # existing_session = engine.execute("""SELECT COUNT(*) FROM sessions WHERE session_id = '%s'""" % session_id).first()[0] > 0
#     # if existing_session > 0: 
#     #     return True 

#     else: 
#         placeholder_1 = st.sidebar.empty()
#         placeholder_2 = st.sidebar.empty()
#         placeholder_3 = st.sidebar.empty()

#         username = placeholder_1.text_input("Enter username: ")
#         password = placeholder_2.text_input("Enter password: ")
#         sign_in = placeholder_3.button("Sign in")

#         if sign_in and ((username == 'paul' and password == 'test') or (username == 'kejia' and password == 'test2')):
#             today = str(datetime.now())
#             # engine.execute("""
#             #     INSERT INTO sessions(session_id, created_on, username) 
#             #     VALUES ('%s', '%s', '%s')
#             # """ % (session_id, today, username))

#             placeholder_1.empty()
#             placeholder_2.empty()
#             placeholder_3.empty()

#             return True

#         elif sign_in and ~((username == 'paul' and password == 'test') or (username == 'kejia' and password == 'test2')):
#             st.subheader('Login failed, please retry!')

#         else: 
#             return False

@st.cache
def load_activities_fast(topic_id, activity_count):
    top_matches = pd.read_csv('activities_export_small_TEST.csv', low_memory=False)
    top_matches_filtered = top_matches[top_matches['topic_id'] == topic_id]
    return top_matches_filtered[:activity_count]

@st.cache
def load_activities(topic_id, activity_count):

    activities = pd.read_csv('activities_export_TEST.csv', low_memory=False)
#     activities['photo_url'] = activities['details'].apply(lambda x: extract_photo_url(x))
    relevant_columns = [
        'uid',
        'title',
        'user_uid',
        'summary',
        'published_at',
        'total_enrollment',
        'total_first_time_enrollment',
        'age_min',
        'age_max',
#         'photo_url',
        'tokens',
        'topic_0_id_global',
        'topic_0_score_global',
        'topic_1_id_global',
        'topic_1_score_global',
        'topic_2_id_global',
        'topic_2_score_global',
        'topic_3_id_global',
        'topic_3_score_global',
        'topic_4_id_global',
        'topic_4_score_global'
    ]
    activities['total_enrollment'] = activities['total_enrollment'].replace(np.nan,0)
    activities['total_first_time_enrollment'] = activities['total_first_time_enrollment'].replace(np.nan,0)
    activities_filtered = activities[relevant_columns].copy()

    rank_1_match = activities_filtered[(activities_filtered['topic_0_id_global'] == topic_id)].copy() 
    rank_1_match['match_rank'] = 0
    rank_1_match['match_score'] = rank_1_match['topic_0_score_global']

    rank_2_match = activities_filtered[(activities_filtered['topic_1_id_global'] == topic_id)].copy() 
    rank_2_match['match_rank'] = 1
    rank_2_match['match_score'] = rank_2_match['topic_1_score_global']

    rank_3_match = activities_filtered[(activities_filtered['topic_2_id_global'] == topic_id)].copy() 
    rank_3_match['match_rank'] = 2
    rank_3_match['match_score'] = rank_3_match['topic_2_score_global']

    rank_4_match = activities_filtered[(activities_filtered['topic_3_id_global'] == topic_id)].copy() 
    rank_4_match['match_rank'] = 3
    rank_4_match['match_score'] = rank_4_match['topic_3_score_global']

    rank_5_match = activities_filtered[(activities_filtered['topic_4_id_global'] == topic_id)].copy() 
    rank_5_match['match_rank'] = 4
    rank_5_match['match_score'] = rank_5_match['topic_4_score_global']

    top_matches = pd.concat([
        rank_1_match,
        rank_2_match,
        rank_3_match,
        rank_4_match,
        rank_5_match
    ])
    top_matches.sort_values(by='match_score', ascending=False, inplace=True)
    return top_matches[:activity_count]

@st.cache
def load_activity_stats(topic_id):
    stats = pd.read_csv('topic_stats_sample.csv', low_memory=False)
    stats_filtered = stats[stats['topic_id'] == topic_id]
    return stats_filtered

@st.cache
# def load_image(activity_url, photo_url):
#     html = "<a href='{url}'><img src='{src}' width='300'></a>".format(url=activity_url, src=photo_url)
#     return html

def load_topic():
    df = pd.read_csv('topic_labels_w_description.csv')
    df['full_label'] = df['topic_id'].astype('str') + ' - ' + df['label_lvl_1']

    topic_label = st.sidebar.selectbox(
        'Which topic would you like to explore?',
        df['full_label']
    )
    topic_id = int(topic_label.split(' - ')[0])

    activity_count_label = st.sidebar.selectbox(
        'How many activities samples do you want to review?',
        [10,20,50,100]
    )

    df_filtered = df[df['full_label'] == topic_label].copy()

    st.header(topic_label)
    st.title('')

    df_wordset = df_filtered[['word_0','word_1','word_2','word_3','word_4','word_5','word_6','word_7','word_8']].copy().T
    df_total_activities = df_filtered['size'].iloc[0].astype('str')
    df_activities_w_enrollments = df_filtered['activities_with_enrollments'].iloc[0].astype('str')

    col1, col2 = st.beta_columns(2)
    with col1:
       st.subheader("Word Set")
       st.write(df_wordset)
    with col2:
       st.subheader("Topic Stats")
       st.subheader("")
       st.write("Activities with enrollments: ", df_total_activities)
       st.write("Published activities: ", str(df_activities_w_enrollments))


    st.write('')
    col1, col2 = st.beta_columns(2)
    with col1:
        st.subheader("Topic Review")
        topic_label_appropriateness = st.radio("Activity coherence score (1=terrible, 5=great): ", (1,2,3,4,5))
        improved_topic_label = str(st.text_input("Improved topic label (use comma delimited list for multiple tags): "))
        concerns_opportunities = str(st.text_input("Any concerns or opportunities for improvement: "))
        
        if st.button('Submit'):
            today = str(datetime.now())
            
            st.write('Response saved')

    st.write('')
    df = load_activity_stats(topic_id)
    st.subheader('Monthly Metrics')

    metric_set = {
        # 'total_enrollments': 'Total Enrollments',
        'new_buyers': 'New Buyers',
        'total_buyers': 'Total Buyers',
        'bookings': 'Total Bookings',
        'activities_created': 'Activities Created',
        'total_sellers': 'Total Sellers'
    }
    
    df_pivot = df[df['metric'] != 'total_enrollments'][['month', 'metric', 'metric_total']].pivot(index='month', columns='metric', values='metric_total')
    st.dataframe(df_pivot)


    # for metric in metric_set: 
    #     st.write('')
    #     st.write(metric_set[metric])
    #     bookings = df[df['metric'] == metric].copy()
    #     bookings_filtered = bookings[bookings['month'].notnull()][['topic_id','month','metric_total']].copy()
    #     # bookings_filtered['month'] = bookings_filtered['month'].apply(lambda x: datetime.strptime(x, '%Y_%m_%d'))
    #     bookings_filtered['metric_total'] = bookings_filtered['metric_total'].astype('int')
    #     # bookings_filtered['topic_id'] = bookings_filtered['topic_id'].astype('int')
    #     bookings_pivoted = bookings_filtered.pivot(index='topic_id', columns='month', values='metric_total').fillna(0)
    #     st.dataframe(bookings_pivoted)
    # # st.line_chart(bookings_pivoted)

    # st.write('')
    # st.write('Bookings')
    # bookings = df[df['metric'] == 'bookings'].copy()
    # bookings_filtered = bookings[bookings['month'].notnull()][['topic_id','month','metric_total']].copy()
    # # bookings_filtered['month'] = bookings_filtered['month'].apply(lambda x: datetime.strptime(x, '%Y_%m_%d'))
    # bookings_filtered['metric_total'] = bookings_filtered['metric_total'].astype('int')
    # bookings_filtered['topic_id'] = bookings_filtered['topic_id'].astype('int')
    # bookings_pivoted = bookings_filtered.pivot(index='topic_id', columns='month', values='metric_total').fillna(0)
    # st.dataframe(bookings_pivoted)



    # bookings_formatted = bookings[bookings['month'].notnull()][['month','metric_total']].copy()
    
    # st.dataframe(bookings_formatted.T)
    # st.dataframe(bookings_formatted['month'].values)
    # st.dataframe(bookings_formatted['metric_total'].values)

    # from datetime import datetime
    # # bookings_formatted['month'] = bookings_formatted['month'].apply(lambda x: datetime.strptime(x, '%Y_%m_%d'))
    # st.line_chart(pd.DataFrame(bookings_formatted['metric_total'].values, columns=bookings_formatted['month'].values))


    activity_set = load_activities_fast(topic_id, activity_count_label)

    st.write('')
    st.subheader("Top Activities")

    for i in range(0,len(activity_set)):
        st.write(i)
        row = activity_set.iloc[i]
        activity_url = 'http://www.outschool.com/classes/' + row['uid']
        col1, col2 = st.beta_columns(2)
#         with col1:
#             photo_url = row['photo_url']
#             st.markdown(load_image(activity_url, photo_url), unsafe_allow_html=True)
            # html = "<a href='{url}'><img src='{src}' width='300'></a>".format(url=activity_url, src=photo_url)
            # st.markdown(html, unsafe_allow_html=True)  
            
        with col2: 
            st.markdown("""[**%s**] (%s)""" % (row['title'], activity_url))
            total_enrollment = row['total_enrollment']
            if math.isnan(total_enrollment):
                total_enrollment = 0
            total_first_time_enrollment = row['total_first_time_enrollment']
            if math.isnan(total_first_time_enrollment):
                total_first_time_enrollment = 0

            details_1 = 'Ages: ' + str(int(row['age_min'])) + '-' + str(int(row['age_max'])) 
            details_2 = str(int(total_enrollment)) + ' enrolls (' + str(int(total_first_time_enrollment)) + ' first time)' 
            st.write('*' + details_1 + ' | ' + details_2 + '*')
            # st.markdown("""[Activity URL] (%s)""" % activity_url)
            st.write('Summary: ' + row['summary'])

        st.markdown("---")


    if st.button('See more'):
        activity_count_label = 20




st.markdown(
        f"""
<style>
    .reportview-container .main .block-container{{
        max-width: 2000px;
        padding-left: 10rem;
        padding-right: 10rem;
    }}
</style>
""",
        unsafe_allow_html=True,
    )

# engine = create_engine('postgresql://paulpisani:topsecret@localhost:5432/paul2')
####### st.set_page_config(page_title='Topic Explorer', page_icon=None, layout='wide', initial_sidebar_state='auto')
session_id = get_report_ctx().session_id
session_id = session_id.replace('-','_')

# refresh_tables()
# if authenticate_user():
#     load_topic()
load_topic()
