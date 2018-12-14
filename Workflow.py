import os
import certifi
from elasticsearch import Elasticsearch
import pandas as pd
from pandas import ExcelWriter
import numpy as np

userid = ' '

user = str(os.getenv('KIBANA-ANALYTICS-USER'))
pw = str(os.getenv('KIBANA-ANALYTICS-PASSWORD'))


def es_auth(user, pw):
    try:
        es = Elasticsearch(
            [' '],
            http_auth=(user, pw),
            port=443,
            use_ssl=True,
            verify_certs=True,
            ca_certs=certifi.where()
        )
        es.info()
    except:
        print("Unable to Auth to ES")
        exit()
    return es


def make_query_PSL(list_of_user_ids, es):
    return es.search(index=" *", body={
        "size": 10000,
        "query": {
            "bool": {
                "should": [
                    {
                        "terms": {
                            "feed.user_id": list_of_user_ids
                        }
                    }
                ]
            }
        }
    })


def build_df_PSL(response):
    d = []
    for log in response['hits']['hits']:
        tag = log['_source']['tag']
        feed = log['_source']['feed']
        if tag.get('urlref') is None:
            urlref = None
            origurl = None
        else:
            origurl = tag.get('url').get('original_url')
            urlref = tag.get('urlref').get('original_url')
        d.append({
            'searchterm': tag.get('searchterm'),
            'usersearch': tag.get('usersearch'),
            'lnpracticearea': tag.get('lnpracticearea'),
            'prefilter PA': tag.get('searchpalist'),
            'wa_value': tag.get('wa_value'),
            'referrer': urlref,
            'original_url': origurl,
            'lnaction': tag.get('lnaction'),
            'userdocument': tag.get('userdocument'),
            'lndocresultype': tag.get('lndocresultype'),
            'docno': tag.get('docno'),
            'result count': tag.get('resultcount'),
            'gmt_timestamp': feed['gmt_timestamp'],
            'epoch_seconds_timestamp': feed['epoch_seconds_timestamp'],
            'lndoctitle': tag.get('lndoctitle'),
            'wa_recid': tag.get('wa_recid'),
            'sessionid': tag.get('sessionid'),
            'user': feed['user_id'],
            'wa_event': tag.get('wa_event'),
            'docreachtype': tag.get('docreachtype'),
            'lndoccsi': tag.get('lndoccsi'),
            'lndoclni': tag.get('lndoclni'),
            'lndocselector': tag.get('lndocselector'),
            'lndoctype': tag.get('lndoctype'),
            'lnmasterpa': tag.get('lnmasterpa'),
            'lnpa': tag.get('lnpa'),
            'lnsubtopicn': tag.get('lnsubtopicn'),
            'lntopicn': tag.get('lntopicn'),
            'lnut': tag.get('lnut'),
            'userevent': tag.get('userevent'),
            'e_a': tag.get('e_a'),
            'e_c': tag.get('e_c'),
            'stage_title': tag.get('stage_title'),
            'wa_result': tag.get('wa_result')

        })
    return pd.DataFrame(d)


def make_query_lib(list_of_user_ids, es):
    return es.search(index=" *", body={
        "size": 10000,
        "query": {
            "bool": {
                "should": [
                    {
                        "terms": {
                            "feed.user_id": list_of_user_ids
                        }
                    }
                ]
            }
        }
    })


def build_df_lib(response_lib):
    d = []
    for log in response_lib['hits']['hits']:
        tag = log['_source']['tag']
        feed = log['_source']['feed']
        if tag.get('urlref') is None:
            urlref = None
            origurl = None
        else:
            origurl = tag.get('url').get('original_url')
            urlref = tag.get('urlref').get('original_url')
        d.append({
            'searchquery': tag.get('searchquery'),
            'referrer': urlref,
            'original_url': origurl,
            'gmt_timestamp': feed['gmt_timestamp'],
            'epoch_seconds_timestamp': feed['epoch_seconds_timestamp'],
            'wa_recid': tag.get('wa_recid'),
            'sessionid': tag.get('sessionid'),
            'user': feed['user_id'],
            'formid': tag.get('formid'),
            'formName': tag.get('formname'),
            'searchFormType': tag.get('searchformtype'),
            'enhSearchType': tag.get('enhsearchtype'),
            'typeofsearch': tag.get('typeofsearch'),
            'sourceUsedForSearch': tag.get('sourceusedforsearch'),
            'NoOfSrchResults': tag.get('noofsrchresults'),
            'NoOfPslSrchResults': tag.get('noofpslsrchresults'),
            'flapId': tag.get('flapid'),
            'orgDocNo': tag.get('orgdocno'),
            'homeCSI': tag.get('homecsi'),
            'chapterTitle': tag.get('chaptertitle'),
            'documentTitle': tag.get('documenttitle'),
            'useraction': tag.get('useraction'),
            'pageTitle': tag.get('page_title'),
            'requestURL': tag.get('requesturl'),
            'browse': tag.get('browse'),
            'originationCode': tag.get('originationcode'),
            'isnodocresults': tag.get('isnodocresults')
        })
    return pd.DataFrame(d)


def new_30m_session(data):
    data = data.sort_values(by='epoch_seconds_timestamp')
    data['timeDiff'] = data.groupby(['user'])['epoch_seconds_timestamp'].diff()
    data['timeDiff'] = data.groupby(['user'])['timeDiff'].shift(-1)
    data['new_session_starts'] = np.where(data['timeDiff'] > 1800, True, False)
    data['new_session'] = np.where(data['new_session_starts'],
                                   data.index,
                                   -1)
    for name, group in data.groupby('user'):
        first_time = 0
        for idx, row in group.iterrows():
            if first_time == 0:
                if row['new_session_starts']:
                    first_time = row['epoch_seconds_timestamp']
                else:
                    data.at[idx, 'new_session'] = -1

                continue

                delta_time = row['epoch_seconds_timestamp'] - first_time

                if delta_time < timeout:
                    data.at[idx, 'new_session'] = -1
                else:
                    if row['new_session_start']:
                        first_time = row['epoch_seconds_timestamp']
                    else:
                        data.at[idx, 'search_session'] = -1

    # fill search sessions down
    data['new_session'] = data['new_session'].replace(to_replace=-1, value=None, method='ffill')
    return data

def get_sessions(UserId):
    elastic = es_auth(user, pw)
    psl_data = make_query_PSL([UserId], elastic)
    response = build_df_PSL(psl_data)
    lib_data = make_query_lib([UserId], elastic)
    response_lib = build_df_lib(lib_data)
    data = pd.concat([response, response_lib], ignore_index=True, sort=True)
    data = new_30m_session(data)
    writer = ExcelWriter(UserId + '.xlsx')
    data.sort_values('epoch_seconds_timestamp').to_excel(writer, columns=[
        'gmt_timestamp',
        'searchterm',
        'docreachtype',
        'lndocselector',
        'lndoctype',
        'lnmasterpa',
        'lnpa',
        'lnsubtopicn',
        'lntopicn',
        'lndoctitle',
        'searchquery',
        'chapterTitle',
        'documentTitle',
        'pageTitle',
        'user',
        'new_session'
        ])
    writer.save()

if __name__ == "__main__":
    get_sessions(userid)
