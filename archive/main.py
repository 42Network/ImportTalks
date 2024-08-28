import os
import requests
import json
import jmespath
import jq
import concurrent.futures as cf

import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:',
                    filename='./logfile.txt', filemode='a')
doc_list = []

base_study_url = "https://www.churchofjesuschrist.org/study"
base_content_url = "https://www.churchofjesuschrist.org/study/api/v3/language-pages/type/dynamic?lang=eng&uri="

###############
# There are two ways to get Conference Talks: /general-conference and /liahona
#
# 1. /general-conference
#    E.g.  https://www.churchofjesuschrist.org/study/api/v3/language-pages/type/dynamic?lang=eng&uri=/general-conference/1973/10
#    Starts in April 1971
#    Has toc sections for each conference session
#    Does NOT have per-talk PDF URLs until 2022????
#
# 2. /liahona
#     E.g. https://www.churchofjesuschrist.org/study/api/v3/language-pages/type/dynamic?lang=eng&uri=/liahona/1987/05
#     Starts in April 1977
#     Does NOT have toc sections for each conference session until 2010
#     Does have per-talk PDF links starting October 2008 (sporadically found in 2007 also)
#     Must add one to month number since magazine is published one month after conference
#

def get_conference_toc(year: int, month:int):
    if year < 1977:
        using_gc_link = True
        conf_url = f"{base_content_url}/general-conference/{year}/{month:02d}"
    else:
        using_gc_link = False
        magazine_month = month + 1  # Add one because Liahona comes out one month after conference
        conf_url = f"{base_content_url}/liahona/{year}/{magazine_month:02d}"
    logging.info(f"Looking up TOC for {year=} {month=} using {conf_url=}")
    try:
        r = requests.get(conf_url)
        r.raise_for_status()
    except (OSError, requests.exceptions.HTTPError) as err:
        # raise SystemExit(err)
        logging.warning(f"Error getting Conference TOC with {conf_url=}: {err}")
        return False
    j = r.json()
    if not 'toc' in j:
        logging.warning(f"No TOC found for {conf_url=}")
        return False
    logging.info(f"TOC found TOC for {year=} {month=}")
    return j

conferences = []
for year in (1971, 1973, 1977, 1987, 1991, 2007, 2008, 2010, 2023):
#for year in (2008, 2023):
    for month in (4, 10):
        conferences =+ (year, month)
    using_gc_link = False
    for month in (4, 10):
        if year < 1977:
            using_gc_link = True
            conf_url = f"{base_content_url}/general-conference/{year}/{month:02d}"
        else:
            using_gc_link = False
            magazine_month = month + 1  # Add one because Liahona comes out one month after conference
            conf_url = f"{base_content_url}/liahona/{year}/{magazine_month:02d}"
        #print(f"{conf_url=}")

        try:
            r = requests.get(conf_url)
            r.raise_for_status()

        except (OSError, requests.exceptions.HTTPError) as err:
            #raise SystemExit(err)
            print(f"Error for base URL {err}")
        # print(f"{r=}")
        j = r.json()
        # if 'toc'  in j:
        #     print(j['toc']['entries'])
        #print(f"{j=}")
        if 'toc' in j:
            #titles = jmespath.search("toc.entries[].section | {Session: title, Title: entries.content.title, Speaker: entries.content.subtitle, Uri: entries.content.uri}" , j)
            #titles = jmespath.search("toc.entries[].section.entries[].content.{Title: title, Speaker: subtitle, Uri: uri}" , j)

            jq_query = '''\
                .toc | .title as $magazine_title | .category as $category | .entries[]?.content 
                | {Category: $category, Magazine: $magazine_title, Session: "Not Specified", Title: .title, Speaker: .subtitle, Uri: .uri}
                '''

            jq_query_sessions = '''\
                .toc | .title as $magazine_title | .category as $category | .entries[].section 
                | .title as $session_title | .entries[]?.content 
                | {Category: $category, Magazine: $magazine_title, Session: $session_title, Title: .title, Speaker: .subtitle, Uri: .uri}
                '''
            if using_gc_link or (not using_gc_link and year >= 2010):
                jq_query = jq_query_sessions

            titles = jq.compile(jq_query).input_value(j)
            for item in titles:
                talk = {}
                if item['Uri'] is None:
                    continue # this is a bad entry, go to next
                talk['date'] = f"{year}-{month:02d}-01"
                talk['reference'] = f"{item['Category']} - {item['Magazine']}"
                if 'Session' in item:
                    talk['session'] = item['Session']
                talk['title'] = item['Title']
                talk['speaker'] = item['Speaker']
                canonical_uri = item['Uri']
                talk_content_url = f"{base_content_url}{canonical_uri}"
                talk_study_url = f"{base_study_url}{canonical_uri}"
                talk['canonical_uri'] = canonical_uri
                talk['talk_content_url'] = talk_content_url
                talk['talk_study_url'] = talk_study_url

                try:
                    r2 = requests.get(talk_content_url)
                    r2.raise_for_status()
                except (OSError, requests.exceptions.HTTPError) as err:
                    # raise SystemExit(err)
                    print(f"Error for talk URL {err}")
                #print(f"{r2.text=}")
                j2 = r2.json()
                pdf_url = jq.first('.content.meta.pdf.source', j2)
                talk['pdf_url'] = pdf_url
                doc_list.append(talk)
                print(talk)
                if pdf_url:
                    response = requests.get(pdf_url)
                    file_pathname = "/tmp/gc_download/" + os.path.basename(pdf_url)
                    with open(file_pathname, 'wb') as f:
                        f.write(response.content)
                #print(f'{year},{month},"{talk_title}","{talk_speaker}","{talk_url}"')
            # print(f"{titles=}")
        print()

# https://www.churchofjesuschrist.org/study/api/v3/language-pages/type/content?lang=eng
#   &uri=/liahona/2012/05/saturday-morning-session/teaching-our-children-to-understand

#/toc/entries/1/section/title

#   toc,
#       entries (just one for whole conference),
#           section (for each session),
#               title (of session),
#               entries (for each talk),
#                   content,
#                       uri, title, subtitle (speaker)
#curl "https://www.churchofjesuschrist.org/study/api/v3/language-pages/type/dynamic?lang=eng&uri=/general-conference/2020/04" | jq '..|.content?.uri?,.title?,.subtitle?'