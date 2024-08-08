import os
import requests
import jq
import json
import concurrent.futures as cf
import itertools as it

import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:',
                    filename='./logfile.txt', filemode='a')

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
# Basically,
#

def get_conference_toc(year: int, month: int):
    """ Get General Conference Table of Contents for year/month and return as JSON object
    :param year:
    :param month:
    """
    if year < 1977:
        using_gc_link = True
        conf_url = f"{base_content_url}/general-conference/{year}/{month:02d}"
    else:
        magazine_month = month + 1  # Add one because Liahona comes out one month after conference
        conf_url = f"{base_content_url}/liahona/{year}/{magazine_month:02d}"
    logging.info(f"Looking up TOC for {year=} {month=} using {conf_url=}")
    try:
        r = requests.get(conf_url, timeout=5)
        r.raise_for_status()
    except (OSError, requests.exceptions.HTTPError) as err:
        # raise SystemExit(err)
        logging.warning(f"Error getting Conference TOC with {conf_url=}: {err}")
        return False
    j = r.json()
    if not 'toc' in j:
        logging.warning(f"No TOC found for {conf_url=}")
        return False
    logging.info(f"TOC found TOC for {year=} {month=} {j['toc']}")
    return j


def toc_runner(year, month):
    return year, month, get_conference_toc(year, month)


def get_conference_talk(talk_content_url: str):
    try:
        r = requests.get(talk_content_url, timeout=1)
        r.raise_for_status()

    except (OSError, requests.exceptions.HTTPError) as err:
        # raise SystemExit(err)
        logging.warning(f"Error getting talk content with {talk_content_url=}: {err}")
        return False
    # print(f"{r2.text=}")
    j = r.json()
    pdf_url = jq.first('.content.meta.pdf.source', j)
    if not pdf_url:
        logging.warning(f"No PDF URL found for {talk_content_url=}")
        return False
    logging.info(f"PDF URL found: {pdf_url}")
    return pdf_url


def talk_runner(talk):
    talk['pdf_url'] = get_conference_talk(talk['talk_content_url'])
    return talk


def analyze_talks(talks):
    num_talks = with_pdf = with_speaker = with_speaker_and_pdf = 0
    print(f"Conference Talks|w/Spkr|w/PDFs|w/Both Sessions|Speakers")
    for talk in talks:
        logging.debug(talk)
        num_talks += 1
        if talk['speaker']:
            with_speaker += 1
        if talk['pdf_url']:
            with_pdf += 1
        if talk['pdf_url'] and talk['speaker']:
            with_speaker_and_pdf += 1
    print(f"Overall   {num_talks:>5d}|{with_speaker:>6d}|{with_pdf:>6d}|{with_speaker_and_pdf:>6d}")

    for date, talks in it.groupby(talks, key=lambda item: item['year_month']):
        num_talks = with_pdf = with_speaker = with_speaker_and_pdf = 0
        for talk in talks:
            num_talks += 1
            if talk['speaker']:
                with_speaker += 1
            if talk['pdf_url']:
                with_pdf += 1
            if talk['pdf_url'] and talk['speaker']:
                with_speaker_and_pdf += 1

        print(f"{date} {num_talks:>5d}|{with_speaker:>6d}|{with_pdf:>6d}|{with_speaker_and_pdf:>6d}")
        # if with_speaker_and_pdf < 5:
        #     logging.warning(f"Low number of speaker PDFs for {date}: {talks}")


def get_toc_list(years, months):
    with cf.ThreadPoolExecutor(max_workers=50) as executor:
        tocs = list(executor.map(toc_runner, years, months))

    print(f"Found {len(tocs)} TOCs from {len(months)} conferences")
    return tocs


def get_talks(tocs):
    total_talk_counter = 0
    doc_list = []
    for (year, month, toc) in tocs:
        logging.debug(f"{year=} {month=} {toc=}")
        if toc is False:
            logging.warning(f"No TOC found for {year=} {month=}")
            continue
        jq_query = '''\
            .toc | .title as $magazine | .category as $category | 
                (.entries[].section | .title as $session | .entries[]?.content | 
                    {$category, $magazine, $session, title: .title, speaker: .subtitle, uri: .uri}), 
                (.entries[]?.content | 
                    {$category, $magazine, session: "Not Specified", title: .title, speaker: .subtitle, uri: .uri})
            '''

        jq_query_sessions = '''\
            .toc | .title as $magazine_title | .category as $category | .entries[].section
            | .title as $session_title | .entries[]?.content
            | {Category: $category, Magazine: $magazine_title, Session: $session_title, Title: .title, Speaker: .subtitle, Uri: .uri}
            '''

        # if using_gc_link or (not using_gc_link and year >= 2010):
        #    < 1977   or ( >= 1977 and >= 2010
        #     jq_query = jq_query_sessions
        # has_sections = jq.all('.toc.entries[].section', toc)
        # if (int(has_sections) > 0):
        #     jq_query = jq_query_sessions
        # print(f"{year} {month} {has_sections}")
        # logging.info(f"{has_sections=}")

        conference_talk_counter = 0
        titles = jq.compile(jq_query).input_value(toc)
        for item in titles:
            logging.debug(f"{item=}")
            talk = {}
            if item['uri'] is None:
                continue  # this is a bad entry, go to next
            talk['year_month'] = f"{year}-{month:02d}-01"
            talk['reference'] = f"{item['category']}-{item['magazine']}"
            talk['session'] = item['session']
            talk['title'] = item['title']
            talk['speaker'] = item['speaker']
            canonical_uri = item['uri']
            talk_content_url = f"{base_content_url}{canonical_uri}"
            talk_study_url = f"{base_study_url}{canonical_uri}"
            talk['canonical_uri'] = canonical_uri
            talk['talk_content_url'] = talk_content_url
            talk['talk_study_url'] = talk_study_url
            conference_talk_counter += 1
            total_talk_counter += 1
            talk['conference_talk_counter'] = conference_talk_counter
            talk['total_talk_counter'] = total_talk_counter
            doc_list.append(talk)
            # print(talk)

    with cf.ThreadPoolExecutor(max_workers=50) as executor:
        talks = list(executor.map(talk_runner, doc_list))
    return talks


if __name__ == "__main__":
    import time

    years = []
    months = []
    # for year in (1971, 1973, 1977, 1987, 1991, 2007, 2008, 2010, 2023):
    for year in (2018, 2020):
        # for year in range(1971, 2025):
        for month in (4, 10):
            years.append(year)
            months.append(month)
    print(years, months)

    t0 = time.time()
    tocs = get_toc_list(years, months)
    t1 = time.time()
    print(f"Time: get_toc_list = {t1 - t0:.2f} seconds")
    t0 = time.time()
    talks = get_talks(tocs)
    t1 = time.time()
    print(f"Time: get_talks = {t1 - t0:.2f} seconds")

    analyze_talks(talks)

    #
    #             try:
    #                 r2 = requests.get(talk_content_url)
    #                 r2.raise_for_status()
    #             except (OSError, requests.exceptions.HTTPError) as err:
    #                 # raise SystemExit(err)
    #                 print(f"Error for talk URL {err}")
    #             #print(f"{r2.text=}")
    #             j2 = r2.json()
    #             pdf_url = jq.first('.content.meta.pdf.source', j2)
    #             talk['pdf_url'] = pdf_url
    #             doc_list.append(talk)
    #             print(talk)
    #             if pdf_url:
    #                 response = requests.get(pdf_url)
    #                 file_pathname = "/tmp/gc_download/" + os.path.basename(pdf_url)
    #                 with open(file_pathname, 'wb') as f:
    #                     f.write(response.content)
    #             #print(f'{year},{month},"{talk_title}","{talk_speaker}","{talk_url}"')
    #         # print(f"{titles=}")
    #     print()

# https://www.churchofjesuschrist.org/study/api/v3/language-pages/type/content?lang=eng
#   &uri=/liahona/2012/05/saturday-morning-session/teaching-our-children-to-understand

# /toc/entries/1/section/title

#   toc,
#       entries (just one for whole conference),
#           section (for each session),
#               title (of session),
#               entries (for each talk),
#                   content,
#                       uri, title, subtitle (speaker)
# curl "https://www.churchofjesuschrist.org/study/api/v3/language-pages/type/dynamic?lang=eng&uri=/general-conference/2020/04" | jq '..|.content?.uri?,.title?,.subtitle?'
