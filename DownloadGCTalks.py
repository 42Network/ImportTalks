import calendar
import os
import requests
import jq
from datetime import datetime, timedelta
import json
import concurrent.futures as cf
import itertools as it
import pandas as pd
import asyncio
from playwright.async_api import async_playwright
import pickle
from urllib.parse import urlparse

import logging.handlers

LOG_FILENAME = 'ImportTalks.log'

# Set up a specific logger with our desired output level
#my_logger = logging.getLogger('MyLogger')
#my_logger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
rotate_handler = logging.handlers.RotatingFileHandler(
              LOG_FILENAME, maxBytes=30000000, backupCount=5)
formatter = logging.Formatter('%(asctime)s %(levelname)8s %(message)s')
rotate_handler.setFormatter(formatter)
logger.addHandler(rotate_handler)

# logging.basicConfig(level=logging.DEBUG,
#                     format='%(asctime)s %(levelname)8s %(message)s',
#                     datefmt='%Y-%m-%d %H:%M:',
#                     filename='./logfile.txt', filemode='a')


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
    if year < 2025:
        using_gc_link = True
        conf_url = f"{base_content_url}/general-conference/{year}/{month:02d}"
    else:
        magazine_month = month + 1  # Add one because Liahona comes out one month after conference
        conf_url = f"{base_content_url}/liahona/{year}/{magazine_month:02d}"
    logger.info(f"Looking up TOC for {year=} {month=} using {conf_url=}")
    try:
        r = requests.get(conf_url, timeout=5)
        r.raise_for_status()
    except (OSError, requests.exceptions.HTTPError) as err:
        # raise SystemExit(err)
        logger.warning(f"Error getting Conference TOC with {conf_url=}: {err}")
        logger.warning(f"Trying with ensign URL")
        try:
            magazine_month = month + 1  # Add one because Liahona comes out one month after conference
            conf_url = f"{base_content_url}/ensign/{year}/{magazine_month:02d}"
            r = requests.get(conf_url, timeout=5)
            r.raise_for_status()
        except (OSError, requests.exceptions.HTTPError) as err:
            # raise SystemExit(err)
            logger.warning(f"Second error getting Conference TOC with {conf_url=}: {err}")
            return False
    j = r.json()
    if not 'toc' in j:
        logger.warning(f"No TOC found for {conf_url=}")
        return False
    logger.info(f"TOC found TOC for {year=} {month=} ")
    with open(f"{args.download_dir}/toc/{year}-{month:02d}.json", 'w', encoding='utf-8') as f:
          json.dump(j['toc'], f, ensure_ascii=False, indent=4)
    return j


def toc_runner(year, month):
    """ Get the TOC via concurrent.futures
    :param year:
    :param month:
    :return:
    """
    return year, month, get_conference_toc(year, month)

def get_toc_list(years, months):
    with cf.ThreadPoolExecutor(max_workers=1) as executor:
        tocs = list(executor.map(toc_runner, years, months))

    print(f"Found {len(tocs)} TOCs from {len(months)} conferences")
    return tocs

def lookup_talk_pdf_url(talk_content_url: str):
    """ Get PDF URL talk based on URL from TOC
    :param talk_content_url:
    :return pdf_url
    """
    try:
        r = requests.get(talk_content_url, timeout=1)
        r.raise_for_status()

    except (OSError, requests.exceptions.HTTPError) as err:
        # raise SystemExit(err)
        logger.warning(f"Error getting talk content with {talk_content_url=}: {err}")
        return False
    # print(f"{r2.text=}")
    j = r.json()
    pdf_url = jq.first('.content.meta.pdf.source', j)
    if not pdf_url:
        logger.warning(f"No PDF URL found for {talk_content_url=}")
        return False
    logger.info(f"PDF URL found: {pdf_url}")
    return pdf_url

def lookup_talk_pdf_runner(talk):
    talk['talk_pdf_url'] = lookup_talk_pdf_url(talk['talk_content_url'])
    return talk

def get_first_sunday(year, month):
    # Find the first day of the month
    first_day = datetime(year, month, 1)

    # Calculate the first Sunday (6th weekday = Sunday)
    days_to_add = (6 - first_day.weekday()) % 7
    first_sunday = first_day + timedelta(days=days_to_add)

    return first_sunday
def generate_talk_list(tocs):
    """
    For each conference in the list of TOCs, parse the JSON to generate talk metadata:
        JSON attributes                                 |   Metadata
        ------------------------------------------------|------------------------------
        Magazine title and category                     |   reference
        Conference session (not always specified)       |   session
        Talk title                                      |   title
        Talk speaker                                    |   speaker
        Canonical URI for the talk                      |   canonical_uri
        Talk content URL (talk JSON)                    |   talk_content_url
        Talk study URL (Gospel Library link)            |   talk_study_url
        URL for entire conference PDF                   |   conf_pdf_url

    :param tocs:
    :return:
    """
    total_talk_counter = 0
    doc_list = []
    for (year, month, toc) in tocs:
        logger.debug(f"{year=} {month=} {toc=}")
        if toc is False:
            logger.warning(f"No TOC found for {year=} {month=}")
            continue
        jq_query = '''\
            .toc | .title as $magazine | .category as $category | .pdfDownloads[]?.source as $confPDFurl |
                (.entries[].section | .title as $session | .entries[]?.content | 
                    {$category, $magazine, $session, title: .title, speaker: .subtitle, uri: .uri, $confPDFurl}), 
                (.entries[]?.content | 
                    {$category, $magazine, session: "Not Specified", title: .title, speaker: .subtitle, uri: .uri, $confPDFurl})
            '''
        jq_query = '''\
        .toc |.title as $magazine |.category as $category |
        (.entries[].section |.title as $session |.entries[]?.content |
         {$category, $magazine, $session, title:.title, speaker:.subtitle, uri:.uri}),
        (.entries[]?.content |
         {$category, $magazine, session: "Not Specified", title:.title, speaker:.subtitle, uri:.uri})
         '''
        conference_talk_counter = 0
        titles = jq.compile(jq_query).input_value(toc)
        for item in titles:
            logger.debug(f"{item=}")
            talk = {}
            if item['uri'] is None:
                continue  # this is a bad entry, go to next
            canonical_uri = item['uri']
            talk_content_url = f"{base_content_url}{canonical_uri}"
            talk_study_url = f"{base_study_url}{canonical_uri}"
            talk_date = get_first_sunday(year, month) - timedelta(days=1)
            talk['talk_date'] = talk_date.strftime('%Y-%m-%d')
            talk['talk_conference'] = f'{calendar.month_name[month]} {year}'
            talk['talk_session'] = item['session']
            talk['talk_speaker'] = item['speaker']
            talk['talk_title'] = item['title']
            talk['talk_study_url'] = talk_study_url
            #talk['conf_pdf_url'] = item['confPDFurl']
            talk['reference'] = f"{item['category']}-{item['magazine']}"
            talk['talk_canonical_uri'] = canonical_uri
            talk['talk_content_url'] = talk_content_url

            # conference_talk_counter += 1
            # total_talk_counter += 1
            # talk['conference_talk_counter'] = conference_talk_counter
            # talk['total_talk_counter'] = total_talk_counter
            doc_list.append(talk)
            # print(talk)
    # use ThreadPool to update talk list with per-talk PDR URL
    with cf.ThreadPoolExecutor(max_workers=10) as executor:
        talks = list(executor.map(lookup_talk_pdf_runner, doc_list))
    return talks

def analyze_talks(talks):
    num_talks = with_pdf = with_speaker = with_speaker_and_pdf = with_conf_pdf = downloaded = printed = 0
    print(f"Conference    Talks|w/Spkr|w/PDFs|w/Both|Dnld|Print")
    for talk in talks:
        #logger.debug(talk)
        num_talks += 1
        if talk['talk_speaker']:
            with_speaker += 1
        if talk['talk_pdf_url']:
            with_pdf += 1
        if talk['talk_pdf_url'] and talk['talk_speaker']:
            with_speaker_and_pdf += 1
        # if talk['conf_pdf_url']:
        #     with_conf_pdf += 1
        if 'talk_pdf_filename' in talk and talk['talk_pdf_filename']:
            downloaded += 1
        if 'talk_print_filename' in talk and talk['talk_print_filename']:
            printed += 1
    print(f"Overall       {num_talks:>5d}|{with_speaker:>6d}|{with_pdf:>6d}|{with_speaker_and_pdf:>6d}|{downloaded:>4d}|{printed:>5d}")

    for date, talks in it.groupby(talks, key=lambda item: item['talk_date']):
        num_talks = with_pdf = with_speaker = with_speaker_and_pdf = with_conf_pdf = downloaded = printed = 0
        for talk in talks:
            num_talks += 1
            if talk['talk_speaker']:
                with_speaker += 1
            if talk['talk_pdf_url']:
                with_pdf += 1
            if talk['talk_pdf_url'] and talk['talk_speaker']:
                with_speaker_and_pdf += 1
            # if talk['conf_pdf_url']:
            #     with_conf_pdf += 1
            if 'talk_pdf_filename' in talk and talk['talk_pdf_filename']:
                downloaded += 1
            if 'talk_print_filename' in talk and talk['talk_print_filename']:
                printed += 1
        print(f"{date}    {num_talks:>5d}|{with_speaker:>6d}|{with_pdf:>6d}|{with_speaker_and_pdf:>6d}|{downloaded:>4d}|{printed:>5d}")
        # if with_speaker_and_pdf < 5:
        #     logger.warning(f"Low number of speaker PDFs for {date}: {talks}")

def download_talk_pdf(url, path):
    if not url:
        return False
    #logger.debug(f'{path=} {url=}')
    url_path = urlparse(url).path
    file_pathname = path + url_path
    if os.path.isfile(file_pathname) and os.path.getsize(file_pathname) > 0:
        logger.debug(f"Already got {file_pathname}")
    else:
        logger.debug(f"Downloading {file_pathname}")
        try:
            response = requests.get(url)
            response.raise_for_status()
        except (OSError, requests.exceptions.HTTPError) as err:
            # raise SystemExit(err)
            logger.warning(f"Error downloading talk with {url=}: {err}")
            return False
        os.makedirs(os.path.dirname(file_pathname), exist_ok=True)
        with open(file_pathname, 'wb') as f:
            f.write(response.content)
    return file_pathname

async def url_to_pdf(url, output_path):
    # https://apitemplate.io/blog/how-to-convert-html-to-pdf-using-python/
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.goto(url)
        await page.pdf(path=output_path, display_header_footer=True,
                       margin={"top": "40px", "bottom": "40px"}
                       )
        await browser.close()
def print_talk_to_pdf(url, file_pathname):
    if not url:
        return False
    #logger.debug(f'{path=} {url=}')
    if os.path.isfile(file_pathname) and os.path.getsize(file_pathname) > 0:
        logger.debug(f"Already printed {file_pathname}")
        return file_pathname
    else:
        logger.debug(f"Print to PDF of  {file_pathname}")
        # HTML(url).write_pdf(file_pathname) # Doesn't print footnotes
        asyncio.run(url_to_pdf(url, file_pathname))
        if os.path.isfile(file_pathname) and os.path.getsize(file_pathname) > 0:
            return file_pathname
        else:
            logger.debug(f"Error printing to pdf {file_pathname}")
            return False
def download_talks_runner(talk):

    pdf_url = talk['talk_pdf_url']
    if pdf_url and args.download_talk_pdfs:
        talk['talk_pdf_filename'] = download_talk_pdf(pdf_url, args.download_dir + '/talk_pdfs')
    else:
        talk['talk_pdf_filename'] = False
    # Only try print to PDF if PDF download fails
    if talk['talk_pdf_filename'] == False and args.download_talk_prints:
        pfile = talk['talk_date'] + '-' + os.path.basename(talk['talk_canonical_uri']) + '.pdf'
        try:
            talk['talk_print_filename'] = print_talk_to_pdf(talk['talk_study_url'],
                                                        args.download_dir + '/talk_prints/' + pfile)
        except Exception as err:
            # raise SystemExit(err)
            logger.warning(f"Error printing talk PDF {talk['talk_study_url']}: {err}")
            return False

    else:
        talk['talk_print_filename'] = False
    talk['talk_filename'] = talk['talk_pdf_filename'] or talk['talk_print_filename']
    return talk

def download_talks(talks, path):
    os.makedirs(path, exist_ok=True)
    os.makedirs(path + '/talk_prints/', exist_ok=True)
    os.makedirs(path + '/talk_pdfs/', exist_ok=True)

    with cf.ThreadPoolExecutor(max_workers=10) as executor:
        new_list = list(executor.map(download_talks_runner, talks))
    return new_list

if __name__ == "__main__":
    import time
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--download-talk-pdfs', '-D', action='store_true')
    parser.add_argument('--download-talk-prints', '-P', action='store_true')
    parser.add_argument('--download-dir', type=str, default='/tmp/gc_download')
    parser.add_argument('--output-file', '-O', default='talks')
    parser.add_argument('--pickle-file', default='talks.pickle')
    parser.add_argument('--analyze', '-A', action='store_true')
    args = parser.parse_args()

    years = []
    months = []
    #for year in (1971, 1973, 1977, 1987, 1991, 2007, 2008, 2010, 2023):
    #for year in (2008, 2018, 2020):
    for year in range(2022, 2025):
        for month in (4, 10):
            years.append(year)
            months.append(month)
    print(years, months)

    t1 = time.time()
    tocs = get_toc_list(years, months)
    print(f"Time: get_toc_list = {time.time() - t1:.2f} seconds")
    t1 = time.time()
    talks = generate_talk_list(tocs)
    print(f"Time: generate_talk_list = {time.time() - t1:.2f} seconds")

    t1 = time.time()
    talks = download_talks(talks, args.download_dir)
    print(f"Time: download_talks = {time.time() - t1:.2f} seconds")

    df = pd.DataFrame(talks)
    #conf_date, conf_session, talk_title, talk_speaker, talk_study_url, conf_pdf_url, reference, talk_canonical_uri, talk_content_url, talk_pdf_url
    with open(args.pickle_file, 'wb') as f:
        pickle.dump(talks, f)


    # df.reindex(columns=["talk_pdf_filename","conf_date","conf_session","talk_speaker","talk_title","talk_study_url",
    #                     "conf_pdf_url","talk_pdf_url","reference","canonical_uri","talk_content_url"])
    df = df[["talk_filename","talk_canonical_uri","talk_date","talk_speaker","talk_title","talk_conference","talk_session","talk_study_url",
             "talk_pdf_url","reference","talk_content_url","talk_pdf_filename","talk_print_filename"]]
    df_no_pdf = df.query('talk_filename == False')
    df.to_csv('all_' + args.output_file + '.csv', index=False)

    writer = pd.ExcelWriter('all_' + args.output_file + '.xlsx', engine="xlsxwriter")
    df.to_excel(writer, sheet_name="Conference Talks", index=False)
    workbook = writer.book
    worksheet = writer.sheets["Conference Talks"]
    (max_row, max_col) = df.shape
    column_settings = [{"header": column} for column in df.columns]
    worksheet.add_table(0, 0, max_row, max_col - 1, {"columns": column_settings})
    worksheet.set_column(0, max_col - 1, 12)
    worksheet.autofilter(0, 0, max_row, max_col - 1)

    df_no_pdf.to_excel(writer, sheet_name="No PDFs", index=False)
    writer.close()
    if args.analyze:
        analyze_talks(talks)

    #

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
