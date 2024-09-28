# ImportTalks
A set of scripts to facilitate downloading General Conference talk PDF files and importing them into DevonThink 
with standardized metadata. This makes it easy to research, analyze, annotate, cross-link, etc. General Conference talks
over the years. Two Apple Script helper files for each talk of each session of every General Conference starting in 1971.
The year range is controlled by line 357 of DownloadGCTalk.py

## Installation
`pip install jmespath jq pandas playwright Requests`

## Usage
`python DownloadGCTalks.py -ADP`
