

tell application id "DNtp"
	set doc to (item 1 of (selection as list)) -- Handling a csv file selected in DEVONthink

	set docContents to (cells of doc) -- Get the contents of the cells in the file
  -- Column headers in talk.csv file
	-- talk_filename,talk_canonical_uri,talk_date,talk_speaker,talk_title,talk_conference,talk_session,talk_study_url,talk_pdf_url,reference,talk_content_url,talk_pdf_filename,talk_print_filename
	-- 1             2                  3         4            5          6               7            8              9            10        11               12                13
	repeat with csvItem in docContents
		set newRecord to import (item 1 of csvItem) to current group -- Import the file
		set the URL of newRecord to (item 8 of csvItem) -- set URL to talk_study_url
		set the creation date of newRecord to (item 3 of csvItem) -- set creation date to talk date

		-- Add the custom metadata
		add custom meta data (item 2 of csvItem) for "Canonical URI" to newRecord
		add custom meta data (item 4 of csvItem) for "Speaker" to newRecord
		add custom meta data (item 5 of csvItem) for "Title" to newRecord
		add custom meta data (item 6 of csvItem) for "Conference" to newRecord
		add custom meta data (item 7 of csvItem) for "Session" to newRecord
		add custom meta data (item 9 of csvItem) for "Talk PDF URL" to newRecord
		add custom meta data (item 10 of csvItem) for "Reference" to newRecord
		add custom meta data (item 11 of csvItem) for "Talk Content URL" to newRecord
	end repeat
end tell