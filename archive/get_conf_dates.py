from datetime import datetime, timedelta


def get_general_conference_dates(start_year, end_year):
    conference_dates = []
    for year in range(start_year, end_year + 1):
        # Get the first Saturday in April and October
        april_conference_sunday = get_first_sunday(year, 4)
        april_conference_saturday = april_conference_sunday - timedelta(days=1)
        october_conference = get_first_sunday(year, 10)  - timedelta(days=1)

        # Add the dates to the list
        conference_dates.append(april_conference)
        conference_dates.append(october_conference)

    return conference_dates


def get_first_sunday(year, month):
    # Find the first day of the month
    first_day = datetime(year, month, 1)

    # Calculate the first Sunday (6th weekday = Sunday)
    days_to_add = (6 - first_day.weekday()) % 7
    first_sunday = first_day + timedelta(days=days_to_add)

    return first_sunday


if __name__ == "__main__":
    # Generate conference dates from 1971 to 2024
    conference_dates = get_general_conference_dates(1971, 2024)

    # Print the list of conference dates
    for date in conference_dates:
        print(date)
