from dateutil import parser
from dateutil.relativedelta import relativedelta
import json

data = {
    "rainfall": {
        "new": {
            "month": ["1990-01", "2022-03"]
        },
        "legacy": {
            "month": ["1920-01", "2012-12"]
        }
    },
    "temperature": {
        "min": {
            "day": ["1990-01-01", "2022-04-07"],
            "month": ["1990-01", "2018-12"]
        },
        "max": {
            "day": ["1990-01-01", "2022-04-07"],
            "month": ["1990-01", "2018-12"]
        },
        "mean": {
            "day": ["1990-01-01", "2022-04-07"],
            "month": ["1990-01", "2018-12"]
        }
    }
}

extent = "statewide"

#don't want to completely slam the files api so just handle one year from each, send off one doc

fnum = 0

#rainfall
for datatype in data:
    prods_or_aggs = data[datatype]
    for prod_or_agg in prods_or_aggs:
        periods = prods_or_aggs[prod_or_agg]
        for period in periods:
            dates = periods[period]
            start = parser.parse(dates[0])
            end = parser.parse(dates[1])
            date = start
            while(date <= end):
                year = date.year
                dataset = {
                    "datatype": datatype,
                    "period": period,
                    "extent": extent,
                    "year": year,
                    "dates": []
                }
                part_label = "production" if datatype == "rainfall" else "aggregation"
                dataset[part_label] = prod_or_agg
                date_format = "%Y-%m" if period == "month" else "%Y-%m-%d"
                delta = relativedelta(months = 1) if period == "month" else relativedelta(days = 1)
                #expand dates in year
                while date.year == year and date <= end:
                    date_s = date.strftime(date_format)
                    dataset["dates"].append(date_s)
                    date += delta
                fname = f"configs/{fnum}.json"
                with open(fname, "w") as f:
                    json.dump(dataset, f)
                fnum += 1