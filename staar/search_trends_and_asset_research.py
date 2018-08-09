import numpy as np
import pandas as pd
from bokeh.io import output_file, show
from bokeh.models import LinearAxis, Range1d, HoverTool, ColumnDataSource, NumeralTickFormatter
from bokeh.plotting import figure
from pytrends.request import TrendReq

from data_fetcher import get_bitcoin_prices


def get_search_trends(asset_search_terms=[]):
    pytrend = TrendReq()
    pytrend.build_payload(kw_list=asset_search_terms)
    interest_over_time_df = pytrend.interest_over_time()
    interest_over_time_df = interest_over_time_df.reset_index()
    interest_over_time_df.columns = ['Date', 'Trend', 'IsPartial']
    interest_over_time_df = interest_over_time_df.set_index(['Date'])
    return interest_over_time_df


def get_underlying_data():
    btc_trend = get_search_trends(['bitcoin'])
    btc_df = get_bitcoin_prices()
    btc_df = btc_df[['Mean', 'Volume']]
    re_btcdf = btc_df.resample("W").mean()
    final = pd.merge(btc_trend, re_btcdf, how='inner', on='Date')
    return final


def get_smoothed_data(data, daysToMean=8):
    moving_avg_data = data[['Mean', 'Volume', 'Trend']].rolling(center=False, window=daysToMean).mean()
    return moving_avg_data


def get_correlation(data):
    corr_prices = np.corrcoef(data['Trend'], data['Mean'])[1, 0]
    corr_volume = np.corrcoef(data['Trend'], data['Volume'])[1, 0]
    print(corr_prices)
    print(corr_volume)


def visualize(data, columnName='Mean'):
    output_file("%s_trends.html" % columnName)

    f = lambda x: str(x)[:10]
    data["datetime_s"] = map(f, data.index.date)

    cds = ColumnDataSource(data)
    print(cds.data.keys())

    TOOLS = "crosshair,pan,wheel_zoom,box_zoom,reset,previewsave"

    p = figure(plot_width=800, plot_height=400, title="BTC %s vs BTC Trend" % columnName, x_axis_type="datetime",
               tools=TOOLS)

    r = p.line('Date', columnName, line_width=2, legend="BTC Prices" if columnName == 'Mean' else "BTC %s" % columnName,
               source=cds)
    p.extra_y_ranges = {'Trend': Range1d(data['Trend'].min(), data['Trend'].max())}
    p.line('Date', 'Trend', color="red", y_range_name="Trend", line_width=2, legend="BTC Trends", source=cds)

    yaxis2 = LinearAxis(y_range_name="Trend")
    yaxis2.axis_label = "BTC Trend"
    p.add_layout(yaxis2, 'right')

    p.xaxis.axis_label = 'Date'
    p.yaxis[0].axis_label = 'BTC %s' % columnName
    if columnName == "Mean":
        p.yaxis[0].formatter = NumeralTickFormatter(format="$0.00")
    else:
        p.yaxis[0].formatter = NumeralTickFormatter(format="0,0")

    if columnName == "Mean":
        tool_tip_col = 'Price'
        tool_tip_val = '$@Mean{0,0}'
        tool_tip_fmt = 'printf'
    else:
        tool_tip_col = columnName
        tool_tip_val = '@%s{0,0}' % columnName
        tool_tip_fmt = 'numeral'

    p.add_tools(HoverTool(
        tooltips=[
            ("Date", '@datetime_s'),
            (tool_tip_col, tool_tip_val),
            ('Trend', '@Trend'),
        ],
        formatters={"Date": "datetime", tool_tip_col: tool_tip_fmt, "Trend": "numeral"},
        renderers=[r],
        mode='vline'
    ))

    p.legend.location = "top_left"
    show(p)


if __name__ == '__main__':
    data = get_underlying_data()
    visualize(data, columnName="Mean")
    visualize(data, columnName="Volume")
    smooth_data = get_smoothed_data(data)
    visualize(smooth_data, columnName="Mean")
    visualize(smooth_data, columnName="Volume")
    get_correlation(data)
