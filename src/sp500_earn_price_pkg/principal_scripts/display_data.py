'''This program reads selected data from the output of update_data.py
   It then produces .pdf documents displaying these data which compare
   the earnings-price ratios to the 10-year TIPS interest rate.
   
   The addresses of documents within this project appear in this program's 
   paths.py script
'''

import polars as pl
import matplotlib.pyplot as plt

from .code_segments.display_data import read_data_for_display as read
from .code_segments.display_data import plot_func as pf
    
from ..helper_func_module import display_helper_func as dh
from ..helper_func_module import helper_func as hp
    
import config.config_paths as config
import config.set_params as params

env = config.Fixed_locations()
param = params.Display_param()

# https://mateuspestana.github.io/tutorials/pandas_to_polars/
# https://www.rhosignal.com/posts/polars-pandas-cheatsheet/
# https://www.rhosignal.com/tags/polars/
# https://jrycw.github.io/ezp2p/
# https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.filter.html
# https://fralfaro.github.io/DS-Cheat-Sheets/examples/polars/polars/


def display():
    
## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## +++++++++ Read record_dict, history, proj_dict +++++++++++++++++++++
## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    
    record_dict, date_this_projn, yr_qtr_current_projn = \
        read.record()
    
    data_df = read.history(record_dict)
    
    proj_dict, proj_dict_keys_set = read.projection()
        
## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## +++++++++ Display the data +++++++++++++++++++++++++++++++++++++++++
## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.subplot_mosaic.html
    # https://matplotlib.org/stable/api/axes_api.html
    # https://matplotlib.org/stable/api/axes_api.html#axes-position

# page zero  ======================
# shows:  projected eps for current cy and future cy
# the projections shown for each quarter are the latest
# made in the quarter

    # proj_dict_keys: the dates for the data (x axis)
    # data in data_df should conform
    data_df = data_df.filter(pl.col("yr_qtr")
                             .is_in(proj_dict_keys_set))

    # create graphs
    fig = plt.figure(figsize=(8.5, 11), 
                     layout="constrained")
    # one plot above the other
    ax = fig.subplot_mosaic([['operating'],
                             ['reported']])
    fig.suptitle(
        f'{param.PAGE0_SUPTITLE}\n{date_this_projn}',
        fontsize=13,
        fontweight='bold')
    fig.supxlabel(param.PAGE0_SOURCE, fontsize= 8)

    # subsets of columns for op eps (top panel)
    # use rows that match keys for proj_dict
    df = data_df.select(['yr_qtr', '12m_op_eps'])
    p_dict_columns = ['12m_op_eps', 'yr_qtr']
    
    df = dh.page0_df(df, proj_dict, p_dict_columns, '12m_op_eps')\
                .rename({'12m_op_eps': 'actual'})\
                .sort(by= 'yr_qtr')
    
    xlabl = '\ndate of projection\n'
    ylabl = '\nearnings per share\n'
    
    pf.plots_page0(ax['operating'], df,
                title= ' \nProjections of Operating EPS',
                ylim= (100, None),
                xlabl= xlabl,
                ylabl= ylabl)
    
    # subsets of columns for rep eps (bottom panel)
    df = data_df.select(['yr_qtr', '12m_rep_eps'])
    p_dict_columns = ['12m_rep_eps', 'yr_qtr']
    
    df = dh.page0_df(df, proj_dict, p_dict_columns, '12m_rep_eps')\
                .rename({'12m_rep_eps': 'actual'})\
                .sort(by= 'yr_qtr')

    pf.plots_page0(ax['reported'], df,
                title= ' \nProjections of Reported EPS',
                ylim= (75, None),
                xlabl= xlabl,
                ylabl= ylabl)
    
    # show the figure
    hp.message([
        f'{env.DISPLAY_0_ADDR}'
    ])
    fig.savefig(str(env.DISPLAY_0_ADDR))
    
# page one  ======================
# shows:  historical 12m trailing pe plus
#    forward 12m trailing pe, using current p

    # create graphs
    fig = plt.figure(figsize=(8.5, 11), 
                     layout="constrained")
    # one plot above the other
    ax = fig.subplot_mosaic([['operating'],
                             ['reported']])
    fig.suptitle(
        f'{param.PAGE1_SUPTITLE}\n{date_this_projn}\n ',
        fontsize=13,
        fontweight='bold')
    fig.supxlabel(param.PAGE1_SOURCE, fontsize= 8)
    
    # create the top and bottom graphs for op and rep pe
    # new DF with cols for p/e and alt p/e, both using 12m trailing E
        #   also yr_qtr and actual cy
        #       0) yr_qtr (from df) 
        #       1) historical 12m trailing p/e (from df)
        #       2) alt1 using constant p for proj quarters
        #       3) alt2 using p growing at ROG for proj quarters
        #       4) rolling 12m E (hist+proj) for proj quarters
    
    # top panel
    df = data_df.select(['yr_qtr', '12m_op_eps', 'price'])
               
    p_df = proj_dict[yr_qtr_current_projn]\
                .select(['yr_qtr', '12m_op_eps'])
    
    df = dh.page1_df(df, p_df, '12m_op_eps', param.ROGQ )
    
    denom = 'divided by projected earnings'
    legend1 = f'price (constant after {date_this_projn})\n{denom}'
    legend2 = f'price (increases {param.ROG_AR}% ar after {date_this_projn})\n{denom}'
    
    df = df.rename({'pe': 'historical',
               'fix_proj_p/e': legend1,
               'incr_proj_p/e': legend2})
    
    title = 'Ratio: Price to 12-month Trailing Operating Earnings'
   
    pf.plots_page1(ax['operating'], df,
                    ylim= (None, None),
                    title= title,
                    ylabl= ' \n',
                    xlabl= ' \n')

    # bottom panel
    df = data_df.select(['yr_qtr', '12m_rep_eps', 'price'])
    
    p_df = proj_dict[yr_qtr_current_projn]\
               .select(['yr_qtr', '12m_rep_eps'])
    
    df = dh.page1_df(df, p_df, '12m_rep_eps', param.ROGQ )
    
    df = df.rename({'pe': 'historical',
                    'fix_proj_p/e': legend1,
                    'incr_proj_p/e': legend2})
    
    title = 'Ratio: Price to 12-month Trailing Reported Earnings'
    
    pf.plots_page1(ax['reported'], df,
                    ylim= (None, None),
                    title= title,
                    ylabl= ' \n',
                    xlabl= ' \n')
    
    hp.message([
        f'{env.DISPLAY_1_ADDR}'
    ])
    fig.savefig(str(env.DISPLAY_1_ADDR))
    
# page two  ======================
# shows:  historical data for margins and 
# historical and current estimates for equity premium
    
    # create graphs
    fig = plt.figure(figsize=(8.5, 11), 
                     layout="constrained")
    # stack plots
    ax = fig.subplot_mosaic([['margin'],
                             ['quality'],
                             ['premium']])
    fig.suptitle(
        f'{param.PAGE2_SUPTITLE}\n{date_this_projn}\n',
        fontsize=13,
        fontweight='bold')
    fig.supxlabel(param.PAGE2_SOURCE, fontsize= 8)
    
    # create the top and bottom graphs for margins and premiums
    # create working df for op margins (top panel)

    df = data_df.rename({'op_margin' : 'margin'})\
                .select('yr_qtr', 'margin')\
                .with_columns((pl.col('margin') * 100)
                            .alias('margin100'))\
                .drop('margin')\
                .rename({'margin100': 'margin'})\
                .sort(by= 'yr_qtr')
    
    title = 'Margin: quarterly operating earnings relative to revenue'
    
    pf.plots_page2(ax['margin'], df,
                    ylim= (None, None),
                    title= title,
                    ylabl= ' \npercent\n ',
                    xlabl= ' \n ',
                    hrzntl_vals= [10.0])
    
    # create working df for ratio: reported / operating E
    df = data_df.rename({'12m_rep_eps': 'reported',
                         '12m_op_eps': 'operating'})\
                .select('yr_qtr', 'reported', 'operating')\
                .with_columns((pl.col('reported') / 
                               pl.col('operating') * 100)
                              .cast(pl.Int8)
                              .alias('quality'))\
                .drop('reported', 'operating')\
                .sort(by= 'yr_qtr')
    title = 'Quality of Earnings: ratio of 12-month reported to operating earnings'
    
    pf.plots_page2(ax['quality'], df,
                    ylim= (None, None),
                    title= title,
                    ylabl= ' \npercent\n ',
                    xlabl= ' \n ',
                    hrzntl_vals= [80, 90])

    # create working df for premia (bottom panel)
    df = data_df.rename({'real_int_rate' : 'real_rate'})\
                .select('yr_qtr', '12m_rep_eps', 
                        'real_rate', 'price')\
                .with_columns(((pl.col('12m_rep_eps') /
                                pl.col('price')) * 100 -
                                pl.col('real_rate'))
                            .alias('premium'))\
                .drop('12m_rep_eps', 'real_rate', 'price')\
                .sort(by= 'yr_qtr')

    title = 'Equity Premium: \nratio of 12-month trailing reported earnings to price, '
    title += 'less 10-year TIPS rate'

    pf.plots_page2(ax['premium'], df,
                    ylim= (None, None),
                    title= title,
                    ylabl= ' \npercent\n ',
                    xlabl= ' \n ',
                    hrzntl_vals= [2.0, 4.0])
    
    hp.message([
        f'{env.DISPLAY_2_ADDR}'
    ])
    fig.savefig(str(env.DISPLAY_2_ADDR))
    #plt.savefig(f'{output_dir}/eps_page2.pdf', bbox_inches='tight')
    
# page three  ======================
# shows:  components of the equity premium,
# using 12m forward projected earnings
    
    # create graphs
    fig = plt.figure(figsize=(8.5, 11), 
                     layout="constrained")
    # upper and lower plots
    ax = fig.subplot_mosaic([['operating'],
                             ['reported']])
    fig.suptitle(
        f'{param.PAGE3_SUPTITLE}\n{date_this_projn}\n',
        fontsize=13,
        fontweight='bold')
    fig.supxlabel(param.PAGE3_SOURCE, fontsize= 8)
    
    xlabl = '\nquarter of projection, price, and TIPS rate\n\n'
    ylabl = ' \npercent\n '
    
    # create the top and bottom graphs for premiums

    # create working df for op premium (top panel)
    # add a col: proj eps over the next 4 qtrs
    df = data_df.select('yr_qtr', 'price', 'real_int_rate',
                        'op_eps')
    df = dh.contemp_12m_fwd_proj(data_df, proj_dict,
                                 'op_eps', 'fwd_12mproj_op_eps')
    
    df = dh.page3_df(df, 'fwd_12mproj_op_eps')
    
    df = df.rename({'earnings / price': 'projected earnings / price'})
    
    title = 'Operating Earnings: projected over next 4 quarters'

    pf.plots_page3(ax['operating'], df,
                ylim= (None, 9),
                title= title,
                ylabl= ylabl,
                xlabl= xlabl,
                hrzntl_vals= [2.0, 4.0])
    
    # bottom panel
    df = data_df.select('yr_qtr', 'price', 'real_int_rate',
                        'rep_eps')
    
    # add a col : proj eps over the next 4 qtrs
    df = dh.contemp_12m_fwd_proj(data_df, proj_dict,
                                 'rep_eps', 'fwd_12mproj_rep_eps')
    df = dh.page3_df(df, 'fwd_12mproj_rep_eps')
    
    df = df.rename({'earnings / price': 'projected earnings / price'})
    
    title = 'Reported Earnings: projected over next 4 quarters'

    pf.plots_page3(ax['reported'], df,
                ylim= (None, 9),
                title= title,
                ylabl= ylabl,
                xlabl= xlabl,
                hrzntl_vals= [2.0, 4.0])
    
    hp.message([
        f'{env.DISPLAY_3_ADDR}'
    ])
    fig.savefig(str(env.DISPLAY_3_ADDR))
    #plt.savefig(f'{output_dir}/eps_page3.pdf', bbox_inches='tight')
    
    return
