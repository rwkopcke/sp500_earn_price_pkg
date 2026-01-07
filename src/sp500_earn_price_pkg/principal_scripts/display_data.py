'''This program reads selected data from the output of update_data.py
   It then produces .pdf documents displaying these data which compare
   the earnings-price ratios to the 10-year TIPS interest rate.
   
   The addresses of documents within this project appear in this program's 
   paths.py script
'''

import polars as pl
import matplotlib.pyplot as plt

from sp500_earn_price_pkg.principal_scripts.code_segments.display_data \
    import read_data_for_display as read
from sp500_earn_price_pkg.principal_scripts.code_segments.display_data \
    import plot_func as pf
    
from sp500_earn_price_pkg.helper_func_module import display_helper_func as dh
from sp500_earn_price_pkg.helper_func_module import helper_func as hp
    
import config.config_paths as config
import config.set_params as params

env = config.Fixed_locations
param = params.Update_param
disp = params.Display_param

yr_qtr = param.YR_QTR_NAME
ann_op_eps = param.ANN_OP_EPS
ann_rep_eps = param.ANN_REP_EPS
price = param.PRICE_NAME
r_rate = param.RR_COL_NAME

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
    data_df = data_df.filter(pl.col(yr_qtr)
                             .is_in(proj_dict_keys_set))

    # create graphs
    fig = plt.figure(figsize=(8.5, 11), 
                     layout="constrained")
    # one plot above the other
    ax = fig.subplot_mosaic([['operating'],
                             ['reported']])
    fig.suptitle(
        f'{disp.PAGE0_SUPTITLE}\n{date_this_projn}',
        fontsize=13,
        fontweight='bold')
    fig.supxlabel(disp.PAGE0_SOURCE, fontsize= 8)

    # subsets of columns for op eps (top panel)
    # use rows that match keys for proj_dict
    df = data_df.select([yr_qtr, 
                         ann_op_eps])
    p_dict_columns = [ann_op_eps, yr_qtr]
    
    df = dh.page0_df(df, proj_dict, p_dict_columns, ann_op_eps)\
                .rename({ann_op_eps: disp.PAGE0_ACTUAL_TAG})\
                .sort(by= yr_qtr)
    
    pf.plots_page0(ax['operating'], df,
                title= disp.PAGE0_UP_SUBTITLE,
                ylim= disp.PAGE0_UP_Y_LIMIT,
                xlabl= disp.PAGE0_UP_X_LABEL,
                ylabl= disp.PAGE0_UP_Y_LABEL)
    
    # subsets of columns for rep eps (bottom panel)
    df = data_df.select([yr_qtr, ann_rep_eps])
    p_dict_columns = [ann_rep_eps, yr_qtr]
    
    df = dh.page0_df(df, proj_dict, p_dict_columns, ann_rep_eps)\
                .rename({ann_rep_eps: disp.PAGE0_ACTUAL_TAG})\
                .sort(by= yr_qtr)

    pf.plots_page0(ax['reported'], df,
                title= disp.PAGE0_LW_SUBTITLE,
                ylim= disp.PAGE0_DN_Y_LIMIT,
                xlabl= disp.PAGE0_DN_X_LABEL,
                ylabl= disp.PAGE0_DN_Y_LABEL)
    
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
        f'{disp.PAGE1_SUPTITLE}\n{date_this_projn}\n ',
        fontsize=13,
        fontweight='bold')
    fig.supxlabel(disp.PAGE1_SOURCE, fontsize= 8)
    
    # create the top and bottom graphs for op and rep pe
    # new DF with cols for p/e and alt p/e, both using 12m trailing E
        #   also yr_qtr and actual cy
        #       0) yr_qtr (from df) 
        #       1) historical 12m trailing p/e (from df)
        #       2) alt1 using constant p for proj quarters
        #       3) alt2 using p growing at ROG for proj quarters
        #       4) rolling 12m E (hist+proj) for proj quarters
    
    # top panel
    df = data_df.select([yr_qtr, ann_op_eps, price])
               
    p_df = proj_dict[yr_qtr_current_projn]\
                .select([yr_qtr, ann_op_eps])
    
    df = dh.page1_df(df, p_df, ann_op_eps, disp.ROGQ )
    
    legend1 = disp.PAGE1_LEGEND1 + f'{date_this_projn})\n{disp.PAGE1_DENOM}'
    legend2 = disp.PAGE1_LEGEND2 + f'{date_this_projn})\n{disp.PAGE1_DENOM}'
    
    # keys are local temp working names dh.page1_df
    df = df.rename({'pe': disp.PAGE1_HISTORICAL_TAG,
               'fix_proj_p/e': legend1,
               'incr_proj_p/e': legend2})
   
    pf.plots_page1(ax['operating'], df,
                    ylim= disp.PAGE1_UP_Y_LIMIT,
                    title= disp.PAGE1_UP_SUBTITLE,
                    ylabl= disp.PAGE1_UP_Y_LABEL,
                    xlabl= disp.PAGE1_UP_X_LABEL)

    # bottom panel
    df = data_df.select([yr_qtr, ann_rep_eps, price])
    
    p_df = proj_dict[yr_qtr_current_projn]\
               .select([yr_qtr, ann_rep_eps])
    
    df = dh.page1_df(df, p_df, ann_rep_eps, disp.ROGQ )
    
    # keys are local temp working names from dh.page1_df
    df = df.rename({'pe': disp.PAGE1_HISTORICAL_TAG,
                    'fix_proj_p/e': legend1,
                    'incr_proj_p/e': legend2})
    
    pf.plots_page1(ax['reported'], df,
                    ylim= disp.PAGE1_DN_Y_LIMIT,
                    title= disp.PAGE1_DN_SUBTITLE,
                    ylabl= disp.PAGE1_DN_Y_LABEL,
                    xlabl= disp.PAGE1_DN_X_LABEL)
    
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
        f'{disp.PAGE2_SUPTITLE}\n{date_this_projn}\n',
        fontsize=13,
        fontweight='bold')
    fig.supxlabel(disp.PAGE2_SOURCE, fontsize= 8)
    
    # create the top and bottom graphs for margins and premiums
    # create working df for op margins (top panel)
    # 'margin' and 'margin100' are local temp working names
    df = data_df.rename({param.MARG_COL_NAME: 'margin'})\
                .select(yr_qtr, 'margin')\
                .with_columns((pl.col('margin') * 100)
                            .alias('margin100'))\
                .drop('margin')\
                .rename({'margin100': disp.PAGE2_DISP_MARGIN})\
                .sort(by= yr_qtr)
    
    pf.plots_page2(ax['margin'], df,
                    ylim= disp.PAGE2_TP_Y_LIMIT,
                    title= disp.PAGE2_TP_SUBTITLE,
                    ylabl= disp.PAGE2_TP_Y_LABEL,
                    xlabl= disp.PAGE2_TP_X_LABEL,
                    hrzntl_vals= disp.PAGE2_TP_HORZ_LINES)
    
    # create working df for ratio: reported / operating E
    # 'quality', 'reported', 'operating' are local temp working names
    df = data_df.rename({ann_rep_eps: 'reported',
                         ann_op_eps: 'operating'})\
                .select(yr_qtr, 'reported', 'operating')\
                .with_columns((pl.col('reported') / 
                               pl.col('operating') * 100)
                              .cast(pl.Int8)
                              .alias('quality'))\
                .drop('reported', 'operating')\
                .sort(by= yr_qtr)
    
    pf.plots_page2(ax['quality'], df,
                    ylim= disp.PAGE2_MD_Y_LIMIT,
                    title= disp.PAGE2_MD_SUBTITLE,
                    ylabl= disp.PAGE2_MD_Y_LABEL,
                    xlabl= disp.PAGE2_MD_X_LABEL,
                    hrzntl_vals= disp.PAGE2_MD_HORZ_LINES)

    # create working df for premia (bottom panel)
    # 'real_rate', 'premium' are local temp working names
    df = data_df.rename({r_rate : 'real_rate'})\
                .select(yr_qtr, ann_rep_eps, 
                        'real_rate', price)\
                .with_columns(((pl.col(ann_rep_eps) /
                                pl.col(price)) * 100 -
                                pl.col('real_rate'))
                            .alias('premium'))\
                .drop(ann_rep_eps, 'real_rate', price)\
                .sort(by= yr_qtr)

    pf.plots_page2(ax['premium'], df,
                    ylim= disp.PAGE2_BM_Y_LIMIT,
                    title= disp.PAGE2_BM_SUBTITLE,
                    ylabl= disp.PAGE2_BM_Y_LABEL,
                    xlabl= disp.PAGE2_BM_X_LABEL,
                    hrzntl_vals= disp.PAGE2_BM_HORZ_LINES)
    
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
        f'{disp.PAGE3_SUPTITLE}\n{date_this_projn}\n',
        fontsize=13,
        fontweight='bold')
    fig.supxlabel(disp.PAGE3_SOURCE, fontsize= 8)
    
    # create the top and bottom graphs for premiums

    # create working df for op premium (top panel)
    # add a col: proj eps over the next 4 qtrs
    # 'fwd ...' and '.../...' ratios are local temp working names
    df = data_df.select(yr_qtr, price, r_rate,
                        param.OP_EPS)
    df = dh.contemp_12m_fwd_proj(data_df, proj_dict,
                                 param.OP_EPS, 'fwd_12mproj_op_eps')
    
    df = dh.page3_df(df, 'fwd_12mproj_op_eps')
    df = df.rename({'earnings / price': 'projected earnings / price'})

    pf.plots_page3(ax['operating'], df,
                ylim= disp.PAGE3_UP_Y_LIMIT,
                title= disp.PAGE3_UP_SUBTITLE,
                ylabl= disp.PAGE3_Y_LABEL,
                xlabl= disp.PAGE3_X_LABEL,
                hrzntl_vals= disp.PAGE3_UP_HORZ_LINES)
    
    # bottom panel
    df = data_df.select(yr_qtr, price, r_rate,
                        param.REP_EPS)
    
    # add a col : proj eps over the next 4 qtrs
    df = dh.contemp_12m_fwd_proj(data_df, proj_dict,
                                 param.REP_EPS, 'fwd_12mproj_rep_eps')
    df = dh.page3_df(df, 'fwd_12mproj_rep_eps')
    
    df = df.rename({'earnings / price': 'projected earnings / price'})

    pf.plots_page3(ax['reported'], df,
                ylim= disp.PAGE3_DN_Y_LIMIT,
                title= disp.PAGE3_DN_SUBTITLE,
                ylabl= disp.PAGE3_Y_LABEL,
                xlabl= disp.PAGE3_X_LABEL,
                hrzntl_vals= disp.PAGE3_DN_HORZ_LINES)
    
    hp.message([
        f'{env.DISPLAY_3_ADDR}'
    ])
    fig.savefig(str(env.DISPLAY_3_ADDR))
    #plt.savefig(f'{output_dir}/eps_page3.pdf', bbox_inches='tight')
    
    return
