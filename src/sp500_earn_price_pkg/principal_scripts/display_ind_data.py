import polars as pl
import polars.selectors as cs
import matplotlib.pyplot as plt
import seaborn as sn
import numpy as np

from sp500_earn_price_pkg.principal_scripts.code_segments.display_ind \
    import read_ind_for_display
    
from sp500_earn_price_pkg.helper_func_module import helper_func as hp
    
import config.config_paths as config
import config.set_params as params

env = config.Fixed_locations()
param = params.Update_param()
disp = params.Display_ind_param()

year = param.ANNUAL_DATE
earnings_metric = param.E_METRIC_COL_NAME
earnings_type = param.E_TYPE_COL_NAME


def display_ind():
    ind_df = read_ind_for_display.read()
    
# SEABORN SCATTERPLOTS WITH JITTER +++++++++++++++++++++++++++++++++
# https://matplotlib.org/stable/users/explain/axes/constrainedlayout_guide.html#sphx-glr-users-explain-axes-constrainedlayout-guide-py
# https://matplotlib.org/stable/users/explain/axes/tight_layout_guide.html#sphx-glr-users-explain-axes-tight-layout-guide-py
# https://matplotlib.org/stable/users/explain/axes/arranging_axes.html
# https://drzinph.com/how-to-box-plot-with-python/
    # PAGE 4\
    
    # create tall DF with year, industry, and p/e as columns
    df = ind_df.filter(pl.col(earnings_metric) == param.EARN_METRICS[1],
                       pl.col(earnings_type) == param.EARN_TYPES[0])\
               .drop(pl.col(earnings_metric),
                     pl.col(earnings_type),
                     pl.col(param.RR_NAME),
                     pl.col(param.IDX_E_COL_NAME))
    # simplify col headings
    df.columns = [name.replace("_", " ")
                  for name in df.columns]
    df = df.unpivot(index= year,
                    variable_name= 'name',
                    value_name= 'value')
    
    fig = plt.figure(figsize=(10.5, 8.5), 
                     layout="constrained")
    # plt.tight_layout(pad= 0.5)
    
    fig.suptitle(
        '\n' + disp.PAGE4_SUPTITLE,
        fontsize=13,
        fontweight='bold')
    fig.supxlabel(f'{disp.PAGE4_SOURCE}\n ', fontsize= 8)
    
    ax = fig.subplots()
    
    sn.stripplot(
        df,
        x= year,
        y= "value",
        hue= "name",
        ax=ax,
    )
    
    gf = ind_df.filter(pl.col(earnings_metric) == param.EARN_METRICS[1],
                       pl.col(earnings_type) == param.EARN_TYPES[0])\
               .select(pl.col(year, param.IDX_E_COL_NAME))
    
    sn.scatterplot(gf,
                   x= year,
                   y= param.IDX_E_COL_NAME,
                   label= param.SP500,
                   marker="|", s=4, linewidth=25
)
    plt.xticks(rotation = 30)
    ax.set_ylim(ymin= -50, ymax= 60)
    ax.set_xlabel(disp.XLABL, fontweight= 'bold')
    ax.set_ylabel(' \nprice-earnings ratio', fontweight= 'bold')
    sn.move_legend(ax, 'lower left')
    box = ax.get_position()
    ax.set_position([box.x0, box.y0 + 0.06, 
                    box.width * 0.75, box.height * 0.93])
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.7),
              reverse= True)
    # sn.move_legend(ax, 'upper left', bbox_to_anchor= (1, 1))
    
    hp.message([
        env.DISPLAY_4_ADDR
    ])
    fig.savefig(str(env.DISPLAY_4_ADDR))
    
# P/E CORRELATION HEAT MAP ++++++++++++++++++++++++++++++++++++++++++++
# https://seaborn.pydata.org/generated/seaborn.heatmap.html
# https://seaborn.pydata.org/examples/structured_heatmap.html
# https://stackoverflow.com/questions/67879908/lower-triangle-mask-with-seaborn-clustermap
# https://likegeeks.com/seaborn-heatmap-colorbar/
# https://stackoverflow.com/questions/67909597/seaborn-clustermap-colorbar-adjustment
# https://matplotlib.org/stable/api/figure_api.html#matplotlib.figure.Figure.colorbar
# https://matplotlib.org/stable/api/_as_gen/matplotlib.figure.Figure.colorbar.html#matplotlib.figure.Figure.colorbar
# https://www.pythonfixing.com/2021/10/fixed-seaborn-clustermap-colorbar.html
# https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.colorbar.html
# https://stackoverflow.com/questions/27988846/how-to-express-classes-on-the-axis-of-a-heatmap-in-seaborn/27992943#27992943

    # PAGE 5
    
    # tidy and convert to pandas
    df = ind_df.filter(pl.col(earnings_metric) == param.EARN_METRICS[1],
                       pl.col(earnings_type) == param.EARN_TYPES[0])\
               .drop(pl.col(earnings_metric),
                     pl.col(earnings_type),
                     pl.col(year))
    # simplify col headings
    df.columns = [name.replace("_", " ")
                  for name in df.columns]
    pdf = df.to_pandas()
    pdf.rename(columns={param.IDX_E_COL_NAME: param.SP500}, inplace=True)
    
    # this creates several axes: row_dendrogram, col_dendrogram, cbar
    cg = sn.clustermap(pdf.corr(),
                       #annot=True, fmt=".1f",
                       #cmap= 'Blues',
                       cmap= 'RdYlGn',
                       figsize=(8.5, 11),
                       # cbar_kws= {'shrink': 'left'},
                       )
    
    # cg.ax_col_dendrogram.remove()
    cg.ax_row_dendrogram.remove()
    # cg.ax_cbar.remove()
    
    # make room at the top of the whole fig, not just the plot
    # add suptitle
    cg.figure.subplots_adjust(top=0.87)
    
    cg.figure.suptitle(
        f' \n{disp.PAGE5_SUPTITLE}',
        fontsize=13,
        fontweight='bold')
    # plt.tight_layout(pad= 0.5)
    
    # cg.figure.supxlabel(f'{PAGE4_SOURCE}\n ', fontsize= 8)
    
    # ???
    # cg.figure.subplots_adjust(right=0.7)
    
    # cbar_position shows the relative positions for x and y
    # and the relative sizes of width and height
    # all relative to the size of the ax's dimensions
    x0, y0, w_, h_ = cg.cbar_pos
    # print(cg.cbar_pos)
    cg.ax_cbar.set_position([0.04, 
                             0.4, 
                             0.05, 
                             0.18])
   
    # NB the set_position above moves the cbar, but
    # does not change the cbar_pos values
    # print(cg.cbar_pos)
    
    '''
    # this throws a warning and yields an unexpected result
    cg.ax_cbar.set_yticklabels([-0.5, -0.2, 0.0, 0.2, 0.5, 0.8, 1.0])
    '''
    
    '''
    x0, _y0, _w, _h = g.cbar_pos
    g.ax_cbar.set_position([x0, 0.9, g.ax_row_dendrogram.get_position().width, 0.02])
    g.ax_cbar.set_title('colorbar title')
    g.ax_cbar.tick_params(axis='x', length=10)
    for spine in g.ax_cbar.spines:
        g.ax_cbar.spines[spine].set_color('crimson')
        g.ax_cbar.spines[spine].set_linewidth(2)
    '''
    
    '''
    # to print only upper triangle
    mask = np.tril(np.ones_like(corr))
    values = cg.ax_heatmap.collections[0].get_array().reshape(corr.shape)
    new_values = np.ma.array(values, mask=mask)
    cg.ax_heatmap.collections[0].set_array(new_values)
    '''
    hp.message([
        env.DISPLAY_5_ADDR
    ])
    cg.savefig(str(env.DISPLAY_5_ADDR))
    
# DISTRIBUTION of E using Matplotlib and Numpy ++++++++++++++++++++++++
# https://seaborn.pydata.org/examples/structured_heatmap.html
# https://matplotlib.org/stable/gallery/lines_bars_and_markers/bar_stacked.html
# https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.legend.html
# https://matplotlib.org/stable/users/explain/axes/arranging_axes.html
# https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.tight_layout.html
# https://matplotlib.org/stable/users/explain/axes/tight_layout_guide.html#sphx-glr-users-explain-axes-tight-layout-guide-py
# https://how2matplotlib.com/how-to-change-order-of-items-in-matplotlib-legend.html
# https://www.geeksforgeeks.org/matplotlib-pyplot-legend-in-python/

    # PAGE 6
    
    fig = plt.figure(figsize=(11, 8.5), 
                     layout="constrained")
    # padding is relative to font size
    # plt.tight_layout(pad= 0.5)
    # one plot
    ax = fig.subplots()
    fig.suptitle(
        '\n' + disp.PAGE6_SUPTITLE,
        fontsize= 13,
        fontweight= 'bold')
    fig.supxlabel(disp.PAGE4_SOURCE, fontsize= 8)
    
    # shift from pe to op E data and remove cols w/o E data for ind
    df = ind_df.filter(pl.col(earnings_metric) == param.EARN_METRICS[0],
                       pl.col(earnings_type) == param.EARN_TYPES[0])\
               .drop(pl.col(earnings_metric),
                     pl.col(earnings_type),
                     pl.col(param.RR_NAME),
                     pl.col(param.IDX_E_COL_NAME))
    yr_series = pl.Series(df.select(year)).to_list()
    df = df.drop(pl.col(year))
    # simplify col headings
    ind_names = [name.replace("_", " ")
                 for name in df.columns]
    df.columns = ind_names
    
    # prepare data, remove negative E
    mat_np = df.to_numpy()
    mat_np[mat_np < 0] = 0
    
    # sort ind_name and ind_size together by ind_size
    ind_size = mat_np.sum(axis=0).tolist()
    iterate = sorted(list(zip(ind_names, ind_size)),
                     key= lambda x: x[1],
                     reverse= True)
    ind_names_sorted = [x[0] for x in iterate]
    
    mat_np = df.select(ind_names_sorted)\
               .to_numpy()
    mat_np[mat_np < 0] = 0.
    mat_np = (mat_np / 
              mat_np.sum(axis=1)[:, np.newaxis]).T
    
    # create the plot
    width = 0.5
    bottom = np.zeros(len(yr_series))
    # iterate through np array
    # a col of mat_np should contain the data for a bar
    # so mat_np is transposed from yr x industry to ind x yr
    # this loop loads each row (layer) to allocate to each year's bar
    # initialize bottom at zero for all years, then increase
    # after each layer's value for each year
    for idx in range(len(mat_np)):
        p = ax.bar(yr_series, mat_np[idx, :], width, 
                   label= ind_names_sorted[idx], bottom= bottom)
        bottom += mat_np[idx, :]
        
    #ax.legend(loc="lower center", reverse= True)
    plt.xticks(rotation = 30)
    ax.set_ylabel(' \n')
    box = ax.get_position()
    ax.set_position([box.x0, box.y0 + 0.04, 
                     box.width * 0.75, box.height * 0.95])
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5),
              reverse= True)
    
    hp.message([
        env.DISPLAY_6_ADDR
    ])
    fig.savefig(str(env.DISPLAY_6_ADDR))

    return
    